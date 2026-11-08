/**
 * Local OpenAI-compatible proxy that translates requests to Cursor's gRPC protocol.
 *
 * Accepts POST /v1/chat/completions in OpenAI format, translates to Cursor's
 * protobuf/HTTP2 Connect protocol, and streams back OpenAI-format SSE.
 *
 * Tool calling uses Cursor's native MCP tool protocol:
 * - OpenAI tool defs → McpToolDefinition in RequestContext
 * - Cursor toolCallStarted/Delta/Completed → OpenAI tool_calls SSE chunks
 * - mcpArgs exec → pause stream, return tool_calls to caller
 * - Follow-up request with tool results → resume bridge with mcpResult
 *
 * HTTP/2 transport is delegated to a Node child process (h2-bridge.mjs)
 * because Bun's node:http2 module is broken.
 */
import { create, fromBinary, fromJson, type JsonValue, toBinary, toJson } from "@bufbuild/protobuf";
import { ValueSchema } from "@bufbuild/protobuf/wkt";
import {
  AgentClientMessageSchema,
  AgentRunRequestSchema,
  AgentServerMessageSchema,
  ClientHeartbeatSchema,
  ConversationActionSchema,
  ConversationStateStructureSchema,
  ConversationStepSchema,
  AgentConversationTurnStructureSchema,
  ConversationTurnStructureSchema,
  AssistantMessageSchema,
  BackgroundShellSpawnResultSchema,
  ComputerUseErrorSchema,
  ComputerUseResultSchema,
  DeleteResultSchema,
  DeleteRejectedSchema,
  DiagnosticsResultSchema,
  DiagnosticsSuccessSchema,
  ExecClientMessageSchema,
  ExecClientControlMessageSchema,
  ExecClientHeartbeatSchema,
  FetchErrorSchema,
  FetchResultSchema,
  GetBlobResultSchema,
  GrepErrorSchema,
  GrepResultSchema,
  KvClientMessageSchema,
  ListMcpResourcesExecResultSchema,
  ListMcpResourcesSuccessSchema,
  LsRejectedSchema,
  LsResultSchema,
  McpErrorSchema,
  McpResultSchema,
  McpSuccessSchema,
  McpTextContentSchema,
  McpToolDefinitionSchema,
  McpToolResultContentItemSchema,
  ModelDetailsSchema,
  ReadMcpResourceErrorSchema,
  ReadMcpResourceExecResultSchema,
  ReadRejectedSchema,
  ReadResultSchema,
  RecordScreenFailureSchema,
  RecordScreenResultSchema,
  RequestContextResultSchema,
  RequestedModelSchema,
  RequestedModel_ModelParameterbytesSchema,
  RequestContextSchema,
  RequestContextSuccessSchema,
  ResumeActionSchema,
  SetBlobResultSchema,
  ShellRejectedSchema,
  ShellResultSchema,
  ShellStreamSchema,
  UserMessageActionSchema,
  UserMessageSchema,
  WriteRejectedSchema,
  WriteResultSchema,
  WriteShellStdinErrorSchema,
  WriteShellStdinResultSchema,
  type AgentServerMessage,
  type ConversationStateStructure,
  type ExecServerControlMessage,
  type ExecServerMessage,
  type InteractionQuery,
  type KvServerMessage,
  type McpToolDefinition,
} from "./proto/agent_pb.ts";
import { resolveCursorModel } from "./catalog.ts";
import {
  redirectNativeExec,
  sendNativeExecResult,
  type NativeExecBinding,
} from "./native-tools.ts";
import { spawn, type ChildProcess } from "node:child_process";
import http from "node:http";
import { createHash } from "node:crypto";
import { gunzipSync } from "node:zlib";
import { chmod, mkdir, readFile, readdir, stat, unlink, writeFile } from "node:fs/promises";
import { resolve as pathResolve } from "node:path";
import { z } from "zod";
import {
  deriveBridgeKey,
  deriveConversationKey,
  pauseBridgeKey,
  pauseConversationKey,
} from "./session-key.ts";
import { classifyExec, streamCloseMessage, unavailableExec } from "./exec-reply.ts";
import {
  buildAskQuestionResponse,
  buildAskQuestionResult,
  buildEmptyInteractionResponse,
  buildQuestionToolArgs,
  buildRejectedInteractionResponse,
  QUESTION_TOOL_NAME,
  type AskQuestionInteraction,
} from "./ask-question.ts";
import { proxyLog } from "./log.ts";

const CURSOR_API_URL = process.env.CURSOR_API_URL ?? "https://api2.cursor.sh";
const CONNECT_END_STREAM_FLAG = 0b00000010;
const CONNECT_COMPRESSED_FLAG = 0b00000001;
const BRIDGE_PATH = pathResolve(import.meta.dirname, "h2-bridge.mjs");
const SSE_HEADERS = {
  "Content-Type": "text/event-stream",
  "Cache-Control": "no-cache",
  Connection: "keep-alive",
} as const;

interface OpenAIToolCall {
  id: string;
  type: "function";
  function: { name: string; arguments: string };
}

/** A single element in an OpenAI multi-part content array. */
interface ContentPart {
  type: string;
  text?: string;
}

interface OpenAIMessage {
  role: "system" | "user" | "assistant" | "tool";
  content: string | null | ContentPart[];
  tool_call_id?: string;
  tool_calls?: OpenAIToolCall[];
}

interface OpenAIToolDef {
  type: "function";
  function: {
    name: string;
    description?: string;
    parameters?: Record<string, unknown>;
  };
}

interface ChatCompletionRequest {
  model: string;
  messages: OpenAIMessage[];
  stream?: boolean;
  temperature?: number;
  max_tokens?: number;
  tools?: OpenAIToolDef[];
  tool_choice?: unknown;
}


interface CursorRequestPayload {
  requestBytes: Uint8Array;
  blobStore: Map<string, Uint8Array>;
  mcpTools: McpToolDefinition[];
  /** System prompt forwarded via RequestContext.cloudRule (issue #21). */
  cloudRule?: string;
}

/** A pending tool execution waiting for results from the caller. */
export interface PendingExec {
  execId: string;
  execMsgId: number;
  toolCallId: string;
  toolName: string;
  /** Decoded arguments JSON string for SSE tool_calls emission. */
  decodedArgs: string;
  /** Set when this exec is a redirected native tool call (issue #21/#29). */
  native?: NativeExecBinding;
  /** Set when this tool call is a Cursor AskQuestion waiting for the user. */
  interaction?: AskQuestionInteraction;
  /** Per-exec liveness ping while the tool runs; cleared when answered. */
  heartbeatTimer?: NodeJS.Timeout;
}

/** A bridge kept alive across requests for tool result continuation. */
interface ActiveBridge {
  bridge: Bridge;
  heartbeatTimer: NodeJS.Timeout;
  blobStore: Map<string, Uint8Array>;
  mcpTools: McpToolDefinition[];
  cloudRule?: string;
  pendingExecs: PendingExec[];
  /** Detaches the previous response's close handler when resumed. */
  detachClose?: () => void;
  /** When the bridge was paused for a tool result; used for TTL eviction. */
  createdAt: number;
}

/**
 * Tear a paused bridge down: stop its heartbeat, drop it from the resumable
 * map (only if that entry is still this bridge), and end the subprocess.
 * Safe to call from several paths because each step tolerates repetition.
 */
function destroyActiveBridge(key: string, active: ActiveBridge): void {
  clearInterval(active.heartbeatTimer);
  clearPendingExecTimers(active.pendingExecs);
  if (activeBridges.get(key) === active) activeBridges.delete(key);
  // An unanswered AskQuestion must not leave the upstream waiting on the
  // interaction after we reap the bridge. Tell it the question was rejected.
  for (const exec of active.pendingExecs) {
    if (!exec.interaction) continue;
    const rejected = buildRejectedInteractionResponse(
      exec.interaction.id,
      "askQuestionInteractionQuery",
    );
    if (!rejected) continue;
    try {
      active.bridge.write(frameConnectMessage(rejected));
      proxyLog(`askQuestion answered id=${exec.interaction.id} result=rejected (teardown)`);
    } catch {
      // The bridge is already gone.
    }
  }
  closeBridgeGracefully(active.bridge);
}

/**
 * End the bridge's stdin so it can finish the upstream stream, then reap it if
 * it is still alive shortly after. Killing in the same tick as end() races the
 * zero-length frame through the pipe and turns a clean cancellation into a TCP
 * teardown, which Cursor bills as an abandoned run.
 */
function closeBridgeGracefully(bridge: Bridge): void {
  try {
    bridge.end();
  } catch {
    // Already closed.
  }
  const timer = setTimeout(() => bridge.kill(), 250);
  timer.unref?.();
}

// Active bridges keyed by a session token (derived from conversation state).
// When tool_calls are returned, the bridge stays alive. The next request
// with tool results looks up the bridge and sends mcpResult messages.
const activeBridges = new Map<string, ActiveBridge>();

/** How often to emit an SSE comment while the model is silent. */
const SSE_KEEPALIVE_MS = (() => {
  const raw = process.env.CURSOR_SSE_KEEPALIVE_MS;
  const parsed = raw === undefined ? Number.NaN : Number.parseInt(raw, 10);
  // An unparseable or non-positive value falls back to the default rather
  // than failing a request.
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 15_000;
})();

/** How often to prove to Cursor that an outstanding exec is still alive. */
const EXEC_HEARTBEAT_MS = (() => {
  const raw = process.env.CURSOR_EXEC_HEARTBEAT_MS;
  const parsed = raw === undefined ? Number.NaN : Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 3_000;
})();

/**
 * How long a turn may go without client-visible progress before the proxy
 * gives up. SSE keepalives exist so reqwest's 120 s low-speed timeout never
 * fires during a long think, but maki re-arms its own 300 s deadline on every
 * SSE *line*, comments included. A bridge that is alive (Cursor heartbeats)
 * but silent would therefore hang forever. This deadline is measured on
 * model progress only: heartbeats and usage-only updates do not reset it.
 */
const STREAM_STALL_TIMEOUT_MS = (() => {
  const raw = process.env.CURSOR_STREAM_STALL_TIMEOUT_MS;
  const parsed = raw === undefined ? Number.NaN : Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 300_000;
})();

/** A Connect message larger than this cannot be a real model frame. */
const MAX_CONNECT_FRAME_BYTES = 64 * 1024 * 1024;
const MAX_BRIDGE_MESSAGE_BYTES = MAX_CONNECT_FRAME_BYTES + 1024;

interface StoredConversation {
  conversationId: string;
  checkpoint: Uint8Array | null;
  blobStore: Map<string, Uint8Array>;
  lastAccessMs: number;
}

const conversationStates = new Map<string, StoredConversation>();
const CONVERSATION_TTL_MS = 30 * 60 * 1000; // 30 minutes

// Conversation state also persists to disk so context survives proxy/opencode
// restarts instead of failing with "Blob not found" (issues #22/#29).
const CONVERSATION_DISK_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

function conversationDir(): string {
  const configured = process.env.MAKI_CURSOR_CONVERSATIONS;
  if (configured) return configured;
  return pathResolve(import.meta.dirname, "..", "..", "conversations");
}

const PersistedConversationSchema = z.object({
  conversationId: z.string(),
  checkpoint: z.string().nullable(),
  blobs: z.record(z.string()),
  lastAccessMs: z.number(),
});

/** Write a conversation snapshot where only this user can read it. */
export async function saveConversation(
  convKey: string,
  stored: StoredConversation,
  dir = conversationDir(),
): Promise<string | undefined> {
  if (!/^[a-f0-9]+$/.test(convKey)) return undefined;
  const payload = {
    conversationId: stored.conversationId,
    checkpoint: stored.checkpoint
      ? Buffer.from(stored.checkpoint).toString("base64")
      : null,
    blobs: Object.fromEntries(
      [...stored.blobStore].map(([id, data]) => [
        id,
        Buffer.from(data).toString("base64"),
      ]),
    ),
    lastAccessMs: stored.lastAccessMs,
  } satisfies z.infer<typeof PersistedConversationSchema>;
  await mkdir(dir, { recursive: true, mode: 0o700 });
  await chmod(dir, 0o700);
  const file = pathResolve(dir, `${convKey}.json`);
  await writeFile(file, JSON.stringify(payload), { mode: 0o600 });
  await chmod(file, 0o600);
  return file;
}

function persistConversation(convKey: string, stored: StoredConversation): void {
  void saveConversation(convKey, stored).catch(() => {});
}

async function loadPersistedConversation(
  convKey: string,
): Promise<StoredConversation | undefined> {
  try {
    const raw = await readFile(
      pathResolve(conversationDir(), `${convKey}.json`),
      "utf8",
    );
    const parsed = PersistedConversationSchema.parse(JSON.parse(raw));
    if (Date.now() - parsed.lastAccessMs > CONVERSATION_DISK_TTL_MS) {
      return undefined;
    }
    return {
      conversationId: parsed.conversationId,
      checkpoint: parsed.checkpoint
        ? new Uint8Array(Buffer.from(parsed.checkpoint, "base64"))
        : null,
      blobStore: new Map(
        Object.entries(parsed.blobs).map(([id, data]) => [
          id,
          new Uint8Array(Buffer.from(data, "base64")),
        ]),
      ),
      lastAccessMs: Date.now(),
    };
  } catch {
    return undefined;
  }
}

/** Best-effort removal of conversation files past the disk TTL. */
function pruneStaleConversationFiles(): void {
  void (async () => {
    try {
      const entries = await readdir(conversationDir());
      const cutoff = Date.now() - CONVERSATION_DISK_TTL_MS;
      for (const entry of entries) {
        const file = pathResolve(conversationDir(), entry);
        const info = await stat(file);
        if (info.mtimeMs < cutoff) await unlink(file);
      }
    } catch {}
  })();
}

function evictStaleConversations(): void {
  const now = Date.now();
  for (const [key, stored] of conversationStates) {
    if (now - stored.lastAccessMs > CONVERSATION_TTL_MS) {
      conversationStates.delete(key);
    }
  }
}

/**
 * Drop bridges paused for tool results that were never resumed. Without this
 * a caller that exits mid tool call leaves a subprocess and a heartbeat
 * interval running for the life of the proxy.
 */
function evictStaleBridges(): void {
  const now = Date.now();
  for (const [key, active] of activeBridges) {
    if (now - active.createdAt > CONVERSATION_TTL_MS) {
      proxyLog(`bridge ${key.slice(0, 12)} evicted after tool result timeout`);
      destroyActiveBridge(key, active);
    }
  }
}

/** Length-prefix a message: [4-byte BE length][payload] */
function lpEncode(data: Uint8Array): Buffer {
  const buf = Buffer.alloc(4 + data.length);
  buf.writeUInt32BE(data.length, 0);
  buf.set(data, 4);
  return buf;
}

/** Connect protocol frame: [1-byte flags][4-byte BE length][payload] */
function frameConnectMessage(data: Uint8Array, flags = 0): Buffer {
  const frame = Buffer.alloc(5 + data.length);
  frame[0] = flags;
  frame.writeUInt32BE(data.length, 1);
  frame.set(data, 5);
  return frame;
}

/** An AgentClientMessage telling Cursor an outstanding exec is still alive. */
function makeExecHeartbeat(id: number): Uint8Array {
  const control = create(ExecClientControlMessageSchema, {
    message: {
      case: "heartbeat",
      value: create(ExecClientHeartbeatSchema, { id }),
    },
  });
  const client = create(AgentClientMessageSchema, {
    message: { case: "execClientControlMessage", value: control },
  });
  return toBinary(AgentClientMessageSchema, client);
}

/** Stop every per-exec heartbeat a paused bridge still owns. */
function clearPendingExecTimers(execs: readonly PendingExec[]): void {
  for (const exec of execs) {
    if (!exec.heartbeatTimer) continue;
    clearInterval(exec.heartbeatTimer);
    exec.heartbeatTimer = undefined;
  }
}

/**
 * Spawn the Node H2 bridge and return read/write handles.
 * The bridge uses length-prefixed framing on stdin/stdout.
 */
interface SpawnBridgeOptions {
  accessToken: string;
  rpcPath: string;
  url?: string;
  /** When true, use application/proto for unary RPCs instead of Connect streaming. */
  unary?: boolean;
}

/** How much bridge stderr to keep for diagnostics. */
const BRIDGE_STDERR_BYTES = 4 * 1024;

type FrameSink = {
  onMessage(bytes: Uint8Array): void;
  onEndStream(bytes: Uint8Array): void;
  onProtocolError(message: string): void;
  isClosed(): boolean;
};

type Bridge = {
  proc: ChildProcess;
  write: (data: Uint8Array) => void;
  end: () => void;
  onData: (cb: (chunk: Buffer) => void) => void;
  /** Attach the current response's message sink to the bridge-wide decoder. */
  attachSink: (sink: FrameSink) => void;
  detachSink: () => void;
  onClose: (cb: (code: number) => void) => () => void;
  /** Last few KB the bridge wrote to stderr, for error reporting. */
  stderrTail: () => string;
  /** Terminate the subprocess; safe to call more than once. */
  kill: () => void;
  /** True while the bridge subprocess is still running. */
  get alive(): boolean;
};

// Every live bridge, including ones that are not resumable (unary calls,
// non-streaming runs). stopProxy() and request aborts use this to make sure
// no child process outlives the request that started it.
const liveBridges = new Set<Bridge>();

/** Kill every live bridge; used when the proxy shuts down. */
function killLiveBridges(): void {
  for (const bridge of liveBridges) bridge.kill();
  liveBridges.clear();
}

/** Live bridge count; test-only so abort reaping can be asserted. */
export function liveBridgeCountForTests(): number {
  return liveBridges.size;
}

function spawnBridge(options: SpawnBridgeOptions): Bridge {
  let stderrTail = "";
  let proc: ChildProcess;
  try {
    proc = spawn(process.execPath, [BRIDGE_PATH], {
      stdio: ["pipe", "pipe", "pipe"],
    });
  } catch (err) {
    proxyLog(`bridge spawn failed: ${err instanceof Error ? err.message : String(err)}`);
    throw err;
  }
  const stdin = proc.stdin;
  const stdout = proc.stdout;
  const stderr = proc.stderr;
  if (!stdin || !stdout) {
    proxyLog("bridge spawn failed: no stdin/stdout pipe");
    // Do not orphan the child just because a pipe is missing.
    try {
      proc.kill();
    } catch {
      // Already gone.
    }
    throw new Error("failed to pipe the cursor bridge");
  }
  // A write into a pipe whose child died surfaces asynchronously as an
  // 'error' event. Without this listener it is an uncaught exception, and
  // cli.ts exits the whole proxy, killing every in-flight session.
  stdin.on("error", () => {});
  // Drain stderr so the child can never block on a full pipe, keeping only
  // the tail. The bridge never prints tokens, so this is safe to log.
  stderr?.on("data", (chunk: Buffer) => {
    stderrTail = (stderrTail + chunk.toString("utf8")).slice(-BRIDGE_STDERR_BYTES);
  });
  stderr?.on("error", () => {});

  const config = JSON.stringify({
    accessToken: options.accessToken,
    url: options.url ?? CURSOR_API_URL,
    path: options.rpcPath,
    unary: options.unary ?? false,
  });
  stdin.write(lpEncode(new TextEncoder().encode(config)));

  const framed = !(options.unary ?? false);
  const cbs = {
    data: null as ((chunk: Buffer) => void) | null,
  };
  const closeHandlers = new Set<(code: number) => void>();
  const early: Buffer[] = [];

  let exited = false;
  let exitCode = 1;
  let pending = Buffer.alloc(0);
  let sink: FrameSink | null = null;
  let protocolFailure: string | null = null;
  let oversized = false;

  // One decoder per bridge: the upstream byte stream outlives any single SSE
  // response, so a resume must not lose a partially received frame.
  const decoder = framed
    ? createConnectFrameParser(
        (bytes) => { if (sink) sink.onMessage(bytes); },
        (bytes) => { if (sink) sink.onEndStream(bytes); },
        (message) => {
          // A live response handles its own error; only a failure that lands
          // with no sink is latched for the response that attaches next.
          if (sink) {
            sink.onProtocolError(message);
          } else {
            protocolFailure = message;
            proxyLog(`cursor protocol error: ${message}`);
          }
        },
      )
    : null;

  const takeFrames = (value: Buffer) => {
    if (oversized) return;
    pending = Buffer.concat([pending, value]);
    while (pending.length >= 4) {
      const len = pending.readUInt32BE(0);
      if (len > MAX_BRIDGE_MESSAGE_BYTES) {
        oversized = true;
        pending = Buffer.alloc(0);
        proxyLog(`bridge pid ${proc.pid ?? "?"} oversized stdout message (${len} bytes)`);
        try {
          proc.kill();
        } catch {
          // Already reaped.
        }
        return;
      }
      if (pending.length < 4 + len) break;
      const payload = Buffer.from(pending.subarray(4, 4 + len));
      pending = pending.subarray(4 + len);
      if (decoder) decoder.push(payload);
      else if (cbs.data) cbs.data(payload);
      else early.push(payload);
    }
  };

  stdout.on("data", (value: Buffer) => takeFrames(value));
  stdout.on("error", () => {});

  const bridge: Bridge = {
    proc,
    get alive() {
      return !exited;
    },
    write(data) {
      if (exited) return;
      try {
        stdin.write(lpEncode(data));
      } catch {
        // The bridge already closed.
      }
    },
    end() {
      if (exited) return;
      try {
        stdin.write(lpEncode(new Uint8Array(0)));
        stdin.end();
      } catch {
        // The bridge already closed.
      }
    },
    stderrTail() {
      return stderrTail.trim();
    },
    kill() {
      if (exited) return;
      try {
        proc.kill();
      } catch {
        // Already reaped.
      }
    },
    onData(cb) {
      cbs.data = cb;
      for (const chunk of early) cb(chunk);
      early.length = 0;
    },
    attachSink(next) {
      // Replacing a parked (already closed) sink is the normal resume path;
      // only a live replacement is suspicious enough to log.
      if (sink && !sink.isClosed()) {
        proxyLog(
          `bridge pid ${proc.pid ?? "?"} sink replaced while live pending=${decoder?.pendingLength() ?? 0}`,
        );
      }
      sink = next;
      if (protocolFailure) {
        const failure = protocolFailure;
        protocolFailure = null;
        next.onProtocolError(failure);
      }
    },
    detachSink() {
      sink = null;
    },
    onClose(cb) {
      if (exited) {
        queueMicrotask(() => cb(exitCode));
        return () => {};
      }
      closeHandlers.add(cb);
      return () => {
        closeHandlers.delete(cb);
      };
    },
  };

  liveBridges.add(bridge);

  const finish = (code: number) => {
    if (exited) return;
    exited = true;
    exitCode = code;
    liveBridges.delete(bridge);
    // A bridge that dies on its own is worth a log line; stdin/stdout
    // shutdowns already have a caller that knows why.
    if (code !== 0) {
      const tail = bridge.stderrTail();
      proxyLog(
        `bridge pid ${proc.pid ?? "?"} exited ${code}${tail ? `: ${tail}` : ""}`,
      );
    }
    const handlers = [...closeHandlers];
    closeHandlers.clear();
    for (const cb of handlers) cb(code);
  };
  proc.on("exit", (code) => finish(code ?? 1));
  proc.on("error", (err) => {
    proxyLog(`bridge pid ${proc.pid ?? "?"} error: ${err.message}`);
    finish(1);
  });

  return bridge;
}

interface CursorUnaryRpcOptions {
  accessToken: string;
  rpcPath: string;
  requestBody: Uint8Array;
  url?: string;
  timeoutMs?: number;
}

export async function callCursorUnaryRpc(
  options: CursorUnaryRpcOptions,
 ): Promise<{ body: Uint8Array; exitCode: number; timedOut: boolean }> {
  const bridge = spawnBridge({
    accessToken: options.accessToken,
    rpcPath: options.rpcPath,
    url: options.url,
    unary: true,
  });
  const chunks: Buffer[] = [];
  const { promise, resolve } = Promise.withResolvers<{
    body: Uint8Array;
    exitCode: number;
    timedOut: boolean;
  }>();
  let timedOut = false;
  const timeoutMs = options.timeoutMs ?? 5_000;
  const timeout = timeoutMs > 0
    ? setTimeout(() => {
        timedOut = true;
        try { bridge.proc.kill(); } catch {}
      }, timeoutMs)
    : undefined;

  bridge.onData((chunk) => {
    chunks.push(Buffer.from(chunk));
  });
  bridge.onClose((exitCode) => {
    if (timeout) clearTimeout(timeout);
    resolve({
      body: Buffer.concat(chunks),
      exitCode,
      timedOut,
    });
  });

  // Unary: send raw protobuf body (no Connect framing)
  bridge.write(options.requestBody);
  bridge.end();

  return promise;
}

let proxyServer: http.Server | undefined;
let proxyPort: number | undefined;
let proxyAccessTokenProvider: (() => Promise<string>) | undefined;
let proxyModels: Array<{ id: string; name: string }> = [];
let proxyToken = "";
let bridgeSweepTimer: NodeJS.Timeout | undefined;

function buildOpenAIModelList(models: ReadonlyArray<{ id: string; name: string }>): Array<{
  id: string;
  object: "model";
  created: number;
  owned_by: string;
}> {
  return models.map((model) => ({
    id: model.id,
    object: "model",
    created: 0,
    owned_by: "cursor",
  }));
}

export function getProxyPort(): number | undefined {
  return proxyPort;
}

export function replaceProxyModels(models: ReadonlyArray<{ id: string; name: string }>): void {
  proxyModels = models.map((model) => ({
    id: model.id,
    name: model.name,
  }));
}

/** Drop fields maki's OpenAI codec may add that Cursor's agent request does not understand. */
export function asCompletionRequest(value: unknown): ChatCompletionRequest {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("request body must be an object");
  }
  const record = { ...(value as Record<string, unknown>) };
  delete record.thinking;
  delete record.reasoning_effort;
  if (typeof record.model !== "string" || !Array.isArray(record.messages)) {
    throw new Error("model and messages are required");
  }
  return record as unknown as ChatCompletionRequest;
}

function headerPairs(headers: http.IncomingHttpHeaders): [string, string][] {
  const pairs: [string, string][] = [];
  for (const [key, value] of Object.entries(headers)) {
    if (value == null) continue;
    if (Array.isArray(value)) {
      for (const item of value) pairs.push([key, item]);
    } else {
      pairs.push([key, value]);
    }
  }
  return pairs;
}

/**
 * Resolve true when the socket can take more bytes again, false when it died
 * or stayed blocked for longer than any real turn can justify. A client that
 * stops reading must not park the request (and its bridge) forever.
 */
function awaitWriteDrain(res: http.ServerResponse): Promise<boolean> {
  if (res.destroyed || res.writableEnded) return Promise.resolve(false);
  return new Promise((resolvePromise) => {
    const timer = setTimeout(() => settle(false), 30_000);
    timer.unref?.();
    const settle = (writable: boolean) => {
      clearTimeout(timer);
      res.off("drain", onDrain);
      res.off("close", onGone);
      res.off("error", onGone);
      resolvePromise(writable);
    };
    const onDrain = () => settle(true);
    const onGone = () => settle(false);
    res.on("drain", onDrain);
    res.on("close", onGone);
    res.on("error", onGone);
  });
}

/**
 * Copy a fetch Response to a Node response.
 *
 * Headers are flushed before the first byte of body so a caller that streams
 * slowly still sees a response (and so an idle-timeout client is never left
 * waiting on a socket with no reply). Cancelling the reader on close runs the
 * stream's cancel() handler, which is what propagates a client abort to the
 * upstream bridge instead of leaking it.
 */
export async function writeNodeResponse(
  res: http.ServerResponse,
  response: Response,
): Promise<void> {
  const headers: Record<string, string> = {};
  response.headers.forEach((value, key) => {
    headers[key] = value;
  });
  res.writeHead(response.status, headers);
  // Send headers now: the SSE body may stay quiet for a long time.
  res.flushHeaders();

  if (!response.body) {
    endWritable(res);
    return;
  }

  const reader = response.body.getReader();
  let aborted = false;
  let clientGone = false;
  const onClose = () => {
    if (aborted) return;
    clientGone = true;
    aborted = true;
    // Client hung up (or the socket errored): stop pulling so the stream's
    // cancel() can tear the bridge down. Never throw from this path.
    void reader.cancel().catch(() => {});
  };
  res.on("close", onClose);
  res.on("error", onClose);

  try {
    while (!aborted) {
      const { done, value } = await reader.read();
      if (done) break;
      if (res.destroyed || res.writableEnded) {
        aborted = true;
        break;
      }
      if (!res.write(Buffer.from(value))) {
        // Honour backpressure, but do not wait forever on a dead or stalled
        // client: a stalled reader is an abort, not a reason to hold a bridge.
        const writable = await awaitWriteDrain(res);
        if (!writable || res.destroyed || res.writableEnded) {
          aborted = true;
          break;
        }
      }
    }
  } catch {
    // A broken pipe or a source stream that errored (controller.error) lands
    // here. Neither is a normal finish: do not leave the socket open without
    // a body or an end.
    aborted = true;
  } finally {
    // Detach before ending: res.end() emits "close", which must not be read
    // as a client abort and cancel an already-finished stream.
    res.off("close", onClose);
    res.off("error", onClose);
    if (aborted) {
      void reader.cancel().catch(() => {});
      // The client may still be connected (source error or a stalled reader).
      // Destroy the response so it sees a truncated transfer, not a hang.
      if (!clientGone && !res.destroyed) res.destroy();
    } else {
      reader.releaseLock();
      endWritable(res);
    }
  }
}

/** End a response without ever throwing on an already-dead socket. */
function endWritable(res: http.ServerResponse): void {
  if (res.destroyed || res.writableEnded) return;
  try {
    res.end();
  } catch {
    // The client is already gone.
  }
}

function loopbackHost(req: Request): boolean {
  const host = (req.headers.get("host") ?? "").toLowerCase();
  const name = host.startsWith("[")
    ? host.slice(1, host.indexOf("]"))
    : host.split(":")[0];
  return name === "127.0.0.1" || name === "localhost";
}

function bearerOk(req: Request): boolean {
  return proxyToken !== "" && req.headers.get("authorization") === `Bearer ${proxyToken}`;
}

async function proxyFetch(req: Request): Promise<Response> {
  const url = new URL(req.url);
  if (!loopbackHost(req)) {
    return new Response("Forbidden", { status: 403 });
  }
  if (!bearerOk(req)) {
    return new Response("Unauthorized", { status: 401 });
  }
  if (req.method === "POST") {
    const type = (req.headers.get("content-type") ?? "").toLowerCase();
    if (!type.startsWith("application/json")) {
      return new Response("Unsupported Media Type", { status: 415 });
    }
  }
  if (req.method === "GET" && url.pathname === "/health") {
    return new Response("ok");
  }
  if (req.method === "GET" && url.pathname === "/v1/models") {
    return new Response(
      JSON.stringify({
        object: "list",
        data: buildOpenAIModelList(proxyModels),
      }),
      { headers: { "Content-Type": "application/json" } },
    );
  }
  if (req.method === "POST" && url.pathname === "/v1/chat/completions") {
    try {
      const body = asCompletionRequest(await req.json());
      if (!proxyAccessTokenProvider) {
        throw new Error("Cursor proxy access token provider not configured");
      }
      const accessToken = await proxyAccessTokenProvider();
      return handleChatCompletion(body, accessToken, req.headers.get("x-maki-session") ?? "");
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return new Response(
        JSON.stringify({
          error: { message, type: "server_error", code: "internal_error" },
        }),
        { status: 500, headers: { "Content-Type": "application/json" } },
      );
    }
  }
  return new Response("Not Found", { status: 404 });
}

export async function startProxy(
  getAccessToken: () => Promise<string>,
  models: ReadonlyArray<{ id: string; name: string }> = [],
  listenPort = 0,
  token = "",
): Promise<number> {
  if (!token) throw new Error("cursor proxy requires a token");
  proxyAccessTokenProvider = getAccessToken;
  proxyToken = token;
  proxyModels = models.map((model) => ({
    id: model.id,
    name: model.name,
  }));
  if (proxyServer && proxyPort) return proxyPort;

  pruneStaleConversationFiles();

  const server = http.createServer(async (req, res) => {
    try {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(Buffer.from(chunk));
      const body = Buffer.concat(chunks);
      const host = req.headers.host ?? "127.0.0.1";
      const request = new Request(`http://${host}${req.url ?? "/"}`, {
        method: req.method,
        headers: headerPairs(req.headers),
        body: req.method === "GET" || req.method === "HEAD" ? undefined : body,
      });
      await writeNodeResponse(res, await proxyFetch(request));
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      proxyLog(`request failed: ${message}`);
      if (res.headersSent || res.destroyed) {
        // The stream already started (or died): ending the socket is all
        // that is left, and writing a second head would corrupt the response.
        if (!res.writableEnded) res.destroy();
        return;
      }
      try {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: { message, type: "server_error" } }));
      } catch {
        // Client disconnected while we were replying.
      }
    }
  });

  // Keep pooled connections alive longer than the client's idle window so a
  // reused socket is never closed underneath a request maki is about to send.
  server.keepAliveTimeout = 65_000;
  server.headersTimeout = 66_000;

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(listenPort, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("Failed to bind proxy to a port");
  }
  proxyServer = server;
  proxyPort = address.port;

  // Reap bridges abandoned mid-tool-call even when no further request ever
  // arrives for them. Running the sweeper only on the next request (the old
  // behaviour) let an exited maki leave a child and upstream run forever.
  bridgeSweepTimer = setInterval(() => evictStaleBridges(), 60_000);
  bridgeSweepTimer.unref?.();

  return proxyPort;
}

export function stopProxy(): void {
  if (proxyServer) {
    proxyServer.close();
    proxyServer = undefined;
    proxyPort = undefined;
    proxyAccessTokenProvider = undefined;
    proxyToken = "";
    proxyModels = [];
  }
  if (bridgeSweepTimer !== undefined) {
    clearInterval(bridgeSweepTimer);
    bridgeSweepTimer = undefined;
  }
  for (const [key, active] of activeBridges) {
    destroyActiveBridge(key, active);
  }
  activeBridges.clear();
  killLiveBridges();
  conversationStates.clear();
}

async function handleChatCompletion(
  body: ChatCompletionRequest,
  accessToken: string,
  sessionId = "",
): Promise<Response> {
  const { systemPrompts, userText, history, toolResults } = parseMessages(body.messages);
  const modelId = body.model;
  const tools = body.tools ?? [];

  if (!userText && history.length === 0) {
    return new Response(
      JSON.stringify({
        error: {
          message: "No user message found",
          type: "invalid_request_error",
        },
      }),
      { status: 400, headers: { "Content-Type": "application/json" } },
    );
  }

  const lookupBridgeKey = deriveBridgeKey(modelId, body.messages, sessionId);
  const lookupConvKey = deriveConversationKey(body.messages, sessionId);
  const storeBridgeKey = pauseBridgeKey(modelId, body.messages, sessionId);
  const storeConvKey = pauseConversationKey(body.messages, sessionId);

  let stored = conversationStates.get(lookupConvKey);
  if (!stored) {
    // Fall back to the disk cache so conversations survive proxy restarts
    // and in-memory TTL eviction (issues #22/#29).
    stored = (await loadPersistedConversation(lookupConvKey)) ?? {
      conversationId: deterministicUuid(`cursor-conv-id:${lookupConvKey}`),
      checkpoint: null,
      blobStore: new Map(),
      lastAccessMs: Date.now(),
    };
  }
  conversationStates.set(storeConvKey, stored);
  if (storeConvKey !== lookupConvKey) {
    conversationStates.delete(lookupConvKey);
  }
  stored.lastAccessMs = Date.now();
  evictStaleConversations();

  // Reclaim any bridge paused for a tool result that is now too old, and any
  // stale entry under this key, before we decide whether to resume.
  evictStaleBridges();

  const activeBridge = activeBridges.get(lookupBridgeKey);
  const isResume = Boolean(activeBridge && toolResults.length > 0);
  proxyLog(
    `request model=${modelId} messages=${body.messages.length} tools=${tools.length} ` +
      `results=${toolResults.length} resume=${isResume} key=${lookupBridgeKey}`,
  );
  if (!activeBridge && toolResults.length > 0) {
    // A resume whose key does not match the paused bridge would silently start
    // a new conversation. Make that visible; it is otherwise indistinguishable
    // from a normal request.
    proxyLog(`resume key=${lookupBridgeKey} missed; starting a fresh bridge`);
  }

  if (activeBridge && toolResults.length > 0) {
    activeBridges.delete(lookupBridgeKey);

    if (activeBridge.bridge.alive) {
      // Resume the live bridge with tool results. Ownership of the bridge
      // (and its heartbeat) moves to the new stream, which clears both when
      // it ends.
      return handleToolResultResume(
        activeBridge,
        toolResults,
        userText,
        modelId,
        storeBridgeKey,
        storeConvKey,
      );
    }

    // Bridge died (timeout, server disconnect, etc.).
    // Clean up and fall through to start a fresh bridge.
    destroyActiveBridge(lookupBridgeKey, activeBridge);
  }

  // Clean up stale bridge if present
  if (activeBridge && activeBridges.has(lookupBridgeKey)) {
    destroyActiveBridge(lookupBridgeKey, activeBridge);
  }

  // Build the request. When the bridge died mid tool-call, history already
  // contains the tool results and the request goes out as a resumeAction.
  const mcpTools = buildMcpToolDefinitions(tools);
  const payload = buildCursorRequest(
    modelId, systemPrompts, userText, history,
    stored.conversationId, stored.checkpoint, stored.blobStore,
  );
  payload.mcpTools = mcpTools;

  if (body.stream === false) {
    return handleNonStreamingResponse(payload, accessToken, modelId, storeConvKey);
  }
  return handleStreamingResponse(
    payload,
    accessToken,
    modelId,
    storeBridgeKey,
    storeConvKey,
  );
}

interface ToolResultInfo {
  toolCallId: string;
  content: string;
}

/** One prior conversation event, in message order. */
type HistoryEntry =
  | { kind: "user"; text: string }
  | { kind: "assistant"; text: string }
  | { kind: "tool"; text: string };

interface ParsedMessages {
  systemPrompts: string[];
  /** Trailing user message that becomes the active action ("" = resume). */
  userText: string;
  /** Everything before the active user message, in order. */
  history: HistoryEntry[];
  toolResults: ToolResultInfo[];
}

/** Normalize OpenAI message content to a plain string. */
function textContent(content: OpenAIMessage["content"]): string {
  if (content == null) return "";
  if (typeof content === "string") return content;
  return content
    .filter((p) => p.type === "text" && p.text)
    .map((p) => p.text!)
    .join("\n");
}

function parseMessages(messages: OpenAIMessage[]): ParsedMessages {
  const systemPrompts = messages
    .filter((m) => m.role === "system")
    .map((m) => textContent(m.content))
    .filter((text) => text.length > 0);

  const toolResults: ToolResultInfo[] = [];
  const history: HistoryEntry[] = [];

  for (const msg of messages) {
    if (msg.role === "tool") {
      const content = textContent(msg.content);
      toolResults.push({
        toolCallId: msg.tool_call_id ?? "",
        content,
      });
      if (content) history.push({ kind: "tool", text: content });
    } else if (msg.role === "user") {
      history.push({ kind: "user", text: textContent(msg.content) });
    } else if (msg.role === "assistant") {
      // Pure tool_calls messages carry no text; the paired tool entries
      // preserve that part of the transcript.
      const text = textContent(msg.content);
      if (text) history.push({ kind: "assistant", text });
    }
  }

  // A trailing user message is the active action; anything else (e.g. tool
  // results after a dead bridge) leaves userText empty and the request is
  // sent as a resumeAction over the reconstructed history.
  let userText = "";
  const last = history[history.length - 1];
  if (last?.kind === "user") {
    userText = last.text;
    history.pop();
  }

  return { systemPrompts, userText, history, toolResults };
}

/** Convert OpenAI tool definitions to Cursor's MCP tool protobuf format. */
function buildMcpToolDefinitions(tools: OpenAIToolDef[]): McpToolDefinition[] {
  return tools.map((t) => {
    const fn = t.function;
    const jsonSchema: JsonValue =
      fn.parameters && typeof fn.parameters === "object"
        ? (fn.parameters as JsonValue)
        : { type: "object", properties: {}, required: [] };
    const inputSchema = toBinary(ValueSchema, fromJson(ValueSchema, jsonSchema));
    return create(McpToolDefinitionSchema, {
      name: fn.name,
      description: fn.description || "",
      providerIdentifier: "opencode",
      toolName: fn.name,
      inputSchema,
    });
  });
}

/** Decode a Cursor MCP arg value (protobuf Value bytes) to a JS value. */
function decodeMcpArgValue(value: Uint8Array): unknown {
  try {
    const parsed = fromBinary(ValueSchema, value);
    return toJson(ValueSchema, parsed);
  } catch {}
  return new TextDecoder().decode(value);
}

/** Decode a map of MCP arg values. */
function decodeMcpArgsMap(args: Record<string, Uint8Array>): Record<string, unknown> {
  const decoded: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(args)) {
    decoded[key] = decodeMcpArgValue(value);
  }
  return decoded;
}

function buildCursorRequest(
  modelId: string,
  systemPrompts: string[],
  userText: string,
  history: HistoryEntry[],
  conversationId: string,
  checkpoint: Uint8Array | null,
  existingBlobStore?: Map<string, Uint8Array>,
): CursorRequestPayload {
  const blobStore = new Map<string, Uint8Array>(existingBlobStore ?? []);

  // Every `bytes` field in the *Structure messages is a sha256 blob ID —
  // server-produced checkpoints content-address turns, user messages, and
  // steps alike. Inlining data where an ID is expected makes Cursor fail
  // with "Connect error internal: Blob not found" (issues #22/#29).
  const storeBlob = (bytes: Uint8Array): Uint8Array => {
    const blobId = new Uint8Array(createHash("sha256").update(bytes).digest());
    blobStore.set(Buffer.from(blobId).toString("hex"), bytes);
    return blobId;
  };
  const storeJsonBlob = (obj: unknown): Uint8Array =>
    storeBlob(new TextEncoder().encode(JSON.stringify(obj)));

  const prompts = systemPrompts.length > 0
    ? systemPrompts
    : ["You are a helpful assistant."];
  const systemBlobIds = prompts.map((content) =>
    storeJsonBlob({ role: "system", content }),
  );

  // Cursor's server builds the model prompt from `rootPromptMessagesJson`,
  // not from `turns[]`. Sending only the system prompt here makes multi-turn
  // conversations lose all prior context after a proxy restart, so the full
  // history is rebuilt on every request. Server-echoed checkpoints replace
  // historical user entries with empty placeholders, which is why the
  // checkpoint's own rootPromptMessagesJson cannot be reused.
  const rootPromptMessagesJson = [...systemBlobIds];
  for (const entry of history) {
    if (entry.kind === "assistant") {
      rootPromptMessagesJson.push(
        storeJsonBlob({ role: "assistant", content: [{ type: "text", text: entry.text }] }),
      );
    } else {
      const text = entry.kind === "tool" ? `[Tool Result]\n${entry.text}` : entry.text;
      rootPromptMessagesJson.push(
        storeJsonBlob({ role: "user", content: [{ type: "text", text }] }),
      );
    }
  }

  // turns[]: one entry per user turn, with assistant/tool texts as steps.
  // Deterministic message IDs keep blob IDs stable across rebuilds.
  const turnBlobIds: Uint8Array[] = [];
  let currentTurn: { userMessageBlobId: Uint8Array; stepBlobIds: Uint8Array[] } | null = null;
  const flushTurn = () => {
    if (!currentTurn) return;
    const agentTurn = create(AgentConversationTurnStructureSchema, {
      userMessage: currentTurn.userMessageBlobId,
      steps: currentTurn.stepBlobIds,
    });
    const turnStructure = create(ConversationTurnStructureSchema, {
      turn: { case: "agentConversationTurn", value: agentTurn },
    });
    turnBlobIds.push(storeBlob(toBinary(ConversationTurnStructureSchema, turnStructure)));
    currentTurn = null;
  };
  for (const entry of history) {
    if (entry.kind === "user") {
      flushTurn();
      const userMsg = create(UserMessageSchema, {
        text: entry.text,
        messageId: deterministicUuid(`u:${turnBlobIds.length}:${entry.text}`),
      });
      currentTurn = {
        userMessageBlobId: storeBlob(toBinary(UserMessageSchema, userMsg)),
        stepBlobIds: [],
      };
    } else if (currentTurn) {
      const text = entry.kind === "tool" ? `[Tool Result]\n${entry.text}` : entry.text;
      const step = create(ConversationStepSchema, {
        message: {
          case: "assistantMessage",
          value: create(AssistantMessageSchema, { text }),
        },
      });
      currentTurn.stepBlobIds.push(storeBlob(toBinary(ConversationStepSchema, step)));
    }
  }
  flushTurn();

  // Preserve non-history checkpoint fields (todos, file states, summaries)
  // when the system prompt is unchanged; otherwise start fresh.
  let baseState: ConversationStateStructure | null = null;
  if (checkpoint) {
    try {
      const decoded = fromBinary(ConversationStateStructureSchema, checkpoint);
      const head = decoded.rootPromptMessagesJson.slice(0, systemBlobIds.length);
      const matches =
        head.length === systemBlobIds.length &&
        systemBlobIds.every((id, idx) => Buffer.from(head[idx]!).equals(Buffer.from(id)));
      if (matches) baseState = decoded;
    } catch {}
  }

  const conversationState = baseState
    ? create(ConversationStateStructureSchema, {
        ...baseState,
        rootPromptMessagesJson,
        turns: turnBlobIds,
      })
    : create(ConversationStateStructureSchema, {
        rootPromptMessagesJson,
        turns: turnBlobIds,
        todos: [],
        pendingToolCalls: [],
        previousWorkspaceUris: [],
        fileStates: {},
        fileStatesV2: {},
        summaryArchives: [],
        turnTimings: [],
        subagentStates: {},
        selfSummaryCount: 0,
        readPaths: [],
      });

  // No trailing user message (e.g. tool results after a dead bridge) →
  // resume over the reconstructed history instead of faking a user turn.
  const action = userText
    ? create(ConversationActionSchema, {
        action: {
          case: "userMessageAction",
          value: create(UserMessageActionSchema, {
            userMessage: create(UserMessageSchema, {
              text: userText,
              messageId: crypto.randomUUID(),
            }),
          }),
        },
      })
    : create(ConversationActionSchema, {
        action: { case: "resumeAction", value: create(ResumeActionSchema, {}) },
      });

  // "auto" is the proxy's pseudo-model for Cursor's server-side Auto
  // routing; the Run API expects modelId "default" for it.
  // A picker id from AvailableModels (glm-5p3-flash-high) is the family plus
  // the effort parameters. GetUsableModels ids that are not in that catalogue
  // pass through unchanged.
  const picked = modelId === "auto"
    ? { modelId: "default", parameters: [], maxMode: false }
    : resolveCursorModel(modelId) ?? { modelId, parameters: [], maxMode: false };
  const displayName = modelId === "auto" ? "Auto" : modelId;
  const requestedModel = create(RequestedModelSchema, {
    modelId: picked.modelId,
    maxMode: picked.maxMode,
    parameters: picked.parameters.map((parameter) => create(RequestedModel_ModelParameterbytesSchema, parameter)),
  });
  const modelDetails = create(ModelDetailsSchema, {
    modelId: picked.modelId,
    displayModelId: picked.modelId,
    displayName,
    displayNameShort: displayName,
  });

  const runRequest = create(AgentRunRequestSchema, {
    conversationState,
    action,
    modelDetails,
    requestedModel,
    conversationId,
  });

  const clientMessage = create(AgentClientMessageSchema, {
    message: { case: "runRequest", value: runRequest },
  });

  return {
    requestBytes: toBinary(AgentClientMessageSchema, clientMessage),
    blobStore,
    mcpTools: [],
    cloudRule: prompts.join("\n\n").trim() || undefined,
  };
}

function parseConnectEndStream(data: Uint8Array): Error | null {
  try {
    const payload = JSON.parse(new TextDecoder().decode(data));
    const error = payload?.error;
    if (error) {
      const code = error.code ?? "unknown";
      const message = error.message ?? "Unknown error";
      return new Error(`Connect error ${code}: ${message}`);
    }
    return null;
  } catch {
    return new Error("Failed to parse Connect end stream");
  }
}

function makeHeartbeatBytes(): Uint8Array {
  const heartbeat = create(AgentClientMessageSchema, {
    message: {
      case: "clientHeartbeat",
      value: create(ClientHeartbeatSchema, {}),
    },
  });
  return frameConnectMessage(toBinary(AgentClientMessageSchema, heartbeat));
}

interface ConnectFrameParser {
  push(incoming: Buffer): void;
  pendingLength(): number;
  consumedBytes(): number;
  failed(): boolean;
}

/**
 * Create a stateful parser for Connect protocol frames.
 * Handles buffering partial data across chunks.
 *
 * A body that is not Connect-framed (for example an edge proxy returning
 * `{"error":...}` with a 200) must never be mistaken for a frame: the old
 * parser read the JSON as flags+length and waited forever for gigabytes.
 * Invalid flags/length call `onProtocolError` with a short body excerpt so
 * the caller can surface a real error instead of hanging.
 */
function createConnectFrameParser(
  onMessage: (bytes: Uint8Array) => void,
  onEndStream: (bytes: Uint8Array) => void,
  onProtocolError?: (message: string) => void,
): ConnectFrameParser {
  let pending = Buffer.alloc(0);
  let failed = false;
  let consumed = 0;
  const fail = (why: string) => {
    if (failed) return;
    failed = true;
    const body = pending.subarray(0, 400).toString("utf8").replace(/\s+/g, " ").trim();
    const detail = `${why} [consumed=${consumed} pending=${pending.length}]`;
    pending = Buffer.alloc(0);
    onProtocolError?.(body ? `${detail}: ${body}` : detail);
  };
  const push = (incoming: Buffer) => {
    if (failed) return;
    consumed += incoming.length;
    pending = Buffer.concat([pending, incoming]);
    while (pending.length >= 5) {
      const flags = pending[0]!;
      // Connect/gRPC flags only define compressed (0x01) and end-stream
      // (0x02). Anything else is an unframed body, not a length header.
      if ((flags & ~0x03) !== 0) {
        fail(`invalid Connect frame flags 0x${flags.toString(16)}`);
        return;
      }
      const msgLen = pending.readUInt32BE(1);
      if (msgLen > MAX_CONNECT_FRAME_BYTES) {
        fail(`Connect frame too large (${msgLen} bytes)`);
        return;
      }
      if (pending.length < 5 + msgLen) break;
      let messageBytes: Uint8Array = pending.subarray(5, 5 + msgLen);
      pending = pending.subarray(5 + msgLen);
      if (flags & CONNECT_COMPRESSED_FLAG) {
        try {
          messageBytes = gunzipSync(messageBytes, {
            maxOutputLength: MAX_CONNECT_FRAME_BYTES,
          });
        } catch (err) {
          fail(
            "compressed Connect frame could not be decoded: " +
              (err instanceof Error ? err.message : String(err)),
          );
          return;
        }
      }
      if (flags & CONNECT_END_STREAM_FLAG) {
        onEndStream(messageBytes);
      } else {
        onMessage(messageBytes);
      }
    }
  };
  return {
    push,
    pendingLength: () => pending.length,
    consumedBytes: () => consumed,
    failed: () => failed,
  };
}

/**
 * True when a server message is real model progress. Cursor keeps a run alive
 * with `heartbeat` updates and usage-only `tokenDelta` updates; counting those
 * as progress is what let a stalled turn survive for tens of minutes.
 */
export function serverMessageIsProgress(msg: AgentServerMessage): boolean {
  const messageCase = msg.message.case;
  if (messageCase === undefined) return false;
  if (messageCase !== "interactionUpdate") return true;
  const updateCase = (msg.message.value as { message?: { case?: string } }).message?.case;
  return updateCase !== "heartbeat" && updateCase !== "tokenDelta";
}

const THINKING_TAG_NAMES = ['think', 'thinking', 'reasoning', 'thought', 'think_intent'];
const MAX_THINKING_TAG_LEN = 16; // </think_intent> is 15 chars

/**
 * Strip thinking tags from streamed text, routing tagged content to reasoning.
 * Buffers partial tags across chunk boundaries.
 */
function createThinkingTagFilter(): {
  process(text: string): { content: string; reasoning: string };
  flush(): { content: string; reasoning: string };
} {
  let buffer = '';
  let inThinking = false;

  return {
    process(text: string) {
      const input = buffer + text;
      buffer = '';
      let content = '';
      let reasoning = '';
      let lastIdx = 0;

      const re = new RegExp(`<(/?)(?:${THINKING_TAG_NAMES.join('|')})\\s*>`, 'gi');
      let match: RegExpExecArray | null;
      while ((match = re.exec(input)) !== null) {
        const before = input.slice(lastIdx, match.index);
        if (inThinking) reasoning += before;
        else content += before;
        inThinking = match[1] !== '/';
        lastIdx = re.lastIndex;
      }

      const rest = input.slice(lastIdx);
      // Buffer a trailing '<' that could be the start of a thinking tag.
      const ltPos = rest.lastIndexOf('<');
      if (ltPos >= 0 && rest.length - ltPos < MAX_THINKING_TAG_LEN && /^<\/?[a-z_]*$/i.test(rest.slice(ltPos))) {
        buffer = rest.slice(ltPos);
        const before = rest.slice(0, ltPos);
        if (inThinking) reasoning += before;
        else content += before;
      } else {
        if (inThinking) reasoning += rest;
        else content += rest;
      }

      return { content, reasoning };
    },
    flush() {
      const b = buffer;
      buffer = '';
      if (!b) return { content: '', reasoning: '' };
      return inThinking ? { content: '', reasoning: b } : { content: b, reasoning: '' };
    },
  };
}

interface StreamState {
  toolCallIndex: number;
  pendingExecs: PendingExec[];
  outputTokens: number;
  totalTokens: number;
}

function computeUsage(state: StreamState) {
  const completion_tokens = state.outputTokens;
  const total_tokens = state.totalTokens || completion_tokens;
  const prompt_tokens = Math.max(0, total_tokens - completion_tokens);
  return { prompt_tokens, completion_tokens, total_tokens };
}

function processServerMessage(
  msg: AgentServerMessage,
  blobStore: Map<string, Uint8Array>,
  mcpTools: McpToolDefinition[],
  cloudRule: string | undefined,
  sendFrame: (data: Uint8Array) => void,
  state: StreamState,
  onText: (text: string, isThinking?: boolean) => void,
  onMcpExec: (exec: PendingExec) => void,
  onTurnEnded?: () => void,
  onCheckpoint?: (checkpointBytes: Uint8Array) => void,
): void {
  const msgCase = msg.message.case;

  try {
    if (msgCase === "interactionUpdate") {
      handleInteractionUpdate(msg.message.value, state, onText, onTurnEnded);
    } else if (msgCase === "kvServerMessage") {
      handleKvMessage(msg.message.value as KvServerMessage, blobStore, sendFrame);
    } else if (msgCase === "execServerMessage") {
      handleExecMessage(
        msg.message.value as ExecServerMessage,
        mcpTools,
        cloudRule,
        sendFrame,
        onMcpExec,
      );
    } else if (msgCase === "interactionQuery") {
      handleInteractionQuery(
        msg.message.value as InteractionQuery,
        mcpTools,
        sendFrame,
        onMcpExec,
      );
    } else if (msgCase === "execServerControlMessage") {
      handleExecControlMessage(msg.message.value as ExecServerControlMessage, state);
    } else if (msgCase === "conversationCheckpointUpdate") {
      const stateStructure = msg.message.value as ConversationStateStructure;
      if (stateStructure.tokenDetails) {
        state.totalTokens = stateStructure.tokenDetails.usedTokens;
      }
      if (onCheckpoint) {
        onCheckpoint(toBinary(ConversationStateStructureSchema, stateStructure));
      }
    }
  } catch (err) {
    // A malformed message must not drop a request the server is waiting on.
    const message = err instanceof Error ? err.message : String(err);
    proxyLog(`message handler failed case=${msgCase ?? "none"}: ${message}`);
    if (msgCase === "execServerMessage") {
      const exec = msg.message.value as ExecServerMessage;
      // If the handler already emitted the tool call, its result will answer
      // the exec; a second throw here would race that answer.
      const pending = state.pendingExecs.some((entry) => entry.execMsgId === exec.id);
      if (!pending) {
        for (const frame of unavailableExec(exec.id, `handler failed: ${message}`)) {
          sendFrame(frameConnectMessage(frame));
        }
      }
    } else if (msgCase === "interactionQuery") {
      const query = msg.message.value as InteractionQuery;
      const pending = state.pendingExecs.some(
        (entry) => entry.interaction?.id === query.id,
      );
      if (!pending) {
        sendFrame(frameConnectMessage(buildEmptyInteractionResponse(query.id)));
      }
    }
  }
}

/**
 * Cursor cancels an exec it already asked for (`ExecServerControlMessage`
 * abort). The exec is gone upstream, so stop tracking it and never answer it;
 * the bridge itself stays open because other execs may still be pending.
 */
export function handleExecControlMessage(
  control: ExecServerControlMessage,
  state: { pendingExecs: PendingExec[] },
): void {
  if (control.message.case !== "abort") {
    // Unknown or empty control: log and keep the turn running.
    proxyLog(`exec control ignored case=${control.message.case ?? "none"}`);
    return;
  }
  const abortId = control.message.value.id;
  const removed = removePendingExec(state.pendingExecs, abortId);
  proxyLog(
    `exec abort id=${abortId} ${removed ? "dropped pending exec" : "no matching pending exec"}`,
  );
}

/**
 * Drop the pending exec whose `ExecServerMessage.id` matches, in place.
 * `ActiveBridge.pendingExecs` aliases `StreamState.pendingExecs`, so a later
 * tool-result resume only sees the removal if this same array is mutated.
 */
export function removePendingExec(
  pendingExecs: PendingExec[],
  execMsgId: number,
): boolean {
  const index = pendingExecs.findIndex((exec) => exec.execMsgId === execMsgId);
  if (index === -1) return false;
  const [removed] = pendingExecs.splice(index, 1);
  if (removed?.heartbeatTimer) clearInterval(removed.heartbeatTimer);
  return true;
}

/**
 * Cursor asks for user input through an InteractionQuery on the open run.
 * Answering it with the matching InteractionResponse is mandatory: ignoring it
 * leaves the model waiting forever, which is what a plan/build turn looked
 * like when AskQuestion was used. AskQuestion is bridged to maki's `question`
 * tool; every other query gets an immediate rejection so it cannot stall.
 */
function handleInteractionQuery(
  query: InteractionQuery,
  mcpTools: McpToolDefinition[],
  sendFrame: (data: Uint8Array) => void,
  onMcpExec: (exec: PendingExec) => void,
): void {
  const queryCase = query.query.case;

  if (queryCase === "askQuestionInteractionQuery") {
    const value = query.query.value;
    const args = value.args;
    if (args && (args.runAsync || args.asyncOriginalToolCallId)) {
      // The async protocol expects an AskQuestionResult.async marker and a
      // later AsyncAskQuestionCompletionAction. This proxy cannot send that
      // action, so reject immediately instead of falsely succeeding later.
      const rejected = buildRejectedInteractionResponse(query.id, queryCase);
      if (rejected) sendFrame(frameConnectMessage(rejected));
      proxyLog(`askQuestion interaction id=${query.id} rejected (async unsupported)`);
      return;
    }
    if (args && mcpTools.some((tool) => tool.name === QUESTION_TOOL_NAME)) {
      proxyLog(
        `askQuestion interaction id=${query.id} questions=${args.questions.length} bridged`,
      );
      onMcpExec({
        execId: "",
        execMsgId: 0,
        toolCallId: value.toolCallId || `ask_${query.id}`,
        toolName: QUESTION_TOOL_NAME,
        decodedArgs: buildQuestionToolArgs(args),
        interaction: { id: query.id, args },
      });
      return;
    }
    const rejected = buildRejectedInteractionResponse(query.id, queryCase);
    if (rejected) sendFrame(frameConnectMessage(rejected));
    proxyLog(`askQuestion interaction id=${query.id} rejected (no question tool)`);
    return;
  }

  const response = buildRejectedInteractionResponse(query.id, queryCase);
  if (response) {
    sendFrame(frameConnectMessage(response));
    return;
  }
  // A query this schema cannot name (newer Cursor features) must still get a
  // response on the open run, or the model waits forever. An id-only
  // InteractionResponse is the safe answer.
  sendFrame(frameConnectMessage(buildEmptyInteractionResponse(query.id)));
  proxyLog(`unsupported interaction query ${queryCase ?? "none"} answered empty`);
}

function handleInteractionUpdate(
  update: any,
  state: StreamState,
  onText: (text: string, isThinking?: boolean) => void,
  onTurnEnded?: () => void,
): void {
  const updateCase = update.message?.case;

  if (updateCase === "textDelta") {
    const delta = update.message.value.text || "";
    if (delta) onText(delta, false);
  } else if (updateCase === "thinkingDelta") {
    const delta = update.message.value.text || "";
    if (delta) onText(delta, true);
  } else if (updateCase === "tokenDelta") {
    state.outputTokens += update.message.value.tokens ?? 0;
  } else if (updateCase === "turnEnded") {
    // The server declared the turn complete. Some servers keep the HTTP/2
    // stream open after this, so terminate the SSE here instead of waiting
    // for a close that may never come.
    onTurnEnded?.();
  }
  // toolCallStarted, partialToolCall, toolCallDelta, toolCallCompleted
  // are intentionally ignored. MCP tool calls flow through the exec
  // message path (mcpArgs → mcpResult), not interaction updates.
}

/** Send a KV client response back to Cursor. */
function sendKvResponse(
  kvMsg: KvServerMessage,
  messageCase: string,
  value: unknown,
  sendFrame: (data: Uint8Array) => void,
): void {
  const response = create(KvClientMessageSchema, {
    id: kvMsg.id,
    message: { case: messageCase as any, value: value as any },
  });
  const clientMsg = create(AgentClientMessageSchema, {
    message: { case: "kvClientMessage", value: response },
  });
  sendFrame(frameConnectMessage(toBinary(AgentClientMessageSchema, clientMsg)));
}

function handleKvMessage(
  kvMsg: KvServerMessage,
  blobStore: Map<string, Uint8Array>,
  sendFrame: (data: Uint8Array) => void,
): void {
  const kvCase = kvMsg.message.case;

  if (kvCase === "getBlobArgs") {
    const blobId = kvMsg.message.value.blobId;
    const blobIdKey = Buffer.from(blobId).toString("hex");
    const blobData = blobStore.get(blobIdKey);
    if (process.env.CURSOR_PROXY_DEBUG) {
      console.error(`[proxy] getBlob ${blobIdKey.slice(0, 16)} ${blobData ? `hit (${blobData.length}b)` : "MISS"}`);
    }
    sendKvResponse(
      kvMsg, "getBlobResult",
      create(GetBlobResultSchema, blobData ? { blobData } : {}),
      sendFrame,
    );
  } else if (kvCase === "setBlobArgs") {
    const { blobId, blobData } = kvMsg.message.value;
    blobStore.set(Buffer.from(blobId).toString("hex"), blobData);
    if (process.env.CURSOR_PROXY_DEBUG) {
      console.error(`[proxy] setBlob ${Buffer.from(blobId).toString("hex").slice(0, 16)} (${blobData.length}b)`);
    }
    sendKvResponse(
      kvMsg, "setBlobResult",
      create(SetBlobResultSchema, {}),
      sendFrame,
    );
  } else {
    // An unnamed KV op leaves Cursor waiting if it expects a reply; at least
    // make it visible instead of silently dropping the message.
    proxyLog(`kv message ignored case=${kvCase ?? "none"}`);
  }
}

function handleExecMessage(
  execMsg: ExecServerMessage,
  mcpTools: McpToolDefinition[],
  cloudRule: string | undefined,
  sendFrame: (data: Uint8Array) => void,
  onMcpExec: (exec: PendingExec) => void,
): void {
  const disposition = classifyExec(execMsg, mcpTools);
  if (disposition.kind === "immediate") {
    if (process.env.CURSOR_PROXY_DEBUG) {
      const fields = (execMsg.$unknown ?? []).map((field) => field.no).join(",") || "none";
      console.error(`[proxy] closed exec id=${execMsg.id} fields=${fields}`);
    }
    for (const frame of disposition.frames) sendFrame(frameConnectMessage(frame));
    return;
  }
  const effective = disposition.kind === "shell" ? disposition.variant.message : execMsg;
  const resultField = disposition.kind === "shell" ? disposition.variant.resultField : undefined;
  if (process.env.CURSOR_PROXY_DEBUG && resultField) {
    console.error(`[proxy] shell variant result field ${resultField}`);
  }
  const execCase = effective.message.case;
  if (process.env.CURSOR_PROXY_DEBUG) {
    const extra = (execMsg.$unknown ?? []).map((field) => field.no).join(",");
    console.error(`[proxy] exec: ${execCase}${extra ? ` extra=${extra}` : ""}`);
  }

  if (execCase === "requestContextArgs") {
    // cloudRule is the prompt channel Cursor's agent actually honors; plain
    // system messages are ignored server-side (issue #21).
    const requestContext = create(RequestContextSchema, {
      rules: [],
      cloudRule,
      repositoryInfo: [],
      tools: mcpTools,
      gitRepos: [],
      projectLayouts: [],
      mcpInstructions: [],
      fileContents: {},
      customSubagents: [],
    });
    const result = create(RequestContextResultSchema, {
      result: {
        case: "success",
        value: create(RequestContextSuccessSchema, { requestContext }),
      },
    });
    sendExecResult(execMsg, "requestContextResult", result, sendFrame);
    return;
  }

  if (execCase === "mcpArgs") {
    const mcpArgs = execMsg.message.value;
    const decoded = decodeMcpArgsMap(mcpArgs.args ?? {});
    onMcpExec({
      execId: execMsg.execId,
      execMsgId: execMsg.id,
      toolCallId: mcpArgs.toolCallId || crypto.randomUUID(),
      toolName: mcpArgs.toolName || mcpArgs.name,
      decodedArgs: JSON.stringify(decoded),
    });
    return;
  }

  // --- Native Cursor tools ---
  // The model tries these before the MCP tools. When the client provides an
  // equivalent tool, redirect the call to it; otherwise reject so the model
  // falls back to the MCP tools registered via RequestContext.
  const redirect = redirectNativeExec(effective, mcpTools);
  if (redirect) {
    if (resultField) redirect.binding.resultField = resultField;
    if (process.env.CURSOR_PROXY_DEBUG) {
      console.error(`[proxy] redirect ${execCase} -> ${redirect.toolName}`);
    }
    onMcpExec({
      execId: execMsg.execId,
      execMsgId: execMsg.id,
      toolCallId: redirect.toolCallId,
      toolName: redirect.toolName,
      decodedArgs: redirect.decodedArgs,
      native: redirect.binding,
    });
    return;
  }

  const REJECT_REASON = "Tool not available in this environment. Use the MCP tools provided instead.";

  if (execCase === "readArgs") {
    const args = effective.message.value;
    const result = create(ReadResultSchema, {
      result: { case: "rejected", value: create(ReadRejectedSchema, { path: args.path, reason: REJECT_REASON }) },
    });
    sendExecResult(execMsg, "readResult", result, sendFrame);
    return;
  }
  if (execCase === "lsArgs") {
    const args = execMsg.message.value;
    const result = create(LsResultSchema, {
      result: { case: "rejected", value: create(LsRejectedSchema, { path: args.path, reason: REJECT_REASON }) },
    });
    sendExecResult(execMsg, "lsResult", result, sendFrame);
    return;
  }
  if (execCase === "grepArgs") {
    const result = create(GrepResultSchema, {
      result: { case: "error", value: create(GrepErrorSchema, { error: REJECT_REASON }) },
    });
    sendExecResult(execMsg, "grepResult", result, sendFrame);
    return;
  }
  if (execCase === "writeArgs") {
    const args = execMsg.message.value;
    const result = create(WriteResultSchema, {
      result: { case: "rejected", value: create(WriteRejectedSchema, { path: args.path, reason: REJECT_REASON }) },
    });
    sendExecResult(execMsg, "writeResult", result, sendFrame);
    return;
  }
  if (execCase === "deleteArgs") {
    const args = execMsg.message.value;
    const result = create(DeleteResultSchema, {
      result: { case: "rejected", value: create(DeleteRejectedSchema, { path: args.path, reason: REJECT_REASON }) },
    });
    sendExecResult(execMsg, "deleteResult", result, sendFrame);
    return;
  }
  if (execCase === "shellArgs" || execCase === "shellStreamArgs") {
    const args = effective.message.value;
    const rejected = create(ShellRejectedSchema, {
      command: args.command ?? "",
      workingDirectory: args.workingDirectory ?? "",
      reason: REJECT_REASON,
      isReadonly: false,
    });
    if (execCase === "shellStreamArgs") {
      // Cursor asked on the streaming shell field, so the rejection has to
      // come back on that same field; a shellResult leaves the caller waiting.
      const result = create(ShellStreamSchema, {
        event: { case: "rejected", value: rejected },
      });
      sendExecResult(execMsg, "shellStream", result, sendFrame);
      return;
    }
    const result = create(ShellResultSchema, {
      result: { case: "rejected", value: rejected },
    });
    sendExecResult(execMsg, "shellResult", result, sendFrame);
    return;
  }
  if (execCase === "backgroundShellSpawnArgs") {
    const args = execMsg.message.value;
    const result = create(BackgroundShellSpawnResultSchema, {
      result: {
        case: "rejected",
        value: create(ShellRejectedSchema, {
          command: args.command ?? "",
          workingDirectory: args.workingDirectory ?? "",
          reason: REJECT_REASON,
          isReadonly: false,
        }),
      },
    });
    sendExecResult(execMsg, "backgroundShellSpawnResult", result, sendFrame);
    return;
  }
  if (execCase === "writeShellStdinArgs") {
    const result = create(WriteShellStdinResultSchema, {
      result: { case: "error", value: create(WriteShellStdinErrorSchema, { error: REJECT_REASON }) },
    });
    sendExecResult(execMsg, "writeShellStdinResult", result, sendFrame);
    return;
  }
  if (execCase === "fetchArgs") {
    const args = execMsg.message.value;
    const result = create(FetchResultSchema, {
      result: { case: "error", value: create(FetchErrorSchema, { url: args.url ?? "", error: REJECT_REASON }) },
    });
    sendExecResult(execMsg, "fetchResult", result, sendFrame);
    return;
  }
  if (execCase === "diagnosticsArgs") {
    const args = effective.message.value;
    // An empty success is a real "no diagnostics" answer; a bare
    // DiagnosticsResult (no oneof case) reads as an unknown result.
    const result = create(DiagnosticsResultSchema, {
      result: {
        case: "success",
        value: create(DiagnosticsSuccessSchema, {
          path: args.path ?? "",
          diagnostics: [],
          totalDiagnostics: 0,
        }),
      },
    });
    sendExecResult(execMsg, "diagnosticsResult", result, sendFrame);
    return;
  }

  // MCP resource/screen/computer exec types. Each has its own result type, so
  // a generic McpResult leaves Cursor unable to decode the answer.
  if (execCase === "listMcpResourcesExecArgs") {
    const result = create(ListMcpResourcesExecResultSchema, {
      result: {
        case: "success",
        value: create(ListMcpResourcesSuccessSchema, { resources: [] }),
      },
    });
    sendExecResult(execMsg, "listMcpResourcesExecResult", result, sendFrame);
    return;
  }
  if (execCase === "readMcpResourceExecArgs") {
    const args = effective.message.value;
    const result = create(ReadMcpResourceExecResultSchema, {
      result: {
        case: "error",
        value: create(ReadMcpResourceErrorSchema, {
          uri: args.uri ?? "",
          error: "Reading MCP resources is not available in this environment.",
        }),
      },
    });
    sendExecResult(execMsg, "readMcpResourceExecResult", result, sendFrame);
    return;
  }
  if (execCase === "recordScreenArgs") {
    const result = create(RecordScreenResultSchema, {
      result: {
        case: "failure",
        value: create(RecordScreenFailureSchema, {
          error: "Screen recording is not available in this environment.",
        }),
      },
    });
    sendExecResult(execMsg, "recordScreenResult", result, sendFrame);
    return;
  }
  if (execCase === "computerUseArgs") {
    const result = create(ComputerUseResultSchema, {
      result: {
        case: "error",
        value: create(ComputerUseErrorSchema, {
          error: "Computer use is not available in this environment.",
          actionCount: 0,
          durationMs: 0,
        }),
      },
    });
    sendExecResult(execMsg, "computerUseResult", result, sendFrame);
    return;
  }

  for (const frame of unavailableExec(
    execMsg.id,
    "Tool not available in this environment. Use the MCP tools provided instead.",
  )) {
    sendFrame(frameConnectMessage(frame));
  }
}

/** Send an exec client message back to Cursor. */
function sendExecResult(
  execMsg: ExecServerMessage,
  messageCase: string,
  value: unknown,
  sendFrame: (data: Uint8Array) => void,
): void {
  const execClientMessage = create(ExecClientMessageSchema, {
    id: execMsg.id,
    execId: execMsg.execId,
    message: { case: messageCase as any, value: value as any },
  });
  const clientMessage = create(AgentClientMessageSchema, {
    message: { case: "execClientMessage", value: execClientMessage },
  });
  sendFrame(frameConnectMessage(toBinary(AgentClientMessageSchema, clientMessage)));
  sendFrame(frameConnectMessage(streamCloseMessage(execMsg.id)));
}

/** Deterministic v4-shaped UUID from a seed (first 16 bytes of SHA-256).
 *  Keeps conversation and message IDs stable across proxy restarts so
 *  Cursor's server-side caches stay warm. */
function deterministicUuid(seed: string): string {
  const hex = createHash("sha256").update(seed).digest("hex").slice(0, 32);
  // Format as UUID: xxxxxxxx-xxxx-4xxx-Nxxx-xxxxxxxxxxxx
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    `4${hex.slice(13, 16)}`,
    `${(0x8 | (parseInt(hex[16], 16) & 0x3)).toString(16)}${hex.slice(17, 20)}`,
    hex.slice(20, 32),
  ].join("-");
}

/** Render a bridge stderr tail as a log suffix, or "" when there is none. */
function stderrNote(tail: string): string {
  return tail === "" ? "" : `: ${tail}`;
}

/** Create an SSE streaming Response that reads from a live bridge. */
function createBridgeStreamResponse(
  bridge: Bridge,
  heartbeatTimer: NodeJS.Timeout,
  blobStore: Map<string, Uint8Array>,
  mcpTools: McpToolDefinition[],
  cloudRule: string | undefined,
  modelId: string,
  bridgeKey: string,
  convKey: string,
): Response {
  const completionId = `chatcmpl-${crypto.randomUUID().replace(/-/g, "").slice(0, 28)}`;
  const created = Math.floor(Date.now() / 1000);

  // Timers and the "did the client leave" decision live outside the stream
  // source so both the stream's cancel() and every exit path can run the same
  // shutdown exactly once.
  let keepaliveTimer: NodeJS.Timeout | undefined;
  let turnEndTimer: NodeJS.Timeout | undefined;
  let stallTimer: NodeJS.Timeout | undefined;
  let shutdown: () => void = () => {};
  let offClose: () => void = () => {};

  const stream = new ReadableStream({
    cancel() {
      // The client aborted (or writeNodeResponse cancelled the reader).
      // Stop pulling, drop the pause entry, and end the subprocess: nothing
      // will ever consume this bridge again.
      shutdown();
    },
    start(controller) {
      const encoder = new TextEncoder();
      let closed = false;
      /** True once tool_calls were sent and the bridge is parked for resume. */
      let pausedForToolResult = false;

      const enqueue = (text: string) => {
        if (closed) return;
        try {
          controller.enqueue(encoder.encode(text));
        } catch {
          // The stream was cancelled out from under us.
          closed = true;
        }
      };
      const sendSSE = (data: object) => enqueue(`data: ${JSON.stringify(data)}\n\n`);
      const sendDone = () => enqueue("data: [DONE]\n\n");

      const stopKeepalive = () => {
        if (keepaliveTimer === undefined) return;
        clearInterval(keepaliveTimer);
        keepaliveTimer = undefined;
      };

      const closeController = () => {
        stopKeepalive();
        stopStall();
        if (closed) return;
        closed = true;
        try {
          controller.close();
        } catch {
          // Already cancelled.
        }
      };

      /**
       * Persist the conversation before the bridge is reaped. The close
       * handler may no longer be registered at that point (shutdown detaches
       * it), so the state has to be written here as well.
       */
      const persistNow = () => {
        const stored = conversationStates.get(convKey);
        if (!stored) return;
        for (const [k, v] of blobStore) stored.blobStore.set(k, v);
        stored.lastAccessMs = Date.now();
        persistConversation(convKey, stored);
      };

      /**
       * The single teardown path. Every exit (clean end, end-stream error,
       * bridge exit, client abort) goes through here, so timers are cleared
       * and the controller closed at most once.
       */
      shutdown = () => {
        stopKeepalive();
        stopStall();
        if (turnEndTimer !== undefined) {
          clearTimeout(turnEndTimer);
          turnEndTimer = undefined;
        }
        if (pausedForToolResult) {
          // Parked bridge: keep the heartbeat and the activeBridges entry so
          // the tool-result request can resume this same stream.
          return;
        }
        clearPendingExecTimers(state.pendingExecs);
        clearInterval(heartbeatTimer);
        if (activeBridges.get(bridgeKey)?.bridge === bridge) {
          activeBridges.delete(bridgeKey);
        }
        // The client is gone (or the turn is over); no one will read this
        // bridge again, so end it and reap the subprocess.
        persistNow();
        bridge.detachSink();
        offClose();
        closeBridgeGracefully(bridge);
      };

      // An SSE comment immediately: it flushes headers through maki's client
      // and starts the idle clock on a connection that may stay quiet for
      // minutes while the model thinks.
      enqueue(": keepalive\n\n");
      keepaliveTimer = setInterval(() => enqueue(": keepalive\n\n"), SSE_KEEPALIVE_MS);
      // Do not hold the process open just for a heartbeat comment.
      keepaliveTimer.unref?.();
      armStall();

      const makeChunk = (
        delta: Record<string, unknown>,
        finishReason: string | null = null,
      ) => ({
        id: completionId,
        object: "chat.completion.chunk",
        created,
        model: modelId,
        choices: [{ index: 0, delta, finish_reason: finishReason }],
      });

      const makeUsageChunk = () => {
        const { prompt_tokens, completion_tokens, total_tokens } = computeUsage(state);
        return {
          id: completionId,
          object: "chat.completion.chunk",
          created,
          model: modelId,
          choices: [],
          usage: { prompt_tokens, completion_tokens, total_tokens },
        };
      };

      const state: StreamState = {
        toolCallIndex: 0,
        pendingExecs: [],
        outputTokens: 0,
        totalTokens: 0,
      };
      const tagFilter = createThinkingTagFilter();

      let mcpExecReceived = false;

      // --- Stall watchdog -------------------------------------------------
      // Cursor keeps a run alive with heartbeats, and maki re-arms its own
      // deadline on every SSE line (keepalives included). Without a progress
      // deadline here, "alive but stuck" looks identical to "still thinking"
      // forever. Only client-visible model progress resets this timer.
      const caseCounts = new Map<string, number>();
      const bumpCase = (messageCase: string) => {
        caseCounts.set(messageCase, (caseCounts.get(messageCase) ?? 0) + 1);
      };
      const formatCaseCounts = () =>
        [...caseCounts.entries()].map(([name, count]) => `${name}:${count}`).join(",") || "none";
      const stalledSeconds = Math.max(1, Math.round(STREAM_STALL_TIMEOUT_MS / 1000));

      function stopStall() {
        if (stallTimer === undefined) return;
        clearTimeout(stallTimer);
        stallTimer = undefined;
      }

      function onStall() {
        stallTimer = undefined;
        if (closed) return;
        proxyLog(
          `bridge ${bridgeKey} stalled: no model progress for ${stalledSeconds}s ` +
            `(server messages ${formatCaseCounts()})`,
        );
        sendSSE(makeChunk({
          content: `\n[Error: cursor stream stalled for ${stalledSeconds}s without output; retry the request]`,
        }));
        sendSSE(makeChunk({}, "stop"));
        sendSSE(makeUsageChunk());
        sendDone();
        closeController();
        shutdown();
      }

      function armStall() {
        stopStall();
        if (STREAM_STALL_TIMEOUT_MS <= 0) return;
        stallTimer = setTimeout(onStall, STREAM_STALL_TIMEOUT_MS);
        stallTimer.unref?.();
      }

      /** Reset the deadline only for client-visible model progress. */
      const noteProgress = () => armStall();

      /** Close the turn without tearing the bridge down immediately. */
      const finishTurn = () => {
        if (pausedForToolResult) return;
        const flushed = tagFilter.flush();
        if (flushed.reasoning) sendSSE(makeChunk({ reasoning_content: flushed.reasoning }));
        if (flushed.content) sendSSE(makeChunk({ content: flushed.content }));
        sendSSE(makeChunk({}, "stop"));
        sendSSE(makeUsageChunk());
        sendDone();
        closeController();
        // Let the server send a final checkpoint or end-stream before the
        // bridge is reaped. The SSE is already complete, so the caller is not
        // waiting on this grace period.
        if (turnEndTimer === undefined) {
          turnEndTimer = setTimeout(() => {
            turnEndTimer = undefined;
            shutdown();
          }, 5_000);
          turnEndTimer.unref?.();
        }
      };

      const frameSink: FrameSink = {
        onMessage: (messageBytes) => {
          try {
            const serverMessage = fromBinary(
              AgentServerMessageSchema,
              messageBytes,
            );
            bumpCase(serverMessage.message.case ?? "none");
            if (serverMessageIsProgress(serverMessage)) noteProgress();
            processServerMessage(
              serverMessage,
              blobStore,
              mcpTools,
              cloudRule,
              (data) => bridge.write(data),
              state,
              (text, isThinking) => {
                if (isThinking) {
                  sendSSE(makeChunk({ reasoning_content: text }));
                } else {
                  const { content, reasoning } = tagFilter.process(text);
                  if (reasoning) sendSSE(makeChunk({ reasoning_content: reasoning }));
                  if (content) sendSSE(makeChunk({ content }));
                }
              },
              // onMcpExec — the model wants to execute a tool.
              (exec) => {
                state.pendingExecs.push(exec);
                mcpExecReceived = true;

                // Cursor's exec handler expects a heartbeat every few seconds
                // while a tool is outstanding; the agent-level heartbeat alone
                // does not cover a long bash/grep run.
                if (exec.execMsgId !== 0 && !exec.heartbeatTimer) {
                  exec.heartbeatTimer = setInterval(() => {
                    bridge.write(frameConnectMessage(makeExecHeartbeat(exec.execMsgId)));
                  }, EXEC_HEARTBEAT_MS);
                  exec.heartbeatTimer.unref?.();
                }

                const flushed = tagFilter.flush();
                if (flushed.reasoning) sendSSE(makeChunk({ reasoning_content: flushed.reasoning }));
                if (flushed.content) sendSSE(makeChunk({ content: flushed.content }));

                const toolCallIndex = state.toolCallIndex++;
                sendSSE(makeChunk({
                  tool_calls: [{
                    index: toolCallIndex,
                    id: exec.toolCallId,
                    type: "function",
                    function: {
                      name: exec.toolName,
                      arguments: exec.decodedArgs,
                    },
                  }],
                }));

                // Keep the bridge alive for tool result continuation. This is
                // the one path where shutdown() must not touch the heartbeat
                // or the map entry.
                pausedForToolResult = true;
                activeBridges.set(bridgeKey, {
                  bridge,
                  heartbeatTimer,
                  blobStore,
                  mcpTools,
                  cloudRule,
                  pendingExecs: state.pendingExecs,
                  detachClose: offClose,
                  createdAt: Date.now(),
                });

                sendSSE(makeChunk({}, "tool_calls"));
                sendDone();
                // Only the SSE response ends here; the bridge stays parked.
                closeController();
                stopKeepalive();
              },
              // onTurnEnded — the server declared the turn complete. Close the
              // SSE even if it keeps the HTTP/2 stream open.
              finishTurn,
              (checkpointBytes) => {
                const stored = conversationStates.get(convKey);
                if (stored) {
                  stored.checkpoint = checkpointBytes;
                  // Merge live blobs before persisting: the checkpoint may
                  // reference blobs set during this stream.
                  for (const [k, v] of blobStore) stored.blobStore.set(k, v);
                  stored.lastAccessMs = Date.now();
                  persistConversation(convKey, stored);
                }
              },
            );
          } catch {
            // Skip unparseable messages
          }
        },
        onEndStream: (endStreamBytes) => {
          noteProgress();
          if (endStreamBytes.length === 0) {
            if (pausedForToolResult) {
              proxyLog(`bridge ${bridgeKey} ended upstream while parked for a tool result`);
              pausedForToolResult = false;
              shutdown();
              return;
            }
            finishTurn();
            return;
          }
          const endError = parseConnectEndStream(endStreamBytes);
          if (process.env.CURSOR_PROXY_DEBUG) {
            console.error(`[proxy] endStream: ${endError ? endError.message : "clean"}`);
          }
          if (endError) {
            // Surface the error and shut down: the server is done with this
            // stream, and heartbeats would otherwise keep the bridge (and the
            // SSE response) open forever.
            proxyLog(`cursor end stream error: ${endError.message}`);
            if (activeBridges.get(bridgeKey)?.bridge === bridge) {
              activeBridges.delete(bridgeKey);
            }
            if (!closed) {
              sendSSE(makeChunk({ content: `\n[Error: ${endError.message}]` }));
              sendSSE(makeChunk({}, "stop"));
              sendSSE(makeUsageChunk());
              sendDone();
            }
            closeController();
            pausedForToolResult = false;
            shutdown();
            return;
          }
          if (pausedForToolResult) {
            // The run is over, so the parked bridge can never be resumed.
            proxyLog(`bridge ${bridgeKey} ended upstream while parked for a tool result`);
            if (activeBridges.get(bridgeKey)?.bridge === bridge) {
              activeBridges.delete(bridgeKey);
            }
            closeController();
            pausedForToolResult = false;
            shutdown();
          }
        },
        onProtocolError: (protocolError) => {
          // A 200 response whose body is not Connect-framed (often an edge
          // proxy's JSON error) must become a visible failure, not silence.
          proxyLog(`cursor protocol error: ${protocolError}`);
          if (activeBridges.get(bridgeKey)?.bridge === bridge) {
            activeBridges.delete(bridgeKey);
          }
          if (closed) {
            pausedForToolResult = false;
            shutdown();
            return;
          }
          sendSSE(makeChunk({
            content: `\n[Error: cursor upstream sent an unframed response: ${protocolError}]`,
          }));
          sendSSE(makeChunk({}, "stop"));
          sendSSE(makeUsageChunk());
          sendDone();
          closeController();
          pausedForToolResult = false;
          shutdown();
        },
        isClosed: () => closed,
      };

      bridge.attachSink(frameSink);

      offClose = bridge.onClose((code) => {
        // The subprocess is gone, so its heartbeat has nowhere to go. Clear it
        // here rather than in shutdown(), which deliberately leaves the paused
        // bridge's interval alone.
        clearInterval(heartbeatTimer);
        clearPendingExecTimers(state.pendingExecs);
        persistNow();
        // A clean exit can still happen while parked (upstream closed after
        // sending tool calls). Drop the entry here, unconditionally, so a dead
        // bridge never waits 30 minutes for TTL eviction.
        if (activeBridges.get(bridgeKey)?.bridge === bridge) {
          activeBridges.delete(bridgeKey);
        }
        if (!mcpExecReceived) {
          const flushed = tagFilter.flush();
          if (flushed.reasoning) sendSSE(makeChunk({ reasoning_content: flushed.reasoning }));
          if (flushed.content) sendSSE(makeChunk({ content: flushed.content }));
          if (code !== 0) {
            // The bridge crashed before finishing the turn. Tell the caller
            // instead of ending the answer as if it were complete.
            proxyLog(`bridge exit ${code} mid-stream${stderrNote(bridge.stderrTail())}`);
            sendSSE(makeChunk({ content: `\n[Error: cursor bridge exited (${code})]` }));
          }
          sendSSE(makeChunk({}, "stop"));
          sendSSE(makeUsageChunk());
          sendDone();
          closeController();
        } else if (code !== 0) {
          // Bridge died while tool calls are pending (timeout, crash, etc.).
          // Close the SSE stream so the client doesn't hang forever.
          proxyLog(`bridge exit ${code} with pending tool calls${stderrNote(bridge.stderrTail())}`);
          if (!pausedForToolResult) {
            sendSSE(makeChunk({ content: "\n[Error: bridge connection lost]" }));
            sendSSE(makeChunk({}, "stop"));
            sendSSE(makeUsageChunk());
            sendDone();
            closeController();
          }
          // Remove stale entry so the next request doesn't try to resume it.
          if (activeBridges.get(bridgeKey)?.bridge === bridge) {
            activeBridges.delete(bridgeKey);
          }
        }
        // The subprocess is gone; release the interval and any map entry.
        shutdown();
      });
    },
  });

  return new Response(stream, { headers: SSE_HEADERS });
}

/** Spawn a bridge, send the initial request frame, and start heartbeat. */
function startBridge(
  accessToken: string,
  requestBytes: Uint8Array,
): { bridge: Bridge; heartbeatTimer: NodeJS.Timeout } {
  const bridge = spawnBridge({
    accessToken,
    rpcPath: "/agent.v1.AgentService/Run",
  });
  bridge.write(frameConnectMessage(requestBytes));
  const heartbeatTimer = setInterval(() => bridge.write(makeHeartbeatBytes()), 5_000);
  // A paused bridge should not be the reason the process cannot exit.
  heartbeatTimer.unref?.();
  return { bridge, heartbeatTimer };
}

function handleStreamingResponse(
  payload: CursorRequestPayload,
  accessToken: string,
  modelId: string,
  bridgeKey: string,
  convKey: string,
): Response {
  const { bridge, heartbeatTimer } = startBridge(accessToken, payload.requestBytes);
  return createBridgeStreamResponse(
    bridge, heartbeatTimer,
    payload.blobStore, payload.mcpTools, payload.cloudRule,
    modelId, bridgeKey, convKey,
  );
}

export interface AnsweredExec {
  execId: string;
  execMsgId: number;
  toolCallId: string;
  native?: NativeExecBinding;
  interaction?: AskQuestionInteraction;
  heartbeatTimer?: NodeJS.Timeout;
}

/**
 * Send each paused exec's tool result. A native result that cannot be typed
 * faithfully, including every write and shell, is an mcpResult carrying
 * maki's text.
 */
export function answerPendingExecs(
  pendingExecs: AnsweredExec[],
  toolResults: ToolResultInfo[],
  userText: string,
  write: (frame: Uint8Array) => void,
): void {
  for (const [index, exec] of pendingExecs.entries()) {
    if (exec.heartbeatTimer) {
      clearInterval(exec.heartbeatTimer);
      exec.heartbeatTimer = undefined;
    }
    const result = toolResults.find(
      (r) => r.toolCallId === exec.toolCallId,
    );
    const baseText = result ? result.content : "Tool result not provided";
    // A user message sent alongside tool results (e.g. the explanation typed
    // after rejecting an edit) would otherwise be dropped. Attach it only to
    // an MCP result: the array position is the reliable "last" marker because
    // `execId` is empty for most execs. Native typed results have no field for
    // it, and appending it to their content would corrupt file/shell output.
    const withUser = () =>
      userText && index === pendingExecs.length - 1
        ? `${baseText}\n\n<user_message>\n${userText}\n</user_message>`
        : baseText;

    // AskQuestion was bridged to maki's `question` tool. The upstream is
    // waiting on an InteractionResponse, not an exec result, so answer the
    // interaction and let the same bridge continue.
    if (exec.interaction) {
      const answer = buildAskQuestionResult(exec.interaction.args, baseText);
      proxyLog(
        `askQuestion answered id=${exec.interaction.id} result=${answer.result.case ?? "none"}`,
      );
      write(
        frameConnectMessage(buildAskQuestionResponse(exec.interaction.id, answer)),
      );
      continue;
    }

    if (result && exec.native) {
      const sent = sendNativeExecResult(exec, exec.native, baseText, (bytes) =>
        write(frameConnectMessage(bytes)),
      );
      if (sent) {
        write(frameConnectMessage(streamCloseMessage(exec.execMsgId)));
        continue;
      }
    }

    const text = withUser();

    // maki reports tool failure as text. Passing it as a success makes the
    // model try to use an error string as if it were output; surface it as an
    // MCP error result instead.
    const toolFailed =
      result !== undefined &&
      (/^error[:\s]/i.test(text) || text.startsWith("Permission denied"));
    const mcpResult = result && !toolFailed
      ? create(McpResultSchema, {
          result: {
            case: "success",
            value: create(McpSuccessSchema, {
              content: [
                create(McpToolResultContentItemSchema, {
                  content: {
                    case: "text",
                    value: create(McpTextContentSchema, { text }),
                  },
                }),
              ],
              isError: false,
            }),
          },
        })
      : create(McpResultSchema, {
          result: {
            case: "error",
            value: create(McpErrorSchema, { error: text }),
          },
        });

    const execClientMessage = create(ExecClientMessageSchema, {
      id: exec.execMsgId,
      execId: exec.execId,
      message: {
        case: "mcpResult" as never,
        value: mcpResult as never,
      },
    });

    const clientMessage = create(AgentClientMessageSchema, {
      message: { case: "execClientMessage", value: execClientMessage },
    });

    write(frameConnectMessage(toBinary(AgentClientMessageSchema, clientMessage)));
    write(frameConnectMessage(streamCloseMessage(exec.execMsgId)));
  }
}

/** Resume a paused bridge by sending MCP results and continuing to stream. */
function handleToolResultResume(
  active: ActiveBridge,
  toolResults: ToolResultInfo[],
  userText: string,
  modelId: string,
  bridgeKey: string,
  convKey: string,
): Response {
  const { bridge, heartbeatTimer, blobStore, mcpTools, cloudRule, pendingExecs } = active;

  const response = createBridgeStreamResponse(
    bridge, heartbeatTimer,
    blobStore, mcpTools, cloudRule,
    modelId, bridgeKey, convKey,
  );
  active.detachClose?.();
  answerPendingExecs(pendingExecs, toolResults, userText, (frame) => bridge.write(frame));
  return response;
}

function handleNonStreamingResponse(
  payload: CursorRequestPayload,
  accessToken: string,
  modelId: string,
  convKey: string,
): Response {
  const completionId = `chatcmpl-${crypto.randomUUID().replace(/-/g, "").slice(0, 28)}`;
  const created = Math.floor(Date.now() / 1000);
  const abort = new AbortController();
  const encoder = new TextEncoder();

  // Return the Response before the model finishes, so writeNodeResponse can
  // flush headers immediately. The old code awaited the whole generation here
  // and left the client with no response head for over two minutes.
  const stream = new ReadableStream({
    async start(controller) {
      try {
        const { text, usage, exitCode, errorDetail } = await collectFullResponse(
          payload,
          accessToken,
          convKey,
          abort.signal,
        );
        const failed = exitCode !== 0;
        const detail = errorDetail?.trim() ? `: ${errorDetail.trim()}` : "";
        const failure = `cursor bridge exited (${exitCode})${detail}`;
        const message = failed
          ? { role: "assistant", content: `\n[Error: ${failure}]` }
          : { role: "assistant", content: text };
        const payloadBody: Record<string, unknown> = {
          id: completionId,
          object: "chat.completion",
          created,
          model: modelId,
          choices: [
            {
              index: 0,
              message,
              finish_reason: failed ? "error" : "stop",
            },
          ],
          usage,
        };
        if (failed) {
          payloadBody.error = {
            message: failure,
            type: "upstream_error",
          };
        }
        controller.enqueue(encoder.encode(JSON.stringify(payloadBody)));
        controller.close();
      } catch (err) {
        // A cancelled client no longer has anywhere to put an error.
        if (!abort.signal.aborted) controller.error(err);
      }
    },
    cancel() {
      // writeNodeResponse cancels the reader when the client disconnects.
      abort.abort();
    },
  });

  return new Response(stream, { headers: { "Content-Type": "application/json" } });
}

interface CollectedResponse {
  text: string;
  usage: { prompt_tokens: number; completion_tokens: number; total_tokens: number };
  exitCode: number;
  /** Server error or bridge stderr tail, for the non-streaming error body. */
  errorDetail?: string;
}

async function collectFullResponse(
  payload: CursorRequestPayload,
  accessToken: string,
  convKey: string,
  signal?: AbortSignal,
): Promise<CollectedResponse> {
  const { promise, resolve, reject } = Promise.withResolvers<CollectedResponse>();
  let fullText = "";

  const { bridge, heartbeatTimer } = startBridge(accessToken, payload.requestBytes);

  // Connect's end-stream frame carries a server error after the last message.
  // Without recording it, a failed run looked like a clean "stop" with
  // whatever text had streamed so far.
  let endError: Error | null = null;

  // Same progress deadline as the streaming path: a non-streaming request must
  // not sit on an alive-but-stuck bridge forever either. These are defined
  // before onAbort because an already-aborted signal calls it synchronously.
  let stallTimer: NodeJS.Timeout | undefined;
  const stopStall = () => {
    if (stallTimer === undefined) return;
    clearTimeout(stallTimer);
    stallTimer = undefined;
  };
  const stalledSeconds = Math.max(1, Math.round(STREAM_STALL_TIMEOUT_MS / 1000));
  const onStall = () => {
    stallTimer = undefined;
    if (endError) return;
    endError = new Error(`cursor stream stalled for ${stalledSeconds}s without output`);
    proxyLog(`non-streaming bridge stalled: no model progress for ${stalledSeconds}s`);
    clearInterval(heartbeatTimer);
    closeBridgeGracefully(bridge);
  };
  const armStall = () => {
    stopStall();
    if (STREAM_STALL_TIMEOUT_MS <= 0) return;
    stallTimer = setTimeout(onStall, STREAM_STALL_TIMEOUT_MS);
    stallTimer.unref?.();
  };
  const noteProgress = () => armStall();

  const onAbort = () => {
    stopStall();
    clearInterval(heartbeatTimer);
    closeBridgeGracefully(bridge);
    reject(new Error("client aborted"));
  };
  if (signal?.aborted) {
    onAbort();
  } else {
    signal?.addEventListener("abort", onAbort, { once: true });
  }

  const state: StreamState = {
    toolCallIndex: 0,
    pendingExecs: [],
    outputTokens: 0,
    totalTokens: 0,
  };
  const tagFilter = createThinkingTagFilter();

  bridge.attachSink({
    onMessage: (messageBytes) => {
      try {
        const serverMessage = fromBinary(
          AgentServerMessageSchema,
          messageBytes,
        );
        if (serverMessageIsProgress(serverMessage)) noteProgress();
        processServerMessage(
          serverMessage,
          payload.blobStore,
          payload.mcpTools,
          payload.cloudRule,
          (data) => bridge.write(data),
          state,
          (text, isThinking) => {
            if (isThinking) return;
            const { content } = tagFilter.process(text);
            fullText += content;
          },
          (exec) => {
            // A non-streaming turn cannot pause for a tool result. Reject an
            // AskQuestion explicitly so the model does not wait forever for
            // an answer this path can never deliver.
            if (exec.interaction) {
              const rejected = buildRejectedInteractionResponse(
                exec.interaction.id,
                "askQuestionInteractionQuery",
              );
              if (rejected) bridge.write(frameConnectMessage(rejected));
              return;
            }
            // Any other exec (MCP tool call or redirected native tool) would
            // also park the upstream waiting for a result this path can never
            // deliver. Answer it with throw + streamClose instead of silence.
            if (exec.execMsgId !== 0) {
              proxyLog(
                `non-streaming exec ${exec.toolName} id=${exec.execMsgId} refused`,
              );
              for (const frame of unavailableExec(
                exec.execMsgId,
                "Tools are not available in a non-streaming request.",
              )) {
                bridge.write(frameConnectMessage(frame));
              }
            }
          },
          () => {
            // Non-streaming: the server said the turn is done. Reap the
            // bridge so the request settles even if the stream stays open.
            clearInterval(heartbeatTimer);
            closeBridgeGracefully(bridge);
          },
          (checkpointBytes) => {
            const stored = conversationStates.get(convKey);
            if (stored) {
              stored.checkpoint = checkpointBytes;
              for (const [k, v] of payload.blobStore) stored.blobStore.set(k, v);
              stored.lastAccessMs = Date.now();
              persistConversation(convKey, stored);
            }
          },
        );
      } catch {
        // Skip
      }
    },
    onEndStream: (endStreamBytes) => {
      noteProgress();
      // A zero-length end-stream frame carries no JSON; treat it as a clean
      // end rather than a parse failure.
      if (endStreamBytes.length === 0) {
        stopStall();
        clearInterval(heartbeatTimer);
        closeBridgeGracefully(bridge);
        return;
      }
      const error = parseConnectEndStream(endStreamBytes);
      if (!error) return;
      endError = error;
      proxyLog(`cursor end stream error: ${error.message}`);
      // The server is done with this run; reap the bridge so onClose settles
      // now instead of waiting for the child to notice the stream end.
      clearInterval(heartbeatTimer);
      closeBridgeGracefully(bridge);
    },
    onProtocolError: (protocolError) => {
      endError = new Error(`unframed upstream response: ${protocolError}`);
      proxyLog(`cursor protocol error: ${endError.message}`);
      clearInterval(heartbeatTimer);
      closeBridgeGracefully(bridge);
    },
    isClosed: () => endError !== null,
  });
  armStall();

  bridge.onClose((code) => {
    stopStall();
    clearInterval(heartbeatTimer);
    signal?.removeEventListener("abort", onAbort);
    const stored = conversationStates.get(convKey);
    if (stored) {
      for (const [k, v] of payload.blobStore) stored.blobStore.set(k, v);
      stored.lastAccessMs = Date.now();
      persistConversation(convKey, stored);
    }
    const flushed = tagFilter.flush();
    fullText += flushed.content;

    const usage = computeUsage(state);
    resolve({
      text: fullText,
      usage,
      // A server-reported end-stream error is a failed turn even when the
      // process exited cleanly.
      exitCode: endError ? 1 : code,
      errorDetail: endError?.message ?? bridge.stderrTail(),
    });
  });

  return promise;
}
