/**
 * "Alive but stuck" must never look like "still thinking".
 *
 * Cursor keeps a run alive with `heartbeat` InteractionUpdates, and the proxy
 * sends `: keepalive` comments so reqwest's 120 s low-speed timeout never
 * fires. maki re-arms its own stream deadline on every SSE line, comments
 * included, so before the progress watchdog a bridge that was alive but
 * produced no model output hung a tool-result resume for tens of minutes with
 * no error (the 2026-11-08 ooxml_rs incident).
 *
 * CURSOR_STREAM_STALL_TIMEOUT_MS and CURSOR_API_URL are read at module load,
 * so the fake upstream and the short deadline are installed before the import.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { once } from "node:events";
import http2 from "node:http2";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { after } from "node:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  AgentClientMessageSchema,
  AgentServerMessageSchema,
  ExecServerMessageSchema,
  HeartbeatUpdateSchema,
  InteractionUpdateSchema,
  McpArgsSchema,
  TextDeltaUpdateSchema,
  TokenDeltaUpdateSchema,
  TurnEndedUpdateSchema,
  type AgentClientMessage,
} from "../src/proto/agent_pb.ts";

const TOKEN = "test-token";
const STALL_MS = 400;

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const guard = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms);
  });
  try {
    return await Promise.race([promise, guard]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

// --- Shared fake upstream (h2c) -------------------------------------------------

/** Per-test upstream behavior; replaced before each request. */
let onStream: (stream: http2.ServerHttp2Stream) => void = () => {};

const sessions = new Set<http2.ServerHttp2Session>();
const upstream = http2.createServer();
upstream.on("session", (session) => {
  sessions.add(session);
  session.on("close", () => sessions.delete(session));
  session.on("error", () => {});
});
upstream.on("stream", (stream) => {
  stream.on("error", () => {});
  stream.on("data", () => {});
  onStream(stream);
});
upstream.listen(0, "127.0.0.1");
await once(upstream, "listening");
const upstreamPort = (upstream.address() as AddressInfo).port;

const conversationDir = mkdtempSync(join(tmpdir(), "maki-cursor-stall-conv-"));
const logDir = mkdtempSync(join(tmpdir(), "maki-cursor-stall-log-"));
const logPath = join(logDir, "proxy.log");

process.env.CURSOR_API_URL = `http://127.0.0.1:${upstreamPort}`;
process.env.CURSOR_SSE_KEEPALIVE_MS = "50";
process.env.CURSOR_EXEC_HEARTBEAT_MS = "50";
process.env.CURSOR_STREAM_STALL_TIMEOUT_MS = String(STALL_MS);
process.env.MAKI_CURSOR_CONVERSATIONS = conversationDir;
process.env.MAKI_CURSOR_CREDENTIALS = join(logDir, "credentials.json");
delete process.env.CURSOR_PROXY_DEBUG;

const { liveBridgeCountForTests, serverMessageIsProgress, startProxy, stopProxy } = await import(
  "../src/proxy.ts"
);

after(async () => {
  const closed = once(upstream, "close").catch(() => undefined);
  for (const session of sessions) session.destroy();
  upstream.close();
  await Promise.race([closed, delay(2_000)]);
  rmSync(conversationDir, { recursive: true, force: true });
  rmSync(logDir, { recursive: true, force: true });
});

// --- Frame helpers ----------------------------------------------------------------

function connectFrame(bytes: Uint8Array): Buffer {
  const frame = Buffer.alloc(5 + bytes.length);
  frame[0] = 0;
  frame.writeUInt32BE(bytes.length, 1);
  frame.set(bytes, 5);
  return frame;
}

function serverFrame(message: ReturnType<typeof create<typeof AgentServerMessageSchema>>): Buffer {
  return connectFrame(toBinary(AgentServerMessageSchema, message));
}

function updateFrame(
  update: ReturnType<typeof create<typeof InteractionUpdateSchema>>,
): Buffer {
  return serverFrame(
    create(AgentServerMessageSchema, { message: { case: "interactionUpdate", value: update } }),
  );
}

function textFrame(text: string): Buffer {
  return updateFrame(
    create(InteractionUpdateSchema, {
      message: { case: "textDelta", value: create(TextDeltaUpdateSchema, { text }) },
    }),
  );
}

function heartbeatFrame(): Buffer {
  return updateFrame(
    create(InteractionUpdateSchema, {
      message: { case: "heartbeat", value: create(HeartbeatUpdateSchema, {}) },
    }),
  );
}

function tokenDeltaFrame(tokens: number): Buffer {
  return updateFrame(
    create(InteractionUpdateSchema, {
      message: { case: "tokenDelta", value: create(TokenDeltaUpdateSchema, { tokens }) },
    }),
  );
}

function turnEndedFrame(): Buffer {
  return updateFrame(
    create(InteractionUpdateSchema, {
      message: { case: "turnEnded", value: create(TurnEndedUpdateSchema, {}) },
    }),
  );
}

/** One Connect data frame carrying an MCP exec request (a tool call). */
function mcpExecFrame(id: number, toolCallId: string, toolName = "bash"): Buffer {
  const exec = create(ExecServerMessageSchema, {
    id,
    execId: `exec-${id}`,
    message: {
      case: "mcpArgs",
      value: create(McpArgsSchema, { name: toolName, toolName, toolCallId, args: {} }),
    },
  });
  return serverFrame(
    create(AgentServerMessageSchema, { message: { case: "execServerMessage", value: exec } }),
  );
}

/** Decode every AgentClientMessage the proxy writes on this upstream stream. */
function attachClientParser(
  stream: http2.ServerHttp2Stream,
  onMessage?: (message: AgentClientMessage) => void,
): AgentClientMessage[] {
  const messages: AgentClientMessage[] = [];
  let pendingBytes = Buffer.alloc(0);
  stream.on("data", (chunk: Buffer) => {
    pendingBytes = Buffer.concat([pendingBytes, chunk]);
    while (pendingBytes.length >= 5) {
      const length = pendingBytes.readUInt32BE(1);
      if (pendingBytes.length < 5 + length) break;
      const payload = pendingBytes.subarray(5, 5 + length);
      pendingBytes = pendingBytes.subarray(5 + length);
      try {
        const message = fromBinary(AgentClientMessageSchema, payload);
        messages.push(message);
        onMessage?.(message);
      } catch {
        // Not a client message frame; ignore.
      }
    }
  });
  return messages;
}

function respondSilent(stream: http2.ServerHttp2Stream): void {
  try {
    stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
  } catch {
    // The client already went away.
  }
}

function upstreamSetup(behavior: (stream: http2.ServerHttp2Stream) => void): void {
  onStream = behavior;
}

function streamBody() {
  return {
    model: "test-model",
    messages: [{ role: "user", content: "hi" }],
    stream: true,
  };
}

/** The resume maki would send after executing the bash tool call. */
function resumeBody(toolCallId: string) {
  return {
    model: "test-model",
    stream: true,
    messages: [
      { role: "user", content: "hi" },
      {
        role: "assistant",
        content: null,
        tool_calls: [
          {
            id: toolCallId,
            type: "function",
            function: { name: "bash", arguments: "{}" },
          },
        ],
      },
      { role: "tool", tool_call_id: toolCallId, content: "command output" },
    ],
  };
}

function chatRequest(
  port: number,
  body: Record<string, unknown> = streamBody(),
  session = "",
): Promise<Response> {
  return fetch(`http://127.0.0.1:${port}/v1/chat/completions`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${TOKEN}`,
      "content-type": "application/json",
      ...(session ? { "x-maki-session": session } : {}),
    },
    body: JSON.stringify(body),
  });
}

async function readAll(response: Response): Promise<string> {
  const body = response.body;
  assert.ok(body, "the SSE response has no body");
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let text = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) text += decoder.decode(value, { stream: true });
  }
  return text + decoder.decode();
}

function logText(): string {
  try {
    return readFileSync(logPath, "utf8");
  } catch {
    return "";
  }
}

/** SIGKILL every h2-bridge child of this test process. */
function killBridgeChildren(): number {
  const out = execFileSync(
    "pgrep",
    ["-P", String(process.pid), "-f", "h2-bridge.mjs"],
    { encoding: "utf8" },
  ).trim();
  const pids = out.split(/\s+/).filter(Boolean).map((pid) => Number(pid));
  assert.ok(pids.length > 0, "no h2-bridge child was running");
  for (const pid of pids) process.kill(pid, "SIGKILL");
  return pids.length;
}

// --- Tests ------------------------------------------------------------------------

test("a resumed turn with only heartbeats stalls with an error instead of hanging", { timeout: 20_000 }, async () => {
  let upstreamStream: http2.ServerHttp2Stream | undefined;
  const { promise: resumeSeen, resolve: onResume } = Promise.withResolvers<void>();

  upstreamSetup((stream) => {
    upstreamStream = stream;
    respondSilent(stream);
    attachClientParser(stream, (message) => {
      if (message.message.case === "execClientMessage") onResume();
    });
    // Turn 1: one tool call, then the bridge is parked for the result.
    stream.write(mcpExecFrame(7, "call_stall"));
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const first = await withTimeout(
      chatRequest(port, streamBody(), "session-stall"),
      5_000,
      "no response headers",
    );
    const firstText = await withTimeout(readAll(first), 10_000, "the tool-call turn hung");
    assert.match(firstText, /"tool_calls"/);

    // Turn 2: the resume. The upstream accepts the exec result but only sends
    // heartbeats, exactly like a run stuck behind capacity.
    const second = await withTimeout(
      chatRequest(port, resumeBody("call_stall"), "session-stall"),
      5_000,
      "the resume got no response headers",
    );
    await withTimeout(resumeSeen, 5_000, "the exec result never reached the upstream");

    const heartbeat = setInterval(() => {
      if (upstreamStream && !upstreamStream.closed && !upstreamStream.destroyed) {
        upstreamStream.write(heartbeatFrame());
      }
    }, 50);
    try {
      const secondText = await withTimeout(
        readAll(second),
        5_000,
        "the stalled resume hung without an error (the incident)",
      );
      assert.match(
        secondText,
        /"error":\{"code":504,"type":"server_error","message":"cursor stream stalled for 1s without output/,
      );
      assert.doesNotMatch(secondText, /"finish_reason"/);
      assert.match(secondText, /data: \[DONE\]/);
    } finally {
      clearInterval(heartbeat);
    }

    // The log must explain what the upstream was doing (heartbeats, no text).
    assert.match(logText(), /stalled: no model progress/);
    assert.match(logText(), /interactionUpdate:\d+/);
  } finally {
    stopProxy();
  }
});

test("a fresh turn that only sends heartbeats stalls with an error", { timeout: 15_000 }, async () => {
  let upstreamStream: http2.ServerHttp2Stream | undefined;
  upstreamSetup((stream) => {
    upstreamStream = stream;
    respondSilent(stream);
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(chatRequest(port), 5_000, "no response headers");
    const heartbeat = setInterval(() => {
      if (upstreamStream && !upstreamStream.closed && !upstreamStream.destroyed) {
        upstreamStream.write(heartbeatFrame());
      }
    }, 50);
    try {
      const text = await withTimeout(
        readAll(response),
        5_000,
        "a heartbeat-only turn never failed",
      );
      assert.match(text, /"error":\{"code":504,"type":"server_error","message":"cursor stream stalled/);
      assert.match(text, /data: \[DONE\]/);
    } finally {
      clearInterval(heartbeat);
    }
  } finally {
    stopProxy();
  }
});

test("a 200 JSON error body is surfaced instead of being misparsed as a frame", { timeout: 15_000 }, async () => {
  upstreamSetup((stream) => {
    respondSilent(stream);
    // A gateway-style error that is not Connect-framed. The old parser read
    // `{"error"...` as flags+length and waited for a huge frame forever.
    stream.write(Buffer.from('{"error":{"message":"prefill_cap_refused: at capacity"}}', "utf8"));
    // Keep the stream open: the failure must come from the parser, not a close.
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(chatRequest(port), 5_000, "no response headers");
    const text = await withTimeout(readAll(response), 5_000, "an unframed body hung the client");
    assert.match(text, /"error":\{"code":502,"type":"provider_unavailable","message":"cursor upstream sent an unframed response/);
    assert.match(text, /prefill_cap_refused/);
    assert.match(text, /data: \[DONE\]/);
  } finally {
    stopProxy();
  }
});

test("real model progress keeps resetting the stall deadline", { timeout: 15_000 }, async () => {
  let upstreamStream: http2.ServerHttp2Stream | undefined;
  upstreamSetup((stream) => {
    upstreamStream = stream;
    respondSilent(stream);
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(chatRequest(port), 5_000, "no response headers");
    // Stream text for longer than STALL_MS in total, with each gap well under
    // it. A fixed wall-clock limit would abort this legitimate turn.
    let sent = 0;
    const interval = setInterval(() => {
      if (!upstreamStream || upstreamStream.closed || upstreamStream.destroyed) return;
      sent += 1;
      upstreamStream.write(textFrame(`chunk${sent} `));
      if (sent === 8) {
        clearInterval(interval);
        upstreamStream.write(turnEndedFrame());
      }
    }, 100);
    try {
      const text = await withTimeout(readAll(response), 5_000, "the progressing turn hung");
      assert.doesNotMatch(text, /stalled/, "progress must not be treated as a stall");
      assert.match(text, /chunk8/);
      assert.match(text, /data: \[DONE\]/);
    } finally {
      clearInterval(interval);
    }
  } finally {
    stopProxy();
  }
});

test("a killed bridge ends the client stream and the proxy keeps serving", { timeout: 20_000 }, async () => {
  let upstreamStream: http2.ServerHttp2Stream | undefined;
  const { promise: streamSeen, resolve: onSeen } = Promise.withResolvers<void>();
  upstreamSetup((stream) => {
    upstreamStream = stream;
    respondSilent(stream);
    onSeen();
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(chatRequest(port), 5_000, "no response headers");
    await withTimeout(streamSeen, 5_000, "the upstream stream never opened");

    // Kill the child while the parent still holds its stdin. The old bridge
    // write path turned the resulting EPIPE into an uncaught exception that
    // killed the whole proxy (every session), not just this request.
    const killed = killBridgeChildren();
    assert.ok(killed >= 1);

    const text = await withTimeout(readAll(response), 5_000, "a dead bridge hung the client");
    assert.match(
      text,
      /"error":\{"code":502,"type":"provider_unavailable","message":"(cursor bridge exited|bridge connection lost)/,
    );
    assert.match(text, /data: \[DONE\]/);

    // Give an async pipe error time to surface, then prove the proxy still
    // serves a request on a new bridge.
    await delay(300);
    upstreamSetup((stream) => {
      respondSilent(stream);
      stream.write(textFrame("alive"));
      setTimeout(() => {
        if (!stream.closed && !stream.destroyed) stream.end();
      }, 50);
    });
    const next = await withTimeout(chatRequest(port), 5_000, "the proxy stopped serving");
    const nextText = await withTimeout(readAll(next), 5_000, "the next request hung");
    assert.match(nextText, /"content":"alive"/);
    assert.match(nextText, /data: \[DONE\]/);
    assert.equal(liveBridgeCountForTests(), 0);
  } finally {
    stopProxy();
    void upstreamStream;
  }
});

test("heartbeats and usage-only updates are not model progress", () => {
  const heartbeat = create(AgentServerMessageSchema, {
    message: {
      case: "interactionUpdate",
      value: create(InteractionUpdateSchema, {
        message: { case: "heartbeat", value: create(HeartbeatUpdateSchema, {}) },
      }),
    },
  });
  const tokens = create(AgentServerMessageSchema, {
    message: {
      case: "interactionUpdate",
      value: create(InteractionUpdateSchema, {
        message: { case: "tokenDelta", value: create(TokenDeltaUpdateSchema, { tokens: 3 }) },
      }),
    },
  });
  const text = create(AgentServerMessageSchema, {
    message: {
      case: "interactionUpdate",
      value: create(InteractionUpdateSchema, {
        message: { case: "textDelta", value: create(TextDeltaUpdateSchema, { text: "hi" }) },
      }),
    },
  });
  const exec = create(AgentServerMessageSchema, {
    message: {
      case: "execServerMessage",
      value: create(ExecServerMessageSchema, {
        id: 1,
        execId: "exec-1",
        message: { case: "mcpArgs", value: create(McpArgsSchema, { name: "bash" }) },
      }),
    },
  });

  assert.equal(serverMessageIsProgress(heartbeat), false);
  assert.equal(serverMessageIsProgress(tokens), false);
  assert.equal(serverMessageIsProgress(text), true);
  assert.equal(serverMessageIsProgress(exec), true);
});
