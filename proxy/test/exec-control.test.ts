/**
 * Exec control messages and execs on a non-streaming turn.
 *
 * - Cursor cancels an exec it already asked for with `ExecServerControlMessage`
 *   abort. The proxy must drop that exec from the parked set (so a later
 *   tool-result resume never answers it) and must not close the bridge.
 * - A non-streaming turn cannot pause for a tool result. An MCP/redirected exec
 *   there must be answered with throw + streamClose instead of silence, or the
 *   upstream waits forever.
 *
 * The fake h2c upstream has to be listening before proxy.ts is imported
 * because CURSOR_API_URL is read at module load.
 */
import assert from "node:assert/strict";
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
  ExecServerAbortSchema,
  ExecServerControlMessageSchema,
  ExecServerMessageSchema,
  InteractionUpdateSchema,
  McpArgsSchema,
  TextDeltaUpdateSchema,
  type AgentClientMessage,
} from "../src/proto/agent_pb.ts";

const TOKEN = "test-token";

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

async function pollUntil(check: () => boolean, ms: number, message: string): Promise<void> {
  const deadline = Date.now() + ms;
  while (!check()) {
    if (Date.now() > deadline) throw new Error(message);
    await delay(25);
  }
}

// --- Shared fake upstream (h2c) ---------------------------------------------------

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
  // Always drain so a chatty client can never block on flow control.
  stream.on("error", () => {});
  stream.on("data", () => {});
  onStream(stream);
});
upstream.listen(0, "127.0.0.1");
await once(upstream, "listening");
const upstreamPort = (upstream.address() as AddressInfo).port;

const conversationDir = mkdtempSync(join(tmpdir(), "maki-cursor-exec-control-conv-"));
const logDir = mkdtempSync(join(tmpdir(), "maki-cursor-exec-control-log-"));
const logPath = join(logDir, "proxy.log");

process.env.CURSOR_API_URL = `http://127.0.0.1:${upstreamPort}`;
process.env.CURSOR_SSE_KEEPALIVE_MS = "100";
process.env.CURSOR_EXEC_HEARTBEAT_MS = "50";
process.env.MAKI_CURSOR_CONVERSATIONS = conversationDir;
process.env.MAKI_CURSOR_CREDENTIALS = join(logDir, "credentials.json");
delete process.env.CURSOR_PROXY_DEBUG;

const { handleExecControlMessage, removePendingExec, startProxy, stopProxy } = await import(
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

// --- Frame helpers -----------------------------------------------------------------

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

/** One Connect data frame carrying an MCP exec request. */
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

/** One Connect data frame carrying an exec abort control message. */
function abortControlFrame(id: number): Buffer {
  const control = create(ExecServerControlMessageSchema, {
    message: { case: "abort", value: create(ExecServerAbortSchema, { id }) },
  });
  return serverFrame(
    create(AgentServerMessageSchema, {
      message: { case: "execServerControlMessage", value: control },
    }),
  );
}

/** One Connect data frame carrying an exec control message with no case. */
function emptyControlFrame(): Buffer {
  const control = create(ExecServerControlMessageSchema, {});
  return serverFrame(
    create(AgentServerMessageSchema, {
      message: { case: "execServerControlMessage", value: control },
    }),
  );
}

/** One Connect data frame carrying an agent text delta. */
function textFrame(text: string): Buffer {
  const update = create(InteractionUpdateSchema, {
    message: { case: "textDelta", value: create(TextDeltaUpdateSchema, { text }) },
  });
  return serverFrame(
    create(AgentServerMessageSchema, { message: { case: "interactionUpdate", value: update } }),
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

function streamBody() {
  return {
    model: "test-model",
    messages: [{ role: "user", content: "hi" }],
    stream: true,
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

// --- Unit tests ---------------------------------------------------------------------

test("removePendingExec drops the matching exec in place", () => {
  const pending = [
    { execId: "exec-1", execMsgId: 1, toolCallId: "call_1", toolName: "bash", decodedArgs: "{}" },
    { execId: "exec-7", execMsgId: 7, toolCallId: "call_7", toolName: "bash", decodedArgs: "{}" },
  ];
  // ActiveBridge holds the same array reference; a resume sees the removal
  // only because this call mutates the array in place.
  const alias = pending;

  assert.equal(removePendingExec(pending, 7), true);
  assert.deepEqual(
    alias.map((exec) => exec.execMsgId),
    [1],
  );
  assert.equal(removePendingExec(pending, 7), false, "an id is only dropped once");
});

test("an empty exec control case is ignored without throwing", () => {
  const state = {
    toolCallIndex: 0,
    pendingExecs: [
      { execId: "exec-1", execMsgId: 1, toolCallId: "call_1", toolName: "bash", decodedArgs: "{}" },
    ],
    outputTokens: 0,
    totalTokens: 0,
  };
  assert.doesNotThrow(() =>
    handleExecControlMessage(create(ExecServerControlMessageSchema, {}), state),
  );
  assert.equal(state.pendingExecs.length, 1);
});

test("a parked exec sends exec heartbeat frames", { timeout: 15_000 }, async () => {
  let clientMessages: AgentClientMessage[] = [];

  onStream = (stream) => {
    respondSilent(stream);
    clientMessages = attachClientParser(stream);
    stream.write(mcpExecFrame(7, "call_heartbeat"));
  };

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const first = await withTimeout(
      chatRequest(port, streamBody(), "session-heartbeat"),
      5_000,
      "no response headers",
    );
    await withTimeout(readAll(first), 10_000, "the tool-call turn hung");

    await pollUntil(
      () => clientMessages.some(
        (message) =>
          message.message.case === "execClientControlMessage" &&
          message.message.value.message.case === "heartbeat" &&
          message.message.value.message.value.id === 7,
      ),
      1_500,
      "the proxy never sent an exec heartbeat",
    );
  } finally {
    stopProxy();
  }
});

// --- Integration tests ---------------------------------------------------------------

test("an exec abort drops the parked exec, logs it, and leaves the bridge open", { timeout: 30_000 }, async () => {
  let upstreamStream: http2.ServerHttp2Stream | undefined;
  let clientMessages: AgentClientMessage[] = [];

  onStream = (stream) => {
    upstreamStream = stream;
    respondSilent(stream);
    clientMessages = attachClientParser(stream);
    stream.write(mcpExecFrame(7, "call_abort"));
  };

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const first = await withTimeout(
      chatRequest(port, streamBody(), "session-abort"),
      5_000,
      "no response headers",
    );
    const firstText = await withTimeout(readAll(first), 10_000, "the tool-call turn hung");
    assert.match(firstText, /"id":"call_abort"/);
    assert.match(firstText, /data: \[DONE\]/);

    assert.ok(upstreamStream, "the upstream stream was never opened");
    // Cursor cancels the exec it already asked for, then sends a control
    // message with no case at all.
    upstreamStream.write(abortControlFrame(7));
    upstreamStream.write(emptyControlFrame());

    await pollUntil(
      () => /exec abort id=7 dropped pending exec/.test(logText()),
      5_000,
      `the abort was not logged; log was:\n${logText()}`,
    );

    // Resume the parked turn with the result of the aborted call. The proxy
    // must not answer exec 7, and the bridge must still be usable.
    const resumed = await withTimeout(
      chatRequest(
        port,
        {
          model: "test-model",
          stream: true,
          messages: [
            { role: "user", content: "hi" },
            {
              role: "assistant",
              content: null,
              tool_calls: [
                {
                  id: "call_abort",
                  type: "function",
                  function: { name: "bash", arguments: "{}" },
                },
              ],
            },
            { role: "tool", tool_call_id: "call_abort", content: "output" },
          ],
        },
        "session-abort",
      ),
      5_000,
      "the resume request got no headers",
    );

    await delay(400);
    const answeredIds = clientMessages
      .filter((message) => message.message.case === "execClientMessage")
      .map((message) =>
        message.message.case === "execClientMessage" ? message.message.value.id : -1,
      );
    assert.deepEqual(answeredIds, [], "the aborted exec must not be answered on resume");
    assert.match(logText(), /exec control ignored case=none/);

    // The same run continues after the abort, on the same bridge.
    upstreamStream.write(textFrame("after abort"));
    setTimeout(() => {
      if (!upstreamStream!.closed && !upstreamStream!.destroyed) upstreamStream!.end();
    }, 100);
    const resumedText = await withTimeout(readAll(resumed), 10_000, "the resumed turn hung");
    assert.match(resumedText, /"content":"after abort"/);
    assert.match(resumedText, /data: \[DONE\]/);
  } finally {
    stopProxy();
  }
});

test("a non-streaming exec is refused with throw and streamClose instead of hanging", { timeout: 30_000 }, async () => {
  const { promise: refused, resolve: onRefused } = Promise.withResolvers<void>();
  let sawThrow = false;
  let sawStreamClose = false;
  let upstreamStream: http2.ServerHttp2Stream | undefined;
  let clientMessages: AgentClientMessage[] = [];

  onStream = (stream) => {
    upstreamStream = stream;
    respondSilent(stream);
    clientMessages = attachClientParser(stream, (message) => {
      if (message.message.case !== "execClientControlMessage") return;
      const control = message.message.value.message;
      if (control.case === "throw" && control.value.id === 11) sawThrow = true;
      if (control.case === "streamClose" && control.value.id === 11) sawStreamClose = true;
      if (sawThrow && sawStreamClose) onRefused();
    });
    stream.write(mcpExecFrame(11, "call_nonstream"));
  };

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(
      chatRequest(port, { ...streamBody(), stream: false }),
      5_000,
      "non-streaming request got no headers",
    );

    // The upstream finishes only after the proxy refuses the exec, so the
    // turn cannot complete just because the upstream gave up first.
    await withTimeout(refused, 5_000, "the proxy never refused the non-streaming exec");
    const thrown = clientMessages
      .flatMap((message) =>
        message.message.case === "execClientControlMessage"
          ? [message.message.value.message]
          : [],
      )
      .find((control) => control.case === "throw");
    assert.ok(thrown && thrown.case === "throw");
    assert.match(thrown.value.error, /non-streaming/);

    assert.ok(upstreamStream, "the upstream stream was never opened");
    upstreamStream.end();

    const payload = JSON.parse(await withTimeout(response.text(), 10_000, "no JSON body"));
    assert.equal(payload.object, "chat.completion");
    assert.equal(payload.choices[0].finish_reason, "stop");
    assert.equal(payload.choices[0].message.content, "");
  } finally {
    stopProxy();
  }
});
