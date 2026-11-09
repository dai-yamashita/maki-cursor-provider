/**
 * Upstream failures must be reported as failures.
 *
 * Cursor can end a Connect stream with an error frame (for example
 * `resource_exhausted` while the account is rate limited). Reporting that as
 * assistant content with `finish_reason:"stop"` made maki count the turn as a
 * success, so maki-loop immediately started the next iteration and sustained
 * the rate limit. The proxy now emits an OpenAI in-band error event whose
 * status tells the client how to retry.
 *
 * CURSOR_API_URL and the timing knobs are read at module load, so the fake
 * upstream is installed before the import.
 */
import assert from "node:assert/strict";
import { once } from "node:events";
import http2 from "node:http2";
import { mkdtempSync, rmSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { after } from "node:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  AgentClientMessageSchema,
  AgentServerMessageSchema,
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

// --- Fake upstream (h2c) ---------------------------------------------------------

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

const conversationDir = mkdtempSync(join(tmpdir(), "maki-cursor-upstream-error-conv-"));
const logDir = mkdtempSync(join(tmpdir(), "maki-cursor-upstream-error-log-"));

process.env.CURSOR_API_URL = `http://127.0.0.1:${upstreamPort}`;
process.env.CURSOR_SSE_KEEPALIVE_MS = "50";
process.env.CURSOR_EXEC_HEARTBEAT_MS = "50";
// Long enough that the stall watchdog never masks an end-stream error.
process.env.CURSOR_STREAM_STALL_TIMEOUT_MS = "30000";
process.env.MAKI_CURSOR_CONVERSATIONS = conversationDir;
process.env.MAKI_CURSOR_CREDENTIALS = join(logDir, "credentials.json");
delete process.env.CURSOR_PROXY_DEBUG;

const { classifyConnectError, startProxy, stopProxy } = await import("../src/proxy.ts");

after(async () => {
  const closed = once(upstream, "close").catch(() => undefined);
  for (const session of sessions) session.destroy();
  upstream.close();
  await Promise.race([closed, delay(2_000)]);
  rmSync(conversationDir, { recursive: true, force: true });
  rmSync(logDir, { recursive: true, force: true });
});

// --- Frame helpers ---------------------------------------------------------------

function connectFrame(bytes: Uint8Array, flags = 0): Buffer {
  const frame = Buffer.alloc(5 + bytes.length);
  frame[0] = flags;
  frame.writeUInt32BE(bytes.length, 1);
  frame.set(bytes, 5);
  return frame;
}

/** A Connect end-stream frame (flag 0x02) carrying a JSON error. */
function errorEndStreamFrame(code: string, message: string): Buffer {
  const payload = new TextEncoder().encode(JSON.stringify({ error: { code, message } }));
  return connectFrame(payload, 0b10);
}

function textFrame(text: string): Buffer {
  const update = create(InteractionUpdateSchema, {
    message: { case: "textDelta", value: create(TextDeltaUpdateSchema, { text }) },
  });
  const message = create(AgentServerMessageSchema, {
    message: { case: "interactionUpdate", value: update },
  });
  return connectFrame(toBinary(AgentServerMessageSchema, message));
}

function mcpExecFrame(id: number, toolCallId: string): Buffer {
  const exec = create(ExecServerMessageSchema, {
    id,
    execId: `exec-${id}`,
    message: {
      case: "mcpArgs",
      value: create(McpArgsSchema, { name: "bash", toolName: "bash", toolCallId, args: {} }),
    },
  });
  const message = create(AgentServerMessageSchema, {
    message: { case: "execServerMessage", value: exec },
  });
  return connectFrame(toBinary(AgentServerMessageSchema, message));
}

function attachClientParser(
  stream: http2.ServerHttp2Stream,
  onMessage: (message: AgentClientMessage) => void,
): void {
  let pending = Buffer.alloc(0);
  stream.on("data", (chunk: Buffer) => {
    pending = Buffer.concat([pending, chunk]);
    while (pending.length >= 5) {
      const length = pending.readUInt32BE(1);
      if (pending.length < 5 + length) break;
      const payload = pending.subarray(5, 5 + length);
      pending = pending.subarray(5 + length);
      try {
        onMessage(fromBinary(AgentClientMessageSchema, payload));
      } catch {
        // Not a client message frame; ignore.
      }
    }
  });
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
          { id: toolCallId, type: "function", function: { name: "bash", arguments: "{}" } },
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

// --- Unit: Connect code -> status ------------------------------------------------

test("Connect codes map to the status that drives the client's retry policy", () => {
  const cases: Array<[string, number, string]> = [
    // Cursor's capacity/quota reply must back off and retry, not end the turn.
    ["resource_exhausted", 503, "service_unavailable_error"],
    ["unavailable", 503, "service_unavailable_error"],
    ["internal", 500, "api_error"],
    ["deadline_exceeded", 504, "server_error"],
    ["unauthenticated", 401, "authentication_error"],
    ["permission_denied", 403, "permission_error"],
    ["invalid_argument", 400, "invalid_request_error"],
    ["canceled", 499, "invalid_request_error"],
    ["unknown", 500, "api_error"],
    // A code this proxy has never seen is a gateway problem, not a clean stop.
    ["not_a_connect_code", 502, "provider_unavailable"],
  ];
  for (const [code, status, type] of cases) {
    const failure = classifyConnectError(code, "boom");
    assert.equal(failure.status, status, `status for ${code}`);
    assert.equal(failure.type, type, `type for ${code}`);
    assert.equal(failure.code, code);
    assert.match(failure.message, new RegExp(`Connect error ${code}: boom`));
  }
});

// --- Streaming -------------------------------------------------------------------

test("resource_exhausted ends the stream as an SSE error, not a completed turn", { timeout: 15_000 }, async () => {
  upstreamSetup((stream) => {
    respondSilent(stream);
    stream.write(errorEndStreamFrame("resource_exhausted", "Error"));
    setTimeout(() => {
      if (!stream.closed && !stream.destroyed) stream.end();
    }, 50);
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(chatRequest(port), 5_000, "no response headers");
    const text = await withTimeout(readAll(response), 10_000, "the error stream hung");

    assert.match(
      text,
      /"error":\{"code":503,"type":"service_unavailable_error","message":"Connect error resource_exhausted: Error"\}/,
    );
    // The old failure mode: a fake completed answer that maki counted as end_turn.
    assert.doesNotMatch(text, /"finish_reason"/);
    assert.doesNotMatch(text, /\[Error:/);
    assert.match(text, /data: \[DONE\]/);
  } finally {
    stopProxy();
  }
});

test("partial content is followed by the error event, never a stop", { timeout: 15_000 }, async () => {
  upstreamSetup((stream) => {
    respondSilent(stream);
    stream.write(textFrame("partial"));
    setTimeout(() => {
      if (!stream.closed && !stream.destroyed) {
        stream.write(errorEndStreamFrame("unavailable", "gone"));
        stream.end();
      }
    }, 40);
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(chatRequest(port), 5_000, "no response headers");
    const text = await withTimeout(readAll(response), 10_000, "the partial stream hung");

    assert.match(text, /"content":"partial"/);
    assert.match(text, /"error":\{"code":503,"type":"service_unavailable_error"/);
    assert.doesNotMatch(text, /"finish_reason":"(stop|error)"/);
    assert.match(text, /data: \[DONE\]/);
  } finally {
    stopProxy();
  }
});

test("a resumed turn reports the upstream error instead of ending the answer", { timeout: 20_000 }, async () => {
  upstreamSetup((stream) => {
    respondSilent(stream);
    attachClientParser(stream, (message) => {
      if (message.message.case === "execClientMessage") {
        // The tool result reached the upstream. It refuses the run.
        stream.write(errorEndStreamFrame("resource_exhausted", "Error"));
      }
    });
    stream.write(mcpExecFrame(7, "call_error"));
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const first = await withTimeout(
      chatRequest(port, streamBody(), "session-upstream-error"),
      5_000,
      "no response headers",
    );
    const firstText = await withTimeout(readAll(first), 10_000, "the tool-call turn hung");
    assert.match(firstText, /"tool_calls"/);

    const second = await withTimeout(
      chatRequest(port, resumeBody("call_error"), "session-upstream-error"),
      5_000,
      "the resume got no response headers",
    );
    const secondText = await withTimeout(readAll(second), 10_000, "the resumed error hung");
    assert.match(secondText, /"error":\{"code":503,"type":"service_unavailable_error"/);
    assert.doesNotMatch(secondText, /"finish_reason"/);
    assert.match(secondText, /data: \[DONE\]/);
  } finally {
    stopProxy();
  }
});

// --- Non-streaming ---------------------------------------------------------------

test("non-streaming carries the mapped status and type in its error body", { timeout: 15_000 }, async () => {
  upstreamSetup((stream) => {
    respondSilent(stream);
    stream.write(errorEndStreamFrame("resource_exhausted", "Error"));
    setTimeout(() => {
      if (!stream.closed && !stream.destroyed) stream.end();
    }, 50);
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(
      chatRequest(port, { ...streamBody(), stream: false }),
      5_000,
      "no response headers",
    );
    const payload = (await withTimeout(response.json(), 10_000, "no JSON body")) as {
      choices: Array<{ finish_reason: string; message: { content: string } }>;
      error?: { message: string; type: string; code: number };
    };

    assert.equal(payload.choices[0]!.finish_reason, "error");
    assert.equal(payload.error?.code, 503);
    assert.equal(payload.error?.type, "service_unavailable_error");
    assert.match(payload.error?.message ?? "", /resource_exhausted/);
    assert.match(payload.choices[0]!.message.content, /\[Error:/);
  } finally {
    stopProxy();
  }
});
