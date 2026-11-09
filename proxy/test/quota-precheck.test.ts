/**
 * An exhausted Cursor named-model quota is permanent until the billing period
 * rolls over. It arrives as the same `resource_exhausted` code as a transient
 * capacity blip, so the proxy reads GetCurrentPeriodUsage and blocks the run
 * locally with a fatal 400. Sending it upstream (and reporting 503) made maki
 * retry forever and let maki-loop keep consuming the exhausted bucket.
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

// --- Fake upstream (h2c) ---------------------------------------------------------

let onStream: (stream: http2.ServerHttp2Stream, path: string) => void = () => {};
let runRequests = 0;

const sessions = new Set<http2.ServerHttp2Session>();
const upstream = http2.createServer();
upstream.on("session", (session) => {
  sessions.add(session);
  session.on("close", () => sessions.delete(session));
  session.on("error", () => {});
});
upstream.on("stream", (stream, headers) => {
  stream.on("error", () => {});
  stream.on("data", () => {});
  onStream(stream, String(headers[":path"] ?? ""));
});
upstream.listen(0, "127.0.0.1");
await once(upstream, "listening");
const upstreamPort = (upstream.address() as AddressInfo).port;

const conversationDir = mkdtempSync(join(tmpdir(), "maki-cursor-quota-conv-"));
const logDir = mkdtempSync(join(tmpdir(), "maki-cursor-quota-log-"));

process.env.CURSOR_API_URL = `http://127.0.0.1:${upstreamPort}`;
process.env.CURSOR_SSE_KEEPALIVE_MS = "50";
process.env.CURSOR_EXEC_HEARTBEAT_MS = "50";
process.env.CURSOR_STREAM_STALL_TIMEOUT_MS = "30000";
process.env.MAKI_CURSOR_CONVERSATIONS = conversationDir;
process.env.MAKI_CURSOR_LOG = join(logDir, "proxy.log");

const { startProxy, stopProxy, refreshCursorUsage } = await import("../src/proxy.ts");
const { decodeCurrentPeriodUsage, setUsageSnapshot, resetUsageForTests } = await import(
  "../src/usage.ts"
);

after(async () => {
  stopProxy();
  const closed = once(upstream, "close").catch(() => undefined);
  for (const session of sessions) session.destroy();
  upstream.close();
  await Promise.race([closed, delay(2_000)]);
  rmSync(conversationDir, { recursive: true, force: true });
  rmSync(logDir, { recursive: true, force: true });
});

// --- Protobuf payload ------------------------------------------------------------

function varint(value: number): number[] {
  const bytes: number[] = [];
  let rest = value;
  while (rest > 0x7f) {
    bytes.push((rest & 0x7f) | 0x80);
    rest = Math.floor(rest / 128);
  }
  bytes.push(rest);
  return bytes;
}

function fieldVarint(field: number, value: number): number[] {
  return [...varint(field << 3), ...varint(value)];
}

function fieldString(field: number, value: string): number[] {
  const bytes = [...Buffer.from(value)];
  return [...varint((field << 3) | 2), ...varint(bytes.length), ...bytes];
}

function fieldMessage(field: number, body: number[]): number[] {
  return [...varint((field << 3) | 2), ...varint(body.length), ...body];
}

function fieldDouble(field: number, value: number): number[] {
  const bytes = Buffer.alloc(8);
  bytes.writeDoubleLE(value, 0);
  return [...varint((field << 3) | 1), ...bytes];
}

function usagePayload(options: { apiPercent?: number | null; remaining?: number } = {}): Uint8Array {
  const apiPercent = options.apiPercent === undefined ? 100 : options.apiPercent;
  const remaining = options.remaining ?? 0;
  const plan: number[] = [
    ...fieldVarint(1, 31209),
    ...fieldVarint(2, 2000),
    ...fieldVarint(4, remaining),
    ...fieldVarint(5, 2000),
    ...fieldVarint(6, 0),
    ...fieldDouble(12, 10.5),
    ...fieldDouble(14, 25),
  ];
  if (apiPercent !== null) plan.push(...fieldDouble(13, apiPercent));
  return Uint8Array.from([
    ...fieldVarint(2, Date.parse("2026-12-04T01:51:41.000Z")),
    ...fieldMessage(3, plan),
    ...fieldString(7, "You've hit your usage limit"),
    ...fieldString(12, "You've used 100% of your included API usage"),
    ...fieldString(13, "default"),
    ...fieldString(13, "composer-2.5"),
  ]);
}

// --- Connect frames --------------------------------------------------------------

function connectFrame(bytes: Uint8Array, flags = 0): Buffer {
  const frame = Buffer.alloc(5 + bytes.length);
  frame[0] = flags;
  frame.writeUInt32BE(bytes.length, 1);
  frame.set(bytes, 5);
  return frame;
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

function respondRun(stream: http2.ServerHttp2Stream): void {
  runRequests += 1;
  stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
  stream.write(textFrame("pong"));
  stream.write(connectFrame(new Uint8Array(), 0b10));
  stream.end();
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

function errorEndStreamFrame(code: string, message: string): Buffer {
  const payload = new TextEncoder().encode(JSON.stringify({ error: { code, message } }));
  return connectFrame(payload, 0b10);
}

function cleanEndStreamFrame(): Buffer {
  return connectFrame(new Uint8Array(), 0b10);
}

function onClientMessage(
  stream: http2.ServerHttp2Stream,
  callback: (message: AgentClientMessage) => void,
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
        callback(fromBinary(AgentClientMessageSchema, payload));
      } catch {
        // Heartbeats and other frames are irrelevant here.
      }
    }
  });
}

// --- HTTP helpers ----------------------------------------------------------------

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

function chatRequest(port: number, body: Record<string, unknown>): Promise<Response> {
  return fetch(`http://127.0.0.1:${port}/v1/chat/completions`, {
    method: "POST",
    headers: { authorization: `Bearer ${TOKEN}`, "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

function body(model: string, stream = true): Record<string, unknown> {
  return { model, stream, messages: [{ role: "user", content: "hi" }] };
}

// --- Tests -----------------------------------------------------------------------

test("startup usage fetch records the exhausted named-model quota", async () => {
  onStream = (stream, path) => {
    if (path === "/aiserver.v1.DashboardService/GetCurrentPeriodUsage") {
      stream.respond({ ":status": 200, "content-type": "application/proto" });
      stream.end(Buffer.from(usagePayload()));
      return;
    }
    respondRun(stream);
  };
  resetUsageForTests();
  const snapshot = await refreshCursorUsage(TOKEN);
  assert.ok(snapshot);
  assert.equal(snapshot.apiExhausted, true);
  assert.equal(snapshot.apiPercentUsed, 100);
  assert.equal(snapshot.resetAt, "2026-12-04T01:51:41.000Z");
  assert.deepEqual([...snapshot.autoBucketModels].sort(), ["composer-2.5", "default"]);
});

test("a blocked named model fails locally with a fatal 400", { timeout: 15_000 }, async () => {
  setUsageSnapshot(decodeCurrentPeriodUsage(usagePayload()));
  runRequests = 0;
  onStream = (_stream, _path) => respondRun(_stream);

  const port = await startProxy(async () => TOKEN, [], 0, TOKEN);
  try {
    const response = await chatRequest(port, body("glm-5p3-high"));
    const text = await readAll(response);
    assert.match(text, /"error":\{"code":400,"type":"insufficient_quota"/);
    assert.match(text, /Cursor included API usage is exhausted/);
    assert.match(text, /resets 2026-12-04T01:51:41.000Z/);
    assert.match(text, /cursor\/auto/);
    assert.equal(runRequests, 0, "the blocked request must not reach Cursor");
  } finally {
    stopProxy();
  }
});

test("auto and composer are allowed through to the upstream run", { timeout: 20_000 }, async () => {
  setUsageSnapshot(decodeCurrentPeriodUsage(usagePayload()));
  onStream = (_stream, _path) => respondRun(_stream);

  const port = await startProxy(async () => TOKEN, [], 0, TOKEN);
  try {
    for (const model of ["auto", "composer-2.5"]) {
      runRequests = 0;
      const text = await readAll(await chatRequest(port, body(model)));
      assert.equal(runRequests, 1, `${model} should reach the upstream run`);
      assert.match(text, /"content":"pong"/);
      assert.match(text, /"finish_reason":"stop"/);
      assert.doesNotMatch(text, /insufficient_quota/);
    }
  } finally {
    stopProxy();
  }
});

test("non-streaming returns HTTP 400 with the quota error", { timeout: 15_000 }, async () => {
  setUsageSnapshot(decodeCurrentPeriodUsage(usagePayload()));
  runRequests = 0;
  onStream = (_stream, _path) => respondRun(_stream);

  const port = await startProxy(async () => TOKEN, [], 0, TOKEN);
  try {
    const response = await chatRequest(port, body("deepseek-v41-flash-1017-medium", false));
    assert.equal(response.status, 400);
    const parsed = (await response.json()) as { error?: { type?: string; code?: number } };
    assert.equal(parsed.error?.type, "insufficient_quota");
    assert.equal(parsed.error?.code, 400);
    assert.equal(runRequests, 0);
  } finally {
    stopProxy();
  }
});

test("a healthy quota lets the named model reach the upstream run", { timeout: 20_000 }, async () => {
  setUsageSnapshot(decodeCurrentPeriodUsage(usagePayload({ apiPercent: 99.9 })));
  runRequests = 0;
  onStream = (_stream, _path) => respondRun(_stream);

  const port = await startProxy(async () => TOKEN, [], 0, TOKEN);
  try {
    const text = await readAll(await chatRequest(port, body("glm-5p3-high")));
    assert.equal(runRequests, 1);
    assert.match(text, /"content":"pong"/);
    assert.doesNotMatch(text, /insufficient_quota/);
  } finally {
    resetUsageForTests();
    stopProxy();
  }
});


test("a live tool-result resume is not blocked by the quota pre-check", { timeout: 20_000 }, async () => {
  // The first run is allowed (quota healthy); only then does the account become
  // exhausted. The resume must continue the parked bridge, not be rejected.
  setUsageSnapshot(decodeCurrentPeriodUsage(usagePayload({ apiPercent: 99.9 })));
  runRequests = 0;
  onStream = (stream, path) => {
    if (path === "/aiserver.v1.DashboardService/GetCurrentPeriodUsage") {
      stream.respond({ ":status": 200, "content-type": "application/proto" });
      stream.end(Buffer.from(usagePayload()));
      return;
    }
    runRequests += 1;
    stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
    let resumed = false;
    onClientMessage(stream, (message) => {
      if (resumed) return;
      if (message.message.case !== "execClientMessage") return;
      if (message.message.value.message.case !== "mcpResult") return;
      resumed = true;
      stream.write(textFrame("resumed"));
      stream.write(cleanEndStreamFrame());
      stream.end();
    });
    stream.write(mcpExecFrame(1, "call-1"));
  };

  const port = await startProxy(async () => TOKEN, [], 0, TOKEN);
  try {
    const first = await readAll(await chatRequest(port, body("glm-5p3-high")));
    assert.match(first, /"tool_calls"/);

    setUsageSnapshot(decodeCurrentPeriodUsage(usagePayload()));
    const resume = await readAll(
      await chatRequest(port, {
        model: "glm-5p3-high",
        stream: true,
        messages: [
          { role: "user", content: "hi" },
          {
            role: "assistant",
            content: null,
            tool_calls: [
              { id: "call-1", type: "function", function: { name: "bash", arguments: "{}" } },
            ],
          },
          { role: "tool", tool_call_id: "call-1", content: "ok" },
        ],
      }),
    );
    assert.match(resume, /"content":"resumed"/);
    assert.doesNotMatch(resume, /insufficient_quota/);
    assert.equal(runRequests, 1, "the resume must reuse the parked bridge");
  } finally {
    resetUsageForTests();
    stopProxy();
  }
});

test("an upstream resource_exhausted is upgraded to 400 once usage is known exhausted", { timeout: 20_000 }, async () => {
  setUsageSnapshot(decodeCurrentPeriodUsage(usagePayload({ apiPercent: 99.9 })));
  runRequests = 0;
  onStream = (stream, path) => {
    if (path === "/aiserver.v1.DashboardService/GetCurrentPeriodUsage") {
      stream.respond({ ":status": 200, "content-type": "application/proto" });
      stream.end(Buffer.from(usagePayload()));
      return;
    }
    runRequests += 1;
    stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
    // The account crosses the cap while the run is in flight.
    setUsageSnapshot(decodeCurrentPeriodUsage(usagePayload()));
    stream.write(errorEndStreamFrame("resource_exhausted", "Error"));
    stream.end();
  };

  const port = await startProxy(async () => TOKEN, [], 0, TOKEN);
  try {
    const text = await readAll(await chatRequest(port, body("glm-5p3-high")));
    assert.match(text, /"error":\{"code":400,"type":"insufficient_quota"/);
    assert.match(text, /resets 2026-12-04T01:51:41.000Z/);
    assert.equal(runRequests, 1);
  } finally {
    resetUsageForTests();
    stopProxy();
  }
});
