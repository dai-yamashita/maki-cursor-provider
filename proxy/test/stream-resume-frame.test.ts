/**
 * A Connect frame split across a tool-result resume must be reassembled.
 *
 * The proxy creates a Connect frame decoder per SSE response. A turn that parks
 * for a tool result ends that SSE, and the resume request builds a new decoder.
 * The old decoder is the only holder of a partially received frame, so its
 * buffered bytes are lost and the next DATA payload starts mid-frame. The bug
 * surfaces as `[Error: cursor upstream sent an unframed response: invalid
 * Connect frame flags 0xNN: ...]` and the model's text never reaches the client.
 *
 * CURSOR_API_URL is read at module load, so the fake h2c upstream has to be
 * listening (and the env set) before proxy.ts is imported.
 */
import assert from "node:assert/strict";
import { once } from "node:events";
import http2 from "node:http2";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { after } from "node:test";
import { create, toBinary } from "@bufbuild/protobuf";
import {
  AgentServerMessageSchema,
  ExecServerMessageSchema,
  InteractionUpdateSchema,
  McpArgsSchema,
  TextDeltaUpdateSchema,
  TurnEndedUpdateSchema,
} from "../src/proto/agent_pb.ts";

const TOKEN = "test-token";

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Reject if `promise` has not settled within `ms`. */
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
  // Always consume and drain, so a chatty client can never block on flow
  // control, and an abrupt close is never an unhandled "error".
  stream.on("error", () => {});
  stream.on("data", () => {});
  onStream(stream);
});
upstream.listen(0, "127.0.0.1");
await once(upstream, "listening");
const upstreamPort = (upstream.address() as AddressInfo).port;

const conversationDir = mkdtempSync(join(tmpdir(), "maki-cursor-resume-frame-conv-"));
const logDir = mkdtempSync(join(tmpdir(), "maki-cursor-resume-frame-log-"));
const logPath = join(logDir, "proxy.log");

process.env.CURSOR_API_URL = `http://127.0.0.1:${upstreamPort}`;
process.env.CURSOR_SSE_KEEPALIVE_MS = "100";
process.env.CURSOR_EXEC_HEARTBEAT_MS = "50";
process.env.MAKI_CURSOR_CONVERSATIONS = conversationDir;
process.env.MAKI_CURSOR_LOG = logPath;
delete process.env.CURSOR_PROXY_DEBUG;

const { startProxy, stopProxy } = await import("../src/proxy.ts");

after(async () => {
  const closed = once(upstream, "close").catch(() => undefined);
  for (const session of sessions) session.destroy();
  upstream.close();
  await Promise.race([closed, delay(2_000)]);
  rmSync(conversationDir, { recursive: true, force: true });
  rmSync(logDir, { recursive: true, force: true });
});

// --- Helpers ---------------------------------------------------------------------

/** Install the upstream behavior for the next request. */
function upstreamSetup(behavior: (stream: http2.ServerHttp2Stream) => void): void {
  onStream = behavior;
}

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

/** One Connect data frame carrying an agent text delta. */
function textFrame(text: string): Buffer {
  const update = create(InteractionUpdateSchema, {
    message: { case: "textDelta", value: create(TextDeltaUpdateSchema, { text }) },
  });
  return serverFrame(
    create(AgentServerMessageSchema, { message: { case: "interactionUpdate", value: update } }),
  );
}

/** One Connect data frame carrying an interaction update with no payload. */
function turnEndedFrame(): Buffer {
  const update = create(InteractionUpdateSchema, {
    message: { case: "turnEnded", value: create(TurnEndedUpdateSchema, {}) },
  });
  return serverFrame(
    create(AgentServerMessageSchema, { message: { case: "interactionUpdate", value: update } }),
  );
}

/** Respond 200 with the Connect streaming content type, then stay silent. */
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

// --- Tests -----------------------------------------------------------------------

test(
  "a Connect frame split across a tool-result resume is reassembled",
  { timeout: 30_000 },
  async () => {
    let upstreamStream: http2.ServerHttp2Stream | undefined;

    upstreamSetup((stream) => {
      upstreamStream = stream;
      respondSilent(stream);
      stream.write(mcpExecFrame(1, "call_1", "bash"));
    });

    const session = "session-resume-split";
    const tools = [
      {
        type: "function",
        function: {
          name: "bash",
          description: "",
          parameters: { type: "object", properties: {} },
        },
      },
    ];

    const port = await startProxy(async () => "unused", [], 0, TOKEN);
    try {
      // 1. The first request parks the bridge on a bash tool call.
      const first = await withTimeout(
        chatRequest(port, { ...streamBody(), tools }, session),
        5_000,
        "the first request got no headers",
      );
      const firstText = await withTimeout(readAll(first), 10_000, "the tool-call turn hung");

      const calls = [...firstText.matchAll(/data: (\{.*\})/g)]
        .map((match) => JSON.parse(match[1]!))
        .flatMap((chunk) => chunk.choices?.[0]?.delta?.tool_calls ?? []);
      const call = calls.find((item) => item.function?.name === "bash");
      assert.ok(call, "the exec was not exposed as a bash tool call");
      assert.equal(call.id, "call_1", "the tool call id must be the upstream toolCallId");

      assert.ok(upstreamStream, "the upstream stream was never opened");
      const h2 = upstreamStream;

      // 4. Split the frame at a byte the buggy decoder would misread as
      // Connect flags (any byte with bits outside 0x00-0x03).
      const whole = textFrame("hello world");
      let split = 6;
      while (split < whole.length && (whole[split]! & ~0x03) === 0) split += 1;
      assert.ok(
        split < whole.length,
        `no usable split point in a ${whole.length}-byte frame`,
      );

      // 5. The old decoder buffers this prefix; the SSE response then ends.
      h2.write(whole.subarray(0, split));
      await delay(100);

      // 6. Resume the parked bridge with the tool result.
      const resumeBody = {
        model: "test-model",
        stream: true,
        messages: [
          { role: "user", content: "hi" },
          {
            role: "assistant",
            content: null,
            tool_calls: [
              {
                id: "call_1",
                type: "function",
                function: { name: "bash", arguments: "{}" },
              },
            ],
          },
          { role: "tool", tool_call_id: "call_1", content: "ok" },
        ],
      };
      const resumed = await withTimeout(
        chatRequest(port, resumeBody, session),
        5_000,
        "the resume request got no headers",
      );
      assert.equal(resumed.status, 200);

      // The replacement decoder is attached in the stream's start(); the
      // response headers have already been flushed by then.
      await delay(50);

      // 8. The frame remainder is the first thing the new decoder sees.
      h2.write(whole.subarray(split));
      h2.write(turnEndedFrame());

      const resumedText = await withTimeout(readAll(resumed), 10_000, "the resumed turn hung");

      assert.match(resumedText, /"content":"hello world"/);
      assert.match(resumedText, /data: \[DONE\]/);
      assert.doesNotMatch(resumedText, /unframed/);
      assert.doesNotMatch(resumedText, /invalid Connect frame/);
      assert.doesNotMatch(logText(), /cursor protocol error/);
    } finally {
      stopProxy();
    }
  },
);
