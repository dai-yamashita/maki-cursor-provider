/**
 * End-to-end hardening of the Connect frame layer of the SSE bridge:
 *
 * - a gzip-compressed data frame (flags 0x01) must be inflated, not dropped,
 * - a frame split across one-byte writes must be reassembled,
 * - a compressed end-stream frame (flags 0x03) must be inflated before its
 *   JSON error payload is parsed.
 *
 * CURSOR_API_URL is read at module load, so the fake upstream has to be
 * listening (and the env set) before proxy.ts is imported. One h2c upstream
 * is shared and each test swaps in its behavior.
 */
import assert from "node:assert/strict";
import { once } from "node:events";
import http2 from "node:http2";
import { mkdtempSync, rmSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { after } from "node:test";
import { gzipSync } from "node:zlib";
import { create, toBinary } from "@bufbuild/protobuf";
import {
  AgentServerMessageSchema,
  InteractionUpdateSchema,
  TextDeltaUpdateSchema,
  TurnEndedUpdateSchema,
} from "../src/proto/agent_pb.ts";

const TOKEN = "test-token";
const KEEPALIVE_MS = 100;

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
  // Always consume and drain, so an abort is never an unhandled "error".
  stream.on("error", () => {});
  stream.on("data", () => {});
  onStream(stream);
});
upstream.listen(0, "127.0.0.1");
await once(upstream, "listening");
const upstreamPort = (upstream.address() as AddressInfo).port;

const conversationDir = mkdtempSync(join(tmpdir(), "maki-cursor-frames-conv-"));
const logDir = mkdtempSync(join(tmpdir(), "maki-cursor-frames-log-"));

process.env.CURSOR_API_URL = `http://127.0.0.1:${upstreamPort}`;
process.env.CURSOR_SSE_KEEPALIVE_MS = String(KEEPALIVE_MS);
process.env.MAKI_CURSOR_CONVERSATIONS = conversationDir;
process.env.MAKI_CURSOR_CREDENTIALS = join(logDir, "credentials.json");

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

function streamBody() {
  return {
    model: "test-model",
    messages: [{ role: "user", content: "hi" }],
    stream: true,
  };
}

/** Respond 200 with the Connect streaming content type. */
function respondSilent(stream: http2.ServerHttp2Stream): void {
  try {
    stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
  } catch {
    // The client already went away.
  }
}

/** The inner AgentServerMessage carrying an agent text delta. */
function messageForText(text: string) {
  const update = create(InteractionUpdateSchema, {
    message: {
      case: "textDelta",
      value: create(TextDeltaUpdateSchema, { text }),
    },
  });
  return create(AgentServerMessageSchema, {
    message: { case: "interactionUpdate", value: update },
  });
}

/** One Connect data frame carrying an agent text delta. */
function textFrame(text: string): Buffer {
  return connectFrame(toBinary(AgentServerMessageSchema, messageForText(text)));
}

/** One Connect data frame carrying an interaction update with no payload. */
function turnEndedFrame(): Buffer {
  const update = create(InteractionUpdateSchema, {
    message: { case: "turnEnded", value: create(TurnEndedUpdateSchema, {}) },
  });
  const message = create(AgentServerMessageSchema, {
    message: { case: "interactionUpdate", value: update },
  });
  return connectFrame(toBinary(AgentServerMessageSchema, message));
}

/** Connect frame: [1-byte flags][4-byte BE length][payload]. */
function connectFrame(bytes: Uint8Array, flags = 0): Buffer {
  const frame = Buffer.alloc(5 + bytes.length);
  frame[0] = flags;
  frame.writeUInt32BE(bytes.length, 1);
  frame.set(bytes, 5);
  return frame;
}

function chatRequest(port: number, body = streamBody()): Promise<Response> {
  return fetch(`http://127.0.0.1:${port}/v1/chat/completions`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${TOKEN}`,
      "content-type": "application/json",
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

/** Install the upstream behavior for the next request. */
function upstreamSetup(behavior: (stream: http2.ServerHttp2Stream) => void): void {
  onStream = behavior;
}

// --- Tests -----------------------------------------------------------------------

test(
  "a gzip-compressed Connect frame is decoded instead of silently dropped",
  { timeout: 30_000 },
  async () => {
    // flags 0x01 = compressed payload; the bytes are a gzipped protobuf.
    const raw = connectFrame(
      gzipSync(toBinary(AgentServerMessageSchema, messageForText("zipped"))),
      0x01,
    );

    upstreamSetup((stream) => {
      respondSilent(stream);
      stream.write(raw);
      stream.write(turnEndedFrame());
      setTimeout(() => {
        if (!stream.closed && !stream.destroyed) stream.end();
      }, 50);
    });

    const port = await startProxy(async () => "unused", [], 0, TOKEN);
    try {
      const response = await withTimeout(chatRequest(port), 5_000, "no response headers");
      const text = await withTimeout(readAll(response), 10_000, "the compressed turn hung");

      assert.match(text, /"content":"zipped"/);
      assert.match(text, /data: \[DONE\]/);
    } finally {
      stopProxy();
    }
  },
);

test(
  "a frame delivered one byte at a time is reassembled",
  { timeout: 30_000 },
  async () => {
    upstreamSetup((stream) => {
      respondSilent(stream);
      void (async () => {
        const frame = textFrame("one-byte frames");
        for (const byte of frame) {
          if (stream.closed || stream.destroyed) return;
          stream.write(Buffer.from([byte]));
          // Yield so every byte lands in its own HTTP/2 DATA frame.
          await delay(1);
        }
        if (stream.closed || stream.destroyed) return;
        stream.write(turnEndedFrame());
        await delay(2);
        if (!stream.closed && !stream.destroyed) stream.end();
      })();
    });

    const port = await startProxy(async () => "unused", [], 0, TOKEN);
    try {
      const response = await withTimeout(chatRequest(port), 5_000, "no response headers");
      const text = await withTimeout(readAll(response), 10_000, "the byte-split turn hung");

      assert.match(text, /"content":"one-byte frames"/);
      assert.match(text, /data: \[DONE\]/);
    } finally {
      stopProxy();
    }
  },
);

test(
  "a compressed end-stream frame is parsed as an end-stream error",
  { timeout: 30_000 },
  async () => {
    // flags 0x03 = compressed + end-stream; the bytes are gzipped JSON.
    const raw = connectFrame(
      gzipSync(
        Buffer.from(
          JSON.stringify({ error: { code: "resource_exhausted", message: "compressed end" } }),
        ),
      ),
      0x03,
    );

    upstreamSetup((stream) => {
      respondSilent(stream);
      stream.write(raw);
      setTimeout(() => {
        if (!stream.closed && !stream.destroyed) stream.end();
      }, 50);
    });

    const port = await startProxy(async () => "unused", [], 0, TOKEN);
    try {
      const response = await withTimeout(chatRequest(port), 5_000, "no response headers");
      const text = await withTimeout(readAll(response), 10_000, "the compressed end-stream hung");

      assert.match(
        text,
        /"error":\{"code":503,"type":"service_unavailable_error","message":"Connect error resource_exhausted: compressed end"\}/,
      );
      assert.doesNotMatch(text, /"finish_reason"/);
      assert.match(text, /data: \[DONE\]/);
    } finally {
      stopProxy();
    }
  },
);
