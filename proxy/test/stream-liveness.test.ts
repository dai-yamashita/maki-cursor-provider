/**
 * End-to-end liveness behavior of the SSE bridge:
 *
 * - headers reach the client while the upstream model is still silent,
 * - SSE keepalive comments flow during that silence without becoming data
 *   frames the OpenAI client would see,
 * - a client abort reaps the upstream HTTP/2 stream and the bridge subprocess,
 * - a silent turn that ends normally still terminates with [DONE]/"stop".
 *
 * CURSOR_API_URL and CURSOR_SSE_KEEPALIVE_MS are read at module load, so the
 * fake upstream has to be listening (and the env set) before proxy.ts is
 * imported. One h2c upstream is shared and each test swaps in its behavior.
 */
import assert from "node:assert/strict";
import { once } from "node:events";
import http from "node:http";
import http2 from "node:http2";
import { mkdtempSync, rmSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { after } from "node:test";
import { create, toBinary } from "@bufbuild/protobuf";
import {
  AgentServerMessageSchema,
  InteractionUpdateSchema,
  TextDeltaUpdateSchema,
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

/** Resolve once the server-side stream is gone (closed, reset or aborted). */
function trackStreamClose(stream: http2.ServerHttp2Stream): Promise<void> {
  return new Promise((resolve) => {
    let settled = false;
    const settle = () => {
      if (settled) return;
      settled = true;
      resolve();
    };
    stream.once("close", settle);
    stream.once("aborted", settle);
    stream.once("error", settle);
  });
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

const conversationDir = mkdtempSync(join(tmpdir(), "maki-cursor-liveness-conv-"));
const logDir = mkdtempSync(join(tmpdir(), "maki-cursor-liveness-log-"));

process.env.CURSOR_API_URL = `http://127.0.0.1:${upstreamPort}`;
process.env.CURSOR_SSE_KEEPALIVE_MS = String(KEEPALIVE_MS);
process.env.MAKI_CURSOR_CONVERSATIONS = conversationDir;
process.env.MAKI_CURSOR_CREDENTIALS = join(logDir, "credentials.json");

const { liveBridgeCountForTests, startProxy, stopProxy } = await import("../src/proxy.ts");

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

/** Respond 200 with the Connect streaming content type, then stay silent. */
function respondSilent(stream: http2.ServerHttp2Stream): void {
  try {
    stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
  } catch {
    // The client already went away.
  }
}

/** One Connect data frame carrying an agent text delta. */
function textFrame(text: string): Buffer {
  const update = create(InteractionUpdateSchema, {
    message: {
      case: "textDelta",
      value: create(TextDeltaUpdateSchema, { text }),
    },
  });
  const message = create(AgentServerMessageSchema, {
    message: { case: "interactionUpdate", value: update },
  });
  const bytes = toBinary(AgentServerMessageSchema, message);
  const frame = Buffer.alloc(5 + bytes.length);
  frame[0] = 0;
  frame.writeUInt32BE(bytes.length, 1);
  frame.set(bytes, 5);
  return frame;
}

function chatRequest(port: number, signal?: AbortSignal, body = streamBody()): Promise<Response> {
  return fetch(`http://127.0.0.1:${port}/v1/chat/completions`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${TOKEN}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(body),
    signal,
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

/** Open a streaming POST with a raw socket so the client can hang up mid-body. */
function openStream(
  port: number,
  body: Record<string, unknown> = streamBody(),
): Promise<{ req: http.ClientRequest; res: http.IncomingMessage }> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname: "127.0.0.1",
        port,
        path: "/v1/chat/completions",
        method: "POST",
        headers: {
          authorization: `Bearer ${TOKEN}`,
          "content-type": "application/json",
        },
      },
      (res) => resolve({ req, res }),
    );
    req.once("error", reject);
    req.end(JSON.stringify(body));
  });
}

async function pollUntil(check: () => boolean, ms: number, message: string): Promise<void> {
  const deadline = Date.now() + ms;
  while (!check()) {
    if (Date.now() > deadline) throw new Error(message);
    await delay(25);
  }
}

// --- Tests -----------------------------------------------------------------------

test("headers reach the client while the upstream is still silent", { timeout: 15_000 }, async () => {
  upstreamSetup((stream) => {
    respondSilent(stream);
    // No body bytes for well past the header deadline: an old proxy that
    // waited for the first frame would blow the assertion below.
    setTimeout(() => {
      if (!stream.closed && !stream.destroyed) stream.end();
    }, 600);
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const startedAt = Date.now();
    const response = await withTimeout(chatRequest(port), 5_000, "no response headers");
    const elapsed = Date.now() - startedAt;

    assert.equal(response.status, 200);
    assert.match(response.headers.get("content-type") ?? "", /text\/event-stream/);
    assert.ok(elapsed < 300, `headers took ${elapsed}ms while the upstream was silent`);

    await withTimeout(readAll(response), 5_000, "the silent turn never finished");
  } finally {
    stopProxy();
  }
});

test("keepalive comments flow during silence and never become data frames", { timeout: 15_000 }, async () => {
  upstreamSetup((stream) => {
    respondSilent(stream);
    setTimeout(() => {
      if (!stream.closed && !stream.destroyed) stream.end();
    }, 600);
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(chatRequest(port), 5_000, "no response headers");
    const text = await withTimeout(readAll(response), 10_000, "the stream never finished");

    const keepalives = text.match(/: keepalive\n\n/g) ?? [];
    assert.ok(
      keepalives.length >= 2,
      `expected at least 2 keepalives in ~600ms of silence, saw ${keepalives.length}`,
    );
    assert.match(text, /data: \[DONE\]/);

    // A comment is not an event: nothing the client parses as `data:` may
    // mention the keepalive marker.
    const dataEvents = text
      .split("\n\n")
      .map((event) => event.trim())
      .filter((event) => event.startsWith("data:"));
    assert.ok(dataEvents.length > 0, "no data events were emitted");
    for (const event of dataEvents) {
      assert.doesNotMatch(event, /keepalive/);
    }
  } finally {
    stopProxy();
  }
});

test("a client abort reaps the upstream stream and the bridge subprocess", { timeout: 30_000 }, async () => {
  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const { promise: streamSeen, resolve: onSeen } = Promise.withResolvers<void>();
      let upstreamClosed: Promise<void> = Promise.resolve();

      upstreamSetup((stream) => {
        respondSilent(stream);
        upstreamClosed = trackStreamClose(stream);
        // Never ends on its own: only the abort can close it.
        onSeen();
      });

      const { req, res } = await withTimeout(openStream(port), 5_000, "no response headers");
      res.on("error", () => {});
      const { promise: firstChunk, resolve: onChunk } = Promise.withResolvers<void>();
      res.on("data", () => onChunk());

      await withTimeout(streamSeen, 5_000, "the upstream stream never opened");
      await withTimeout(firstChunk, 5_000, "no SSE chunk reached the client");

      // Hang up mid-body, exactly like a caller that hit Ctrl-C.
      req.destroy();

      await withTimeout(upstreamClosed, 1_500, `upstream stream ${attempt} outlived the abort`);
    }

    await pollUntil(
      () => liveBridgeCountForTests() === 0,
      2_000,
      "bridge subprocesses outlived their aborted requests",
    );
    assert.equal(liveBridgeCountForTests(), 0);
  } finally {
    stopProxy();
  }
});

test("a silent turn that ends normally completes with stop and [DONE]", { timeout: 15_000 }, async () => {
  upstreamSetup((stream) => {
    respondSilent(stream);
    // No agent frames at all: just an orderly end of stream.
    setTimeout(() => {
      if (!stream.closed && !stream.destroyed) stream.end();
    }, 300);
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(chatRequest(port), 5_000, "no response headers");
    const text = await withTimeout(readAll(response), 10_000, "the silent turn hung");

    assert.match(text, /"finish_reason":"stop"/);
    assert.match(text, /data: \[DONE\]/);
  } finally {
    stopProxy();
  }
});

test("a non-200 upstream becomes an error, not an empty success", { timeout: 15_000 }, async () => {
  upstreamSetup((stream) => {
    try {
      stream.respond({ ":status": 403, "content-type": "application/connect+proto" });
    } catch {
      // The client already went away.
    }
    stream.end();
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(chatRequest(port), 5_000, "no response headers");
    const text = await withTimeout(readAll(response), 10_000, "the failed turn hung");

    assert.match(text, /\[Error: cursor bridge exited \(1\)\]/);
    assert.match(text, /data: \[DONE\]/);
  } finally {
    stopProxy();
  }
});

test("non-streaming requests get headers before the model finishes", { timeout: 15_000 }, async () => {
  upstreamSetup((stream) => {
    respondSilent(stream);
    setTimeout(() => {
      if (!stream.closed && !stream.destroyed) stream.end();
    }, 600);
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const startedAt = Date.now();
    const response = await withTimeout(
      chatRequest(port, undefined, { ...streamBody(), stream: false }),
      5_000,
      "non-streaming request got no headers",
    );
    const elapsed = Date.now() - startedAt;
    assert.ok(elapsed < 300, `non-streaming headers took ${elapsed}ms`);

    const payload = JSON.parse(await withTimeout(response.text(), 10_000, "no JSON body"));
    assert.equal(payload.object, "chat.completion");
    assert.equal(payload.choices[0].message.content, "");
  } finally {
    stopProxy();
  }
});

test("a non-streaming abort reaps the upstream bridge", { timeout: 15_000 }, async () => {
  const { promise: streamSeen, resolve: onSeen } = Promise.withResolvers<void>();
  let upstreamClosed: Promise<void> = Promise.resolve();
  upstreamSetup((stream) => {
    respondSilent(stream);
    upstreamClosed = trackStreamClose(stream);
    onSeen();
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const { req } = await withTimeout(
      openStream(port, { ...streamBody(), stream: false }),
      5_000,
      "no response headers",
    );
    await withTimeout(streamSeen, 5_000, "the upstream stream never opened");
    req.destroy();

    await withTimeout(upstreamClosed, 1_500, "the upstream outlived the non-streaming abort");
    await pollUntil(
      () => liveBridgeCountForTests() === 0,
      2_000,
      "the non-streaming bridge outlived its aborted request",
    );
  } finally {
    stopProxy();
  }
});

test("a non-streaming non-200 upstream reports an error, not an empty answer", { timeout: 15_000 }, async () => {
  upstreamSetup((stream) => {
    try {
      stream.respond({ ":status": 403, "content-type": "application/connect+proto" });
    } catch {
      // The client already went away.
    }
    stream.end();
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(
      chatRequest(port, undefined, { ...streamBody(), stream: false }),
      5_000,
      "no response headers",
    );
    const payload = JSON.parse(await withTimeout(response.text(), 10_000, "no JSON body"));
    assert.equal(payload.choices[0].finish_reason, "error");
    assert.equal(payload.error.type, "upstream_error");
    assert.match(payload.choices[0].message.content, /cursor bridge exited \(1\)/);
  } finally {
    stopProxy();
  }
});

test("a text delta reaches the streaming client", { timeout: 15_000 }, async () => {
  upstreamSetup((stream) => {
    respondSilent(stream);
    stream.write(textFrame("hello"));
    setTimeout(() => {
      if (!stream.closed && !stream.destroyed) stream.end();
    }, 50);
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(chatRequest(port), 5_000, "no response headers");
    const text = await withTimeout(readAll(response), 10_000, "the text stream hung");
    assert.match(text, /"content":"hello"/);
    assert.match(text, /data: \[DONE\]/);
  } finally {
    stopProxy();
  }
});

test("non-streaming collects the text delta", { timeout: 15_000 }, async () => {
  upstreamSetup((stream) => {
    respondSilent(stream);
    stream.write(textFrame("hello"));
    setTimeout(() => {
      if (!stream.closed && !stream.destroyed) stream.end();
    }, 50);
  });

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(
      chatRequest(port, undefined, { ...streamBody(), stream: false }),
      5_000,
      "no response headers",
    );
    const payload = JSON.parse(await withTimeout(response.text(), 10_000, "no JSON body"));
    assert.equal(payload.choices[0].message.content, "hello");
    assert.equal(payload.choices[0].finish_reason, "stop");
  } finally {
    stopProxy();
  }
});

/** Install the upstream behavior for the next request. */
function upstreamSetup(behavior: (stream: http2.ServerHttp2Stream) => void): void {
  onStream = behavior;
}
