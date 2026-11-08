#!/usr/bin/env node
/**
 * Dumb HTTP/2 bidirectional pipe for Cursor gRPC.
 *
 * Bun's node:http2 is broken. This Node script acts as a transparent
 * HTTP/2 proxy: it opens a single bidirectional stream and ferries
 * raw bytes between the parent process (via stdin/stdout) and Cursor.
 *
 * Protocol (length-prefixed framing over stdin/stdout):
 *   [4 bytes big-endian length][payload]
 *
 * First message on stdin is JSON config:
 *   { "accessToken": "...", "url": "...", "path": "...", "unary": false }
 *
 * When unary=true, the bridge uses application/proto (raw protobuf) instead
 * of application/connect+proto (Connect streaming). The single stdin message
 * is written as the request body and the stream is ended immediately.
 * After config, subsequent stdin messages are raw bytes to write to the H2 stream.
 * H2 response data is written to stdout using the same length-prefixed framing.
 *
 * Diagnostics go to stderr only, one line per event, and never include the
 * access token, request headers, or the request path query string:
 *   [h2-bridge] upstream status 403
 *   [h2-bridge] upstream error: GOAWAY ...
 *   [h2-bridge] upstream reset code 8
 *   [h2-bridge] idle timeout
 */
import fs from "node:fs";
import http2 from "node:http2";
import crypto from "node:crypto";

const CURSOR_CLIENT_VERSION = "cli-2026.11.05-86047df";

/** Write one length-prefixed message to stdout. */
function writeMessage(data) {
  const lenBuf = Buffer.alloc(4);
  lenBuf.writeUInt32BE(data.length, 0);
  process.stdout.write(lenBuf);
  process.stdout.write(data);
}

// --- Diagnostics ---

/**
 * Write a single diagnostic line to stderr.
 *
 * Uses fs.writeSync(2) so the line survives an immediately following
 * process.exit() (stderr can be an async pipe on POSIX).
 */
function logStderr(text) {
  const line = `[h2-bridge] ${String(text).replace(/[\r\n]+/g, " ")}\n`;
  try {
    fs.writeSync(2, line);
  } catch {
    try {
      process.stderr.write(line);
    } catch {
      // Diagnostics are best-effort; never take the bridge down over them.
    }
  }
}

/** Render an error as one short "CODE message" line fragment. */
function describeError(err) {
  const code = err?.code != null ? String(err.code) : "";
  const message = err?.message ? String(err.message) : "";
  const text = [code, message].filter(Boolean).join(" ") || "unknown error";
  return text.slice(0, 300);
}

let errorReported = false;

/** Report the first upstream failure; ignore follow-up duplicates. */
function reportError(err) {
  if (errorReported) return;
  errorReported = true;
  logStderr(`upstream error: ${describeError(err)}`);
}

let resetReported = false;

/**
 * A stream can also die without an "error" event: HTTP/2 RST_STREAM ends the
 * stream normally but leaves a non-zero rstCode. Report that too, so a reset
 * upstream is never indistinguishable from a clean end.
 */
function reportReset(rstCode) {
  if (resetReported || errorReported) return;
  resetReported = true;
  logStderr(`upstream reset code ${rstCode}`);
}

// --- Buffered stdin reader ---

let stdinBuf = Buffer.alloc(0);
let stdinResolve = null;
let stdinEnded = false;

process.stdin.on("data", (chunk) => {
  stdinBuf = Buffer.concat([stdinBuf, chunk]);
  if (stdinResolve) {
    const r = stdinResolve;
    stdinResolve = null;
    r();
  }
});

process.stdin.on("end", () => {
  stdinEnded = true;
  if (stdinResolve) {
    const r = stdinResolve;
    stdinResolve = null;
    r();
  }
});

function waitForData() {
  return new Promise((resolve) => { stdinResolve = resolve; });
}

async function readExact(n) {
  while (stdinBuf.length < n) {
    if (stdinEnded) return null;
    await waitForData();
  }
  const result = stdinBuf.subarray(0, n);
  stdinBuf = stdinBuf.subarray(n);
  return Buffer.from(result);
}

async function readMessage() {
  const lenBuf = await readExact(4);
  if (!lenBuf) return null;
  const len = lenBuf.readUInt32BE(0);
  if (len === 0) return Buffer.alloc(0);
  return readExact(len);
}

// --- Main ---

const configBuf = await readMessage();
if (!configBuf) process.exit(1);

const config = JSON.parse(configBuf.toString("utf8"));
const { accessToken, url, path: rpcPath, unary } = config;

const client = http2.connect(url || "https://api2.cursor.sh");

// Guard against initial connection failure. Reset on any h2 activity
// so long-running agent conversations (with tool call round-trips) survive.
let timeout = setTimeout(killBridge, 30_000);

function resetTimeout() {
  clearTimeout(timeout);
  timeout = setTimeout(killBridge, 120_000);
}

function killBridge() {
  clearTimeout(timeout);
  logStderr("idle timeout");
  client.destroy();
  process.exit(1);
}

client.on("error", (err) => {
  clearTimeout(timeout);
  reportError(err);
  process.exit(1);
});

const headers = {
  ":method": "POST",
  ":path": rpcPath || "/agent.v1.AgentService/Run",
  "content-type": unary ? "application/proto" : "application/connect+proto",
  te: "trailers",
  authorization: `Bearer ${accessToken}`,
  "x-ghost-mode": "true",
  "x-cursor-client-version": CURSOR_CLIENT_VERSION,
  "x-cursor-client-type": "cli",
  "x-request-id": crypto.randomUUID(),
};
if (!unary) {
  headers["connect-protocol-version"] = "1";
}
const h2Stream = client.request(headers);

// Record the upstream HTTP status so a non-200 response is never silent.
// A non-200 cannot be turned into a useful completion, so end the bridge with
// a non-zero code: the parent turns that into an error instead of an empty
// successful answer.
h2Stream.on("response", (responseHeaders) => {
  const raw = responseHeaders[":status"];
  const status = typeof raw === "number" ? raw : Number(raw);
  if (!Number.isFinite(status)) return;
  if (status !== 200) {
    logStderr(`upstream status ${status}`);
    clearTimeout(timeout);
    client.destroy();
    process.exit(1);
  }
});

// Forward H2 response data → stdout (length-prefixed)
h2Stream.on("data", (chunk) => {
  resetTimeout();
  writeMessage(chunk);
});

h2Stream.on("end", () => {
  clearTimeout(timeout);
  // The server finished its response. Half-close our side on a later tick so
  // a reset delivered in the same turn is still seen: closing synchronously
  // here swallowed RST_STREAM and turned a failed turn into a clean exit.
  setImmediate(() => {
    if (h2Stream.closed || h2Stream.destroyed || h2Stream.rstCode !== 0) return;
    try {
      h2Stream.end();
    } catch {
      // Already ended.
    }
  });
});

h2Stream.on("close", () => {
  // rstCode is 0 for a clean end and non-zero when the peer reset the stream.
  // Decide the exit code here, after 'close' has seen rstCode: exiting from
  // 'end' with 0 made a truncated answer look like a completed one.
  const reset = h2Stream.rstCode !== 0;
  if (reset) reportReset(h2Stream.rstCode);
  clearTimeout(timeout);
  client.close();
  // Give stdout time to flush.
  setTimeout(() => process.exit(reset ? 1 : 0), 100);
});

h2Stream.on("error", (err) => {
  clearTimeout(timeout);
  reportError(err);
  client.close();
  process.exit(1);
});

// GOAWAY arrives on the session, not the stream, and without this handler the
// caller would sit behind keepalives until the child idle timeout.
client.on("goaway", (code) => {
  clearTimeout(timeout);
  reportError(new Error(`upstream goaway code ${code}`));
  process.exit(1);
});

// The parent ends stdin for a graceful cancel and sends SIGTERM only when the
// child did not exit in time. Close the HTTP/2 stream explicitly so Cursor
// sees a cancellation rather than a vanished TCP connection.
process.on("SIGTERM", () => {
  clearTimeout(timeout);
  try {
    h2Stream.close(http2.constants.NGHTTP2_CANCEL);
  } catch {
    // Already closed.
  }
  try {
    client.close();
  } catch {
    // Already closed.
  }
  process.exit(0);
});

// Forward stdin → H2 stream (after config message)
if (unary) {
  // Unary mode: read a single body message, write it, and end the stream.
  const body = await readMessage();
  if (body && body.length > 0 && !h2Stream.closed && !h2Stream.destroyed) {
    h2Stream.end(body);
  } else {
    h2Stream.end();
  }
} else {
  // Streaming mode: forward all stdin messages as Connect frames.
  (async () => {
    while (true) {
      const msg = await readMessage();
      if (!msg || msg.length === 0) {
        // EOF or zero-length = done writing
        break;
      }
      if (!h2Stream.closed && !h2Stream.destroyed) {
        resetTimeout();
        h2Stream.write(msg);
      }
    }

    if (!h2Stream.closed && !h2Stream.destroyed) {
      h2Stream.end();
    }
  })();
}
