import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import http2 from "node:http2";
import net from "node:net";
import type { AddressInfo } from "node:net";
import test from "node:test";
import { fileURLToPath } from "node:url";

const BRIDGE_PATH = fileURLToPath(new URL("../src/h2-bridge.mjs", import.meta.url));
const ACCESS_TOKEN = "test-token";
const RPC_PATH = "/agent.v1.AgentService/Run";

/** Frame a payload with the 4-byte big-endian length prefix the bridge expects. */
function frame(payload: Buffer): Buffer {
  const lenBuf = Buffer.alloc(4);
  lenBuf.writeUInt32BE(payload.length, 0);
  return Buffer.concat([lenBuf, payload]);
}

type BridgeResult = {
  code: number | null;
  signal: NodeJS.Signals | null;
  stderr: string;
  stdout: Buffer;
};

/**
 * Spawn the bridge with a config frame. By default stdin is closed immediately;
 * pass `closeStdin: false` to keep the request stream open like a tool-call
 * bridge does while it waits for the caller.
 */
function runBridge(
  port: number,
  timeoutMs = 5_000,
  closeStdin = true,
): Promise<BridgeResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [BRIDGE_PATH], {
      stdio: ["pipe", "pipe", "pipe"],
    });

    const stderrChunks: string[] = [];
    const stdoutChunks: Buffer[] = [];

    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk: string) => stderrChunks.push(chunk));
    child.stdout.on("data", (chunk: Buffer) => stdoutChunks.push(chunk));

    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      child.kill("SIGKILL");
    }, timeoutMs);

    child.on("error", (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(err);
    });

    child.on("close", (code, signal) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({
        code,
        signal,
        stderr: stderrChunks.join(""),
        stdout: Buffer.concat(stdoutChunks),
      });
    });

    const config = Buffer.from(
      JSON.stringify({
        accessToken: ACCESS_TOKEN,
        url: `http://127.0.0.1:${port}`,
        path: RPC_PATH,
        unary: false,
      }),
      "utf8",
    );
    child.stdin.write(frame(config));
    if (closeStdin) child.stdin.end();
  });
}

/** Start an ephemeral-port h2c upstream; track sessions so teardown cannot hang. */
async function startUpstream(
  onStream: (stream: http2.ServerHttp2Stream) => void,
): Promise<{ server: http2.Http2Server; port: number }> {
  const server = http2.createServer();
  const sessions = new Set<http2.ServerHttp2Session>();
  server.on("session", (session) => {
    sessions.add(session);
    session.on("close", () => sessions.delete(session));
  });
  server.on("stream", (stream) => {
    // A reset after the test finished must not become an uncaught error; the
    // assertions only care about the child's stderr and exit code.
    stream.on("error", () => {});
    stream.on("data", () => {});
    onStream(stream);
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");

  const { port } = server.address() as AddressInfo;
  server.on("close", () => {
    for (const session of sessions) session.destroy();
    sessions.clear();
  });
  return { server, port };
}

/** Close the upstream and wait (briefly) for it to go away. */
async function stopUpstream(server: http2.Http2Server): Promise<void> {
  const closed = once(server, "close").catch(() => undefined);
  server.close();
  await Promise.race([closed, new Promise((resolve) => setTimeout(resolve, 2_000))]);
}

/** Assert the child exited on its own, with a failure code, and never leaked the token. */
function assertFailedExit(result: BridgeResult) {
  assert.equal(result.signal, null, "bridge should exit on its own, not be killed");
  assert.notEqual(result.code, null, "bridge should exit with a code");
  assert.notEqual(result.code, 0, "a failed upstream must not exit cleanly");
  assert.doesNotMatch(result.stderr, /test-token/, "stderr must never contain the access token");
  assert.doesNotMatch(result.stderr, /authorization/i, "stderr must not contain request headers");
}

test("a 403 upstream status is reported on stderr and the token never leaks", { timeout: 15_000 }, async () => {
  // Respond without consuming the request body, like a real gateway rejection.
  const { server, port } = await startUpstream((stream) => {
    stream.respond({ ":status": 403, "content-type": "text/plain" });
    stream.end("forbidden");
  });

  try {
    const result = await runBridge(port);
    assertFailedExit(result);
    assert.match(result.stderr, /\[h2-bridge\] upstream status 403/);
  } finally {
    await stopUpstream(server);
  }
});

test("a GOAWAY after 200 is reported as an upstream error", { timeout: 15_000 }, async () => {
  // Send GOAWAY without destroying the session: the session-level handler must
  // be what notices, or the caller waits behind keepalives until idle timeout.
  const { server, port } = await startUpstream((stream) => {
    stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
    stream.session?.goaway(http2.constants.NGHTTP2_INTERNAL_ERROR);
  });

  try {
    const result = await runBridge(port);
    assertFailedExit(result);
    assert.match(result.stderr, /\[h2-bridge\] upstream error: .*goaway/i);
  } finally {
    await stopUpstream(server);
  }
});

test("an RST_STREAM is reported even though the stream ends without an error", { timeout: 15_000 }, async () => {
  const { server, port } = await startUpstream((stream) => {
    stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
    stream.close(http2.constants.NGHTTP2_CANCEL);
  });

  try {
    // Keep stdin open: a Node http2 server drops close(CANCEL) once the peer
    // has already half-closed, so this must not race the request END_STREAM.
    const result = await runBridge(port, 5_000, false);
    assertFailedExit(result);
    assert.match(result.stderr, /\[h2-bridge\] upstream reset code 8/);
  } finally {
    await stopUpstream(server);
  }
});

function rstStreamFrame(streamId: number, code: number): Buffer {
  const f = Buffer.alloc(13);
  f.writeUIntBE(4, 0, 3);
  f[3] = 0x03;
  f[4] = 0;
  f.writeUInt32BE(streamId, 5);
  f.writeUInt32BE(code, 9);
  return f;
}

function hasHeadersFrame(buf: Buffer): boolean {
  let off = 0;
  while (off + 9 <= buf.length) {
    const len = buf.readUIntBE(off, 3);
    if (off + 9 + len > buf.length) return false;
    if (buf[off + 3] === 0x01) return true;
    off += 9 + len;
  }
  return false;
}

test("a reset after the request half-closes is still a failure", { timeout: 15_000 }, async () => {
  // A Node http2 server drops close(CANCEL) once the peer has half-closed, so
  // the RST_STREAM is injected on the wire to reproduce what a real upstream
  // sends. This is the ordering CI hits: the bridge ends its stdin first.
  const { server: upstream, port: upstreamPort } = await startUpstream((stream) => {
    stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
  });

  const front = net.createServer((client) => {
    const upstreamConn = net.connect(upstreamPort, "127.0.0.1");
    client.on("error", () => {});
    upstreamConn.on("error", () => {});
    client.on("data", (d) => upstreamConn.write(d));
    let seen = Buffer.alloc(0);
    let injected = false;
    upstreamConn.on("data", (d) => {
      client.write(d);
      seen = Buffer.concat([seen, d]);
      if (!injected && hasHeadersFrame(seen)) {
        injected = true;
        setTimeout(() => {
          if (!client.destroyed) client.write(rstStreamFrame(1, http2.constants.NGHTTP2_CANCEL));
        }, 10);
      }
    });
    client.on("end", () => upstreamConn.end());
    upstreamConn.on("end", () => client.end());
  });
  front.listen(0, "127.0.0.1");
  await once(front, "listening");
  const port = (front.address() as AddressInfo).port;

  try {
    const result = await runBridge(port);
    assertFailedExit(result);
    assert.match(result.stderr, /\[h2-bridge\] upstream reset code 8/);
  } finally {
    await new Promise<void>((resolve) => front.close(() => resolve()));
    await stopUpstream(upstream);
  }
});

test("a mid-stream reset exits non-zero even with stdin still open", { timeout: 15_000 }, async () => {
  // A paused tool-call bridge keeps stdin open. If the upstream resets after
  // partial output, that must still be a failure, not a clean empty answer.
  const { server, port } = await startUpstream((stream) => {
    stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
    stream.write(Buffer.alloc(1));
    setTimeout(() => {
      if (!stream.closed && !stream.destroyed) {
        stream.close(http2.constants.NGHTTP2_INTERNAL_ERROR);
      }
    }, 50);
  });

  try {
    const result = await runBridge(port, 5_000, false);
    assertFailedExit(result);
    // Node may surface this as an 'error' event or as a non-zero rstCode;
    // either way the failure must reach stderr and produce a non-zero exit.
    assert.match(
      result.stderr,
      /upstream (reset code 2|error: .*NGHTTP2_INTERNAL_ERROR)/,
    );
  } finally {
    await stopUpstream(server);
  }
});

test("a failed connection is reported as an upstream error", { timeout: 15_000 }, async () => {
  // Listen on an ephemeral port, then close it so the connect attempt is refused.
  const { server, port } = await startUpstream(() => {});
  await stopUpstream(server);

  const result = await runBridge(port);
  assertFailedExit(result);
  assert.match(result.stderr, /\[h2-bridge\] upstream error: .*ECONNREFUSED/);
});

test("a 429 body is reported so the capacity reason reaches the caller", { timeout: 15_000 }, async () => {
  // Cursor answers a refused prefill with a non-200 JSON body. Reporting only
  // "upstream status 429" hid the actionable part ("prefill_cap_refused").
  const { server, port } = await startUpstream((stream) => {
    stream.respond({ ":status": 429, "content-type": "application/json" });
    stream.end(
      '{"error":{"type":"server_error","message":"Upstream request failed: [prefill_cap_refused] the Audit cell is at capacity"}}',
    );
  });

  try {
    const result = await runBridge(port);
    assertFailedExit(result);
    assert.match(result.stderr, /\[h2-bridge\] upstream status 429/);
    assert.match(result.stderr, /prefill_cap_refused/);
  } finally {
    await stopUpstream(server);
  }
});

test("a 200 response with a non-Connect body is reported, not parsed as frames", { timeout: 15_000 }, async () => {
  // A 200 is not enough: an edge error returned as JSON cannot be a Connect
  // frame. The bridge must forward the reason instead of letting the parent
  // wait for a frame that will never arrive.
  const { server, port } = await startUpstream((stream) => {
    stream.respond({ ":status": 200, "content-type": "application/json" });
    stream.end('{"error":{"message":"prefill_cap_refused: at capacity"}}');
  });

  try {
    const result = await runBridge(port);
    assertFailedExit(result);
    assert.match(result.stderr, /\[h2-bridge\] upstream content-type application\/json/);
    assert.match(result.stderr, /prefill_cap_refused/);
  } finally {
    await stopUpstream(server);
  }
});
