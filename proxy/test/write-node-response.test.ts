/**
 * writeNodeResponse must flush headers before any body byte arrives, and a
 * client that aborts must run the source stream's cancel() so the upstream
 * bridge is torn down instead of leaking.
 */
import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";
import { writeNodeResponse } from "../src/proxy.ts";

interface Listening {
  server: http.Server;
  port: number;
}

function listen(
  handler: (req: http.IncomingMessage, res: http.ServerResponse) => void,
): Promise<Listening> {
  const server = http.createServer(handler);
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        reject(new Error("server has no port"));
        return;
      }
      resolve({ server, port: address.port });
    });
  });
}

function closeServer(server: http.Server): Promise<void> {
  return new Promise((resolve) => {
    server.closeAllConnections?.();
    server.close(() => resolve());
  });
}

/** A Response whose body stays silent for `quietMs`, then emits one chunk. */
function silentThenBody(quietMs: number, body: string): Response {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      setTimeout(() => {
        try {
          controller.enqueue(new TextEncoder().encode(body));
          controller.close();
        } catch {
          // The client already cancelled.
        }
      }, quietMs);
    },
    cancel() {
      // Nothing to release in this fixture.
    },
  });
  return new Response(stream, {
    headers: { "content-type": "text/event-stream" },
  });
}

test("headers reach the client before the first body chunk", async () => {
  const { server, port } = await listen((_req, res) => {
    void writeNodeResponse(res, silentThenBody(300, "data: [DONE]\n\n"));
  });
  try {
    const startedAt = Date.now();
    const { headersAt, body } = await new Promise<{
      headersAt: number;
      body: string;
    }>((resolve, reject) => {
      // Resolve as soon as the status line and headers are readable; a slow
      // body must not delay this, which is the whole point of flushHeaders().
      const request = http.get(`http://127.0.0.1:${port}/stream`, (response) => {
        const headersAt = Date.now();
        assert.equal(response.statusCode, 200);
        assert.equal(response.headers["content-type"], "text/event-stream");
        // No body byte can have arrived yet: the fixture waits 300ms.
        assert.equal(response.read(), null, "body byte arrived with the headers");
        const chunks: Buffer[] = [];
        response.on("data", (chunk: Buffer) => chunks.push(chunk));
        response.once("error", reject);
        response.once("end", () =>
          resolve({ headersAt, body: Buffer.concat(chunks).toString("utf8") }),
        );
      });
      request.once("error", reject);
      request.setTimeout(5_000, () => reject(new Error("response headers never arrived")));
    });

    const elapsed = headersAt - startedAt;
    assert.ok(elapsed < 200, `headers took ${elapsed}ms; flushHeaders did not run`);
    // Proves the assertion above measured headers, not the deferred body.
    assert.equal(body, "data: [DONE]\n\n");
  } finally {
    await closeServer(server);
  }
});

test("a client abort runs the stream's cancel handler", async () => {
  const { promise: cancelled, resolve: onCancel } = Promise.withResolvers<void>();
  const { server, port } = await listen((_req, res) => {
    const stream = new ReadableStream<Uint8Array>({
      start() {
        // Never enqueue: keep the body open until the client leaves.
      },
      cancel() {
        onCancel();
      },
    });
    void writeNodeResponse(
      res,
      new Response(stream, { headers: { "content-type": "text/event-stream" } }),
    );
  });
  try {
    await new Promise<void>((resolve, reject) => {
      const request = http.get(`http://127.0.0.1:${port}/stream`, (response) => {
        // Headers are here; hang up without reading the body.
        response.on("error", () => {});
        request.destroy();
        resolve();
      });
      request.on("error", reject);
    });

    await Promise.race([
      cancelled,
      new Promise((_resolve, reject) =>
        setTimeout(() => reject(new Error("cancel() never ran after abort")), 2_000),
      ),
    ]);
  } finally {
    await closeServer(server);
  }
});

test("a source error closes the response instead of hanging", async () => {
  const { server, port } = await listen((_req, res) => {
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("data: hi\n\n"));
        setTimeout(() => controller.error(new Error("source failed")), 50);
      },
      cancel() {
        // Nothing to release in this fixture.
      },
    });
    void writeNodeResponse(
      res,
      new Response(stream, { headers: { "content-type": "text/event-stream" } }),
    );
  });

  try {
    await new Promise<void>((resolve, reject) => {
      const request = http.get(`http://127.0.0.1:${port}/stream`, (response) => {
        response.on("data", () => {});
        // A truncated response must terminate the socket, not leave it open.
        response.once("aborted", () => resolve());
        response.once("error", () => resolve());
        response.once("close", () => {
          if (!response.complete) resolve();
        });
        response.once("end", () =>
          reject(new Error("the response ended cleanly despite a source error")),
        );
      });
      request.once("error", reject);
      request.setTimeout(3_000, () => {
        request.destroy();
        reject(new Error("the response hung after a source error"));
      });
    });
  } finally {
    await closeServer(server);
  }
});
