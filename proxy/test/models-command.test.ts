import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer, type Server } from "node:http";
import test from "node:test";

const node = process.execPath;
const cli = new URL("../src/cli.ts", import.meta.url).pathname;

function listen(): Promise<{ port: number; server: Server; hits: () => number }> {
  let hits = 0;
  const server = createServer((req, res) => {
    if (req.method !== "GET" || req.url !== "/v1/models") {
      res.writeHead(404);
      res.end();
      return;
    }
    const header = req.headers.authorization;
    if (header !== "Bearer test-token") {
      res.writeHead(401);
      res.end();
      return;
    }
    hits += 1;
    const data = hits < 3 ? [] : [{ id: "glm-5.2-high" }, { id: "" }, { id: 1 }];
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ object: "list", data }));
  });
  return new Promise((resolve, reject) => {
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        reject(new Error("models fixture has no port"));
        return;
      }
      resolve({ port: address.port, server, hits: () => hits });
    });
  });
}

test("models waits until discovery publishes an id", async () => {
  const fixture = await listen();
  try {
    const child = spawn(
      node,
      ["--experimental-strip-types", "--disable-warning=ExperimentalWarning", cli, "models"],
      {
        env: {
          ...process.env,
          MAKI_CURSOR_PORT: String(fixture.port),
          MAKI_CURSOR_TOKEN: "test-token",
        },
      },
    );
    let stdout = "";
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdout += chunk;
    });
    const code = await new Promise<number>((resolve, reject) => {
      child.on("error", reject);
      child.on("exit", (status) => resolve(status ?? 1));
    });
    assert.equal(code, 0);
    assert.ok(fixture.hits() >= 3);
    assert.deepEqual(JSON.parse(stdout), {
      object: "list",
      data: [{ id: "glm-5.2-high" }],
    });
  } finally {
    fixture.server.close();
  }
});
