import assert from "node:assert/strict";
import { type ChildProcess, spawn } from "node:child_process";
import { createServer, type Server } from "node:net";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const node = process.execPath;
const cli = new URL("../src/cli.ts", import.meta.url).pathname;

function killProcessGroup(child: ChildProcess | undefined): void {
  if (!child?.pid) return;
  try {
    process.kill(-child.pid, "SIGKILL");
  } catch {
    child.kill("SIGKILL");
  }
}

function spawnServe(env: NodeJS.ProcessEnv): ChildProcess {
  return spawn(
    node,
    ["--experimental-strip-types", "--disable-warning=ExperimentalWarning", cli, "serve"],
    {
      env,
      detached: true,
      stdio: "ignore",
    },
  );
}

function hangServer(): Promise<{ port: number; server: Server }> {
  return new Promise((resolve, reject) => {
    const server = createServer((socket) => {
      socket.on("error", () => {});
    });
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        reject(new Error("hang server has no port"));
        return;
      }
      resolve({ port: address.port, server });
    });
  });
}

test("serve binds with a fixed token before model discovery and restarts on the same address", async () => {
  const dir = mkdtempSync(join(tmpdir(), "maki-cursor-serve-"));
  const ready = join(dir, "ready.json");
  const token = "fixed-bearer-for-serve-address-test";
  const hang = await hangServer();
  let first: ChildProcess | undefined;
  let second: ChildProcess | undefined;
  let boundPort = 0;
  try {
    const baseEnv = {
      ...process.env,
      MAKI_CURSOR_ACCESS: "fake-access",
      MAKI_CURSOR_LOG: join(dir, "proxy.log"),
      MAKI_CURSOR_TOKEN: token,
      MAKI_CURSOR_PORT: "0",
      MAKI_CURSOR_READY: ready,
      CURSOR_API_URL: `http://127.0.0.1:${hang.port}`,
    };
    const startedAt = Date.now();
    first = spawnServe(baseEnv);
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      try {
        const parsed = JSON.parse(readFileSync(ready, "utf8")) as { port?: number; token?: string };
        if (parsed.port && parsed.token === token) {
          boundPort = parsed.port;
          break;
        }
      } catch {
        // not ready yet
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    assert.ok(boundPort > 0, "ready file never appeared");
    assert.equal(JSON.parse(readFileSync(ready, "utf8")).token, token);

    const ok = await fetch(`http://127.0.0.1:${boundPort}/health`, {
      headers: { authorization: `Bearer ${token}` },
    });
    assert.equal(ok.status, 200);
    assert.ok(Date.now() - startedAt < 1000, "health was not ready within one second");

    const denied = await fetch(`http://127.0.0.1:${boundPort}/health`, {
      headers: { authorization: "Bearer wrong-bearer" },
    });
    assert.equal(denied.status, 401);

    killProcessGroup(first);
    first = undefined;
    await new Promise((resolve) => setTimeout(resolve, 100));

    second = spawnServe({
      ...process.env,
      MAKI_CURSOR_ACCESS: "fake-access",
      MAKI_CURSOR_LOG: join(dir, "proxy.log"),
      MAKI_CURSOR_TOKEN: token,
      MAKI_CURSOR_PORT: String(boundPort),
      CURSOR_API_URL: `http://127.0.0.1:${hang.port}`,
    });
    const restartDeadline = Date.now() + 5000;
    let restartOk = false;
    while (Date.now() < restartDeadline) {
      try {
        const response = await fetch(`http://127.0.0.1:${boundPort}/health`, {
          headers: { authorization: `Bearer ${token}` },
        });
        if (response.status === 200) {
          restartOk = true;
          break;
        }
      } catch {
        // The restarted process has not bound the port yet.
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    assert.equal(restartOk, true);
  } finally {
    killProcessGroup(first);
    killProcessGroup(second);
    hang.server.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
