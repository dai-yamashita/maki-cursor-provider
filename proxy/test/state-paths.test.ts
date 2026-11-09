import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { proxyLogPath } from "../src/log.ts";
import { saveConversation } from "../src/proxy.ts";

const node = process.execPath;
const cli = new URL("../src/cli.ts", import.meta.url).pathname;

test("a conversation snapshot is written where MAKI_CURSOR_CONVERSATIONS points", async () => {
  const dir = mkdtempSync(join(tmpdir(), "maki-cursor-state-"));
  const previous = process.env.MAKI_CURSOR_CONVERSATIONS;
  process.env.MAKI_CURSOR_CONVERSATIONS = dir;
  try {
    const file = await saveConversation("abcdef0123456789", {
      conversationId: "conv",
      checkpoint: null,
      blobStore: new Map(),
      lastAccessMs: Date.now(),
    });
    assert.equal(file, join(dir, "abcdef0123456789.json"));
    assert.equal(existsSync(file!), true);
  } finally {
    if (previous === undefined) delete process.env.MAKI_CURSOR_CONVERSATIONS;
    else process.env.MAKI_CURSOR_CONVERSATIONS = previous;
    rmSync(dir, { recursive: true, force: true });
  }
});

test("logout removes the legacy credential file and conversation directory from the environment", () => {
  const dir = mkdtempSync(join(tmpdir(), "maki-cursor-cred-"));
  const creds = join(dir, "nested", "credentials.json");
  const conversations = join(dir, "conversations");
  mkdirSync(dirname(creds), { recursive: true });
  writeFileSync(creds, "{}");
  mkdirSync(conversations);
  writeFileSync(join(conversations, "keep.txt"), "x");
  try {
    const result = spawnSync(node, [
      "--experimental-strip-types",
      "--disable-warning=ExperimentalWarning",
      cli,
      "logout",
    ], {
      env: {
        ...process.env,
        MAKI_CURSOR_CREDENTIALS: creds,
        MAKI_CURSOR_CONVERSATIONS: conversations,
      },
      encoding: "utf8",
    });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(existsSync(creds), false);
    assert.equal(existsSync(conversations), false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("forget-legacy removes the legacy credential file", () => {
  const dir = mkdtempSync(join(tmpdir(), "maki-cursor-forget-"));
  const creds = join(dir, "nested", "credentials.json");
  mkdirSync(dirname(creds), { recursive: true });
  writeFileSync(creds, JSON.stringify({
    access: "legacy-access",
    refresh: "legacy-refresh",
    expires: 1_700_000_000_000,
  }));
  try {
    const result = spawnSync(node, [
      "--experimental-strip-types",
      "--disable-warning=ExperimentalWarning",
      cli,
      "forget-legacy",
    ], {
      env: {
        ...process.env,
        MAKI_CURSOR_CREDENTIALS: creds,
      },
      encoding: "utf8",
    });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(existsSync(creds), false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("refresh prints credentials and writes no legacy file", { timeout: 15_000 }, async () => {
  const dir = mkdtempSync(join(tmpdir(), "maki-cursor-refresh-"));
  const creds = join(dir, "credentials.json");
  const server = http.createServer((_req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ accessToken: "new-access", refreshToken: "new-refresh" }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const { port } = server.address() as AddressInfo;
  try {
    const child = spawn(node, [
      "--experimental-strip-types",
      "--disable-warning=ExperimentalWarning",
      cli,
      "refresh",
    ], {
      env: {
        ...process.env,
        MAKI_CURSOR_REFRESH: "old-refresh",
        MAKI_CURSOR_CREDENTIALS: creds,
        CURSOR_REFRESH_URL: `http://127.0.0.1:${port}/refresh`,
      },
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8").on("data", (chunk: string) => { stdout += chunk; });
    child.stderr.setEncoding("utf8").on("data", (chunk: string) => { stderr += chunk; });
    const [code] = await once(child, "close");

    assert.equal(code, 0, stderr);
    const parsed = JSON.parse(stdout) as { access: string; refresh: string; expires: number };
    assert.equal(parsed.access, "new-access");
    assert.equal(parsed.refresh, "new-refresh");
    assert.equal(typeof parsed.expires, "number");
    assert.equal(existsSync(creds), false);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    rmSync(dir, { recursive: true, force: true });
  }
});

test("MAKI_CURSOR_LOG controls the proxy log path", () => {
  const dir = mkdtempSync(join(tmpdir(), "maki-cursor-log-"));
  const logPath = join(dir, "custom.log");
  const previous = process.env.MAKI_CURSOR_LOG;
  process.env.MAKI_CURSOR_LOG = logPath;
  try {
    assert.equal(proxyLogPath(), logPath);
  } finally {
    if (previous === undefined) delete process.env.MAKI_CURSOR_LOG;
    else process.env.MAKI_CURSOR_LOG = previous;
    rmSync(dir, { recursive: true, force: true });
  }
});
