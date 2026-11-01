import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
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

test("logout removes the credential file and conversation directory from the environment", () => {
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

test("status creates the credential directory named by the environment", () => {
  const dir = mkdtempSync(join(tmpdir(), "maki-cursor-empty-"));
  const creds = join(dir, "fresh", "credentials.json");
  try {
    const result = spawnSync(node, [
      "--experimental-strip-types",
      "--disable-warning=ExperimentalWarning",
      cli,
      "status",
    ], {
      env: {
        ...process.env,
        MAKI_CURSOR_CREDENTIALS: creds,
      },
      encoding: "utf8",
    });
    assert.notEqual(result.status, 0);
    assert.equal(existsSync(dirname(creds)), true);
    assert.equal(existsSync(creds), false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
