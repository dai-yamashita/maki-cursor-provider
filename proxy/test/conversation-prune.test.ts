import assert from "node:assert/strict";
import test from "node:test";
import {
  conversationPruneLimitsFromEnv,
  selectConversationFilesToPrune,
} from "../src/conversation-store.ts";
import type { ConversationFile, ConversationPruneLimits } from "../src/conversation-store.ts";

const DEFAULT_MAX_BYTES = 2 * 1024 ** 3;

function limits(overrides: Partial<ConversationPruneLimits> = {}): ConversationPruneLimits {
  return { ttlMs: 1000, maxFiles: 100, maxBytes: 1_000_000, ...overrides };
}

function file(name: string, mtimeMs: number, size = 10): ConversationFile {
  return { name, mtimeMs, size };
}

test("ttl removes only expired files", () => {
  const entries = [
    file("ancient.json", 1000),
    file("fresh.json", 9999),
    file("boundary.json", 9000),
    file("now.json", 10_000),
  ];
  assert.deepEqual(selectConversationFilesToPrune(entries, 10_000, limits({ ttlMs: 1000 })), [
    "ancient.json",
  ]);
});

test("maxFiles deletes oldest files and keeps the newest", () => {
  const entries = [
    file("c.json", 3000),
    file("a.json", 1000),
    file("d.json", 4000),
    file("b.json", 2000),
  ];
  assert.deepEqual(selectConversationFilesToPrune(entries, 4000, limits({ ttlMs: 10_000, maxFiles: 2 })), [
    "a.json",
    "b.json",
  ]);
});

test("maxBytes deletes oldest files until kept bytes fit", () => {
  const entries = [
    file("a.json", 1000, 400),
    file("b.json", 2000, 400),
    file("c.json", 3000, 400),
  ];
  assert.deepEqual(
    selectConversationFilesToPrune(entries, 3000, limits({ ttlMs: 10_000, maxBytes: 1000 })),
    ["a.json"],
  );
});

test("ttl and both caps apply together", () => {
  const entries = [
    file("expired.json", 100, 50),
    file("a.json", 5000, 300),
    file("b.json", 6000, 300),
    file("c.json", 7000, 300),
    file("d.json", 8000, 300),
  ];
  assert.deepEqual(
    selectConversationFilesToPrune(
      entries,
      8000,
      limits({ ttlMs: 1000, maxFiles: 3, maxBytes: 700 }),
    ),
    ["expired.json", "a.json", "b.json"],
  );
});

test("entries under all limits are left untouched", () => {
  const entries = [file("b.json", 2000, 100), file("a.json", 1000, 100)];
  const snapshot = structuredClone(entries);
  assert.deepEqual(selectConversationFilesToPrune(entries, 3000, limits({ ttlMs: 5000 })), []);
  assert.deepEqual(entries, snapshot);
});

test("env limits parse valid values and fall back to defaults otherwise", () => {
  assert.deepEqual(conversationPruneLimitsFromEnv({
    MAKI_CURSOR_CONVERSATIONS_MAX_FILES: "10",
    MAKI_CURSOR_CONVERSATIONS_MAX_BYTES: "2048",
  }, 1234), { ttlMs: 1234, maxFiles: 10, maxBytes: 2048 });

  const fallback = { ttlMs: 99, maxFiles: 5000, maxBytes: DEFAULT_MAX_BYTES };
  assert.deepEqual(conversationPruneLimitsFromEnv({}, 99), fallback);
  assert.deepEqual(conversationPruneLimitsFromEnv({
    MAKI_CURSOR_CONVERSATIONS_MAX_FILES: "abc",
    MAKI_CURSOR_CONVERSATIONS_MAX_BYTES: "1.5",
  }, 99), fallback);
  assert.deepEqual(conversationPruneLimitsFromEnv({
    MAKI_CURSOR_CONVERSATIONS_MAX_FILES: "0",
    MAKI_CURSOR_CONVERSATIONS_MAX_BYTES: "",
  }, 99), fallback);
  assert.deepEqual(conversationPruneLimitsFromEnv({
    MAKI_CURSOR_CONVERSATIONS_MAX_FILES: "-3",
    MAKI_CURSOR_CONVERSATIONS_MAX_BYTES: "-1",
  }, 99), fallback);
});
