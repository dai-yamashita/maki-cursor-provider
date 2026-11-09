/**
 * A Cursor access token supplied per request in `x-maki-cursor-access` wins
 * over the startup provider; a missing/blank header falls back to it.
 */
import assert from "node:assert/strict";
import test from "node:test";
import { requestAccessToken } from "../src/proxy.ts";

test("a non-empty header token wins and the fallback is not called", async () => {
  let fallbackCalls = 0;
  const token = await requestAccessToken("header-token", async () => {
    fallbackCalls += 1;
    return "fallback-token";
  });
  assert.equal(token, "header-token");
  assert.equal(fallbackCalls, 0);
});

test("surrounding whitespace is trimmed", async () => {
  const token = await requestAccessToken("  header-token\n", async () => "fallback-token");
  assert.equal(token, "header-token");
});

test("an empty or whitespace-only header falls back", async () => {
  assert.equal(await requestAccessToken("", async () => "fallback-token"), "fallback-token");
  assert.equal(await requestAccessToken("   ", async () => "fallback-token"), "fallback-token");
  assert.equal(await requestAccessToken(null, async () => "fallback-token"), "fallback-token");
  assert.equal(await requestAccessToken(undefined, async () => "fallback-token"), "fallback-token");
});

test("no header and no fallback rejects as not configured", async () => {
  await assert.rejects(() => requestAccessToken(null, undefined), /not configured/);
});
