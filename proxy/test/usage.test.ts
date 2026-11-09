import assert from "node:assert/strict";
import test from "node:test";
import {
  allowsModel,
  decodeCurrentPeriodUsage,
  getUsageSnapshot,
  quotaMessage,
  resetUsageForTests,
  setUsageSnapshot,
  type UsageSnapshot,
} from "../src/usage.ts";

const RESET_MS = 1796349101000;
const RESET_ISO = "2026-12-04T01:51:41.000Z";

function concat(parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

function varint(value: number): Uint8Array {
  const bytes: number[] = [];
  let rest = value;
  while (rest > 0x7f) {
    bytes.push((rest & 0x7f) | 0x80);
    rest = Math.floor(rest / 128);
  }
  bytes.push(rest);
  return Uint8Array.from(bytes);
}

function varintField(field: number, value: number): Uint8Array {
  return concat([varint((field << 3) | 0), varint(value)]);
}

function stringField(field: number, value: string): Uint8Array {
  const bytes = new TextEncoder().encode(value);
  return concat([varint((field << 3) | 2), varint(bytes.length), bytes]);
}

function messageField(field: number, body: Uint8Array): Uint8Array {
  return concat([varint((field << 3) | 2), varint(body.length), body]);
}

function doubleField(field: number, value: number): Uint8Array {
  const body = new Uint8Array(8);
  new DataView(body.buffer).setFloat64(0, value, true);
  return concat([varint((field << 3) | 1), body]);
}

interface PlanOptions {
  totalSpend?: number;
  includedSpend?: number;
  bonusSpend?: number;
  remaining?: number;
  limit?: number;
  remainingBonus?: number;
  autoPercent?: number;
  apiPercent?: number;
  totalPercent?: number;
}

function planUsage(options: PlanOptions): Uint8Array {
  const parts: Uint8Array[] = [];
  if (options.totalSpend !== undefined) parts.push(varintField(1, options.totalSpend));
  if (options.includedSpend !== undefined) parts.push(varintField(2, options.includedSpend));
  if (options.bonusSpend !== undefined) parts.push(varintField(3, options.bonusSpend));
  if (options.remaining !== undefined) parts.push(varintField(4, options.remaining));
  if (options.limit !== undefined) parts.push(varintField(5, options.limit));
  if (options.remainingBonus !== undefined) parts.push(varintField(6, options.remainingBonus));
  if (options.autoPercent !== undefined) parts.push(doubleField(12, options.autoPercent));
  if (options.apiPercent !== undefined) parts.push(doubleField(13, options.apiPercent));
  if (options.totalPercent !== undefined) parts.push(doubleField(14, options.totalPercent));
  return concat(parts);
}

function usagePayload(
  options: PlanOptions,
  extra: { namedModelMessage?: string; buckets?: string[]; displayMessage?: string; resetMs?: number } = {},
): Uint8Array {
  const parts: Uint8Array[] = [varintField(2, extra.resetMs ?? RESET_MS), messageField(3, planUsage(options))];
  parts.push(stringField(7, extra.displayMessage ?? "You've hit your API spend limit"));
  parts.push(stringField(12, extra.namedModelMessage ?? "Composer is unavailable with this plan"));
  for (const bucket of extra.buckets ?? ["default", "composer-2.5", "gpt-5.1", "sonnet-4.5", "opus-4.1"]) {
    parts.push(stringField(13, bucket));
  }
  return concat(parts);
}

function exhaustedPayload(): Uint8Array {
  return usagePayload({
    totalSpend: 2500,
    includedSpend: 2000,
    bonusSpend: 300,
    remaining: 0,
    limit: 2000,
    remainingBonus: 0,
    autoPercent: 12.5,
    apiPercent: 100,
    totalPercent: 66.5,
  });
}

test("a full exhausted snapshot decodes every field", () => {
  const snapshot = decodeCurrentPeriodUsage(exhaustedPayload());
  assert.ok(snapshot);
  if (!snapshot) return;
  assert.equal(snapshot.apiExhausted, true);
  assert.equal(snapshot.apiPercentUsed, 100);
  assert.equal(snapshot.autoPercentUsed, 12.5);
  assert.equal(snapshot.totalPercentUsed, 66.5);
  assert.equal(snapshot.limitCents, 2000);
  assert.equal(snapshot.totalSpendCents, 2500);
  assert.equal(snapshot.remainingCents, 0);
  assert.equal(snapshot.remainingBonusCents, 0);
  assert.equal(snapshot.resetAt, RESET_ISO);
  assert.equal(snapshot.displayMessage, "You've hit your API spend limit");
  assert.equal(snapshot.namedModelMessage, "Composer is unavailable with this plan");
  assert.deepEqual([...snapshot.autoBucketModels], ["default", "composer-2.5", "gpt-5.1", "sonnet-4.5", "opus-4.1"]);
});

test("99.9 percent used is below the exhaust threshold", () => {
  const snapshot = decodeCurrentPeriodUsage(usagePayload({ limit: 2000, remaining: 2, apiPercent: 99.9 }));
  assert.ok(snapshot);
  if (!snapshot) return;
  assert.equal(snapshot.apiPercentUsed, 99.9);
  assert.equal(snapshot.apiExhausted, false);
});

test("a missing percent falls back to the spend and remaining rule", () => {
  const snapshot = decodeCurrentPeriodUsage(usagePayload({ limit: 2000, remaining: 0, remainingBonus: 0 }));
  assert.ok(snapshot);
  if (!snapshot) return;
  assert.equal(snapshot.apiPercentUsed, null);
  assert.equal(snapshot.apiExhausted, true);

  const withBonus = decodeCurrentPeriodUsage(usagePayload({ limit: 2000, remaining: 0, remainingBonus: 500 }));
  assert.ok(withBonus);
  if (!withBonus) return;
  assert.equal(withBonus.apiExhausted, false);
});

test("empty, truncated, and garbage payloads decode to null", () => {
  assert.equal(decodeCurrentPeriodUsage(new Uint8Array(0)), null);
  assert.equal(decodeCurrentPeriodUsage(Uint8Array.from([0xde, 0xad, 0xbe, 0xef, 0xff])), null);
  assert.equal(decodeCurrentPeriodUsage(Uint8Array.from([0x08])), null);
  const payload = exhaustedPayload();
  assert.equal(decodeCurrentPeriodUsage(payload.subarray(0, payload.length - 3)), null);
  assert.equal(decodeCurrentPeriodUsage(new Uint8Array([0x78, 0x01])), null);
});

test("only bucket models are allowed", () => {
  const snapshot = decodeCurrentPeriodUsage(usagePayload({ limit: 2000, remaining: 0, apiPercent: 100 }));
  assert.ok(snapshot);
  if (!snapshot) return;
  assert.equal(allowsModel(snapshot, "default"), true);
  assert.equal(allowsModel(snapshot, "composer-2.5"), true);
  assert.equal(allowsModel(snapshot, "glm-5p3"), false);
});

test("the quota message names the picker, the wire id, the reset, and bucket alternatives", () => {
  const snapshot = decodeCurrentPeriodUsage(exhaustedPayload());
  assert.ok(snapshot);
  if (!snapshot) return;
  const message = quotaMessage("glm-5p3-high", "glm-5p3", snapshot);
  assert.match(message, /glm-5p3-high/);
  assert.match(message, /wire glm-5p3/);
  assert.match(message, new RegExp(RESET_ISO.replace(/[.]/g, "\\.")));
  assert.match(message, /cursor\/auto/);
  assert.match(message, /cursor\/composer-2\.5/);
  assert.match(message, /100%/);
  assert.match(message, /\$20\.00/);
  assert.match(message, /\$0\.00/);
  assert.match(message, /You've hit your API spend limit/);
  assert.match(message, /Composer is unavailable with this plan/);
  assert.doesNotMatch(message, /cursor\/default/);
  assert.doesNotMatch(message, /cursor\/opus-4\.1/);
});

test("the snapshot store round trips and resets", () => {
  resetUsageForTests();
  assert.equal(getUsageSnapshot(), null);
  const snapshot: UsageSnapshot = {
    apiExhausted: false,
    apiPercentUsed: 12,
    autoPercentUsed: null,
    totalPercentUsed: null,
    limitCents: 2000,
    totalSpendCents: 240,
    remainingCents: 1760,
    remainingBonusCents: 0,
    resetAt: RESET_ISO,
    displayMessage: "",
    namedModelMessage: "",
    autoBucketModels: new Set(["default"]),
  };
  setUsageSnapshot(snapshot);
  assert.equal(getUsageSnapshot(), snapshot);
  setUsageSnapshot(null);
  assert.equal(getUsageSnapshot(), null);
  resetUsageForTests();
});
