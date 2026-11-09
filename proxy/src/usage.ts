/** Decodes `DashboardService/GetCurrentPeriodUsage` and turns it into the
 * check the proxy applies before forwarding a run: the included API spend and
 * remaining bonus credits gate the non-auto models Cursor exposes. */
import { readMessage, type WireField } from "./proto-wire.ts";

export interface UsageSnapshot {
  apiExhausted: boolean;
  apiPercentUsed: number | null;
  autoPercentUsed: number | null;
  totalPercentUsed: number | null;
  limitCents: number;
  totalSpendCents: number;
  remainingCents: number;
  remainingBonusCents: number;
  resetAt: string | null;
  displayMessage: string;
  namedModelMessage: string;
  autoBucketModels: ReadonlySet<string>;
}

const WIRE_VARINT = 0;
const WIRE_FIXED64 = 1;
const WIRE_LENGTH_DELIMITED = 2;

const decoder = new TextDecoder();

function readLargeVarint(data: Uint8Array): number {
  let value = 0;
  let factor = 1;
  let index = 0;
  while (index < data.length) {
    const byte = data[index]!;
    index += 1;
    value += (byte & 0x7f) * factor;
    if ((byte & 0x80) === 0) return value;
    factor *= 128;
  }
  throw new Error("truncated varint");
}

function varintValue(field: WireField): number | null {
  if (field.wire !== WIRE_VARINT || field.body.length === 0) return null;
  return readLargeVarint(field.body);
}

function doubleValue(field: WireField): number | null {
  if (field.wire !== WIRE_FIXED64 || field.body.length !== 8) return null;
  return new DataView(field.body.buffer, field.body.byteOffset, 8).getFloat64(0, true);
}

function stringValue(field: WireField): string {
  return field.wire === WIRE_LENGTH_DELIMITED ? decoder.decode(field.body) : "";
}

function isoFromMillis(millis: number | null): string | null {
  if (millis === null || !Number.isFinite(millis) || millis <= 0) return null;
  const date = new Date(millis);
  const time = date.getTime();
  if (Number.isNaN(time)) return null;
  return date.toISOString();
}

function firstVarint(fields: WireField[], ...candidates: number[]): number | null {
  for (const candidate of candidates) {
    for (const field of fields) {
      if (field.field !== candidate) continue;
      const value = varintValue(field);
      if (value !== null) return value;
    }
  }
  return null;
}

export function decodeCurrentPeriodUsage(payload: Uint8Array): UsageSnapshot | null {
  try {
    if (!(payload instanceof Uint8Array) || payload.length === 0) return null;
    const fields = readMessage(payload);
    const planFields: WireField[] = [];
    const autoBucketModels = new Set<string>();
    let billingCycleEnd: number | null = null;
    let displayMessage = "";
    let namedModelMessage = "";
    let recognized = false;
    for (const field of fields) {
      switch (field.field) {
        case 2:
          billingCycleEnd = varintValue(field);
          recognized = true;
          break;
        case 3:
          if (field.wire !== WIRE_LENGTH_DELIMITED) break;
          planFields.push(...readMessage(field.body));
          recognized = true;
          break;
        case 7:
          displayMessage = stringValue(field);
          recognized = true;
          break;
        case 11:
          recognized = true;
          break;
        case 12:
          namedModelMessage = stringValue(field);
          recognized = true;
          break;
        case 13: {
          const model = stringValue(field);
          if (model.length > 0) autoBucketModels.add(model);
          recognized = true;
          break;
        }
      }
    }
    if (!recognized) return null;
    const limitCents = firstVarint(planFields, 5) ?? 0;
    const totalSpendCents = firstVarint(planFields, 1) ?? 0;
    const remainingCents = firstVarint(planFields, 4) ?? 0;
    const remainingBonusCents = firstVarint(planFields, 6) ?? 0;
    let autoPercentUsed: number | null = null;
    let apiPercentUsed: number | null = null;
    let totalPercentUsed: number | null = null;
    for (const field of planFields) {
      if (field.field === 12) autoPercentUsed = doubleValue(field) ?? autoPercentUsed;
      else if (field.field === 13) apiPercentUsed = doubleValue(field) ?? apiPercentUsed;
      else if (field.field === 14) totalPercentUsed = doubleValue(field) ?? totalPercentUsed;
    }
    const apiExhausted =
      apiPercentUsed !== null
        ? apiPercentUsed >= 100
        : limitCents > 0 && remainingCents <= 0 && remainingBonusCents <= 0;
    return {
      apiExhausted,
      apiPercentUsed,
      autoPercentUsed,
      totalPercentUsed,
      limitCents,
      totalSpendCents,
      remainingCents,
      remainingBonusCents,
      resetAt: isoFromMillis(billingCycleEnd),
      displayMessage,
      namedModelMessage,
      autoBucketModels,
    };
  } catch {
    return null;
  }
}

let current: UsageSnapshot | null = null;

export function setUsageSnapshot(snapshot: UsageSnapshot | null): void {
  current = snapshot;
}

export function getUsageSnapshot(): UsageSnapshot | null {
  return current;
}

export function resetUsageForTests(): void {
  current = null;
}

export function allowsModel(snapshot: UsageSnapshot, wireModelId: string): boolean {
  return snapshot.autoBucketModels.has(wireModelId);
}

function dollars(cents: number): string {
  return `$${(cents / 100).toFixed(2)}`;
}

function alternatives(snapshot: UsageSnapshot): string[] {
  const picks = ["cursor/auto"];
  let extras = 0;
  for (const model of snapshot.autoBucketModels) {
    if (extras >= 3) break;
    if (model === "default" || model === "auto" || model.length === 0) continue;
    picks.push(`cursor/${model}`);
    extras += 1;
  }
  return picks;
}

export function quotaMessage(pickerId: string, wireModelId: string, snapshot: UsageSnapshot): string {
  const percent = snapshot.apiPercentUsed ?? 100;
  const percentText = Number.isInteger(percent) ? `${percent}` : percent.toFixed(1);
  const reset = snapshot.resetAt ?? "an unknown date";
  const parts = [
    `Cursor included API usage is exhausted: You've used ${percentText}% of your included API usage` +
      ` (limit ${dollars(snapshot.limitCents)}, remaining ${dollars(snapshot.remainingCents)}; resets ${reset}).`,
  ];
  const notices: string[] = [];
  if (snapshot.displayMessage.length > 0) notices.push(snapshot.displayMessage);
  if (snapshot.namedModelMessage.length > 0 && snapshot.namedModelMessage !== snapshot.displayMessage) {
    notices.push(snapshot.namedModelMessage);
  }
  if (notices.length > 0) {
    const report = notices.join(" ").trim();
    parts.push(`Cursor reports: ${/[.!?]$/.test(report) ? report : `${report}.`}`);
  }
  parts.push(`Model ${pickerId} (wire ${wireModelId}) is blocked.`);
  parts.push(`Switch to ${alternatives(snapshot).join(", ")}, or raise the Cursor usage limit.`);
  return parts.join(" ");
}
