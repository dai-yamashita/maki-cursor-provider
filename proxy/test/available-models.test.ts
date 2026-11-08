import assert from "node:assert/strict";
import test from "node:test";
import { decodeAvailableModels } from "../src/available-models.ts";
import { clearAvailableModels, pickerRows, rememberAvailableModels, resolveCursorModel } from "../src/catalog.ts";

function varint(value: number): number[] {
  const bytes: number[] = [];
  let rest = value;
  while (rest > 0x7f) {
    bytes.push((rest & 0x7f) | 0x80);
    rest >>>= 7;
  }
  bytes.push(rest);
  return bytes;
}

function field(number: number, wire: number, body: number[]): number[] {
  return [...varint((number << 3) | wire), ...body];
}

function str(number: number, value: string): number[] {
  const bytes = [...Buffer.from(value)];
  return field(number, 2, [...varint(bytes.length), ...bytes]);
}

function msg(number: number, body: number[]): number[] {
  return field(number, 2, [...varint(body.length), ...body]);
}

function bool(number: number, value: boolean): number[] {
  return field(number, 0, [value ? 1 : 0]);
}

test("AvailableModels adds GLM 5.3 Flash and maps an effort slug onto parameters", () => {
  const parameter = [...str(1, "reasoning"), ...str(2, "high")];
  const variant = [
    ...msg(1, parameter),
    ...str(2, "GLM 5.3 Flash High"),
    ...str(9, "glm-5p3-flash[reasoning=high]"),
    ...str(11, "glm-5p3-flash-high"),
  ];
  const model = [
    ...str(1, "glm-5p3-flash"),
    ...bool(5, true),
    ...str(17, "GLM 5.3 Flash"),
    ...msg(30, variant),
    ...str(36, "glm-5p3-flash"),
    ...str(36, "glm-5p3-flash-low"),
    ...str(36, "glm-5p3-flash-high"),
    ...str(36, "glm-5p3-flash-max"),
  ];
  const payload = Uint8Array.from(msg(2, model));
  const decoded = decodeAvailableModels(payload);
  assert.equal(decoded.length, 1);
  assert.equal(decoded[0]?.displayName, "GLM 5.3 Flash");

  clearAvailableModels();
  rememberAvailableModels(decoded);
  const ids = pickerRows(decoded).map((row) => row.id);
  assert.ok(ids.includes("glm-5p3-flash"));
  assert.ok(ids.includes("glm-5p3-flash-low"));

  const high = resolveCursorModel("glm-5p3-flash-high");
  assert.deepEqual(high, {
    modelId: "glm-5p3-flash",
    parameters: [{ id: "reasoning", value: "high" }],
    maxMode: false,
  });
  const low = resolveCursorModel("glm-5p3-flash-low");
  assert.deepEqual(low, {
    modelId: "glm-5p3-flash",
    parameters: [{ id: "reasoning", value: "low" }],
    maxMode: false,
  });
  clearAvailableModels();
});
