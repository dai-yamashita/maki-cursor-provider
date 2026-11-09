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

interface FixtureModel {
  name: string;
  displayName: string;
  parameter: [string, string];
  legacy: string;
  slugs: string[];
}

function availablePayload(models: readonly FixtureModel[]): Uint8Array {
  const parts: number[] = [];
  for (const item of models) {
    const parameter = [...str(1, item.parameter[0]), ...str(2, item.parameter[1])];
    const variant = [
      ...msg(1, parameter),
      ...str(2, item.displayName),
      ...str(9, `${item.name}[${item.parameter[0]}=${item.parameter[1]}]`),
      ...str(11, item.legacy),
    ];
    const model = [
      ...str(1, item.name),
      ...bool(5, true),
      ...str(17, item.displayName),
      ...msg(30, variant),
      ...item.slugs.flatMap((slug) => str(36, slug)),
    ];
    parts.push(...msg(2, model));
  }
  return Uint8Array.from(parts);
}

const GLM: FixtureModel = {
  name: "glm-5p3-flash",
  displayName: "GLM 5.3 Flash",
  parameter: ["reasoning", "high"],
  legacy: "glm-5p3-flash-high",
  slugs: ["glm-5p3-flash", "glm-5p3-flash-low", "glm-5p3-flash-high", "glm-5p3-flash-max"],
};

const DEEPSEEK: FixtureModel = {
  name: "deepseek-v41-flash-1017",
  displayName: "DeepSeek V4.1 Flash",
  parameter: ["reasoning_effort", "high"],
  legacy: "deepseek-v41-flash-1017-high",
  slugs: [
    "deepseek-v41-flash-1017",
    "deepseek-v41-flash-1017-low",
    "deepseek-v41-flash-1017-medium",
    "deepseek-v41-flash-1017-high",
  ],
};

const CLAUDE: FixtureModel = {
  name: "claude-4p5-sonnet",
  displayName: "Claude 4.5 Sonnet",
  parameter: ["effort", "medium"],
  legacy: "claude-4p5-sonnet-medium",
  slugs: ["claude-4p5-sonnet", "claude-4p5-sonnet-low", "claude-4p5-sonnet-medium"],
};

function load(models: readonly FixtureModel[]): void {
  clearAvailableModels();
  rememberAvailableModels(decodeAvailableModels(availablePayload(models)));
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

test("DeepSeek family uses reasoning_effort and never synthesizes reasoning", () => {
  load([DEEPSEEK, GLM]);
  const medium = resolveCursorModel("deepseek-v41-flash-1017-medium");
  assert.deepEqual(medium, {
    modelId: "deepseek-v41-flash-1017",
    parameters: [{ id: "reasoning_effort", value: "medium" }],
    maxMode: false,
  });
  assert.ok(!(medium?.parameters ?? []).some((item) => item.id === "reasoning"));
  clearAvailableModels();
});

test("Claude family uses effort", () => {
  load([CLAUDE]);
  const low = resolveCursorModel("claude-4p5-sonnet-low");
  assert.deepEqual(low, {
    modelId: "claude-4p5-sonnet",
    parameters: [{ id: "effort", value: "low" }],
    maxMode: false,
  });
  clearAvailableModels();
});

test("an empty catalogue keeps the previous resolution", () => {
  load([DEEPSEEK, GLM]);
  rememberAvailableModels([]);
  assert.deepEqual(resolveCursorModel("deepseek-v41-flash-1017-medium"), {
    modelId: "deepseek-v41-flash-1017",
    parameters: [{ id: "reasoning_effort", value: "medium" }],
    maxMode: false,
  });
  assert.deepEqual(resolveCursorModel("glm-5p3-flash-low"), {
    modelId: "glm-5p3-flash",
    parameters: [{ id: "reasoning", value: "low" }],
    maxMode: false,
  });
  clearAvailableModels();
});

test("an unknown suffix resolves through the longest base prefix", () => {
  load([GLM, DEEPSEEK]);
  assert.deepEqual(resolveCursorModel("glm-5p3-flash-xhigh"), {
    modelId: "glm-5p3-flash",
    parameters: [{ id: "reasoning", value: "xhigh" }],
    maxMode: false,
  });
  clearAvailableModels();
});
