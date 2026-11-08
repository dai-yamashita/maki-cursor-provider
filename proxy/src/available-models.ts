/**
 * `aiserver.v1.AiService/AvailableModels`.
 *
 * The agent CLI's model picker reads this. `GetUsableModels` does not include
 * families that only exist here, such as GLM 5.3 Flash (`glm-5p3-flash`).
 * Field numbers match the CLI's `AvailableModelsRequest` / `AvailableModel` /
 * `ModelVariantConfig` descriptors.
 */
import { callCursorUnaryRpc } from "./proxy.ts";
import type { AvailableModel, AvailableVariant, ModelParameter } from "./catalog.ts";

const AVAILABLE_MODELS_PATH = "/aiserver.v1.AiService/AvailableModels";

export async function fetchAvailableModels(apiKey: string): Promise<AvailableModel[]> {
  try {
    const response = await callCursorUnaryRpc({
      accessToken: apiKey,
      rpcPath: AVAILABLE_MODELS_PATH,
      requestBody: availableModelsRequest(),
      timeoutMs: 15_000,
    });
    if (response.timedOut || response.exitCode !== 0 || response.body.length === 0) return [];
    return decodeAvailableModels(response.body);
  } catch {
    return [];
  }
}

export function availableModelsRequest(): Uint8Array {
  return Buffer.concat([
    encodeBool(5, true),
    encodeBool(6, true),
    encodeBool(7, true),
    encodeBool(8, true),
    encodeBool(11, true),
  ]);
}

export function decodeAvailableModels(payload: Uint8Array): AvailableModel[] {
  const models: AvailableModel[] = [];
  for (const field of readFields(payload)) {
    if (field.number !== 2 || !field.bytes) continue;
    const model = decodeModel(field.bytes);
    if (model) models.push(model);
  }
  return models;
}

function decodeModel(payload: Uint8Array): AvailableModel | null {
  const model: AvailableModel = {
    name: "",
    displayName: "",
    supportsAgent: false,
    chatOnly: false,
    legacySlugs: [],
    variants: [],
  };
  let sawAgent = false;
  for (const field of readFields(payload)) {
    if (field.number === 1 && field.bytes) model.name = text(field.bytes);
    else if (field.number === 4 && field.bool !== undefined) model.chatOnly = field.bool;
    else if (field.number === 5 && field.bool !== undefined) {
      model.supportsAgent = field.bool;
      sawAgent = true;
    } else if (field.number === 17 && field.bytes) model.displayName = text(field.bytes);
    else if (field.number === 30 && field.bytes) {
      const variant = decodeVariant(field.bytes);
      if (variant) model.variants.push(variant);
    } else if (field.number === 36 && field.bytes) {
      const slug = text(field.bytes);
      if (slug) model.legacySlugs.push(slug);
    }
  }
  if (!sawAgent) model.supportsAgent = true;
  if (!model.name) return null;
  if (!model.displayName) model.displayName = model.name;
  return model;
}

function decodeVariant(payload: Uint8Array): AvailableVariant | null {
  const variant: AvailableVariant = {
    displayName: "",
    variant: "",
    legacy: "",
    parameters: [],
    maxMode: false,
  };
  for (const field of readFields(payload)) {
    if (field.number === 1 && field.bytes) {
      const parameter = decodeParameter(field.bytes);
      if (parameter) variant.parameters.push(parameter);
    } else if (field.number === 2 && field.bytes) variant.displayName = text(field.bytes);
    else if (field.number === 3 && field.bool !== undefined) variant.maxMode = field.bool;
    else if (field.number === 9 && field.bytes) variant.variant = text(field.bytes);
    else if (field.number === 11 && field.bytes) variant.legacy = text(field.bytes);
  }
  if (!variant.legacy && !variant.variant) return null;
  return variant;
}

function decodeParameter(payload: Uint8Array): ModelParameter | null {
  let id = "";
  let value = "";
  for (const field of readFields(payload)) {
    if (field.number === 1 && field.bytes) id = text(field.bytes);
    else if (field.number === 2 && field.bytes) value = text(field.bytes);
  }
  if (!id) return null;
  return { id, value };
}

interface Field {
  number: number;
  bool?: boolean;
  bytes?: Uint8Array;
}

function readFields(payload: Uint8Array): Field[] {
  const fields: Field[] = [];
  let offset = 0;
  while (offset < payload.length) {
    const tag = readVarint(payload, offset);
    offset = tag.next;
    const number = Number(tag.value >> 3n);
    const wire = Number(tag.value & 7n);
    if (wire === 0) {
      const body = readVarint(payload, offset);
      offset = body.next;
      fields.push({ number, bool: body.value === 1n });
    } else if (wire === 2) {
      const length = readVarint(payload, offset);
      offset = length.next;
      const end = offset + Number(length.value);
      if (end > payload.length) break;
      fields.push({ number, bytes: payload.subarray(offset, end) });
      offset = end;
    } else if (wire === 5) {
      offset += 4;
    } else if (wire === 1) {
      offset += 8;
    } else {
      break;
    }
  }
  return fields;
}

function readVarint(payload: Uint8Array, offset: number): { value: bigint; next: number } {
  let value = 0n;
  let shift = 0n;
  let index = offset;
  while (index < payload.length) {
    const byte = payload[index]!;
    index += 1;
    value |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) return { value, next: index };
    shift += 7n;
  }
  return { value, next: payload.length };
}

function encodeBool(field: number, value: boolean): Uint8Array {
  return Uint8Array.from([(field << 3) | 0, value ? 1 : 0]);
}

function text(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes);
}
