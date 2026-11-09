/**
 * Cursor publishes two catalogues.
 *
 * `GetUsableModels` is the flat id list (`glm-5.2-high`). The model picker in
 * the agent CLI, and the docs page after "show more models", read
 * `AvailableModels`. That response carries the current families (`glm-5p3-flash`,
 * display name "GLM 5.3 Flash") plus the legacy slugs for each effort.
 * A Run request for one of those slugs is the family id plus its parameters,
 * which is what the CLI sends.
 */

export interface ModelParameter {
  id: string;
  value: string;
}

export interface AvailableVariant {
  displayName: string;
  /** `glm-5p3-flash[reasoning=high]` */
  variant: string;
  /** `glm-5p3-flash-high` */
  legacy: string;
  parameters: ModelParameter[];
  maxMode: boolean;
}

export interface AvailableModel {
  name: string;
  displayName: string;
  supportsAgent: boolean;
  chatOnly: boolean;
  legacySlugs: string[];
  variants: AvailableVariant[];
}

export interface ResolvedModel {
  modelId: string;
  parameters: ModelParameter[];
  maxMode: boolean;
}

const REASONING = new Set(["none", "minimal", "low", "medium", "high", "xhigh", "max"]);

let resolvedByPickerId = new Map<string, ResolvedModel>();

interface BaseModel {
  name: string;
  defaultParams: ModelParameter[];
  maxMode: boolean;
}

let baseModels = new Map<string, BaseModel>();

export function rememberAvailableModels(models: readonly AvailableModel[]): void {
  if (models.length === 0) return;
  const next = new Map<string, ResolvedModel>();
  const bases = new Map<string, BaseModel>();
  for (const model of models) {
    if (!model.supportsAgent || model.chatOnly || model.name === "") continue;
    const fallback = model.variants[0];
    const fallbackParams = fallback?.parameters ?? [];
    const fallbackMax = fallback?.maxMode ?? false;
    bases.set(model.name, {
      name: model.name,
      defaultParams: fallbackParams,
      maxMode: fallbackMax,
    });
    remember(next, model.name, {
      modelId: model.name,
      parameters: fallbackParams,
      maxMode: fallbackMax,
    });
    for (const variant of model.variants) {
      const resolved = {
        modelId: model.name,
        parameters: variant.parameters,
        maxMode: variant.maxMode,
      };
      if (variant.legacy) remember(next, variant.legacy, resolved);
      if (variant.variant) remember(next, variant.variant, resolved);
    }
    for (const slug of model.legacySlugs) {
      if (next.has(slug)) continue;
      remember(next, slug, {
        modelId: model.name,
        parameters: parametersForLegacySlug(model.name, slug, fallbackParams),
        maxMode: fallbackMax,
      });
    }
  }
  resolvedByPickerId = next;
  baseModels = bases;
}

export function clearAvailableModels(): void {
  resolvedByPickerId = new Map();
  baseModels = new Map();
}

/** Translate a picker id into the Run request the CLI would send. */
export function resolveCursorModel(pickerId: string): ResolvedModel | undefined {
  const exact = resolvedByPickerId.get(pickerId);
  if (exact) return exact;
  // proxy.ts owns 'auto'; it never maps onto a catalogue family.
  if (pickerId === "auto") return undefined;
  const base = longestBasePrefix(pickerId);
  if (!base) return undefined;
  return {
    modelId: base.name,
    parameters: parametersForLegacySlug(base.name, pickerId, base.defaultParams),
    maxMode: base.maxMode,
  };
}

function longestBasePrefix(pickerId: string): BaseModel | undefined {
  let best: BaseModel | undefined;
  for (const base of baseModels.values()) {
    if (!pickerId.startsWith(base.name)) continue;
    if (pickerId.length !== base.name.length) {
      const boundary = pickerId.charAt(base.name.length);
      if (/[a-z0-9]/.test(boundary)) continue;
    }
    if (!best || base.name.length > best.name.length) best = base;
  }
  return best;
}

export function pickerRows(models: readonly AvailableModel[]): Array<{ id: string; name: string }> {
  const rows: Array<{ id: string; name: string }> = [];
  const seen = new Set<string>();
  const add = (id: string, name: string) => {
    const trimmed = id.trim();
    if (!trimmed || seen.has(trimmed)) return;
    seen.add(trimmed);
    rows.push({ id: trimmed, name: name.trim() || trimmed });
  };
  for (const model of models) {
    if (!model.supportsAgent || model.chatOnly || model.name === "") continue;
    add(model.name, model.displayName || model.name);
    for (const variant of model.variants) {
      if (variant.legacy) add(variant.legacy, variant.displayName || model.displayName || variant.legacy);
    }
    for (const slug of model.legacySlugs) {
      add(slug, labelForSlug(model, slug));
    }
  }
  return rows;
}

function remember(map: Map<string, ResolvedModel>, id: string, resolved: ResolvedModel): void {
  const trimmed = id.trim();
  if (!trimmed || map.has(trimmed)) return;
  map.set(trimmed, resolved);
}

function labelForSlug(model: AvailableModel, slug: string): string {
  const variant = model.variants.find((item) => item.legacy === slug);
  if (variant?.displayName) return variant.displayName;
  if (slug === model.name) return model.displayName || slug;
  const suffix = suffixAfter(model.name, slug);
  if (!suffix) return model.displayName || slug;
  const extra = suffix.split("-").filter(Boolean).join(" ");
  return model.displayName ? `${model.displayName} ${extra}` : slug;
}

function parametersForLegacySlug(
  name: string,
  slug: string,
  defaults: readonly ModelParameter[],
): ModelParameter[] {
  const suffix = suffixAfter(name, slug);
  if (!suffix) return [...defaults];
  const params = new Map(defaults.map((item) => [item.id, item.value]));
  // Only touch parameters the family already declares; Cursor rejects unknown
  // ids (a synthesized `reasoning` alongside `reasoning_effort` fails).
  const reasoningKey = ["reasoning", "reasoning_effort", "effort"].find((id) => params.has(id));
  for (const token of suffix.split("-")) {
    if (token === "fast") {
      if (params.has("fast")) params.set("fast", "true");
    } else if (REASONING.has(token) && reasoningKey) {
      params.set(reasoningKey, token);
    }
  }
  return [...params].map(([id, value]) => ({ id, value }));
}

function suffixAfter(name: string, slug: string): string {
  const prefix = `${name}-`;
  return slug.startsWith(prefix) ? slug.slice(prefix.length) : "";
}
