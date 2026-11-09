export interface ConversationFile {
  name: string;
  mtimeMs: number;
  size: number;
}

export interface ConversationPruneLimits {
  ttlMs: number;
  maxFiles: number;
  maxBytes: number;
}

const DEFAULT_MAX_FILES = 5000;
const DEFAULT_MAX_BYTES = 2 * 1024 ** 3;

function positiveInteger(raw: string | undefined, fallback: number): number {
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) return fallback;
  return value;
}

function byAge(a: ConversationFile, b: ConversationFile): number {
  if (a.mtimeMs !== b.mtimeMs) return a.mtimeMs - b.mtimeMs;
  if (a.name < b.name) return -1;
  if (a.name > b.name) return 1;
  return 0;
}

/** Parse MAKI_CURSOR_CONVERSATIONS_MAX_FILES (default 5000) and MAKI_CURSOR_CONVERSATIONS_MAX_BYTES (default 2 * 1024 ** 3). Invalid/non-positive values fall back to the default. */
export function conversationPruneLimitsFromEnv(
  env: NodeJS.ProcessEnv,
  ttlMs: number,
): ConversationPruneLimits {
  return {
    ttlMs,
    maxFiles: positiveInteger(env.MAKI_CURSOR_CONVERSATIONS_MAX_FILES, DEFAULT_MAX_FILES),
    maxBytes: positiveInteger(env.MAKI_CURSOR_CONVERSATIONS_MAX_BYTES, DEFAULT_MAX_BYTES),
  };
}

/** Return the names of files that must be deleted. Pure; never mutates input. */
export function selectConversationFilesToPrune(
  entries: readonly ConversationFile[],
  nowMs: number,
  limits: ConversationPruneLimits,
): string[] {
  const cutoff = nowMs - limits.ttlMs;
  const expired: ConversationFile[] = [];
  const kept: ConversationFile[] = [];
  for (const entry of entries) {
    if (entry.mtimeMs < cutoff) expired.push(entry);
    else kept.push(entry);
  }

  const ordered = [...kept].sort(byAge);
  let count = ordered.length;
  let bytes = ordered.reduce((sum, entry) => sum + entry.size, 0);
  const evicted: ConversationFile[] = [];
  let cursor = 0;
  while ((count > limits.maxFiles || bytes > limits.maxBytes) && cursor < ordered.length) {
    const entry = ordered[cursor]!;
    evicted.push(entry);
    count -= 1;
    bytes -= entry.size;
    cursor += 1;
  }

  return [...expired, ...evicted].sort(byAge).map((entry) => entry.name);
}
