/** Cursor native exec fields the redirect reads. Plain data, so tests skip protobuf. */
export interface NativeCall {
  case: string;
  value: {
    toolCallId?: string;
    path?: string;
    command?: string;
    workingDirectory?: string;
    timeout?: number;
    url?: string;
    pattern?: string;
    glob?: string;
    fileText?: string;
    fileBytes?: Uint8Array;
    outputMode?: string;
    context?: number;
    contextBefore?: number;
    contextAfter?: number;
    caseInsensitive?: boolean;
    multiline?: boolean;
    type?: string;
    headLimit?: number;
  };
}

export interface MakiToolCall {
  toolCallId: string;
  toolName: string;
  args: Record<string, unknown>;
}

function pick(available: ReadonlySet<string>, candidates: readonly string[]): string | undefined {
  return candidates.find((name) => available.has(name));
}

function textFrom(value: NativeCall["value"]): string {
  if (value.fileBytes && value.fileBytes.length > 0) {
    return new TextDecoder().decode(value.fileBytes);
  }
  return value.fileText ?? "";
}

/**
 * Map a Cursor built-in tool onto a maki tool call.
 * Returns null when maki did not offer an equivalent tool, so the caller
 * rejects the native call instead of running it here.
 */
export function makiToolArgs(
  call: NativeCall,
  available: ReadonlySet<string>,
  toolCallId: string,
): MakiToolCall | null {
  if (call.case === "readArgs") {
    const toolName = pick(available, ["read"]);
    if (!toolName) return null;
    return {
      toolCallId,
      toolName,
      args: { path: call.value.path ?? "", offset: 1, limit: 0 },
    };
  }

  if (call.case === "writeArgs") {
    const toolName = pick(available, ["write"]);
    if (!toolName) return null;
    return {
      toolCallId,
      toolName,
      args: { path: call.value.path ?? "", content: textFrom(call.value) },
    };
  }

  if (call.case === "fetchArgs") {
    const toolName = pick(available, ["webfetch", "fetch", "web_fetch"]);
    if (!toolName) return null;
    return {
      toolCallId,
      toolName,
      args: { url: call.value.url ?? "", format: "markdown" },
    };
  }

  if (call.case === "shellArgs" || call.case === "shellStreamArgs") {
    const toolName = pick(available, ["bash"]);
    if (!toolName) return null;
    const args: Record<string, unknown> = { command: call.value.command ?? "" };
    if (call.value.workingDirectory) args.workdir = call.value.workingDirectory;
    if ((call.value.timeout ?? 0) > 0) args.timeout = call.value.timeout;
    return { toolCallId, toolName, args };
  }

  if (call.case === "lsArgs") {
    const toolName = pick(available, ["glob"]);
    if (!toolName) return null;
    // maki's `*` is not recursive, but Cursor's ls returns a directory tree.
    return {
      toolCallId,
      toolName,
      args: { pattern: "**/*", path: call.value.path ?? "" },
    };
  }

  if (call.case === "grepArgs") {
    if (!call.value.pattern && call.value.glob) {
      const toolName = pick(available, ["glob"]);
      if (!toolName) return null;
      return {
        toolCallId,
        toolName,
        args: { pattern: call.value.glob, path: call.value.path ?? "" },
      };
    }
    const toolName = pick(available, ["grep"]);
    if (!toolName) return null;
    const args: Record<string, unknown> = { pattern: grepPattern(call.value) };
    if (call.value.path) args.path = call.value.path;
    if (call.value.glob) args.include = call.value.glob;
    // `context` is the shorthand for the same count on both sides.
    const contextBefore = call.value.contextBefore ?? call.value.context;
    const contextAfter = call.value.contextAfter ?? call.value.context;
    if ((contextBefore ?? 0) > 0) args.context_before = contextBefore;
    if ((contextAfter ?? 0) > 0) args.context_after = contextAfter;
    // maki caps one call at MAX_PER_CALL_LIMIT groups and names the arg `limit`.
    if ((call.value.headLimit ?? 0) > 0) args.limit = call.value.headLimit;
    // Cursor's `type` (file type filter) has no maki counterpart; maki's
    // `include` is a glob, so fabricating one from a type would be wrong.
    return { toolCallId, toolName, args };
  }

  return null;
}

/**
 * Cursor spells case and dotall as separate booleans, maki reads them from the
 * regex itself. `(?i)`/`(?s)` are inline flags, so this keeps the pattern a
 * valid regex. A pattern that already opens with its own flag group is left
 * alone: a second group could contradict the author's intent.
 */
const INLINE_FLAG_GROUP = /^\(\?[a-zA-Z]+(?:-[a-zA-Z]+)?[:)]/;

function grepPattern(value: NativeCall["value"]): string {
  let pattern = value.pattern || ".";
  const leadingFlags = pattern.match(INLINE_FLAG_GROUP)?.[1] ?? "";
  if (value.caseInsensitive && !INLINE_FLAG_GROUP.test(pattern)) {
    pattern = `(?i)${pattern}`;
  }
  // maki also auto-enables multiline for a pattern containing "\n", "(?s)",
  // or "(?m)"; adding dotall is harmless and makes `.` span newlines.
  if (value.multiline && !leadingFlags.includes("s") && !pattern.includes("(?s")) {
    pattern = `(?s)${pattern}`;
  }
  return pattern;
}
