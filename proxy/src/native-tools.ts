/**
 * Native tool redirection.
 *
 * Cursor models aggressively call their built-in tools (read, shell, grep,
 * ls, write, fetch) before falling back to MCP tools. Rejecting those calls
 * burns model round-trips and confuses the model ("Tool not available in
 * this environment", issues #21/#29). When the client provides an equivalent
 * OpenAI tool, redirect the native call to it and convert the tool result
 * back into Cursor's typed native result frame.
 */
import { makiToolArgs, type NativeCall } from "./maki-args.ts";
import { create, toBinary } from "@bufbuild/protobuf";
import { lengthDelimited } from "./proto-wire.ts";
import {
  AgentClientMessageSchema,
  ExecClientMessageSchema,
  FetchResultSchema,
  FetchSuccessSchema,
  GrepContentMatchSchema,
  GrepContentResultSchema,
  GrepCountResultSchema,
  GrepFileCountSchema,
  GrepFileMatchSchema,
  GrepFilesResultSchema,
  GrepResultSchema,
  GrepSuccessSchema,
  GrepUnionResultSchema,
  LsDirectoryTreeNodeSchema,
  LsDirectoryTreeNode_FileSchema,
  LsResultSchema,
  LsSuccessSchema,
  ReadResultSchema,
  ReadSuccessSchema,
  ShellFailureSchema,
  ShellPermissionDeniedSchema,
  ShellResultSchema,
  ShellStreamExitSchema,
  ShellStreamSchema,
  ShellStreamStartSchema,
  ShellStreamStdoutSchema,
  ShellSuccessSchema,
  WritePermissionDeniedSchema,
  WriteResultSchema,
  WriteSuccessSchema,
  type ExecServerMessage,
  type LsDirectoryTreeNode,
  type McpToolDefinition,
} from "./proto/agent_pb.ts";

export type NativeResultType =
  | "readResult"
  | "writeResult"
  | "fetchResult"
  | "shellResult"
  | "shellStreamResult"
  | "lsResult"
  | "grepResult";

/** How to answer the paused native exec once the redirected tool result arrives. */
export interface NativeExecBinding {
  resultType: NativeResultType;
  /** Native arg values needed to shape the typed result frame. */
  args: Record<string, string>;
  /** ShellResult field used by a shell variant this schema does not name. */
  resultField?: number;
}

export interface NativeRedirect {
  toolCallId: string;
  toolName: string;
  decodedArgs: string;
  binding: NativeExecBinding;
}

/**
 * Map a native exec request onto a client-provided OpenAI tool.
 * Returns null when no equivalent tool is available (caller rejects as before).
 */
function bindingFor(execCase: string, args: NativeCall["value"]): NativeExecBinding | null {
  if (execCase === "readArgs") {
    return { resultType: "readResult", args: { path: args.path ?? "" } };
  }
  if (execCase === "writeArgs") {
    const content =
      args.fileBytes && args.fileBytes.length > 0
        ? new TextDecoder().decode(args.fileBytes)
        : (args.fileText ?? "");
    return {
      resultType: "writeResult",
      args: {
        path: args.path ?? "",
        fileSize: String(new TextEncoder().encode(content).byteLength),
        linesCreated: String(content.split("\n").length),
      },
    };
  }
  if (execCase === "fetchArgs") {
    return { resultType: "fetchResult", args: { url: args.url ?? "" } };
  }
  if (execCase === "shellArgs" || execCase === "shellStreamArgs") {
    return {
      resultType: execCase === "shellStreamArgs" ? "shellStreamResult" : "shellResult",
      args: {
        command: args.command ?? "",
        workingDirectory: args.workingDirectory ?? "",
      },
    };
  }
  if (execCase === "lsArgs") {
    return { resultType: "lsResult", args: { path: args.path ?? "" } };
  }
  if (execCase === "grepArgs") {
    return {
      resultType: "grepResult",
      args: {
        pattern: args.pattern || args.glob || ".",
        path: args.path ?? "",
        outputMode: !args.pattern && args.glob ? "files_with_matches" : args.outputMode || "content",
        ...(args.multiline ? { multiline: "true" } : undefined),
        ...(args.headLimit != null ? { headLimit: String(args.headLimit) } : undefined),
      },
    };
  }
  return null;
}

/**
 * Map a native exec request onto a client-provided OpenAI tool.
 * Returns null when no equivalent tool is available (caller rejects as before).
 * Argument names come from `makiToolArgs`, so a redirected read is a maki `read`.
 */
export function redirectNativeExec(
  execMsg: ExecServerMessage,
  mcpTools: McpToolDefinition[],
): NativeRedirect | null {
  const execCase = execMsg.message.case;
  if (!execCase) return null;
  const args = execMsg.message.value as NativeCall["value"];
  const available = new Set(
    mcpTools.map((tool) => tool.name || tool.toolName).filter(Boolean),
  );
  const mapped = makiToolArgs(
    { case: execCase, value: args },
    available,
    args.toolCallId || crypto.randomUUID(),
  );
  const binding = bindingFor(execCase, args);
  if (!mapped || !binding) return null;
  return {
    toolCallId: mapped.toolCallId,
    toolName: mapped.toolName,
    decodedArgs: JSON.stringify(mapped.args),
    binding,
  };
}

interface PendingNativeExec {
  execId: string;
  execMsgId: number;
}

/**
 * Convert the redirected tool's text result into the typed native result the
 * paused exec expects. Returns false when no faithful conversion exists
 * (caller falls back to an mcpResult).
 * `sendMessage` receives an unframed AgentClientMessage binary.
 */
export function sendNativeExecResult(
  exec: PendingNativeExec,
  binding: NativeExecBinding,
  text: string,
  sendMessage: (bytes: Uint8Array) => void,
): boolean {
  const args = binding.args;

  const sendExec = (messageCase: string, value: unknown) => {
    const execClientMessage = create(ExecClientMessageSchema, {
      id: exec.execMsgId,
      execId: exec.execId,
      message: {
        case: messageCase as never,
        value: value as never,
      },
    });
    const clientMessage = create(AgentClientMessageSchema, {
      message: { case: "execClientMessage", value: execClientMessage },
    });
    sendMessage(toBinary(AgentClientMessageSchema, clientMessage));
  };

  switch (binding.resultType) {
    case "readResult": {
      sendExec(
        "readResult",
        create(ReadResultSchema, {
          result: {
            case: "success",
            value: create(ReadSuccessSchema, {
              path: args.path ?? "",
              totalLines: text.split("\n").length,
              fileSize: BigInt(new TextEncoder().encode(text).byteLength),
              truncated: false,
              output: { case: "content", value: text },
            }),
          },
        }),
      );
      return true;
    }

    case "writeResult": {
      sendExec("writeResult", writeResult(args, text));
      return true;
    }

    case "shellResult": {
      const built = shellResult(args, text);
      if (binding.resultField) {
        const message = create(ExecClientMessageSchema, {
          id: exec.execMsgId,
          execId: exec.execId,
        });
        message.$unknown = [{
          no: binding.resultField,
          wireType: 2,
          data: lengthDelimited(toBinary(ShellResultSchema, built)),
        }];
        const clientMessage = create(AgentClientMessageSchema, {
          message: { case: "execClientMessage", value: message },
        });
        sendMessage(toBinary(AgentClientMessageSchema, clientMessage));
        return true;
      }
      sendExec("shellResult", built);
      return true;
    }

    case "shellStreamResult": {
      sendShellStream(args, text, sendExec);
      return true;
    }

    case "fetchResult": {
      sendExec(
        "fetchResult",
        create(FetchResultSchema, {
          result: {
            case: "success",
            value: create(FetchSuccessSchema, {
              url: args.url ?? "",
              content: text,
              statusCode: 200,
            }),
          },
        }),
      );
      return true;
    }

    case "lsResult": {
      const built = buildLsResult(text, args.path ?? "");
      if (!built) return false;
      sendExec("lsResult", built);
      return true;
    }

    case "grepResult": {
      const built = buildGrepResult(text, args);
      if (!built) return false;
      sendExec("grepResult", built);
      return true;
    }
  }
}

const PERMISSION_DENIED = "Permission denied";

function isDenied(text: string): boolean {
  return text.startsWith(PERMISSION_DENIED);
}

/** bash puts a non-zero status on its own line. A clean command has no such line. */
function shellOutcome(text: string): { stdout: string; exitCode: number } {
  const lined = text.match(/^(.*)\nExit code: (\d+)$/s);
  if (lined) return { stdout: lined[1], exitCode: Number(lined[2]) };
  const only = text.match(/^Exit code: (\d+)$/);
  if (only) return { stdout: "", exitCode: Number(only[1]) };
  return { stdout: text, exitCode: 0 };
}

function writeResult(args: Record<string, string>, text: string) {
  if (isDenied(text)) {
    return create(WriteResultSchema, {
      result: {
        case: "permissionDenied",
        value: create(WritePermissionDeniedSchema, {
          path: args.path ?? "",
          directory: "",
          operation: "create_file",
          error: text,
          isReadonly: false,
        }),
      },
    });
  }
  return create(WriteResultSchema, {
    result: {
      case: "success",
      value: create(WriteSuccessSchema, {
        path: args.path ?? "",
        fileSize: Number(args.fileSize ?? 0),
        linesCreated: Number(args.linesCreated ?? 0),
      }),
    },
  });
}

function shellResult(args: Record<string, string>, text: string) {
  const command = args.command ?? "";
  const workingDirectory = args.workingDirectory ?? "";
  if (isDenied(text)) {
    return create(ShellResultSchema, {
      result: {
        case: "permissionDenied",
        value: create(ShellPermissionDeniedSchema, {
          command,
          workingDirectory,
          error: text,
          isReadonly: false,
        }),
      },
    });
  }
  const { stdout, exitCode } = shellOutcome(text);
  if (exitCode !== 0) {
    return create(ShellResultSchema, {
      result: {
        case: "failure",
        value: create(ShellFailureSchema, {
          command,
          workingDirectory,
          exitCode,
          signal: "",
          stdout,
          stderr: "",
          executionTime: 0,
        }),
      },
    });
  }
  return create(ShellResultSchema, {
    result: {
      case: "success",
      value: create(ShellSuccessSchema, {
        command,
        workingDirectory,
        exitCode: 0,
        signal: "",
        stdout,
        stderr: "",
        executionTime: 0,
      }),
    },
  });
}

function sendShellStream(
  args: Record<string, string>,
  text: string,
  sendExec: (messageCase: string, value: unknown) => void,
): void {
  const command = args.command ?? "";
  const workingDirectory = args.workingDirectory ?? "";
  if (isDenied(text)) {
    sendExec(
      "shellStream",
      create(ShellStreamSchema, {
        event: {
          case: "permissionDenied",
          value: create(ShellPermissionDeniedSchema, {
            command,
            workingDirectory,
            error: text,
            isReadonly: false,
          }),
        },
      }),
    );
  } else {
    const { stdout, exitCode } = shellOutcome(text);
    sendExec(
      "shellStream",
      create(ShellStreamSchema, {
        event: { case: "start", value: create(ShellStreamStartSchema, {}) },
      }),
    );
    if (stdout) {
      sendExec(
        "shellStream",
        create(ShellStreamSchema, {
          event: { case: "stdout", value: create(ShellStreamStdoutSchema, { data: stdout }) },
        }),
      );
    }
    sendExec(
      "shellStream",
      create(ShellStreamSchema, {
        event: {
          case: "exit",
          value: create(ShellStreamExitSchema, { code: exitCode, cwd: workingDirectory }),
        },
      }),
    );
  }
}

/**
 * Reconstruct Cursor's directory tree from glob output (one path per line).
 *
 * maki's glob output mixes relative and absolute paths: `shorten_path` makes a
 * path relative to the process cwd when it lives there, and leaves it absolute
 * otherwise. The requested root can be either form too, so entries are matched
 * back onto the root before the tree is built.
 */
export function buildLsResult(content: string, rootPath: string) {
  const normalizedRoot = cleanLsPath(rootPath);
  const rawLines = content
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  const root = create(LsDirectoryTreeNodeSchema, {
    absPath: normalizedRoot,
    childrenDirs: [],
    childrenFiles: [],
    childrenWereProcessed: true,
    fullSubtreeExtensionCounts: {},
    numFiles: 0,
  });

  const dirMap = new Map<string, LsDirectoryTreeNode>([[normalizedRoot, root]]);
  const seenFiles = new Set<string>();

  for (const rawLine of rawLines) {
    const relative = relativeToRoot(rawLine, normalizedRoot);
    if (!relative) continue;
    const parts = relative.split("/").filter(Boolean);
    if (parts.length === 0) continue;

    let currentPath = normalizedRoot;
    let currentNode = dirMap.get(normalizedRoot)!;
    for (const segment of parts.slice(0, -1)) {
      const nextPath = joinPath(currentPath, segment);
      let nextNode = dirMap.get(nextPath);
      if (!nextNode) {
        nextNode = create(LsDirectoryTreeNodeSchema, {
          absPath: nextPath,
          childrenDirs: [],
          childrenFiles: [],
          childrenWereProcessed: true,
          fullSubtreeExtensionCounts: {},
          numFiles: 0,
        });
        currentNode.childrenDirs.push(nextNode);
        dirMap.set(nextPath, nextNode);
      }
      currentPath = nextPath;
      currentNode = nextNode;
    }

    const leaf = parts.at(-1)!;
    const filePath = joinPath(currentPath, leaf);
    if (seenFiles.has(filePath)) continue;
    seenFiles.add(filePath);
    currentNode.childrenFiles.push(
      create(LsDirectoryTreeNode_FileSchema, { name: leaf }),
    );
  }

  computeLsStats(root);

  return create(LsResultSchema, {
    result: {
      case: "success",
      value: create(LsSuccessSchema, { directoryTreeRoot: root }),
    },
  });
}

/** Normalize separators, trailing slashes, and a leading "./". */
function cleanLsPath(path: string): string {
  let cleaned = path.trim().replace(/\\/g, "/").replace(/\/{2,}/g, "/");
  while (cleaned.startsWith("./")) cleaned = cleaned.slice(2);
  cleaned = cleaned.replace(/\/+$/, "");
  return cleaned === "" ? "." : cleaned;
}

function splitSegments(path: string): string[] {
  return path.split("/").filter(Boolean);
}

function indexOfSegments(haystack: string[], needle: string[]): number {
  if (needle.length === 0 || needle.length > haystack.length) return -1;
  for (let i = 0; i + needle.length <= haystack.length; i++) {
    let hit = true;
    for (let j = 0; j < needle.length; j++) {
      if (haystack[i + j] !== needle[j]) {
        hit = false;
        break;
      }
    }
    if (hit) return i;
  }
  return -1;
}

/**
 * Path of `file` below the requested root, or null when it is outside it.
 * Handles all four absolute/relative combinations.
 */
function relativeToRoot(file: string, root: string): string | null {
  const cleaned = cleanLsPath(file);
  if (cleaned === "." || cleaned === root) return null;

  if (root === ".") {
    // Everything the root's glob returned is below it. An absolute path is
    // resolved against the process cwd, which is the directory maki searched.
    if (!cleaned.startsWith("/")) return cleaned;
    const cwd = cleanLsPath(process.cwd() ?? "");
    if (cwd !== "." && cleaned.startsWith(`${cwd}/`)) {
      return cleaned.slice(cwd.length + 1);
    }
    // Unknown cwd: keep the path but drop the filesystem root marker.
    return cleaned.slice(1);
  }

  if (cleaned.startsWith(`${root}/`)) return cleaned.slice(root.length + 1);

  const rootSegments = splitSegments(root);
  const fileSegments = splitSegments(cleaned);

  // Absolute root + relative file: the root's trailing segments prefix the file.
  const overlap = Math.min(rootSegments.length, fileSegments.length);
  for (let count = overlap; count > 0; count--) {
    const rootTail = rootSegments.slice(-count).join("/");
    const fileHead = fileSegments.slice(0, count).join("/");
    if (rootTail === fileHead) {
      return fileSegments.slice(count).join("/") || null;
    }
  }

  // Relative root + absolute file: the root appears inside the absolute path.
  const at = indexOfSegments(fileSegments, rootSegments);
  if (at >= 0) {
    return fileSegments.slice(at + rootSegments.length).join("/") || null;
  }

  // A bare relative path names neither the root nor an ancestor, so it is
  // already relative to the root (some glob implementations report this way).
  if (!cleaned.startsWith("/") && !cleaned.startsWith("../")) return cleaned;

  return null;
}

function joinPath(base: string, segment: string): string {
  if (!base || base === ".") return segment;
  return `${base}/${segment}`;
}

function computeLsStats(node: LsDirectoryTreeNode): void {
  const extensionCounts: Record<string, number> = {};
  let numFiles = node.childrenFiles.length;

  for (const file of node.childrenFiles) {
    const dot = file.name.lastIndexOf(".");
    if (dot > 0 && dot < file.name.length - 1) {
      const ext = file.name.slice(dot + 1);
      extensionCounts[ext] = (extensionCounts[ext] ?? 0) + 1;
    }
  }

  for (const child of node.childrenDirs) {
    computeLsStats(child);
    numFiles += child.numFiles;
    for (const [ext, count] of Object.entries(child.fullSubtreeExtensionCounts)) {
      extensionCounts[ext] = (extensionCounts[ext] ?? 0) + count;
    }
  }

  node.numFiles = numFiles;
  node.fullSubtreeExtensionCounts = extensionCounts;
}

/** Parse the client grep tool's text output back into Cursor's structured result. */
function buildGrepResult(content: string, args: Record<string, string>) {
  const pattern = args.pattern ?? "";
  const path = args.path ?? "";
  const outputMode = args.outputMode || "content";

  if (args.multiline === "true") return null;
  if (!["content", "files_with_matches", "count"].includes(outputMode)) {
    return null;
  }

  const unionResult =
    outputMode === "count"
      ? buildGrepCountResult(content, Boolean(args.headLimit))
      : outputMode === "files_with_matches"
        ? buildGrepFilesResult(content, Boolean(args.headLimit))
        : buildGrepContentResult(content, Boolean(args.headLimit));

  // Non-empty tool output that we failed to parse: better to hand the raw
  // text back as an mcpResult than to claim "no matches".
  if (content.trim() && isEmptyGrepUnion(unionResult)) return null;

  return create(GrepResultSchema, {
    result: {
      case: "success",
      value: create(GrepSuccessSchema, {
        pattern,
        path,
        outputMode,
        workspaceResults: {
          [path || "."]: create(GrepUnionResultSchema, { result: unionResult }),
        },
      }),
    },
  });
}

export function buildGrepCountResult(content: string, clientTruncated: boolean) {
  const counts: ReturnType<typeof create<typeof GrepFileCountSchema>>[] = [];
  let totalMatches = 0;

  const parsed = parseGrepMatches(content);
  if (parsed.blockFormat) {
    // Per-file count of real matches; context lines are not matches.
    const perFile = new Map<string, number>();
    for (const match of parsed.matches) {
      if (match.isContextLine) continue;
      perFile.set(match.file, (perFile.get(match.file) ?? 0) + 1);
      totalMatches += 1;
    }
    for (const [file, count] of perFile) {
      counts.push(create(GrepFileCountSchema, { file, count }));
    }
    return countResult(counts, totalMatches, clientTruncated);
  }

  // Legacy `file:count` output.
  for (const rawLine of content.split("\n")) {
    const line = rawLine.replace(/\r$/, "");
    if (!line) continue;
    const separator = line.lastIndexOf(":");
    if (separator === -1) continue;
    const tail = line.slice(separator + 1);
    if (!/^\d+$/.test(tail)) continue;
    const file = line.slice(0, separator);
    const count = Number.parseInt(tail, 10);
    counts.push(create(GrepFileCountSchema, { file, count }));
    totalMatches += count;
  }

  return countResult(counts, totalMatches, clientTruncated);
}

function countResult(
  counts: ReturnType<typeof create<typeof GrepFileCountSchema>>[],
  totalMatches: number,
  clientTruncated: boolean,
) {
  return {
    case: "count" as const,
    value: create(GrepCountResultSchema, {
      counts,
      totalFiles: counts.length,
      totalMatches,
      clientTruncated,
      ripgrepTruncated: false,
    }),
  };
}

export function buildGrepFilesResult(content: string, clientTruncated: boolean) {
  const parsed = parseGrepMatches(content);
  const files: string[] = [];

  if (parsed.blockFormat) {
    // Only files that actually matched; a block of context lines is not a hit.
    const seen = new Set<string>();
    for (const match of parsed.matches) {
      if (match.isContextLine || seen.has(match.file)) continue;
      seen.add(match.file);
      files.push(match.file);
    }
  } else {
    // maki's glob output: one file path per line.
    for (const rawLine of content.split("\n")) {
      const line = rawLine.replace(/\r$/, "").trim();
      if (line) files.push(line);
    }
  }

  return {
    case: "files" as const,
    value: create(GrepFilesResultSchema, {
      files,
      totalFiles: files.length,
      clientTruncated,
      ripgrepTruncated: false,
    }),
  };
}

const MAX_GREP_LINE_NUMBER = 2_147_483_647;

/** GrepContentMatch.line_number is int32; an out-of-range value fails to encode. */
function safeLineNumber(value: string): number | null {
  if (!/^\d+$/.test(value)) return null;
  const number = Number.parseInt(value, 10);
  return Number.isFinite(number) && number <= MAX_GREP_LINE_NUMBER ? number : null;
}

export interface ParsedGrepMatch {
  file: string;
  lineNumber: number;
  content: string;
  isContextLine: boolean;
}

export interface ParsedGrep {
  matches: ParsedGrepMatch[];
  /** True when at least one `path/to/file:` block header was seen. */
  blockFormat: boolean;
}

/**
 * Parse grep tool output into one flat, ordered match list.
 *
 * maki renders one block per file:
 *   path/to/file:
 *     966: matched text
 *     967  context text
 *     --
 *     970: another match
 *
 * File paths can contain "-<digits>-" (a planning id), so the block header is
 * recognised before the legacy context-line fallback ever sees it. The legacy
 * single-line forms (`path:line:text` and `path-line-text`) still parse, which
 * keeps older clients and flat glob output workable.
 */
export function parseGrepMatches(content: string): ParsedGrep {
  const matches: ParsedGrepMatch[] = [];
  let currentFile = "";
  let blockFormat = false;

  const add = (file: string, lineNumber: number, text: string, isContextLine: boolean) => {
    if (!file) return;
    matches.push({ file, lineNumber, content: text, isContextLine });
  };

  for (const rawLine of content.split("\n")) {
    const line = rawLine.replace(/\r$/, "");
    if (line === "" || line.trim() === "--") continue;

    const indented = line.match(/^\s+(\d+)([: ]) ?(.*)$/);
    if (indented) {
      const lineNumber = safeLineNumber(indented[1]!);
      if (lineNumber === null) continue;
      add(currentFile, lineNumber, indented[3]!, indented[2] === " ");
      continue;
    }

    const matchLine = line.match(/^(.+?):(\d+):(.*)/);
    if (matchLine) {
      const lineNumber = safeLineNumber(matchLine[2]!);
      if (lineNumber === null) continue;
      const file = matchLine[1]!;
      if (file !== currentFile) currentFile = file;
      add(file, lineNumber, matchLine[3]!, false);
      continue;
    }

    if (!/^\s/.test(line) && line.endsWith(":")) {
      blockFormat = true;
      currentFile = line.slice(0, -1);
      continue;
    }

    const contextLine = parseGrepContextLine(line, currentFile);
    if (!contextLine) continue;
    if (contextLine.file !== currentFile) currentFile = contextLine.file;
    add(contextLine.file, contextLine.lineNumber, contextLine.content, true);
  }

  return { matches, blockFormat };
}

/** Parse the client grep tool's text output back into Cursor's structured result. */
export function buildGrepContentResult(content: string, clientTruncated: boolean) {
  const fileMatches: ReturnType<typeof create<typeof GrepFileMatchSchema>>[] = [];
  const groups = new Map<
    string,
    ReturnType<typeof create<typeof GrepContentMatchSchema>>[]
  >();
  let totalLines = 0;
  let totalMatchedLines = 0;

  for (const match of parseGrepMatches(content).matches) {
    // totalLines counts context lines too; totalMatchedLines only real matches.
    totalLines += 1;
    if (!match.isContextLine) totalMatchedLines += 1;
    let group = groups.get(match.file);
    if (!group) {
      group = [];
      groups.set(match.file, group);
    }
    group.push(
      create(GrepContentMatchSchema, {
        lineNumber: match.lineNumber,
        content: match.content,
        contentTruncated: false,
        isContextLine: match.isContextLine,
      }),
    );
  }

  for (const [file, matches] of groups) {
    if (matches.length === 0) continue;
    fileMatches.push(create(GrepFileMatchSchema, { file, matches }));
  }

  return {
    case: "content" as const,
    value: create(GrepContentResultSchema, {
      matches: fileMatches,
      totalLines,
      totalMatchedLines,
      clientTruncated,
      ripgrepTruncated: false,
    }),
  };
}

function parseGrepContextLine(line: string, currentFile: string) {
  // maki writes context lines indented under their file header; those are
  // handled above. This fallback only serves the legacy "path-line-text" form.
  if (/^\s/.test(line)) return null;

  if (currentFile) {
    const prefix = `${currentFile}-`;
    if (line.startsWith(prefix)) {
      const match = line.slice(prefix.length).match(/^(\d+)-(.*)$/s);
      const lineNumber = match ? safeLineNumber(match[1]!) : null;
      if (lineNumber !== null) {
        return { file: currentFile, lineNumber, content: match![2]! };
      }
    }
  }

  const fallback = line.match(/^(.+?)-(\d+)-(.*)$/);
  if (!fallback) return null;
  // Require the captured "file" to look like a path, so a stray number in
  // content cannot become the line number.
  if (!fallback[1]!.includes("/") && !fallback[1]!.includes(".")) return null;
  const lineNumber = safeLineNumber(fallback[2]!);
  if (lineNumber === null) return null;
  return {
    file: fallback[1]!,
    lineNumber,
    content: fallback[3]!,
  };
}

function isEmptyGrepUnion(
  result:
    | ReturnType<typeof buildGrepCountResult>
    | ReturnType<typeof buildGrepFilesResult>
    | ReturnType<typeof buildGrepContentResult>,
): boolean {
  if (result.case === "count") return result.value.counts.length === 0;
  if (result.case === "files") return result.value.files.length === 0;
  return result.value.matches.length === 0;
}
