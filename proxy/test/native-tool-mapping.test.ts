import assert from "node:assert/strict";
import test from "node:test";
import { makiToolArgs } from "../src/maki-args.ts";
import {
  buildGrepContentResult,
  buildGrepCountResult,
  buildGrepFilesResult,
  buildLsResult,
} from "../src/native-tools.ts";
import type { LsDirectoryTreeNode } from "../src/proto/agent_pb.ts";

const tools = new Set(["read", "write", "bash", "grep", "glob", "webfetch"]);

function grepCall(value: Record<string, unknown>) {
  const call = makiToolArgs({ case: "grepArgs", value }, tools, "grep-call");
  assert.ok(call, "grep call was not mapped");
  return call;
}

function lsCall(value: Record<string, unknown>) {
  const call = makiToolArgs({ case: "lsArgs", value }, tools, "ls-call");
  assert.ok(call, "ls call was not mapped");
  return call;
}

function contentMatches(content: string) {
  return buildGrepContentResult(content, false).value.matches.map((file) => ({
    file: file.file,
    matches: file.matches.map((match) => ({
      lineNumber: match.lineNumber,
      content: match.content,
      isContextLine: match.isContextLine,
    })),
  }));
}

function countResult(content: string) {
  const result = buildGrepCountResult(content, false);
  return {
    counts: result.value.counts.map((count) => ({
      file: count.file,
      count: count.count,
    })),
    totalFiles: result.value.totalFiles,
    totalMatches: result.value.totalMatches,
  };
}

function filesResult(content: string) {
  const result = buildGrepFilesResult(content, false);
  return { files: [...result.value.files], totalFiles: result.value.totalFiles };
}

/** Nested `{dir: subtree, file: null}` shape, so tree structure is easy to assert. */
type TreeShape = { [name: string]: TreeShape | null };

function treeShape(node: LsDirectoryTreeNode): TreeShape {
  const shape: TreeShape = {};
  for (const file of node.childrenFiles) shape[file.name] = null;
  for (const dir of node.childrenDirs) {
    // Directories carry no name field; the client derives it from absPath.
    const name = dir.absPath.split("/").filter(Boolean).at(-1) ?? dir.absPath;
    shape[name] = treeShape(dir);
  }
  return shape;
}

function lsTree(content: string, root: string) {
  const result = buildLsResult(content, root);
  const node = result.result.value.directoryTreeRoot!;
  return {
    node,
    rootAbsPath: node.absPath,
    numFiles: node.numFiles,
    extensions: { ...node.fullSubtreeExtensionCounts },
    shape: treeShape(node),
  };
}

function absPaths(node: LsDirectoryTreeNode): string[] {
  return [node.absPath, ...node.childrenDirs.flatMap((child) => absPaths(child))];
}

const BLOCK_OUTPUT = [
  "src/a.ts:",
  "  10: hit one",
  "  11  ctx one",
  "  12  ctx two",
  "  --",
  "  20: hit two",
  "",
  "src/b.ts:",
  "  5: other",
].join("\n");

test("maki block output parses into per-file content matches", () => {
  assert.deepEqual(contentMatches(BLOCK_OUTPUT), [
    {
      file: "src/a.ts",
      matches: [
        { lineNumber: 10, content: "hit one", isContextLine: false },
        { lineNumber: 11, content: "ctx one", isContextLine: true },
        { lineNumber: 12, content: "ctx two", isContextLine: true },
        { lineNumber: 20, content: "hit two", isContextLine: false },
      ],
    },
    {
      file: "src/b.ts",
      matches: [{ lineNumber: 5, content: "other", isContextLine: false }],
    },
  ]);

  const totals = buildGrepContentResult(BLOCK_OUTPUT, false).value;
  assert.equal(totals.totalLines, 5, "context lines count toward totalLines");
  assert.equal(totals.totalMatchedLines, 3, "context lines are not matches");
});

test("maki block output yields files with a real match only", () => {
  assert.deepEqual(filesResult(BLOCK_OUTPUT), {
    files: ["src/a.ts", "src/b.ts"],
    totalFiles: 2,
  });

  const contextOnly = [
    "src/context-only.ts:",
    "  7  just context",
    "",
    "src/hit.ts:",
    "  9: real",
  ].join("\n");
  assert.deepEqual(filesResult(contextOnly), {
    files: ["src/hit.ts"],
    totalFiles: 1,
  });
});

test("maki block output counts matches per file, ignoring context", () => {
  assert.deepEqual(countResult(BLOCK_OUTPUT), {
    counts: [
      { file: "src/a.ts", count: 2 },
      { file: "src/b.ts", count: 1 },
    ],
    totalFiles: 2,
    totalMatches: 3,
  });
});

test("legacy single-line grep formats still parse", () => {
  assert.deepEqual(contentMatches("src/a.ts:12:hello\nsrc/a.ts-13-world\nsrc/b.ts:4:other"), [
    {
      file: "src/a.ts",
      matches: [
        { lineNumber: 12, content: "hello", isContextLine: false },
        { lineNumber: 13, content: "world", isContextLine: true },
      ],
    },
    {
      file: "src/b.ts",
      matches: [{ lineNumber: 4, content: "other", isContextLine: false }],
    },
  ]);

  assert.deepEqual(countResult("src/a.ts:3\nsrc/b.ts:2"), {
    counts: [
      { file: "src/a.ts", count: 3 },
      { file: "src/b.ts", count: 2 },
    ],
    totalFiles: 2,
    totalMatches: 5,
  });
});

test("a flat glob list still fills files_with_matches", () => {
  assert.deepEqual(filesResult("src/a.ts\n\ndocs/b.md\n"), {
    files: ["src/a.ts", "docs/b.md"],
    totalFiles: 2,
  });
});

test("grep context maps to context_before/context_after", () => {
  assert.deepEqual(grepCall({ pattern: "foo", context: 2 }).args, {
    pattern: "foo",
    context_before: 2,
    context_after: 2,
  });

  // A specific side wins over the shorthand, the other side falls back to it.
  assert.deepEqual(grepCall({ pattern: "foo", context: 3, contextBefore: 1 }).args, {
    pattern: "foo",
    context_before: 1,
    context_after: 3,
  });

  assert.equal("context_before" in grepCall({ pattern: "foo" }).args, false);
});

test("caseInsensitive and multiline become inline regex flags", () => {
  assert.equal(grepCall({ pattern: "foo", caseInsensitive: true }).args.pattern, "(?i)foo");
  assert.equal(grepCall({ pattern: "foo" }).args.pattern, "foo");

  // A pattern that already opens with its own flag group is left as the author wrote it.
  assert.equal(grepCall({ pattern: "(?i)foo", caseInsensitive: true }).args.pattern, "(?i)foo");
  assert.equal(grepCall({ pattern: "(?m)foo", caseInsensitive: true }).args.pattern, "(?m)foo");

  assert.equal(grepCall({ pattern: "(?s)foo", multiline: true }).args.pattern, "(?s)foo");
  assert.equal(grepCall({ pattern: "a\\nb", multiline: true }).args.pattern, "(?s)a\\nb");
  assert.equal(
    grepCall({ pattern: "foo", caseInsensitive: true, multiline: true }).args.pattern,
    "(?s)(?i)foo",
  );
});

test("headLimit maps to maki's limit; type is not fabricated", () => {
  assert.equal(grepCall({ pattern: "foo", headLimit: 25 }).args.limit, 25);
  assert.equal("limit" in grepCall({ pattern: "foo", headLimit: 0 }).args, false);

  const typed = grepCall({ pattern: "foo", type: "ts" });
  assert.equal("include" in typed.args, false);
  assert.equal("type" in typed.args, false);
  assert.equal(typed.args.pattern, "foo");
});

test("glob still passes through, and a pattern-less grep becomes a glob", () => {
  assert.equal(grepCall({ pattern: "foo", glob: "*.ts" }).args.include, "*.ts");

  const globCall = makiToolArgs(
    { case: "grepArgs", value: { glob: "**/*.ts", path: "src", outputMode: "files_with_matches" } },
    tools,
    "glob-call",
  );
  assert.ok(globCall);
  assert.equal(globCall.toolName, "glob");
  assert.deepEqual(globCall.args, { pattern: "**/*.ts", path: "src" });
});

test("ls maps to a recursive glob", () => {
  const call = lsCall({ path: "src" });
  assert.equal(call.toolName, "glob");
  assert.deepEqual(call.args, { pattern: "**/*", path: "src" });
});

test("ls tree handles absolute glob paths under a relative root", () => {
  const tree = lsTree("/repo/src/a/b.rs\n/repo/src/c.ts\n", "src");
  assert.deepEqual(tree.shape, { a: { "b.rs": null }, "c.ts": null });
  assert.deepEqual(absPaths(tree.node), ["src", "src/a"]);
  assert.equal(tree.numFiles, 2);
  assert.deepEqual(tree.extensions, { rs: 1, ts: 1 });
});

test("ls tree handles relative paths under a relative root", () => {
  const tree = lsTree("src/a/b.rs\nsrc/c.ts\n", "src");
  assert.deepEqual(tree.shape, { a: { "b.rs": null }, "c.ts": null });
  assert.deepEqual(absPaths(tree.node), ["src", "src/a"]);

  // A path already relative to the search root is used as-is.
  const fromRoot = lsTree("a/b.rs\n", "src");
  assert.deepEqual(fromRoot.shape, { a: { "b.rs": null } });
  assert.deepEqual(absPaths(fromRoot.node), ["src", "src/a"]);
});

test("ls tree handles absolute paths under an absolute root", () => {
  const tree = lsTree("/repo/src/a/b.rs\n/repo/src/c.ts\n", "/repo/src");
  assert.equal(tree.rootAbsPath, "/repo/src");
  assert.deepEqual(tree.shape, { a: { "b.rs": null }, "c.ts": null });
  assert.deepEqual(absPaths(tree.node), ["/repo/src", "/repo/src/a"]);
  assert.equal(tree.numFiles, 2);
});

test("ls tree builds nested directories and leaks no root segments", () => {
  const tree = lsTree("/home/me/proj/src/x/y/z/deep.ts\n", "src");
  assert.deepEqual(tree.shape, { x: { y: { z: { "deep.ts": null } } } });
  assert.deepEqual(absPaths(tree.node), ["src", "src/x", "src/x/y", "src/x/y/z"]);
  for (const path of absPaths(tree.node)) {
    assert.equal(path.startsWith("/"), false, `${path} must not stay absolute`);
    assert.equal(path.includes("home"), false, `${path} leaked a root segment`);
    assert.equal(path.includes("proj"), false, `${path} leaked a root segment`);
    assert.equal(path.startsWith("repo"), false, `${path} leaked a root segment`);
  }
});

test("ls tree dedupes and drops paths outside the requested root", () => {
  const tree = lsTree(
    ["/repo/src/a.ts", "src/a.ts", "/repo/test/b.ts", "/repo/src"].join("\n"),
    "src",
  );
  assert.deepEqual(tree.shape, { "a.ts": null });
  assert.equal(tree.numFiles, 1);
});

test("ls tree keeps a cwd-relative tree when the root is the workspace", () => {
  const tree = lsTree("a/b.ts\na/c.md\n", ".");
  assert.equal(tree.rootAbsPath, ".");
  assert.deepEqual(tree.shape, { a: { "b.ts": null, "c.md": null } });
  assert.deepEqual(absPaths(tree.node), [".", "a"]);
  assert.equal(tree.numFiles, 2);
});
