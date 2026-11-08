import assert from "node:assert/strict";
import test from "node:test";
import { fromBinary } from "@bufbuild/protobuf";
import { AgentClientMessageSchema } from "../src/proto/agent_pb.ts";
import {
  buildGrepFilesResult,
  buildGrepResult,
  buildLsResult,
  readResult,
  sendNativeExecResult,
  shellOutcome,
  type NativeExecBinding,
} from "../src/native-tools.ts";

function nativeMessage(binding: NativeExecBinding, text: string) {
  const frames: Uint8Array[] = [];
  const sent = sendNativeExecResult(
    { execId: "exec-1", execMsgId: 1 },
    binding,
    text,
    (bytes) => frames.push(bytes),
  );
  assert.equal(sent, true);
  assert.equal(frames.length, 1);
  const decoded = fromBinary(AgentClientMessageSchema, frames[0]!);
  assert.equal(decoded.message.case, "execClientMessage");
  if (decoded.message.case !== "execClientMessage") throw new Error("unreachable");
  return decoded.message.value.message;
}

function readValue(text: string, path = "/repo/a.ts") {
  const message = nativeMessage({ resultType: "readResult", args: { path } }, text);
  assert.equal(message.case, "readResult");
  if (message.case !== "readResult") throw new Error("unreachable");
  return message.value.result;
}

function writeValue(text: string, path = "/repo/a.ts") {
  const message = nativeMessage({ resultType: "writeResult", args: { path } }, text);
  assert.equal(message.case, "writeResult");
  if (message.case !== "writeResult") throw new Error("unreachable");
  return message.value.result;
}

function fetchValue(text: string, url = "https://example.com") {
  const message = nativeMessage({ resultType: "fetchResult", args: { url } }, text);
  assert.equal(message.case, "fetchResult");
  if (message.case !== "fetchResult") throw new Error("unreachable");
  return message.value.result;
}

function files(content: string) {
  const result = buildGrepFilesResult(content, false);
  return { files: [...result.value.files], totalFiles: result.value.totalFiles };
}

test("read strips maki's line numbers and reads the truncation hint", () => {
  const plain = readResult({ path: "/tmp/a.txt" }, "1: alpha\n2: beta");
  assert.equal(plain.result.case, "success");
  if (plain.result.case === "success") {
    assert.equal(plain.result.value.output.case, "content");
    if (plain.result.value.output.case === "content") {
      assert.equal(plain.result.value.output.value, "alpha\nbeta");
    }
    assert.equal(plain.result.value.totalLines, 2);
    assert.equal(plain.result.value.truncated, false);
  }

  const truncated = readResult(
    { path: "/tmp/a.txt" },
    "  9: a\n 10: b\n\n...\n\nTruncated lines: 11-20. Use offset=11 to read further.",
  );
  assert.equal(truncated.result.case, "success");
  if (truncated.result.case === "success") {
    if (truncated.result.value.output.case === "content") {
      assert.equal(truncated.result.value.output.value, "a\nb");
    }
    assert.equal(truncated.result.value.totalLines, 20);
    assert.equal(truncated.result.value.truncated, true);
  }
});

test("shellOutcome reads a failed command's exit line after a log path", () => {
  assert.deepEqual(shellOutcome("line1\nline2\nExit code: 1\nFull output: /tmp/log"), {
    stdout: "line1\nline2",
    exitCode: 1,
  });
});

test("shellOutcome reads a failed command without a log", () => {
  assert.deepEqual(shellOutcome("boom\nExit code: 2"), { stdout: "boom", exitCode: 2 });
});

test("shellOutcome leaves success output untouched", () => {
  assert.deepEqual(shellOutcome("hi\n"), { stdout: "hi\n", exitCode: 0 });
});

test("shellOutcome treats a lone exit 0 as an empty success", () => {
  assert.deepEqual(shellOutcome("Exit code: 0"), { stdout: "", exitCode: 0 });
});

test("shellOutcome keeps a successful exit code when a log path is appended", () => {
  assert.deepEqual(shellOutcome("hi\nExit code: 0\nFull output: /tmp/log"), {
    stdout: "hi",
    exitCode: 0,
  });
});

test("shellOutcome strips a log pointer from a successful command", () => {
  assert.deepEqual(shellOutcome("hi\nFull output: /tmp/log"), {
    stdout: "hi",
    exitCode: 0,
  });
});

test("shellOutcome sees the status before a full-limit note", () => {
  assert.deepEqual(
    shellOutcome("out\nExit code: 1\n[output passed the full-mode limit; redirect it]"),
    { stdout: "out", exitCode: 1 },
  );
});

test("a permission-denied read is permissionDenied, not a generic error", () => {
  const result = readValue("read error: [Errno 13] Permission denied: /repo/a.ts");
  assert.equal(result.case, "permissionDenied");
});

test("an ls error is an LsResult error, not a file named 'error:'", () => {
  const result = buildLsResult("error: permission denied", "/repo");
  assert.equal(result.result.case, "error");
  if (result.result.case !== "error") return;
  assert.match(result.result.value.error, /permission denied/);
});

test("an ls tree handles ~/ paths", () => {
  const result = buildLsResult("~/proj/src/a.ts", "~/proj/src");
  assert.equal(result.result.case, "success");
  if (result.result.case !== "success") return;
  assert.equal(result.result.value.directoryTreeRoot?.childrenFiles[0]?.name, "a.ts");
});

test("grep output markers mark the result as truncated", () => {
  const result = buildGrepResult(
    "src/a.ts:1:hit\n\n[truncated 10 bytes]",
    { pattern: "hit", outputMode: "files_with_matches" },
  );
  assert.ok(result);
  if (!result) return;
  const union = result.result.value.workspaceResults["."]?.result;
  assert.equal(union?.case, "files");
  if (union?.case !== "files") return;
  assert.equal(union.value.clientTruncated, true);
});

test("read error text becomes a ReadResult error", () => {
  const readError = readValue("read error: ENOENT: no such file");
  assert.equal(readError.case, "error");
  if (readError.case !== "error") return;
  assert.equal(readError.value.path, "/repo/a.ts");
  assert.equal(readError.value.error, "read error: ENOENT: no such file");

  const pathError = readValue("error: path not found: /repo/missing");
  assert.equal(pathError.case, "error");
  if (pathError.case !== "error") return;
  assert.equal(pathError.value.path, "/repo/a.ts");
  assert.equal(pathError.value.error, "error: path not found: /repo/missing");
});

test("read permission denial becomes permissionDenied", () => {
  const denied = readValue("Permission denied: /repo/a.ts");
  assert.equal(denied.case, "permissionDenied");
  if (denied.case !== "permissionDenied") return;
  assert.equal(denied.value.path, "/repo/a.ts");
});

test("read success strips maki's line numbers", () => {
  const success = readValue("1: hi\n2: there");
  assert.equal(success.case, "success");
  if (success.case !== "success") return;
  assert.deepEqual(success.value.output, { case: "content", value: "hi\nthere" });
  assert.equal(success.value.totalLines, 2);
});

test("write error text becomes a WriteResult error", () => {
  for (const text of ["write error: disk full", "error: cannot write"]) {
    const result = writeValue(text);
    assert.equal(result.case, "error");
    if (result.case !== "error") return;
    assert.equal(result.value.path, "/repo/a.ts");
    assert.equal(result.value.error, text);
  }
});

test("write permission denial stays permissionDenied", () => {
  const denied = writeValue("Permission denied: /repo/a.ts");
  assert.equal(denied.case, "permissionDenied");
  if (denied.case !== "permissionDenied") return;
  assert.equal(denied.value.path, "/repo/a.ts");
});

test("write success remains a success", () => {
  const success = writeValue("Wrote 3 lines to /repo/a.ts");
  assert.equal(success.case, "success");
  if (success.case !== "success") return;
  assert.equal(success.value.path, "/repo/a.ts");
});

test("fetch HTTP errors become FetchResult errors with the status text", () => {
  const httpError = fetchValue("error: HTTP 404 Not Found");
  assert.equal(httpError.case, "error");
  if (httpError.case !== "error") return;
  assert.equal(httpError.value.url, "https://example.com");
  assert.equal(httpError.value.error, "error: HTTP 404 Not Found");
});

test("fetch non-HTTP errors become FetchResult errors", () => {
  const result = fetchValue("error: connection refused");
  assert.equal(result.case, "error");
  if (result.case !== "error") return;
  assert.equal(result.value.url, "https://example.com");
  assert.equal(result.value.error, "error: connection refused");
});

test("fetch success keeps the body and status code", () => {
  const success = fetchValue("<html>ok</html>");
  assert.equal(success.case, "success");
  if (success.case !== "success") return;
  assert.equal(success.value.content, "<html>ok</html>");
  assert.equal(success.value.statusCode, 200);
});

test("files mode drops maki's empty and truncation sentinels", () => {
  assert.deepEqual(files("No files found"), { files: [], totalFiles: 0 });

  assert.deepEqual(
    files(
      [
        "src/a.ts",
        "[truncated 123 bytes]",
        "src/b.ts",
        "[output passed the full-mode limit; 5000 bytes]",
      ].join("\n"),
    ),
    { files: ["src/a.ts", "src/b.ts"], totalFiles: 2 },
  );

  assert.deepEqual(files("[truncated 5 lines, 20 bytes]"), { files: [], totalFiles: 0 });
});

test("an empty files result falls back to raw text instead of claiming no matches", () => {
  const result = buildGrepResult("No files found", {
    pattern: "needle",
    path: ".",
    outputMode: "files_with_matches",
  });
  assert.equal(result, null);
});

test("ls drops sentinels so an empty directory is an empty tree", () => {
  const result = buildLsResult("No files found", "src");
  assert.equal(result.result.case, "success");
  if (result.result.case !== "success") return;
  const root = result.result.value.directoryTreeRoot!;
  assert.equal(root.numFiles, 0);
  assert.deepEqual([...root.childrenFiles], []);
  assert.deepEqual([...root.childrenDirs], []);

  const truncated = buildLsResult("src/a.ts\n[truncated 9 bytes]\nNo files found", "src");
  assert.equal(truncated.result.case, "success");
  if (truncated.result.case !== "success") return;
  const truncatedRoot = truncated.result.value.directoryTreeRoot!;
  assert.equal(truncatedRoot.numFiles, 1);
  assert.deepEqual(
    truncatedRoot.childrenFiles.map((file) => file.name),
    ["a.ts"],
  );
});
