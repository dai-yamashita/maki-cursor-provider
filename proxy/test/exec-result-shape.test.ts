/**
 * Shape of the MCP result answerPendingExecs sends back to Cursor.
 *
 * - The follow-up user message must be attached to exactly one exec. Most
 *   execs carry execId "", so keying on the id appended it to every one.
 * - A maki tool result that reports `error:` or `Permission denied` is a
 *   failure, not a successful MCP result with an error-looking text body.
 */
import assert from "node:assert/strict";
import { join } from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";
import { fromBinary } from "@bufbuild/protobuf";
import { AgentClientMessageSchema } from "../src/proto/agent_pb.ts";
import type { AnsweredExec } from "../src/proxy.ts";

// Keep proxyLog out of the checkout: answerPendingExecs logs graph state.
process.env.MAKI_CURSOR_CREDENTIALS = join(
  tmpdir(),
  "maki-cursor-exec-result-shape",
  "credentials.json",
);
const { answerPendingExecs } = await import("../src/proxy.ts");

interface DecodedMcpResult {
  execMsgId: number;
  case: "success" | "error" | "rejected" | undefined;
  text: string;
}

function decodeMcpResult(frame: Uint8Array): DecodedMcpResult | null {
  const message = fromBinary(AgentClientMessageSchema, frame.subarray(5));
  if (message.message.case !== "execClientMessage") return null;
  const exec = message.message.value;
  if (exec.message.case !== "mcpResult") return null;
  const result = exec.message.value.result;
  if (result.case === "success") {
    const item = result.value.content[0];
    return {
      execMsgId: exec.id,
      case: "success",
      text: item?.content.case === "text" ? item.content.value.text : "",
    };
  }
  if (result.case === "error") {
    return { execMsgId: exec.id, case: "error", text: result.value.error };
  }
  return { execMsgId: exec.id, case: result.case, text: "" };
}

function answer(
  execs: AnsweredExec[],
  results: { toolCallId: string; content: string }[],
  userText = "",
): DecodedMcpResult[] {
  const frames: Uint8Array[] = [];
  answerPendingExecs(execs, results, userText, (frame) => frames.push(frame));
  return frames
    .map(decodeMcpResult)
    .filter((result): result is DecodedMcpResult => result !== null);
}

test("the user message is appended to only the last exec when execIds collide", () => {
  const decoded = answer(
    [
      { execId: "", execMsgId: 1, toolCallId: "call_a" },
      { execId: "", execMsgId: 2, toolCallId: "call_b" },
    ],
    [
      { toolCallId: "call_a", content: "A" },
      { toolCallId: "call_b", content: "B" },
    ],
    "please continue",
  );

  assert.equal(decoded.length, 2);
  assert.equal(decoded[0]!.text, "A", "the first exec must not carry the user message");
  assert.equal(
    decoded[1]!.text,
    "B\n\n<user_message>\nplease continue\n</user_message>",
    "the last exec must carry the user message",
  );
  const occurrences = decoded
    .map((result) => result.text.split("please continue").length - 1)
    .reduce((sum, count) => sum + count, 0);
  assert.equal(occurrences, 1, "the user message must appear exactly once");
});

test("a native read result never embeds the user message or line numbers", () => {
  const frames: Uint8Array[] = [];
  answerPendingExecs(
    [
      {
        execId: "exec-read",
        execMsgId: 9,
        toolCallId: "call_read",
        native: { resultType: "readResult", args: { path: "/repo/a.ts" } },
      },
    ],
    [{ toolCallId: "call_read", content: "1: alpha\n2: beta" }],
    "please continue",
    (frame) => frames.push(frame),
  );

  const read = frames
    .map((frame) => fromBinary(AgentClientMessageSchema, frame.subarray(5)))
    .find((message) => message.message.case === "execClientMessage");
  assert.ok(read);
  if (read.message.case !== "execClientMessage") return;
  const exec = read.message.value;
  assert.equal(exec.message.case, "readResult");
  if (exec.message.case !== "readResult") return;
  const result = exec.message.value.result;
  assert.equal(result.case, "success");
  if (result.case !== "success") return;
  const output = result.value.output;
  assert.equal(output.case, "content");
  if (output.case !== "content") return;
  assert.equal(output.value, "alpha\nbeta");
  assert.doesNotMatch(output.value, /<user_message>/);
});

test("an AskQuestion last exec does not leak the user message to earlier execs", () => {
  // The question result strips the suffix itself; the earlier exec must stay
  // clean either way.
  const frames: Uint8Array[] = [];
  answerPendingExecs(
    [
      { execId: "", execMsgId: 5, toolCallId: "call_a" },
      {
        execId: "",
        execMsgId: 0,
        toolCallId: "call_q",
        interaction: {
          id: 7,
          // Only `questions` is read when the answer is parsed.
          args: { questions: [] } as never,
        },
      },
    ],
    [
      { toolCallId: "call_a", content: "A" },
      { toolCallId: "call_q", content: "Q1: Yes" },
    ],
    "typed after the question",
    (frame) => frames.push(frame),
  );

  const mcpFrames = frames
    .map(decodeMcpResult)
    .filter((result): result is DecodedMcpResult => result !== null);
  assert.equal(mcpFrames.length, 1);
  assert.equal(mcpFrames[0]!.text, "A");
  assert.doesNotMatch(mcpFrames[0]!.text, /typed after the question/);
});

test("a tool result starting with error: becomes an MCP error result", () => {
  const [decoded] = answer(
    [{ execId: "exec_err", execMsgId: 3, toolCallId: "call_err" }],
    [{ toolCallId: "call_err", content: "error: command not found" }],
  );

  assert.ok(decoded);
  assert.equal(decoded.case, "error", "a failure must not be encoded as success");
  assert.equal(decoded.text, "error: command not found");
});

test("a permission denial becomes an MCP error result", () => {
  const [decoded] = answer(
    [{ execId: "exec_denied", execMsgId: 4, toolCallId: "call_denied" }],
    [{ toolCallId: "call_denied", content: "Permission denied for write" }],
  );

  assert.ok(decoded);
  assert.equal(decoded.case, "error");
  assert.equal(decoded.text, "Permission denied for write");
});

test("a normal tool result stays a successful MCP result", () => {
  const [decoded] = answer(
    [{ execId: "exec_ok", execMsgId: 5, toolCallId: "call_ok" }],
    [{ toolCallId: "call_ok", content: "file contents" }],
  );

  assert.ok(decoded);
  assert.equal(decoded.case, "success");
  assert.equal(decoded.text, "file contents");
});
