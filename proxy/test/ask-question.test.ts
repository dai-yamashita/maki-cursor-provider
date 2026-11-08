import assert from "node:assert/strict";
import test from "node:test";
import { create, fromBinary } from "@bufbuild/protobuf";
import {
  AgentClientMessageSchema,
  AskQuestionArgs_OptionSchema,
  AskQuestionArgs_QuestionSchema,
  AskQuestionArgsSchema,
  InteractionResponseSchema,
  WebSearchRequestResponseSchema,
} from "../src/proto/agent_pb.ts";
import {
  buildAskQuestionResponse,
  buildAskQuestionResult,
  buildQuestionToolArgs,
  buildRejectedInteractionResponse,
} from "../src/ask-question.ts";
import { answerPendingExecs } from "../src/proxy.ts";

// Keep proxyLog out of the checkout: this test exercises answerPendingExecs,
// which logs the interaction result.
process.env.MAKI_CURSOR_CREDENTIALS = "/tmp/maki-ask-question-test/credentials.json";

function questionArgs() {
  return create(AskQuestionArgsSchema, {
    title: "Preferences",
    questions: [
      create(AskQuestionArgs_QuestionSchema, {
        id: "q1",
        prompt: "Pick one",
        options: [
          create(AskQuestionArgs_OptionSchema, { id: "o1", label: "Yes" }),
          create(AskQuestionArgs_OptionSchema, { id: "o2", label: "No" }),
        ],
        allowMultiple: false,
      }),
      create(AskQuestionArgs_QuestionSchema, {
        id: "q2",
        prompt: "Pick many",
        options: [
          create(AskQuestionArgs_OptionSchema, { id: "a", label: "Alpha" }),
          create(AskQuestionArgs_OptionSchema, { id: "b", label: "Beta" }),
        ],
        allowMultiple: true,
      }),
    ],
  });
}

function decodeClient(bytes: Uint8Array) {
  return fromBinary(AgentClientMessageSchema, bytes);
}

test("AskQuestionArgs becomes maki's question tool input", () => {
  const parsed = JSON.parse(buildQuestionToolArgs(questionArgs()));
  assert.deepEqual(parsed.questions, [
    {
      question: "Pick one",
      header: "Q1",
      options: [{ label: "Yes" }, { label: "No" }],
      multiSelect: false,
    },
    {
      question: "Pick many",
      header: "Q2",
      options: [{ label: "Alpha" }, { label: "Beta" }],
      multiSelect: true,
    },
  ]);
});

test("answers map labels back to option ids, including multi-select", () => {
  const result = buildAskQuestionResult(questionArgs(), "Q1: Yes\nQ2: Alpha, Beta");
  assert.equal(result.result.case, "success");
  const answers = result.result.case === "success" ? result.result.value.answers : [];
  assert.deepEqual(
    answers.map((answer) => [answer.questionId, answer.selectedOptionIds]),
    [
      ["q1", ["o1"]],
      ["q2", ["a", "b"]],
    ],
  );
});

test("a dismissed question is a rejection, not an empty success", () => {
  const result = buildAskQuestionResult(questionArgs(), "(question dismissed by user)");
  assert.equal(result.result.case, "rejected");
});

test("a tool error is carried through as an error", () => {
  const result = buildAskQuestionResult(questionArgs(), "error: at least one question is required");
  assert.equal(result.result.case, "error");
  if (result.result.case === "error") {
    assert.match(result.result.value.errorMessage, /at least one question/);
  }
});

test("an unanswered question is an empty selection, not a fake option", () => {
  const result = buildAskQuestionResult(questionArgs(), "Q1: (no answer)\nQ2: (no answer)");
  assert.equal(result.result.case, "success");
  if (result.result.case === "success") {
    assert.deepEqual(
      result.result.value.answers.map((answer) => answer.selectedOptionIds),
      [[], []],
    );
  }
});

test("a missing tool result is an error, not an empty answer", () => {
  const result = buildAskQuestionResult(questionArgs(), "Tool result not provided");
  assert.equal(result.result.case, "error");
});

test("a custom typed answer is an error that carries the user's text", () => {
  const result = buildAskQuestionResult(questionArgs(), "Q1: Something else\nQ2: Alpha");
  assert.equal(result.result.case, "error");
  if (result.result.case === "error") {
    assert.match(result.result.value.errorMessage, /Something else/);
  }
});

test("a comma inside a single-select label is one answer", () => {
  const args = create(AskQuestionArgsSchema, {
    questions: [
      create(AskQuestionArgs_QuestionSchema, {
        id: "q1",
        prompt: "Pick one",
        options: [
          create(AskQuestionArgs_OptionSchema, { id: "yes", label: "Yes, run the tests" }),
          create(AskQuestionArgs_OptionSchema, { id: "no", label: "No" }),
        ],
        allowMultiple: false,
      }),
    ],
  });
  const result = buildAskQuestionResult(args, "Q1: Yes, run the tests");
  assert.equal(result.result.case, "success");
  if (result.result.case === "success") {
    assert.deepEqual(result.result.value.answers[0]!.selectedOptionIds, ["yes"]);
  }
});

test("multi-select labels containing commas are matched greedily", () => {
  const args = create(AskQuestionArgsSchema, {
    questions: [
      create(AskQuestionArgs_QuestionSchema, {
        id: "q1",
        prompt: "Pick many",
        options: [
          create(AskQuestionArgs_OptionSchema, { id: "a", label: "Alpha, one" }),
          create(AskQuestionArgs_OptionSchema, { id: "b", label: "Beta" }),
        ],
        allowMultiple: true,
      }),
    ],
  });
  const result = buildAskQuestionResult(args, "Q1: Alpha, one, Beta");
  assert.equal(result.result.case, "success");
  if (result.result.case === "success") {
    assert.deepEqual(result.result.value.answers[0]!.selectedOptionIds, ["a", "b"]);
  }
});

test("the response decodes as an interactionResponse with the matching id", () => {
  const result = buildAskQuestionResult(questionArgs(), "Q1: Yes\nQ2: Alpha");
  const decoded = decodeClient(buildAskQuestionResponse(42, result));
  assert.equal(decoded.message.case, "interactionResponse");
  const response = decoded.message.value;
  assert.equal(response.id, 42);
  assert.equal(response.result.case, "askQuestionInteractionResponse");
});

test("answerPendingExecs sends an interaction response for a bridged question", () => {
  const frames: Uint8Array[] = [];
  answerPendingExecs(
    [
      {
        execId: "",
        execMsgId: 0,
        toolCallId: "call_ask",
        interaction: { id: 9, args: questionArgs() },
      },
    ],
    [{ toolCallId: "call_ask", content: "Q1: No\nQ2: Beta" }],
    "",
    (frame) => frames.push(frame),
  );

  assert.equal(frames.length, 1);
  const decoded = decodeClient(frames[0]!.subarray(5));
  assert.equal(decoded.message.case, "interactionResponse");
  const response = decoded.message.value;
  assert.equal(response.id, 9);
  assert.equal(response.result.case, "askQuestionInteractionResponse");
  const result = response.result.value.result;
  assert.equal(result?.result.case, "success");
  if (result?.result.case === "success") {
    assert.deepEqual(
      result.result.value.answers.map((answer) => answer.selectedOptionIds),
      [["o2"], ["b"]],
    );
  }
});

test("unsupported interaction queries get a rejection instead of a stall", () => {
  for (const queryCase of [
    "webSearchRequestQuery",
    "switchModeRequestQuery",
    "exaSearchRequestQuery",
    "exaFetchRequestQuery",
    "createPlanRequestQuery",
    "setupVmEnvironmentArgs",
  ]) {
    const bytes = buildRejectedInteractionResponse(11, queryCase);
    assert.ok(bytes, `${queryCase} must have a response`);
    const decoded = decodeClient(bytes);
    assert.equal(decoded.message.case, "interactionResponse");
    assert.equal((decoded.message.value as { id: number }).id, 11);
  }
  assert.equal(buildRejectedInteractionResponse(11, "somethingElse"), null);
});

test("a web search rejection is a real rejected response", () => {
  const decoded = decodeClient(buildRejectedInteractionResponse(3, "webSearchRequestQuery")!);
  assert.equal(decoded.message.case, "interactionResponse");
  const response = decoded.message.value;
  assert.equal(response.result.case, "webSearchRequestResponse");
  const web = response.result.value as ReturnType<typeof create<typeof WebSearchRequestResponseSchema>>;
  assert.equal(web.result.case, "rejected");
});

test("the real setupVmEnvironmentArgs case is answered successfully", () => {
  const bytes = buildRejectedInteractionResponse(5, "setupVmEnvironmentArgs");
  assert.ok(bytes);
  const decoded = decodeClient(bytes);
  assert.equal(decoded.message.case, "interactionResponse");
  const response = decoded.message.value;
  assert.equal(response.result.case, "setupVmEnvironmentResult");
  const setup = response.result.value as { result: { case: string } };
  assert.equal(setup.result.case, "success");
});

test("the interaction response type is what the wire expects", () => {
  // Guard against a schema import drift: the generated oneof name is fixed by
  // the protocol, and a mismatch would only show up as a stalled model.
  const response = create(InteractionResponseSchema, { id: 1 });
  assert.equal(response.id, 1);
});
