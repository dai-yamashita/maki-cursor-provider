/**
 * Cursor AskQuestion <-> maki question tool.
 *
 * Cursor asks the client a question through an AgentServerMessage
 * `interactionQuery` (AskQuestionInteractionQuery) while the run is open. The
 * proxy must answer with an InteractionResponse on the same stream. maki has
 * no such protocol: it exposes a `question` tool whose result is the text the
 * user picked. This module maps one to the other so the model never waits for
 * an answer that nobody was asked.
 */
import { create, toBinary } from "@bufbuild/protobuf";
import {
  AgentClientMessageSchema,
  AskQuestionErrorSchema,
  AskQuestionInteractionResponseSchema,
  AskQuestionRejectedSchema,
  AskQuestionResultSchema,
  AskQuestionSuccessSchema,
  AskQuestionSuccess_AnswerSchema,
  CreatePlanErrorSchema,
  CreatePlanRequestResponseSchema,
  CreatePlanResultSchema,
  ExaFetchRequestResponseSchema,
  ExaFetchRequestResponse_RejectedSchema,
  ExaSearchRequestResponseSchema,
  ExaSearchRequestResponse_RejectedSchema,
  InteractionResponseSchema,
  SetupVmEnvironmentResultSchema,
  SetupVmEnvironmentSuccessSchema,
  SwitchModeRequestResponseSchema,
  SwitchModeRequestResponse_RejectedSchema,
  WebSearchRequestResponseSchema,
  WebSearchRequestResponse_RejectedSchema,
  type AskQuestionArgs,
  type AskQuestionArgs_Option,
  type AskQuestionResult,
} from "./proto/agent_pb.ts";

export const QUESTION_TOOL_NAME = "question";

export interface AskQuestionInteraction {
  id: number;
  args: AskQuestionArgs;
}

/**
 * maki's question form has a short tab header per question. Use a positional
 * header rather than the model's id: an id can contain ": " or a newline and
 * would break the answer line parser.
 */
function headerFor(index: number): string {
  return `Q${index + 1}`;
}

/** maki's question tool input, built from Cursor's AskQuestionArgs. */
export function buildQuestionToolArgs(args: AskQuestionArgs): string {
  const questions = args.questions.map((question, index) => ({
    question: question.prompt || `Question ${index + 1}`,
    header: headerFor(index),
    options: question.options.map((option) => ({ label: option.label })),
    multiSelect: question.allowMultiple,
  }));
  return JSON.stringify({ questions });
}

/** Strip the user-message suffix answerPendingExecs appends to the last result. */
function stripUserMessage(text: string): string {
  const marker = "\n\n<user_message>\n";
  const index = text.lastIndexOf(marker);
  return index === -1 ? text : text.slice(0, index);
}

/**
 * Split one answer line into labels.
 *
 * maki joins a multi-select answer with ", ", so a label that itself contains
 * a comma cannot be recovered by splitting blindly. Match the known labels
 * longest-first and consume them in order; a single-select answer is one label
 * even when it contains commas.
 */
function splitLabels(raw: string, options: readonly AskQuestionArgs_Option[], multiple: boolean): string[] {
  const text = raw.trim();
  if (text === "") return [];
  if (!multiple) return [text];

  const labels = options
    .map((option) => option.label.trim())
    .filter((label) => label !== "")
    .sort((left, right) => right.length - left.length);

  const selected: string[] = [];
  let rest = text;
  while (rest !== "") {
    const current = rest.trim();
    if (current === "") break;
    const matched = labels.find(
      (label) => current === label || current.startsWith(`${label}, `),
    );
    if (matched) {
      selected.push(matched);
      rest = current.slice(matched.length).replace(/^,\s*/, "");
      continue;
    }
    const comma = current.indexOf(", ");
    if (comma === -1) {
      selected.push(current);
      break;
    }
    selected.push(current.slice(0, comma).trim());
    rest = current.slice(comma + 2);
  }
  return selected;
}

function errorResult(message: string): AskQuestionResult {
  return create(AskQuestionResultSchema, {
    result: {
      case: "error",
      value: create(AskQuestionErrorSchema, { errorMessage: message }),
    },
  });
}

function rejectedResult(reason: string): AskQuestionResult {
  return create(AskQuestionResultSchema, {
    result: {
      case: "rejected",
      value: create(AskQuestionRejectedSchema, { reason }),
    },
  });
}

/**
 * Parse maki's question answer text back into Cursor's typed result.
 *
 * maki renders one line per question (`Q1: label, label`) and returns
 * "(question dismissed by user)" for a dismissal or "(no answer)" when a
 * question was skipped. Cursor has no free-text field: a label that matches no
 * option is a custom answer, and the safest thing is to report an error that
 * carries the user's text rather than inventing an option id.
 */
export function buildAskQuestionResult(args: AskQuestionArgs, rawText: string): AskQuestionResult {
  const text = stripUserMessage(rawText).trim();
  if (text === "") {
    return errorResult("The question tool returned no answer.");
  }
  if (text === "Tool result not provided") {
    return errorResult("The question tool result never arrived.");
  }
  if (/dismissed by user/i.test(text)) {
    return rejectedResult("The user dismissed the question.");
  }
  if (/^error[:\s]/i.test(text)) {
    return errorResult(text);
  }

  const answersByHeader = new Map<string, string[]>();
  for (const line of text.split("\n")) {
    const separator = line.indexOf(": ");
    if (separator === -1) continue;
    const header = line.slice(0, separator).trim();
    const value = line.slice(separator + 2);
    if (header) answersByHeader.set(header, [value]);
  }

  // A non-empty result with no parseable line is format drift, not "the user
  // answered nothing". Surface it so the model can react.
  if (answersByHeader.size === 0) {
    return errorResult(`Could not read the question answer: ${text}`);
  }

  const answers = [];
  for (const [index, question] of args.questions.entries()) {
    const header = headerFor(index);
    const rawValue = answersByHeader.get(header)?.[0] ?? "";
    const labels = splitLabels(rawValue, question.options, question.allowMultiple);
    const selectedOptionIds: string[] = [];
    const used = new Set<number>();
    for (const label of labels) {
      if (label === "(no answer)") continue;
      const optionIndex = question.options.findIndex(
        (option, optionAt) => !used.has(optionAt) && option.label.trim() === label,
      );
      if (optionIndex === -1) {
        return errorResult(
          `The user typed a custom answer that Cursor cannot represent: ${label}`,
        );
      }
      used.add(optionIndex);
      selectedOptionIds.push(question.options[optionIndex]!.id);
    }
    answers.push(
      create(AskQuestionSuccess_AnswerSchema, {
        questionId: question.id,
        selectedOptionIds,
      }),
    );
  }

  return create(AskQuestionResultSchema, {
    result: {
      case: "success",
      value: create(AskQuestionSuccessSchema, { answers }),
    },
  });
}

/** One AgentClientMessage carrying an AskQuestionInteractionResponse. */
export function buildAskQuestionResponse(id: number, result: AskQuestionResult): Uint8Array {
  const response = create(InteractionResponseSchema, {
    id,
    result: {
      case: "askQuestionInteractionResponse",
      value: create(AskQuestionInteractionResponseSchema, { result }),
    },
  });
  const client = create(AgentClientMessageSchema, {
    message: { case: "interactionResponse", value: response },
  });
  return toBinary(AgentClientMessageSchema, client);
}

/** A rejection for a query this client cannot service, so the model never hangs. */
export function buildRejectedInteractionResponse(id: number, queryCase: string): Uint8Array | null {
  if (queryCase === "askQuestionInteractionQuery") {
    return buildAskQuestionResponse(
      id,
      rejectedResult("This client cannot display questions; ask in plain text instead."),
    );
  }

  let response: ReturnType<typeof create<typeof InteractionResponseSchema>> | undefined;
  if (queryCase === "webSearchRequestQuery") {
    response = create(InteractionResponseSchema, {
      id,
      result: {
        case: "webSearchRequestResponse",
        value: create(WebSearchRequestResponseSchema, {
          result: {
            case: "rejected",
            value: create(WebSearchRequestResponse_RejectedSchema, {
              reason: "Web search is not available in this client.",
            }),
          },
        }),
      },
    });
  } else if (queryCase === "switchModeRequestQuery") {
    response = create(InteractionResponseSchema, {
      id,
      result: {
        case: "switchModeRequestResponse",
        value: create(SwitchModeRequestResponseSchema, {
          result: {
            case: "rejected",
            value: create(SwitchModeRequestResponse_RejectedSchema, {
              reason: "Mode switching is controlled by maki.",
            }),
          },
        }),
      },
    });
  } else if (queryCase === "exaSearchRequestQuery") {
    response = create(InteractionResponseSchema, {
      id,
      result: {
        case: "exaSearchRequestResponse",
        value: create(ExaSearchRequestResponseSchema, {
          result: {
            case: "rejected",
            value: create(ExaSearchRequestResponse_RejectedSchema, {
              reason: "Exa search is not available in this client.",
            }),
          },
        }),
      },
    });
  } else if (queryCase === "exaFetchRequestQuery") {
    response = create(InteractionResponseSchema, {
      id,
      result: {
        case: "exaFetchRequestResponse",
        value: create(ExaFetchRequestResponseSchema, {
          result: {
            case: "rejected",
            value: create(ExaFetchRequestResponse_RejectedSchema, {
              reason: "Exa fetch is not available in this client.",
            }),
          },
        }),
      },
    });
  } else if (queryCase === "createPlanRequestQuery") {
    response = create(InteractionResponseSchema, {
      id,
      result: {
        case: "createPlanRequestResponse",
        value: create(CreatePlanRequestResponseSchema, {
          result: create(CreatePlanResultSchema, {
            result: {
              case: "error",
              value: create(CreatePlanErrorSchema, {
                error: "Plan creation is handled by maki, not this client.",
              }),
            },
          }),
        }),
      },
    });
  } else if (queryCase === "setupVmEnvironmentArgs") {
    response = create(InteractionResponseSchema, {
      id,
      result: {
        case: "setupVmEnvironmentResult",
        value: create(SetupVmEnvironmentResultSchema, {
          result: {
            case: "success",
            value: create(SetupVmEnvironmentSuccessSchema, {}),
          },
        }),
      },
    });
  }
  if (!response) return null;

  const client = create(AgentClientMessageSchema, {
    message: { case: "interactionResponse", value: response },
  });
  return toBinary(AgentClientMessageSchema, client);
}
