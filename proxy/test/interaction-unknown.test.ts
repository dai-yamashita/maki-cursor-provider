/**
 * Protocol paths that used to answer nothing.
 *
 * - An InteractionQuery whose oneof case this generated schema does not name
 *   (newer Cursor webFetch/prManagement/mcpAuth/generateImage/replaceEnv/
 *   connectScm queries) must still get an InteractionResponse on the open run.
 *   The fake upstream only finishes the turn after it sees that response, so a
 *   silent proxy makes the assertion time out.
 * - A non-streaming turn must report a Connect end-stream error as a failed
 *   completion instead of "stop" with whatever text happened to stream.
 *
 * CURSOR_API_URL is read at module load, so the fake upstream must be
 * listening (and the env set) before proxy.ts is imported.
 */
import assert from "node:assert/strict";
import { once } from "node:events";
import http2 from "node:http2";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { after } from "node:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  AgentClientMessageSchema,
  AgentServerMessageSchema,
  ExecServerMessageSchema,
  InteractionQuerySchema,
  InteractionUpdateSchema,
  ShellArgsSchema,
  TextDeltaUpdateSchema,
  type AgentClientMessage,
} from "../src/proto/agent_pb.ts";

const TOKEN = "test-token";

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const guard = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms);
  });
  try {
    return await Promise.race([promise, guard]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function pollUntil(check: () => boolean, ms: number, message: string): Promise<void> {
  const deadline = Date.now() + ms;
  while (!check()) {
    if (Date.now() > deadline) throw new Error(message);
    await delay(25);
  }
}

// --- Shared fake upstream (h2c) ---------------------------------------------------

let onStream: (stream: http2.ServerHttp2Stream) => void = () => {};

const sessions = new Set<http2.ServerHttp2Session>();
const upstream = http2.createServer();
upstream.on("session", (session) => {
  sessions.add(session);
  session.on("close", () => sessions.delete(session));
  session.on("error", () => {});
});
upstream.on("stream", (stream) => {
  stream.on("error", () => {});
  stream.on("data", () => {});
  onStream(stream);
});
upstream.listen(0, "127.0.0.1");
await once(upstream, "listening");
const upstreamPort = (upstream.address() as AddressInfo).port;

const conversationDir = mkdtempSync(join(tmpdir(), "maki-cursor-protocol-conv-"));
const logDir = mkdtempSync(join(tmpdir(), "maki-cursor-protocol-log-"));
const logPath = join(logDir, "proxy.log");

process.env.CURSOR_API_URL = `http://127.0.0.1:${upstreamPort}`;
process.env.CURSOR_SSE_KEEPALIVE_MS = "100";
process.env.MAKI_CURSOR_CONVERSATIONS = conversationDir;
process.env.MAKI_CURSOR_CREDENTIALS = join(logDir, "credentials.json");
delete process.env.CURSOR_PROXY_DEBUG;

const { startProxy, stopProxy } = await import("../src/proxy.ts");

after(async () => {
  const closed = once(upstream, "close").catch(() => undefined);
  for (const session of sessions) session.destroy();
  upstream.close();
  await Promise.race([closed, delay(2_000)]);
  rmSync(conversationDir, { recursive: true, force: true });
  rmSync(logDir, { recursive: true, force: true });
});

// --- Frame helpers -----------------------------------------------------------------

function connectFrame(bytes: Uint8Array, flags = 0): Buffer {
  const frame = Buffer.alloc(5 + bytes.length);
  frame[0] = flags;
  frame.writeUInt32BE(bytes.length, 1);
  frame.set(bytes, 5);
  return frame;
}

function serverFrame(
  message: ReturnType<typeof create<typeof AgentServerMessageSchema>>,
): Buffer {
  return connectFrame(toBinary(AgentServerMessageSchema, message));
}

/** One Connect data frame carrying an interaction query this schema cannot name. */
function unknownQueryFrame(id: number): Buffer {
  const query = create(InteractionQuerySchema, { id });
  // Field 9 is a oneof member newer Cursor builds use; $unknown keeps it as an
  // unrecognized field so `query.query.case` stays undefined.
  query.$unknown = [
    { no: 9, wireType: 2, data: Uint8Array.from([0x02, 0x08, 0x01]) },
  ];
  return serverFrame(
    create(AgentServerMessageSchema, { message: { case: "interactionQuery", value: query } }),
  );
}

/** One Connect data frame carrying a shell stream exec this client refuses. */
function shellStreamExecFrame(id: number, command: string): Buffer {
  const exec = create(ExecServerMessageSchema, {
    id,
    execId: `exec-${id}`,
    message: {
      case: "shellStreamArgs",
      value: create(ShellArgsSchema, { command, workingDirectory: "/tmp" }),
    },
  });
  return serverFrame(
    create(AgentServerMessageSchema, { message: { case: "execServerMessage", value: exec } }),
  );
}

/** One Connect data frame carrying an agent text delta. */
function textFrame(text: string): Buffer {
  const update = create(InteractionUpdateSchema, {
    message: { case: "textDelta", value: create(TextDeltaUpdateSchema, { text }) },
  });
  return serverFrame(
    create(AgentServerMessageSchema, { message: { case: "interactionUpdate", value: update } }),
  );
}

/** A Connect end-stream frame (flags 0b10) carrying a server error. */
function errorEndStreamFrame(code: string, message: string): Buffer {
  const payload = new TextEncoder().encode(JSON.stringify({ error: { code, message } }));
  return connectFrame(payload, 0b10);
}

function attachClientParser(
  stream: http2.ServerHttp2Stream,
  onMessage?: (message: AgentClientMessage) => void,
): AgentClientMessage[] {
  const messages: AgentClientMessage[] = [];
  let pending = Buffer.alloc(0);
  stream.on("data", (chunk: Buffer) => {
    pending = Buffer.concat([pending, chunk]);
    while (pending.length >= 5) {
      const length = pending.readUInt32BE(1);
      if (pending.length < 5 + length) break;
      const payload = pending.subarray(5, 5 + length);
      pending = pending.subarray(5 + length);
      try {
        const message = fromBinary(AgentClientMessageSchema, payload);
        messages.push(message);
        onMessage?.(message);
      } catch {
        // Not a client message frame; ignore.
      }
    }
  });
  return messages;
}

function respondSilent(stream: http2.ServerHttp2Stream): void {
  try {
    stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
  } catch {
    // The client already went away.
  }
}

function chatRequest(
  port: number,
  body: Record<string, unknown>,
  session = "",
): Promise<Response> {
  return fetch(`http://127.0.0.1:${port}/v1/chat/completions`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${TOKEN}`,
      "content-type": "application/json",
      ...(session ? { "x-maki-session": session } : {}),
    },
    body: JSON.stringify(body),
  });
}

async function readAll(response: Response): Promise<string> {
  const body = response.body;
  assert.ok(body, "the SSE response has no body");
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let text = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) text += decoder.decode(value, { stream: true });
  }
  return text + decoder.decode();
}

function logText(): string {
  try {
    return readFileSync(logPath, "utf8");
  } catch {
    return "";
  }
}

// --- Tests -------------------------------------------------------------------------

test("an unknown interaction query is answered empty instead of hanging", { timeout: 20_000 }, async () => {
  const { promise: answered, resolve: onAnswered } = Promise.withResolvers<number>();
  let clientMessages: AgentClientMessage[] = [];

  onStream = (stream) => {
    respondSilent(stream);
    clientMessages = attachClientParser(stream, (message) => {
      if (message.message.case !== "interactionResponse") return;
      onAnswered(message.message.value.id);
      // The real upstream waits for the answer; only continue the run after
      // the proxy supplied one.
      stream.write(textFrame("after query"));
      setTimeout(() => {
        if (!stream.closed && !stream.destroyed) stream.end();
      }, 50);
    });
    stream.write(unknownQueryFrame(41));
  };

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(
      chatRequest(port, {
        model: "test-model",
        stream: true,
        messages: [{ role: "user", content: "hi" }],
      }),
      5_000,
      "no response headers",
    );

    const id = await withTimeout(
      answered,
      5_000,
      "the proxy never answered the unknown interaction query",
    );
    assert.equal(id, 41);

    const interaction = clientMessages.find(
      (message) => message.message.case === "interactionResponse",
    );
    assert.ok(interaction && interaction.message.case === "interactionResponse");
    assert.equal(
      interaction.message.value.result.case,
      undefined,
      "an unnamed query gets an id-only response",
    );
    assert.equal(interaction.message.value.id, 41);

    await pollUntil(
      () => /unsupported interaction query none answered empty/.test(logText()),
      5_000,
      `the empty answer was not logged; log was:\n${logText()}`,
    );

    const text = await withTimeout(readAll(response), 10_000, "the turn hung after the query");
    assert.match(text, /"content":"after query"/);
    assert.match(text, /data: \[DONE\]/);
  } finally {
    stopProxy();
  }
});

test("a shellStream exec without a bash tool answers on the shellStream field", { timeout: 20_000 }, async () => {
  const { promise: answered, resolve: onAnswered } = Promise.withResolvers<string>();
  let clientMessages: AgentClientMessage[] = [];

  onStream = (stream) => {
    respondSilent(stream);
    clientMessages = attachClientParser(stream, (message) => {
      if (message.message.case !== "execClientMessage") return;
      const exec = message.message.value;
      if (exec.message.case !== "shellStream") return;
      onAnswered(exec.message.value.event.case ?? "none");
      stream.write(textFrame("shell refused"));
      setTimeout(() => {
        if (!stream.closed && !stream.destroyed) stream.end();
      }, 50);
    });
    stream.write(shellStreamExecFrame(12, "ls"));
  };

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(
      chatRequest(port, {
        model: "test-model",
        stream: true,
        messages: [{ role: "user", content: "hi" }],
      }),
      5_000,
      "no response headers",
    );

    const event = await withTimeout(
      answered,
      5_000,
      "the shell stream rejection never arrived",
    );
    assert.equal(event, "rejected");

    const execMessage = clientMessages.find(
      (message) =>
        message.message.case === "execClientMessage" &&
        message.message.value.message.case === "shellStream",
    );
    assert.ok(execMessage && execMessage.message.case === "execClientMessage");
    const exec = execMessage.message.value;
    assert.ok(exec.message.case === "shellStream");
    assert.equal(exec.message.value.event.case, "rejected");
    if (exec.message.value.event.case === "rejected") {
      assert.equal(exec.message.value.event.value.command, "ls");
    }
    // The same exec must not also come back as a shellResult.
    assert.ok(
      clientMessages.every(
        (message) =>
          message.message.case !== "execClientMessage" ||
          message.message.value.message.case !== "shellResult",
      ),
      "a shellStreamArgs exec must not be answered with shellResult",
    );

    await withTimeout(readAll(response), 10_000, "the turn hung after the shell rejection");
  } finally {
    stopProxy();
  }
});

test("a non-streaming turn reports an end-stream error as a failed completion", { timeout: 20_000 }, async () => {
  onStream = (stream) => {
    respondSilent(stream);
    stream.write(errorEndStreamFrame("internal", "upstream exploded"));
    setTimeout(() => {
      if (!stream.closed && !stream.destroyed) stream.end();
    }, 50);
  };

  const port = await startProxy(async () => "unused", [], 0, TOKEN);
  try {
    const response = await withTimeout(
      chatRequest(port, {
        model: "test-model",
        stream: false,
        messages: [{ role: "user", content: "hi" }],
      }),
      5_000,
      "non-streaming request got no headers",
    );

    const payload = JSON.parse(
      await withTimeout(response.text(), 10_000, "no JSON body"),
    ) as {
      choices: { finish_reason: string }[];
      error?: { type: string; code?: number };
    };
    assert.equal(payload.choices[0]!.finish_reason, "error");
    // `internal` maps to HTTP 500, so maki classifies it as a server error.
    assert.equal(payload.error?.type, "api_error");
    assert.equal(payload.error?.code, 500);
    assert.match(logText(), /cursor end stream error: Connect error internal: upstream exploded/);
  } finally {
    stopProxy();
  }
});
