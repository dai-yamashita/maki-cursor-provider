import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { access, chmod, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import {
  generateCursorAuthParams,
  getTokenExpiry,
  pollCursorAuth,
  refreshCursorToken,
} from "./auth.ts";
import { startFixture } from "./fixture.ts";
import { proxyLogAndWait } from "./log.ts";

const root = resolve(import.meta.dirname, "..");
const credentialsPath = process.env.MAKI_CURSOR_CREDENTIALS ?? resolve(root, "credentials.json");

function listenPort(): number {
  const requested = Number(process.env.MAKI_CURSOR_PORT ?? "0");
  if (!Number.isInteger(requested) || requested < 0 || requested > 65535) {
    throw new Error("MAKI_CURSOR_PORT is not a port");
  }
  return requested;
}

async function ensureStateDirs(): Promise<void> {
  const conversations = process.env.MAKI_CURSOR_CONVERSATIONS;
  if (conversations) await mkdir(conversations, { recursive: true, mode: 0o700 });
  const log = process.env.MAKI_CURSOR_LOG;
  if (log) await mkdir(dirname(log), { recursive: true, mode: 0o700 });
}

function npmCi(): Promise<void> {
  return new Promise((resolvePromise, reject) => {
    const child = spawn("npm", ["ci", "--omit=dev"], { cwd: root, stdio: "inherit" });
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) resolvePromise();
      else reject(new Error(`npm ci exited ${code}`));
    });
  });
}

async function ensure(): Promise<void> {
  try {
    await access(resolve(root, "node_modules/@bufbuild/protobuf/package.json"));
  } catch {
    await npmCi();
  }
}

async function serve(): Promise<void> {
  const access = process.env.MAKI_CURSOR_ACCESS ?? "";
  // Captured before `ensure()` can spawn npm with this environment.
  delete process.env.MAKI_CURSOR_ACCESS;
  const failFromServe = (label: string, err: unknown): void => {
    let line = `${label}: `;
    if (err instanceof Error) {
      line += err.message;
      if (err.stack) line += `\n${err.stack}`;
    } else {
      line += String(err);
    }
    void proxyLogAndWait(line).finally(() => {
      process.exit(1);
    });
  };
  // A bridge's stdin can close mid-write. Every pipe has its own error
  // handler; this is the last line of defence so one dead child never takes
  // down every other session served by this proxy.
  const isChildPipeError = (err: unknown): boolean => {
    const code = (err as NodeJS.ErrnoException | undefined)?.code;
    return code === "EPIPE" || code === "ECONNRESET";
  };
  process.on("uncaughtException", (err) => {
    if (isChildPipeError(err)) {
      void proxyLogAndWait(
        `ignored child pipe error: ${(err as NodeJS.ErrnoException).code}`,
      );
      return;
    }
    failFromServe("uncaughtException", err);
  });
  process.on("unhandledRejection", (reason) => {
    if (isChildPipeError(reason)) {
      void proxyLogAndWait(
        `ignored child pipe rejection: ${(reason as NodeJS.ErrnoException).code}`,
      );
      return;
    }
    failFromServe("unhandledRejection", reason);
  });

  await ensure();
  const { startProxy, replaceProxyModels, refreshCursorUsage } = await import("./proxy.ts");
  const { getCursorModels } = await import("./models.ts");
  const envToken = process.env.MAKI_CURSOR_TOKEN;
  const token =
    typeof envToken === "string" && envToken.length > 0
      ? envToken
      : randomBytes(32).toString("base64url");
  const bound = await startProxy(async () => access, [], listenPort(), token);
  const ready = process.env.MAKI_CURSOR_READY;
  if (ready) {
    await writeFile(ready, JSON.stringify({ port: bound, token }), { mode: 0o600 });
    await chmod(ready, 0o600);
  }
  const models = await getCursorModels(access);
  replaceProxyModels(models.map((model) => ({ id: model.id, name: model.name })));
  // A hard usage cap must be known before the first run, or the first request
  // pays for an upstream round trip that is guaranteed to be rejected.
  await refreshCursorUsage(access);
  await new Promise(() => {});
}

async function loginUrl(): Promise<void> {
  const statePath = process.argv[3];
  if (!statePath) throw new Error("login-url needs a state path");
  const params = await generateCursorAuthParams();
  await writeFile(statePath, JSON.stringify(params), { mode: 0o600 });
  console.log(params.loginUrl);
}

async function loginPoll(): Promise<void> {
  const statePath = process.argv[3];
  if (!statePath) throw new Error("login-poll needs a state path");
  const params = JSON.parse(await readFile(statePath, "utf8")) as {
    uuid: string;
    verifier: string;
  };
  const tokens = await pollCursorAuth(params.uuid, params.verifier);
  const creds = {
    access: tokens.accessToken,
    refresh: tokens.refreshToken,
    expires: getTokenExpiry(tokens.accessToken),
  };
  await rm(statePath, { force: true });
  console.log(JSON.stringify(creds));
}

async function refresh(): Promise<void> {
  const refreshToken = process.env.MAKI_CURSOR_REFRESH;
  if (!refreshToken) throw new Error("run maki auth login cursor");
  const refreshed = await refreshCursorToken(refreshToken);
  console.log(JSON.stringify(refreshed));
}

async function forgetLegacy(): Promise<void> {
  await rm(credentialsPath, { force: true });
}

async function deps(): Promise<void> {
  try {
    await access(resolve(root, "node_modules/@bufbuild/protobuf/package.json"));
  } catch {
    process.exit(1);
  }
}

async function logout(): Promise<void> {
  await rm(credentialsPath, { force: true });
  const dir = process.env.MAKI_CURSOR_CONVERSATIONS;
  if (dir) await rm(dir, { recursive: true, force: true });
}

async function health(): Promise<void> {
  const listen = process.env.MAKI_CURSOR_PORT;
  const token = process.env.MAKI_CURSOR_TOKEN;
  if (!listen || !token) process.exit(1);
  const response = await fetch(`http://127.0.0.1:${listen}/health`, {
    headers: { authorization: `Bearer ${token}` },
  });
  if (!response.ok) process.exit(1);
}

const MODEL_LIST_WAIT_MS = 12_000;

async function models(): Promise<void> {
  const listen = process.env.MAKI_CURSOR_PORT;
  const token = process.env.MAKI_CURSOR_TOKEN;
  if (!listen || !token) process.exit(1);
  const deadline = Date.now() + MODEL_LIST_WAIT_MS;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`http://127.0.0.1:${listen}/v1/models`, {
        headers: { authorization: `Bearer ${token}` },
      });
      if (response.ok) {
        const ids = modelIds(await response.json());
        if (ids.length > 0) {
          console.log(JSON.stringify({
            object: "list",
            data: ids.map((id) => ({ id })),
          }));
          return;
        }
      }
    } catch {
      // The proxy is still starting, or discovery has not filled the list.
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  process.exit(1);
}

function modelIds(body: unknown): string[] {
  if (!body || typeof body !== "object" || !("data" in body)) return [];
  const data = body.data;
  if (!Array.isArray(data)) return [];
  const ids: string[] = [];
  for (const item of data) {
    if (!item || typeof item !== "object" || !("id" in item)) continue;
    const id = item.id;
    if (typeof id !== "string" || id === "") continue;
    ids.push(id);
  }
  return ids;
}

async function nonce(): Promise<void> {
  console.log(randomBytes(16).toString("hex"));
}

async function waitReady(): Promise<void> {
  const file = process.argv[3];
  if (!file) throw new Error("wait-ready needs a ready file");
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    try {
      const parsed = JSON.parse(await readFile(file, "utf8")) as { port?: number; token?: string };
      if (!parsed.port || !parsed.token) throw new Error("ready file is incomplete");
      const response = await fetch(`http://127.0.0.1:${parsed.port}/health`, {
        headers: { authorization: `Bearer ${parsed.token}` },
      });
      if (response.ok) {
        console.log(`${parsed.port} ${parsed.token}`);
        await rm(file, { force: true });
        return;
      }
    } catch {
      // The server is still starting.
    }
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 50));
  }
  throw new Error("cursor proxy did not become ready");
}

async function fixture(): Promise<void> {
  const bound = await startFixture(listenPort());
  console.log(`listening ${bound.port}`);
  await new Promise(() => {});
}

const command = process.argv[2];
try {
  await ensureStateDirs();
  if (command === "ensure") await ensure();
  else if (command === "serve") await serve();
  else if (command === "login-url") await loginUrl();
  else if (command === "login-poll") await loginPoll();
  else if (command === "refresh") await refresh();
  else if (command === "forget-legacy") await forgetLegacy();
  else if (command === "deps") await deps();
  else if (command === "logout") await logout();
  else if (command === "health") await health();
  else if (command === "models") await models();
  else if (command === "nonce") await nonce();
  else if (command === "wait-ready") await waitReady();
  else if (command === "fixture") await fixture();
  else {
    throw new Error(`unknown command ${command ?? ""}`);
  }
} catch (err) {
  const message = err instanceof Error ? err.message : String(err);
  console.error(message);
  process.exit(1);
}
