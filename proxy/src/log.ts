/**
 * The one place the proxy writes diagnostics.
 *
 * The log path comes from MAKI_CURSOR_LOG when maki sets it, and otherwise
 * defaults next to the proxy source. Logging is fire-and-forget: a failure to
 * write must never break a request or keep the process alive.
 * Callers must never pass tokens, request bodies, or other secrets.
 */
import { appendFile } from "node:fs/promises";
import { resolve } from "node:path";

const MAX_LINE_CHARS = 2_000;

/** Absolute path of the proxy log file. */
export function proxyLogPath(): string {
  const configured = process.env.MAKI_CURSOR_LOG;
  if (typeof configured === "string" && configured.length > 0) return resolve(configured);
  return resolve(import.meta.dirname, "..", "proxy.log");
}

/** Collapse to one short line so a stack trace cannot bloat the file. */
function formatLine(message: string): string {
  let line = String(message).replace(/[\r\n]+/g, " ").trim();
  if (line === "") return "";
  if (line.length > MAX_LINE_CHARS) line = `${line.slice(0, MAX_LINE_CHARS)} ...`;
  // The log is shared by every proxy process maki starts; without the pid a
  // resume/stall line cannot be attributed to one session.
  return `${new Date().toISOString()} [${process.pid}] ${line}\n`;
}

function appendLine(line: string): Promise<void> {
  return appendFile(proxyLogPath(), line).catch(() => {});
}

/** Append one timestamped line to the proxy log; never throws, never awaits. */
export function proxyLog(message: string): void {
  try {
    const line = formatLine(message);
    if (line === "") return;
    void appendLine(line);
  } catch {
    // Diagnostics are best-effort.
  }
}

/**
 * Same destination, but awaited. Only for the CLI's fatal-error handler,
 * which exits the process immediately afterwards.
 */
export async function proxyLogAndWait(message: string): Promise<void> {
  try {
    const line = formatLine(message);
    if (line === "") return;
    await appendLine(line);
  } catch {
    // Diagnostics are best-effort.
  }
}
