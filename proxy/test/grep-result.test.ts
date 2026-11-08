import assert from "node:assert/strict";
import test from "node:test";
import { buildGrepContentResult } from "../src/native-tools.ts";

const MAX_INT32 = 2_147_483_647;

function fileMatches(content: string) {
  return buildGrepContentResult(content, false).value.matches;
}

test("maki's block format keeps the path out of the line number", () => {
  // The header path contains "-1794035794037700-", which the old fallback
  // parsed as a line number and then failed to encode as int32.
  const path =
    ".work/reports/p4b-r-planning-matrix/positive-prefix-20261107T071341Z/new-census/planning-1794035794037700-7939/planning-ledger.term";
  const content = [
    `${path}:`,
    "  966: - 元ledger：...",
    "  967  context before",
    "  --",
    "  970: another match",
    "HANDOFF.md:",
    "  12: hello",
  ].join("\n");

  const matches = fileMatches(content);
  assert.equal(matches.length, 2);
  assert.equal(matches[0]!.file, path);
  assert.deepEqual(
    matches[0]!.matches.map((match) => match.lineNumber),
    [966, 967, 970],
  );
  assert.deepEqual(
    matches[0]!.matches.map((match) => match.isContextLine),
    [false, true, false],
  );
  assert.equal(matches[1]!.file, "HANDOFF.md");
  assert.equal(matches[1]!.matches[0]!.lineNumber, 12);
  for (const file of matches) {
    for (const match of file.matches) {
      assert.ok(
        Number.isInteger(match.lineNumber) && match.lineNumber <= MAX_INT32,
        `line number ${match.lineNumber} does not fit int32`,
      );
    }
  }
});

test("legacy single-line grep output still parses", () => {
  const matches = fileMatches(
    "src/a.ts:12:hello\nsrc/a.ts-13-world\nsrc/b.ts:4:other",
  );
  assert.equal(matches.length, 2);
  assert.equal(matches[0]!.file, "src/a.ts");
  assert.deepEqual(
    matches[0]!.matches.map((match) => match.lineNumber),
    [12, 13],
  );
  assert.deepEqual(
    matches[0]!.matches.map((match) => match.isContextLine),
    [false, true],
  );
  assert.equal(matches[1]!.file, "src/b.ts");
});

test("a line number that cannot fit int32 is skipped, not encoded", () => {
  const matches = fileMatches(
    "src/a.ts:999999999999999999999:too big\nsrc/a.ts:7:fine",
  );
  assert.equal(matches.length, 1);
  assert.equal(matches[0]!.matches.length, 1);
  assert.equal(matches[0]!.matches[0]!.lineNumber, 7);
});
