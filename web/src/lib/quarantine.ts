import type { QuarantineCause, QuarantineFile } from "@/gen/openetl/api/v1/pipeline_pb";
import { full } from "@/lib/format";

/** Why a row's value couldn't be held, by notHeld, as a phrase. */
export const notHeldHint: Record<string, string> = {
  key: "the column is in the stream's primary key",
  cursor: "the column is the stream's cursor",
  destination: "the destination can't hold the column",
  not_null: "the destination's column takes no null, and the stream sets no heldValue",
  held_value: "the column's heldValue doesn't fit its type",
  record: "the record isn't JSON, or isn't an object",
  transform: "a SQL statement reads the column",
};

/** A cause as `openetl quarantine` prints it: "type on amount, not held: key". */
export function causeText(c: Pick<QuarantineCause, "code" | "column" | "notHeld">) {
  let s = c.code;
  if (c.column) s += ` on ${c.column}`;
  if (c.notHeld) s += `, not held: ${c.notHeld}`;
  return s;
}

export const rowsLabel = (n: bigint) => (n === 1n ? "1 row" : `${full(n)} rows`);

/** Whether any of the files' rows were quarantined by a stream that holds values that don't fit. */
export const anyNotHeld = (files: QuarantineFile[]) => files.some((f) => f.causes.some((c) => c.notHeld));

/** The CLI command that reads a file's first lines; workspace and pipeline are IDs. */
export function showCommand(workspace: string, pipeline: string, path: string, limit = 100) {
  const ws = workspace === "default" ? "" : `--workspace ${workspace} `;
  return `openetl quarantine show ${ws}--limit ${limit} ${pipeline} ${path}`;
}

/** One line of a quarantine file, read for people: its cause, and its record pretty-printed where it's JSON. */
export type Line = { cause?: string; reason?: string; record: string };

export function parseLine(line: string): Line {
  let v: unknown;
  try {
    v = JSON.parse(line);
  } catch {
    return { record: line };
  }
  if (!v || typeof v !== "object" || Array.isArray(v)) return { record: line };
  const o = v as Record<string, unknown>;
  const str = (k: string) => (typeof o[k] === "string" ? (o[k] as string) : "");
  const record = str("record");
  let pretty = record;
  try {
    // JSON.parse turns numbers into doubles, so a record with a number that
    // doesn't print back as written, such as an ID past 2^53, is shown as it came.
    if (numbersRoundTrip(record)) pretty = JSON.stringify(JSON.parse(record), null, 2);
  } catch {
    // Not JSON, as a record whose code is json: shown as it came.
  }
  return {
    cause: str("code") ? causeText({ code: str("code"), column: str("column"), notHeld: str("notHeld") }) : undefined,
    reason: str("reason") || undefined,
    record: pretty,
  };
}

/** Whether every number in JSON text, outside its strings, prints back from a double exactly as written. */
export function numbersRoundTrip(text: string): boolean {
  let inString = false;
  for (let i = 0; i < text.length; i++) {
    const c = text.charAt(i);
    if (inString) {
      if (c === "\\") i++;
      else if (c === '"') inString = false;
      continue;
    }
    if (c === '"') {
      inString = true;
      continue;
    }
    if (c === "-" || (c >= "0" && c <= "9")) {
      const m = /^-?\d+(\.\d+)?([eE][+-]?\d+)?/.exec(text.slice(i));
      if (!m) continue;
      if (String(Number(m[0])) !== m[0]) return false;
      i += m[0].length - 1;
    }
  }
  return true;
}
