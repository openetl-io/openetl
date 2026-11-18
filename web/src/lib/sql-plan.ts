import { OperatorChange, type OperatorPlan, type StatementPlan, type TransformPlan, type ViewPlan } from "@/gen/openetl/api/v1/pipeline_pb";
import type { Duration } from "@bufbuild/protobuf/wkt";
import { goDuration } from "@/lib/spec-yaml";

// A SQL transform's plan as the console shows it: the plan's problems and
// notes about transform.sql, each at its place in the SQL when it has one.

/** One of the plan's messages about transform.sql. Line and column count from 1. */
export type SqlMessage = { tone: "problem" | "note"; line?: number; column?: number; message: string };

// The API writes them as "transform.sql:LINE:COLUMN: MESSAGE", or
// "transform.sql: MESSAGE" for one that isn't about a place in the text.
const placed = /^transform\.sql(?::(\d+):(\d+))?: ([\s\S]*)$/;

/** Whether a problem or note of the plan is about transform.sql. */
export const aboutSql = (m: string) => m.startsWith("transform.sql:") || m.startsWith("transform.sql ");

function parse(tone: SqlMessage["tone"], m: string): SqlMessage {
  const p = placed.exec(m);
  if (!p) return { tone, message: m };
  return p[1] ? { tone, line: Number(p[1]), column: Number(p[2]), message: p[3]! } : { tone, message: p[3]! };
}

/** The plan's problems and notes about transform.sql, problems first, each in the order of its place. */
export function sqlMessages(problems: string[], notes: string[]): SqlMessage[] {
  const order = (a: SqlMessage, b: SqlMessage) => (a.line ?? 0) - (b.line ?? 0) || (a.column ?? 0) - (b.column ?? 0);
  return [
    ...problems.filter(aboutSql).map((m) => parse("problem", m)).sort(order),
    ...notes.filter(aboutSql).map((m) => parse("note", m)).sort(order),
  ];
}

/** "8:10", or "" for a message that isn't about a place. */
export const position = (m: { line?: number; column?: number }) => (m.line ? `${m.line}:${m.column}` : "");

export type Part = { kind: "statement"; at: StatementPlan } | { kind: "view"; at: ViewPlan };

/** The statements and views in the order they're written. */
export function parts(t: TransformPlan): Part[] {
  const all: Part[] = [...t.statements.map((s) => ({ kind: "statement" as const, at: s })), ...t.views.map((v) => ({ kind: "view" as const, at: v }))];
  return all.sort((a, b) => a.at.line - b.at.line || a.at.column - b.at.column);
}

/**
 * The statement or view a message is about: the last one that starts at or
 * before its place. Undefined for a message before them all, or about no place.
 */
export function partOf(all: Part[], m: SqlMessage): Part | undefined {
  if (!m.line) return undefined;
  let found: Part | undefined;
  for (const p of all) {
    if (p.at.line < m.line || (p.at.line === m.line && p.at.column <= (m.column ?? 0))) found = p;
  }
  return found;
}

/** The streams the plan leaves out because passthrough is none: those withheld that no statement replaces. */
export function notDelivered(t: TransformPlan) {
  const replaced = new Set(t.statements.filter((s) => s.replaces).map((s) => s.output));
  return t.withheld.filter((s) => !replaced.has(s));
}

/** A duration of a plan exactly, as the spec writes one: 1h1s, 90s as 1m30s. */
export const span = (d?: Duration) => (d ? goDuration(`${Number(d.seconds) + d.nanos / 1e9}s`) : "0s");

/**
 * What a stateful operator computes, by the stream's names: "tumble 1m 00s by created_at on region: sum(amount), count(*)".
 * Empty for a dropped one, whose statement is gone.
 */
export function operatorComputes(op: OperatorPlan) {
  const on = `by ${op.eventTime}${op.keys.length ? ` on ${op.keys.join(", ")}` : ""}`;
  if (op.keep) return `dedupe ${op.keep} ${on}`;
  if (op.class === "over") return `over ${on}: ${op.aggregates.join(", ")}`;
  if (!op.window) return "";
  let s = `${op.window} ${span(op.size)}`;
  if (op.slide) s += ` every ${span(op.slide)}`;
  s += ` by ${op.eventTime}`;
  if (op.keys.length) s += ` on ${op.keys.join(", ")}`;
  return `${s}: ${op.aggregates.join(", ")}`;
}

/** What an operator keeps, for how long, and for a dedupe what it guarantees, in a sentence; empty for a dropped one. */
export function operatorKeeps(op: OperatorPlan) {
  if (op.window) return `Each window's state for ${span(op.retention)} of event time, lateness ${span(op.lateness)} included`;
  if (op.keep === "latest") return "Each key's latest row's time, for good: exactly once, as upserts. Its state grows with the number of keys";
  if (op.keep && !op.guarantee) return "Its stream sets no dedupe.retention";
  if (op.keep && op.retention) {
    return `Each key for ${span(op.retention)} of event time from its first row: exactly once within ${span(op.retention)}`;
  }
  if (op.keep) return "Every key, for good: exactly once. Its state grows with the number of keys";
  if (op.class === "over") return "Each key's frame, and its rows until the watermark passes them";
  return "";
}

/** What applying does to an operator's state, against the pipeline's current version. */
export const changeLabel: Record<OperatorChange, string> = {
  [OperatorChange.UNSPECIFIED]: "Unknown",
  [OperatorChange.KEPT]: "Kept",
  [OperatorChange.NEW]: "New",
  [OperatorChange.CHANGED]: "Changed",
  [OperatorChange.DROPPED]: "Dropped",
};

/** For an operator an earlier version had: when its state comes back. Empty for another. */
function earlierText(op: OperatorPlan) {
  if (!op.earlier) return "";
  return "An earlier version had it. If the last checkpoint still holds its state, as when no sync has run since the current version, the next sync restores that state; otherwise it starts empty.";
}

/** Whether the pipeline's last checkpoint holds an operator's state; empty where that isn't known. */
export function heldText(op: OperatorPlan) {
  if (op.held === undefined) return "";
  return op.held ? "The last checkpoint holds its state." : "The last checkpoint holds none of its state.";
}

/** What happens to an operator's state, in a sentence. */
export function changeText(op: OperatorPlan) {
  switch (op.change) {
    case OperatorChange.KEPT:
      return "Keeps its state.";
    case OperatorChange.NEW:
      return earlierText(op) || "Its state starts empty.";
    case OperatorChange.CHANGED:
      return `${earlierText(op) || "Its state starts empty."} It takes the place of ${op.replaces}, whose state is dropped.`;
    case OperatorChange.DROPPED:
      return "No statement keeps it now, so the next sync drops its state.";
  }
  return "The current version wasn't planned when it was applied.";
}

/** row_wise → "Row-wise", and the classes of statements the engine refuses. */
export const classLabel: Record<string, string> = {
  row_wise: "Row-wise",
  group_window: "Group window",
  over: "Over",
  join: "Join",
  updating: "Updating",
};
