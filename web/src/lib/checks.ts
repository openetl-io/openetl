import type { CheckItem, CheckResult } from "@/gen/openetl/api/v1/connection_pb";
import type { PreflightPart, PreflightResult } from "@/gen/openetl/api/v1/pipeline_pb";

/** A run of a template's text, or a placeholder's value: `name` is the param it came from. */
export type TemplatePart = string | { name: string; value: string };

const placeholder = /\{([a-z][a-z0-9_]*)\}/g;

/**
 * The template's text with each {name} that params has replaced by its value, kept apart from the text around it.
 * A placeholder without a param, or not of the grammar ({Host}, {db-host}), stays as written; a value isn't scanned again.
 */
export function templateParts(template: string, params: Record<string, string>): TemplatePart[] {
  const parts: TemplatePart[] = [];
  let text = "";
  let at = 0;
  for (const m of template.matchAll(placeholder)) {
    const name = m[1] ?? "";
    if (!Object.hasOwn(params, name)) continue;
    text += template.slice(at, m.index);
    if (text) parts.push(text);
    text = "";
    parts.push({ name, value: params[name] ?? "" });
    at = m.index + m[0].length;
  }
  text += template.slice(at);
  if (text) parts.push(text);
  return parts;
}

export type ItemStatus = "passed" | "warning" | "failed" | "skipped";

/** The item's status; one the console doesn't know counts as failed. */
export function itemStatus(item: CheckItem): ItemStatus {
  const s = item.status;
  return s === "passed" || s === "warning" || s === "skipped" ? s : "failed";
}

export const itemFailed = (item: CheckItem) => itemStatus(item) === "failed";

/** How many items ended each way, for the result's summary: "4 passed, 1 failed". */
export function checkCounts(items: CheckItem[]): string {
  return (["passed", "warning", "failed", "skipped"] as const)
    .map((s) => [s, items.filter((i) => itemStatus(i) === s).length] as const)
    .filter(([, n]) => n > 0)
    .map(([s, n]) => `${n} ${s === "warning" ? (n === 1 ? "warning" : "warnings") : s}`)
    .join(", ");
}

/** A check failed if the result says so or any of its items failed. */
export function checkFailed(r: CheckResult): boolean {
  return !r.ok || r.items.some(itemFailed);
}

/** A part failed if any of its items failed, or it says it failed and wasn't skipped. */
export function partFailed(p: PreflightPart): boolean {
  return p.items.some(itemFailed) || (!p.ok && !p.skipped);
}

export function preflightFailed(r: PreflightResult): boolean {
  return !r.ok || r.parts.some(partFailed);
}
