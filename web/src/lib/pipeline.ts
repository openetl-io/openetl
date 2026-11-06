import { PipelineMode, type Pipeline, type PipelineSpec } from "@/gen/openetl/api/v1/pipeline_pb";
import type { Sync } from "@/gen/openetl/api/v1/sync_pb";
import { compact, dateOf, duration, durationMs, interval } from "./format";

export function scheduleLabel(spec?: PipelineSpec) {
  if (spec?.mode === PipelineMode.CONTINUOUS) return "Continuous";
  const t = spec?.schedule?.trigger;
  switch (t?.case) {
    case "every":
      return `Every ${interval(durationMs(t.value))}`;
    case "cron":
      return t.value;
    default:
      return "On demand";
  }
}

/**
 * When the pipeline's data last changed: when a continuous pipeline's run
 * last committed, or a scheduled one's last sync that succeeded ended.
 */
export function lastUpdate(p: Pipeline) {
  if (p.spec?.mode === PipelineMode.CONTINUOUS) return dateOf(p.status?.lastSync?.lastCommitTime);
  return dateOf(p.status?.lastSuccessTime);
}

/** The data is stale when the last success is older than two schedule intervals. */
export function stale(p: Pipeline, now = Date.now()) {
  const t = p.spec?.schedule?.trigger;
  const last = dateOf(p.status?.lastSuccessTime);
  if (p.paused || t?.case !== "every" || !last) return false;
  return now - last.getTime() > 2 * durationMs(t.value);
}

/**
 * How far behind its source a run's read was at its latest commit, the most
 * of its streams': in time if the source says, else in bytes. Undefined if
 * the source reports neither.
 */
export function lag(s?: Sync) {
  const ps = s?.progress ?? [];
  const times = ps.filter((p) => p.lag).map((p) => durationMs(p.lag));
  if (times.length) return duration(Math.max(...times));
  const bytes = ps.flatMap((p) => (p.lagBytes === undefined ? [] : [p.lagBytes]));
  if (bytes.length) return `${compact(bytes.reduce((a, b) => (a > b ? a : b)))}B`;
  return undefined;
}
