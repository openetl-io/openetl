import { PipelineMode, type Pipeline, type PipelineSpec } from "@/gen/openetl/api/v1/pipeline_pb";
import { dateOf, durationMs, interval } from "./format";

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

/** The data is stale when the last success is older than two schedule intervals. */
export function stale(p: Pipeline, now = Date.now()) {
  const t = p.spec?.schedule?.trigger;
  const last = dateOf(p.status?.lastSuccessTime);
  if (p.paused || t?.case !== "every" || !last) return false;
  return now - last.getTime() > 2 * durationMs(t.value);
}
