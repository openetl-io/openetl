import { toJson, type JsonValue } from "@bufbuild/protobuf";
import { stringify } from "yaml";
import { file_openetl_api_v1_pipeline, PipelineSpecSchema, type PipelineSpec } from "@/gen/openetl/api/v1/pipeline_pb";

// A pipeline version as the YAML document that applies it: enums in
// lowercase without their prefix, and durations as Go spells them.

const enumNames = new Map<string, string>();
for (const e of file_openetl_api_v1_pipeline.enums) {
  for (const v of e.values) enumNames.set(v.name, v.localName.toLowerCase());
}
const durationKeys = new Set(["every", "jitter", "interval", "resumePointTimeout"]);

/** "5400s" → "1h30m", "0.5s" → "500ms" */
export function goDuration(json: string) {
  const secs = Number.parseFloat(json);
  if (secs === 0) return "0s";
  if (secs < 1) return `${Math.round(secs * 1000)}ms`;
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = +(secs % 60).toFixed(3);
  return `${h ? `${h}h` : ""}${m ? `${m}m` : ""}${s ? `${s}s` : ""}`;
}

function tidy(v: JsonValue, key?: string): JsonValue {
  if (typeof v === "string") {
    if (key && durationKeys.has(key) && /^\d+(\.\d+)?s$/.test(v)) return goDuration(v);
    return enumNames.get(v) ?? v;
  }
  if (Array.isArray(v)) return v.map((x) => tidy(x));
  if (v && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, tidy(x, k)]));
  return v;
}

export function pipelineYAML(name: string, workspace: string, spec: PipelineSpec) {
  return stringify({
    apiVersion: "openetl.dev/v1alpha1",
    kind: "Pipeline",
    metadata: workspace === "default" ? { name } : { name, workspace },
    spec: tidy(toJson(PipelineSpecSchema, spec)),
  });
}
