import { toJson, type JsonObject, type JsonValue } from "@bufbuild/protobuf";
import { ValueSchema, type Value } from "@bufbuild/protobuf/wkt";
import type { FlinkClusterSpec, FlinkResources } from "@/gen/openetl/api/v1/flink_pb";
import type { Pipeline } from "@/gen/openetl/api/v1/pipeline_pb";
import type { SparkClusterSpec } from "@/gen/openetl/api/v1/spark_pb";

export type ClusterKind = "spark" | "flink";

/** One of a cluster's settings: a value, a secret's name, or what applies when it's unset; and a note on what it means. */
export type Setting = { label: string; value?: string; secret?: string; unset?: string; note?: string; mono?: boolean };

/** Where a cluster runs what pipelines give it, and how to reach it. */
export type ClusterTarget = {
  /** What kind of cluster: spark-submit, EMR on EC2, Kubernetes and so on. */
  label: string;
  /** The one setting that tells clusters of the same kind apart, such as a master's URL. */
  where: string;
  settings: Setting[];
  /** Properties for every application or job; values can be secret references. */
  conf?: JsonObject;
};

/** The secret a value names, {"secret": NAME}, if it's a reference. */
export function secretRef(v: JsonValue | undefined): string | undefined {
  if (v && typeof v === "object" && !Array.isArray(v) && Object.keys(v).length === 1 && typeof v.secret === "string") return v.secret;
  return undefined;
}

const json = (v: Value | undefined) => (v ? toJson(ValueSchema, v) : undefined);

/** A setting that takes a value or a secret reference. */
function valueOrSecret(label: string, v: Value | undefined, unset: string): Setting {
  const j = json(v);
  const secret = secretRef(j);
  if (secret) return { label, secret };
  if (typeof j === "string" && j !== "") return { label, value: j, mono: true };
  return { label, unset };
}

function emr(target: { clusterId: string; clusterName: string; region: string }, each: string): Setting[] {
  return [
    target.clusterId
      ? { label: "Cluster ID", value: target.clusterId, mono: true }
      : { label: "Cluster name", value: target.clusterName, mono: true, note: `The one active cluster of that name, at each ${each}` },
    { label: "Region", value: target.region || undefined, unset: "From the runner's AWS settings" },
  ];
}

const emrWhere = (t: { clusterId: string; clusterName: string; region: string }) =>
  [t.clusterId || t.clusterName, t.region].filter(Boolean).join(", ");

export function sparkTarget(spec: SparkClusterSpec | undefined): ClusterTarget {
  const t = spec?.target;
  switch (t?.case) {
    case "submit":
      return {
        label: "spark-submit",
        where: t.value.master,
        settings: [
          { label: "Master", value: t.value.master, mono: true },
          { label: "Web UI", value: t.value.ui || undefined, mono: true, unset: "Port 8080 on the master's host" },
        ],
        conf: t.value.conf,
      };
    case "emr":
      return { label: "EMR on EC2", where: emrWhere(t.value), settings: emr(t.value, "submission") };
    default:
      return { label: "Unknown", where: "", settings: [] };
  }
}

const resources = (r: FlinkResources | undefined) => `${r?.cpu || 1} CPU, ${r?.memory || "2048m"}`;

export function flinkTarget(spec: FlinkClusterSpec | undefined): ClusterTarget {
  const t = spec?.target;
  const conf = spec?.conf;
  switch (t?.case) {
    case "kubernetes":
      return {
        label: "Kubernetes",
        where: t.value.namespace,
        settings: [
          { label: "Namespace", value: t.value.namespace, mono: true },
          { label: "Image", value: t.value.image || undefined, mono: true, unset: "flink:1.20, the default" },
          { label: "Service account", value: t.value.serviceAccount || undefined, mono: true, unset: "flink, the default" },
          valueOrSecret("Kubeconfig", t.value.kubeconfig, "The runner's own service account, in its cluster"),
          { label: "JobManager", value: resources(t.value.jobManager) },
          { label: "TaskManager", value: resources(t.value.taskManager) },
        ],
        conf,
      };
    case "rest":
      return {
        label: "Session cluster",
        where: t.value.url,
        settings: [
          { label: "REST API", value: t.value.url, mono: true },
          valueOrSecret("Bearer token", t.value.auth, "None"),
        ],
        conf,
      };
    case "emr":
      return { label: "EMR on EC2", where: emrWhere(t.value), settings: emr(t.value, "deployment"), conf };
    default:
      return { label: "Unknown", where: "", settings: [], conf };
  }
}

/** How a pipeline's current spec uses a cluster: in each sync, after it, or both; undefined if it doesn't. */
export function clusterUse(p: Pipeline, kind: ClusterKind, cluster: string): string | undefined {
  const engine = p.spec?.transform?.engine;
  const transform = engine?.case === kind && engine.value.cluster === cluster;
  const after = kind === "spark" && (p.spec?.postSync ?? []).some((s) => s.step.case === "spark" && s.step.value.cluster === cluster);
  if (transform && after) return "Transform, and after each sync";
  if (transform) return kind === "flink" ? "Transform" : "Transform in each sync";
  if (after) return "After each sync";
  return undefined;
}
