import { create, fromJson } from "@bufbuild/protobuf";
import { ValueSchema } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "vitest";
import { FlinkClusterSpecSchema } from "@/gen/openetl/api/v1/flink_pb";
import { PipelineSchema } from "@/gen/openetl/api/v1/pipeline_pb";
import { SparkClusterSpecSchema } from "@/gen/openetl/api/v1/spark_pb";
import { clusterUse, flinkTarget, secretRef, sparkTarget } from "../clusters";

const setting = (t: ReturnType<typeof sparkTarget>, label: string) => t.settings.find((s) => s.label === label);

describe("secretRef", () => {
  it("is the name in {secret: NAME}, and nothing for any other value", () => {
    expect(secretRef({ secret: "s3-key" })).toBe("s3-key");
    expect(secretRef({ secret: "s3-key", other: 1 })).toBeUndefined();
    expect(secretRef({ secret: 3 })).toBeUndefined();
    expect(secretRef("s3-key")).toBeUndefined();
    expect(secretRef(undefined)).toBeUndefined();
  });
});

describe("sparkTarget", () => {
  it("says what applies to a submit cluster's unset UI, and keeps its conf", () => {
    const t = sparkTarget(create(SparkClusterSpecSchema, { target: { case: "submit", value: { master: "spark://m:7077", conf: { a: "1" } } } }));
    expect(t.label).toBe("spark-submit");
    expect(t.where).toBe("spark://m:7077");
    expect(setting(t, "Web UI")).toMatchObject({ value: undefined, unset: "Port 8080 on the master's host" });
    expect(t.conf).toEqual({ a: "1" });
  });

  it("names an EMR cluster by ID, or by name with what that means", () => {
    const byId = sparkTarget(create(SparkClusterSpecSchema, { target: { case: "emr", value: { clusterId: "j-1", region: "eu-west-1" } } }));
    expect(byId.where).toBe("j-1, eu-west-1");
    expect(setting(byId, "Cluster ID")?.value).toBe("j-1");
    const byName = sparkTarget(create(SparkClusterSpecSchema, { target: { case: "emr", value: { clusterName: "nightly" } } }));
    expect(byName.where).toBe("nightly");
    expect(setting(byName, "Cluster name")?.note).toMatch(/one active cluster/);
    expect(setting(byName, "Region")?.value).toBeUndefined();
  });
});

describe("flinkTarget", () => {
  it("shows a kubeconfig by its secret, and the defaults of what's unset", () => {
    const t = flinkTarget(
      create(FlinkClusterSpecSchema, {
        target: { case: "kubernetes", value: { namespace: "jobs", kubeconfig: fromJson(ValueSchema, { secret: "kube" }), taskManager: { cpu: 2 } } },
        conf: { "x.y": "z" },
      }),
    );
    expect(t.where).toBe("jobs");
    expect(setting(t, "Kubeconfig")).toEqual({ label: "Kubeconfig", secret: "kube" });
    expect(setting(t, "Image")).toMatchObject({ unset: "flink:1.20, the default" });
    expect(setting(t, "JobManager")?.value).toBe("1 CPU, 2048m");
    expect(setting(t, "TaskManager")?.value).toBe("2 CPU, 2048m");
    expect(t.conf).toEqual({ "x.y": "z" });
  });

  it("says a session cluster without a token has none", () => {
    const t = flinkTarget(create(FlinkClusterSpecSchema, { target: { case: "rest", value: { url: "http://flink:8081" } } }));
    expect(setting(t, "Bearer token")).toMatchObject({ unset: "None" });
  });
});

describe("clusterUse", () => {
  const pipeline = create(PipelineSchema, {
    spec: {
      transform: { engine: { case: "spark", value: { cluster: "a" } } },
      postSync: [{ step: { case: "spark", value: { cluster: "b" } } }, { step: { case: "spark", value: { cluster: "a" } } }],
    },
  });
  it("tells a transform from a step after the sync", () => {
    expect(clusterUse(pipeline, "spark", "a")).toBe("Transform, and after each sync");
    expect(clusterUse(pipeline, "spark", "b")).toBe("After each sync");
    expect(clusterUse(pipeline, "spark", "c")).toBeUndefined();
    expect(clusterUse(pipeline, "flink", "a")).toBeUndefined();
  });
});
