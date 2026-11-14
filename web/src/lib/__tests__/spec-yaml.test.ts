import { create } from "@bufbuild/protobuf";
import { durationFromMs } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "vitest";
import {
  PipelineDestinationSchema,
  PipelineSpecSchema,
  ScheduleSchema,
  WriteMode,
  PipelineMode,
  Passthrough,
} from "@/gen/openetl/api/v1/pipeline_pb";
import { SyncMode } from "@/gen/openetl/api/v1/connection_pb";
import { goDuration, pipelineYAML } from "../spec-yaml";

describe("pipelineYAML", () => {
  it("spells enums and durations as documents do", () => {
    const spec = create(PipelineSpecSchema, {
      mode: PipelineMode.SCHEDULED,
      schedule: create(ScheduleSchema, { trigger: { case: "every", value: durationFromMs(5_400_000) } }),
      destination: create(PipelineDestinationSchema, { ref: "warehouse", write: WriteMode.MERGE }),
    });
    const y = pipelineYAML("shop", "default", spec);
    expect(y).toContain("kind: Pipeline");
    expect(y).toContain("mode: scheduled");
    expect(y).toContain("every: 1h30m");
    expect(y).toContain("write: merge");
    expect(y).not.toContain("workspace");
  });

  it("spells enums of the files the spec imports too", () => {
    const spec = create(PipelineSpecSchema, { source: { ref: "shop", streams: [{ name: "orders", sync: SyncMode.FULL_REFRESH }] }, destination: { ref: "dw", tables: { orders: "fct_orders" } } });
    const y = pipelineYAML("shop", "default", spec);
    expect(y).toContain("sync: full_refresh");
    expect(y).toContain("orders: fct_orders");
  });

  it("writes transform.sql as text, with passthrough beside it", () => {
    const sql = "INSERT INTO contacts\nSELECT id, lower(email) AS email FROM customers;\n";
    const spec = create(PipelineSpecSchema, { transform: { engine: { case: "sql", value: { sql, passthrough: Passthrough.NONE } } } });
    const y = pipelineYAML("shop", "default", spec);
    expect(y).toContain("transform:\n    sql: |\n      INSERT INTO contacts\n      SELECT id, lower(email) AS email FROM customers;\n    passthrough: none\n");
    const all = pipelineYAML("shop", "default", create(PipelineSpecSchema, { transform: { engine: { case: "sql", value: { sql } } } }));
    expect(all).not.toContain("passthrough");
  });

  it("formats durations", () => {
    expect(goDuration("120s")).toBe("2m");
    expect(goDuration("0.250s")).toBe("250ms");
    expect(goDuration("3661s")).toBe("1h1m1s");
  });
});
