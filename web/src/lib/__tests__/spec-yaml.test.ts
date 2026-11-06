import { create } from "@bufbuild/protobuf";
import { durationFromMs } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "vitest";
import {
  PipelineDestinationSchema,
  PipelineSpecSchema,
  ScheduleSchema,
  WriteMode,
  PipelineMode,
} from "@/gen/openetl/api/v1/pipeline_pb";
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

  it("formats durations", () => {
    expect(goDuration("120s")).toBe("2m");
    expect(goDuration("0.250s")).toBe("250ms");
    expect(goDuration("3661s")).toBe("1h1m1s");
  });
});
