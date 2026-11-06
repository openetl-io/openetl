import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { JobSizeSchema, SyncAttemptSchema, UsageSchema } from "@/gen/openetl/api/v1/sync_pb";
import { bytes, cpuTime, sizeAmounts, sizeLabel, syncUse } from "@/lib/sizes";

describe("sizes", () => {
  it("names a size by its preset, or its amounts", () => {
    const m = create(JobSizeSchema, { preset: "m", cpuMillis: 2000n, memoryBytes: 4n << 30n });
    expect(sizeLabel(m)).toBe("m");
    expect(sizeAmounts(m)).toBe("2 vCPU, 4 GiB");
    expect(sizeLabel(create(JobSizeSchema, { cpuMillis: 500n, memoryBytes: 512n << 20n }))).toBe("0.5 vCPU, 512 MiB");
    expect(sizeLabel(undefined)).toBe("");
  });

  it("formats what's used", () => {
    expect(bytes(1610612736n)).toBe("1.5 GiB");
    expect(cpuTime(45)).toBe("45 s");
    expect(cpuTime(192)).toBe("3.2 min");
    expect(cpuTime(5400)).toBe("1.5 h");
  });

  it("adds attempts' CPU, and takes the highest peak", () => {
    const attempt = (cpuSeconds: number, peakMemoryBytes: bigint) =>
      create(SyncAttemptSchema, { usage: create(UsageSchema, { cpuSeconds, peakMemoryBytes, runnerCpuSeconds: 1, runnerMemoryBytes: 10n }) });
    expect(syncUse([attempt(10, 100n), attempt(20, 50n)])).toEqual({ cpu: 32, memory: 110n });
    expect(syncUse([create(SyncAttemptSchema)])).toEqual({ cpu: 0, memory: 0n });
  });
});
