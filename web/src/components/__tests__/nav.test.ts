import { describe, expect, it } from "vitest";
import { visibleNav } from "@/components/nav";

const labels = (can: (p: string) => boolean) => visibleNav(can).flatMap((g) => g.items.map((it) => it.label));

describe("nav", () => {
  it("shows the audit log only to callers who may read it", () => {
    expect(labels(() => false)).not.toContain("Audit log");
    expect(labels((p) => p === "audit.get")).toContain("Audit log");
    expect(labels(() => false)).toContain("Pipelines");
  });
});
