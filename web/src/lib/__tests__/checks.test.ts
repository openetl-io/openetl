import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { CheckItemSchema, CheckResultSchema } from "@/gen/openetl/api/v1/connection_pb";
import { PreflightPartSchema, PreflightResultSchema } from "@/gen/openetl/api/v1/pipeline_pb";
import { checkCounts, checkFailed, itemStatus, partFailed, preflightFailed, templateParts } from "../checks";

describe("templateParts", () => {
  const p = { host: "db.example.com", port: "5432", a: "{host}", empty: "" };
  it("fills a known placeholder, kept apart from the text", () => {
    expect(templateParts("Can't reach {host}.", p)).toEqual(["Can't reach ", { name: "host", value: "db.example.com" }, "."]);
  });
  it("leaves a placeholder without a param as written", () => {
    expect(templateParts("Reach {missing} now", p)).toEqual(["Reach {missing} now"]);
  });
  it("leaves near-misses of the grammar as written", () => {
    for (const t of ["{Host}", "{db-host}", "{1host}", "{}", "{ host }", "{host", '{"error": "x"}']) expect(templateParts(t, p)).toEqual([t]);
  });
  it("fills adjacent placeholders", () => {
    expect(templateParts("{host}:{port}", p)).toEqual([{ name: "host", value: "db.example.com" }, ":", { name: "port", value: "5432" }]);
  });
  it("doesn't fill a value again", () => {
    expect(templateParts("{a} and {{host}}", p)).toEqual([{ name: "a", value: "{host}" }, " and {", { name: "host", value: "db.example.com" }, "}"]);
  });
  it("fills an empty value, and only the params' own names", () => {
    expect(templateParts("[{empty}]", p)).toEqual(["[", { name: "empty", value: "" }, "]"]);
    expect(templateParts("{constructor} {to_string}", {})).toEqual(["{constructor} {to_string}"]);
  });
  it("takes an empty params map and empty text", () => {
    expect(templateParts("Reach {host}", {})).toEqual(["Reach {host}"]);
    expect(templateParts("", p)).toEqual([]);
  });
});

const item = (status: string) => create(CheckItemSchema, { name: status, title: status, status });

describe("checkCounts", () => {
  it("counts a status it doesn't know as failed", () => {
    expect(checkCounts([item("passed"), item("passed"), item("warning"), item("skipped"), item("failed"), item("FAILED"), item("")])).toBe(
      "2 passed, 1 warning, 3 failed, 1 skipped",
    );
    expect(checkCounts([item("warning"), item("warning")])).toBe("2 warnings");
    expect(checkCounts([])).toBe("");
  });
  it("knows only the four statuses", () => {
    expect(["passed", "warning", "failed", "skipped", "unknown", "PASSED"].map((s) => itemStatus(item(s)))).toEqual([
      "passed",
      "warning",
      "failed",
      "skipped",
      "failed",
      "failed",
    ]);
  });
});

describe("pass or fail", () => {
  it("fails a check whose result or any item failed", () => {
    expect(checkFailed(create(CheckResultSchema, { ok: true, items: [item("passed"), item("warning"), item("skipped")] }))).toBe(false);
    expect(checkFailed(create(CheckResultSchema, { ok: true, items: [item("passed"), item("failed")] }))).toBe(true);
    expect(checkFailed(create(CheckResultSchema, { ok: true, items: [item("broken")] }))).toBe(true);
    expect(checkFailed(create(CheckResultSchema, { ok: false, items: [item("passed")] }))).toBe(true);
    expect(checkFailed(create(CheckResultSchema, { ok: false }))).toBe(true);
  });
  it("fails a part by its items, and not when it was skipped", () => {
    const part = (o: { ok: boolean; skipped?: string; status?: string }) =>
      create(PreflightPartSchema, { ok: o.ok, skipped: o.skipped, items: o.status ? [item(o.status)] : [] });
    expect(partFailed(part({ ok: true }))).toBe(false);
    expect(partFailed(part({ ok: true, status: "warning" }))).toBe(false);
    expect(partFailed(part({ ok: true, status: "failed" }))).toBe(true);
    expect(partFailed(part({ ok: true, status: "nonsense" }))).toBe(true);
    expect(partFailed(part({ ok: false }))).toBe(true);
    // The API sends a skipped part as not ok.
    expect(partFailed(part({ ok: false, skipped: "the source's check failed" }))).toBe(false);
    const result = (ok: boolean, ...parts: ReturnType<typeof part>[]) => create(PreflightResultSchema, { ok, parts });
    expect(preflightFailed(result(true, part({ ok: true }), part({ ok: false, skipped: "x" })))).toBe(false);
    expect(preflightFailed(result(true, part({ ok: true, status: "failed" })))).toBe(true);
    expect(preflightFailed(result(false, part({ ok: true })))).toBe(true);
  });
});
