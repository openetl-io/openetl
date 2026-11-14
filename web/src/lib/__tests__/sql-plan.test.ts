import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { TransformPlanSchema } from "@/gen/openetl/api/v1/pipeline_pb";
import { aboutSql, notDelivered, partOf, parts, position, sqlMessages } from "../sql-plan";

describe("sqlMessages", () => {
  it("keeps the plan's messages about transform.sql, with their places, problems first", () => {
    const got = sqlMessages(
      [
        "delivery.require: stream \"orders\" gets at_least_once, below exactly_once",
        "transform.sql:11:27: there's no column mail; there are id, email",
        "transform.sql:8:10: a group window: transforms don't run group windows yet (tier 2)",
        "transform.sql: a transform writes at least one output, with INSERT INTO … SELECT",
      ],
      ["transform.sql:1:13: destination. is a qualifier here", "transform.sql wasn't planned: no runner of data plane \"default\" took it within 30s", "each sync writes new files"],
    );
    expect(got).toEqual([
      { tone: "problem", message: "a transform writes at least one output, with INSERT INTO … SELECT" },
      { tone: "problem", line: 8, column: 10, message: "a group window: transforms don't run group windows yet (tier 2)" },
      { tone: "problem", line: 11, column: 27, message: "there's no column mail; there are id, email" },
      { tone: "note", message: "transform.sql wasn't planned: no runner of data plane \"default\" took it within 30s" },
      { tone: "note", line: 1, column: 13, message: "destination. is a qualifier here" },
    ]);
    expect(position(got[1]!)).toBe("8:10");
    expect(position(got[0]!)).toBe("");
  });

  it("tells messages about transform.sql from the rest", () => {
    expect(aboutSql("transform.sql:1:1: x")).toBe(true);
    expect(aboutSql("transform.sql wasn't planned: x")).toBe(true);
    expect(aboutSql("transform.sqlx")).toBe(false);
    expect(aboutSql("stream \"transform.sql\" x")).toBe(false);
  });
});

describe("partOf", () => {
  const t = create(TransformPlanSchema, {
    views: [{ name: "paid", line: 1, column: 13 }],
    statements: [
      { output: "facts", line: 6, column: 1 },
      { output: "customers", line: 15, column: 1, replaces: true },
    ],
    withheld: ["customers", "events"],
  });
  const all = parts(t);

  it("orders statements and views as they're written", () => {
    expect(all.map((p) => (p.kind === "view" ? p.at.name : p.at.output))).toEqual(["paid", "facts", "customers"]);
  });

  it("finds the statement or view a message is in", () => {
    const at = (line: number, column: number) => partOf(all, { tone: "note", line, column, message: "" });
    expect(at(1, 1)).toBeUndefined();
    expect(at(2, 4)?.kind).toBe("view");
    expect(at(6, 1)?.at).toBe(t.statements[0]);
    expect(at(14, 80)?.at).toBe(t.statements[0]);
    expect(at(16, 2)?.at).toBe(t.statements[1]);
    expect(partOf(all, { tone: "note", message: "" })).toBeUndefined();
  });

  it("leaves out of not delivered the streams a statement replaces", () => {
    expect(notDelivered(t)).toEqual(["events"]);
  });
});
