import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { durationFromMs } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "vitest";
import { OperatorChange, OperatorPlanSchema, TransformPlanSchema } from "@/gen/openetl/api/v1/pipeline_pb";
import { aboutSql, changeLabel, changeText, heldText, notDelivered, span, operatorComputes, partOf, parts, position, sqlMessages } from "../sql-plan";

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

describe("operators", () => {
  const op = (init: MessageInitShape<typeof OperatorPlanSchema>) => create(OperatorPlanSchema, init);

  it("says what an operator computes, by the stream's names", () => {
    const tumble = op({ window: "tumble", size: durationFromMs(60_000), eventTime: "at", keys: ["region"], aggregates: ["count(*)", "sum(amount)"] });
    expect(operatorComputes(tumble)).toBe("tumble 1m by at on region: count(*), sum(amount)");
    const hop = op({ window: "hop", size: durationFromMs(300_000), slide: durationFromMs(60_000), eventTime: "at", aggregates: ["max(qty)"] });
    expect(operatorComputes(hop)).toBe("hop 5m every 1m by at: max(qty)");
    expect(operatorComputes(op({ id: "window-1", change: OperatorChange.DROPPED }))).toBe("");
    expect(operatorComputes(op({ window: "tumble", size: durationFromMs(3_600_000), eventTime: "at" }))).toBe("tumble 1h by at: ");
    expect(operatorComputes(op({ window: "tumble", size: durationFromMs(3_601_000), eventTime: "at" }))).toBe("tumble 1h1s by at: ");
    expect(span(durationFromMs(90_500))).toBe("1m30.5s");
  });

  it("says what applying does to its state", () => {
    expect(changeLabel[OperatorChange.CHANGED]).toBe("Changed");
    expect(changeText(op({ change: OperatorChange.CHANGED, replaces: "window-old" }))).toBe(
      "Its state starts empty. It takes the place of window-old, whose state is dropped.",
    );
    expect(changeText(op({ change: OperatorChange.KEPT }))).toBe("Keeps its state.");
    expect(heldText(op({}))).toBe("");
    expect(heldText(op({ held: true }))).toBe("The last checkpoint holds its state.");
    expect(heldText(op({ held: false }))).toBe("The last checkpoint holds none of its state.");
    expect(changeText(op({ change: OperatorChange.NEW }))).toBe("Its state starts empty.");
    expect(changeText(op({ change: OperatorChange.NEW, earlier: true }))).toContain("the next sync restores that state;");
    expect(changeText(op({ change: OperatorChange.CHANGED, earlier: true, replaces: "window-b" }))).toMatch(
      /^An earlier version had it\..* It takes the place of window-b, whose state is dropped\.$/,
    );
    expect(changeText(op({}))).toBe("The current version wasn't planned when it was applied.");
  });
});
