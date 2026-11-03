import { describe, expect, it } from "vitest";
import { afterSignIn, newPasswordProblem } from "@/lib/sign-in";

describe("newPasswordProblem", () => {
  it("wants 15 to 256 characters, counted as characters", () => {
    expect(newPasswordProblem({ password: "a".repeat(14), confirm: "a".repeat(14) })).toMatch(/at least 15/);
    expect(newPasswordProblem({ password: "🙂".repeat(15), confirm: "🙂".repeat(15) })).toBeUndefined();
    expect(newPasswordProblem({ password: "a".repeat(257), confirm: "a".repeat(257) })).toMatch(/at most 256/);
  });
  it("wants the two to match", () => {
    expect(newPasswordProblem({ password: "correct horse battery", confirm: "correct horse batteryy" })).toMatch(/don't match/);
    expect(newPasswordProblem({ password: "correct horse battery", confirm: "correct horse battery" })).toBeUndefined();
  });
});

describe("afterSignIn", () => {
  it("goes back to a page in the app", () => {
    expect(afterSignIn("/workspaces/default/pipelines?status=failed")).toBe("/workspaces/default/pipelines?status=failed");
  });
  it("goes home for anything else", () => {
    for (const next of [undefined, "", "https://evil.example", "//evil.example", "/\\evil.example", "/sign-in?next=/"]) {
      expect(afterSignIn(next)).toBe("/");
    }
  });
});
