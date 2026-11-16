import { create } from "@bufbuild/protobuf";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";
import { CheckItemList, CheckResultView, Template } from "@/components/check-result";
import { PreflightView } from "@/components/preflight";
import { CheckItemSchema, CheckResultSchema } from "@/gen/openetl/api/v1/connection_pb";
import { PreflightPartSchema, PreflightResultSchema } from "@/gen/openetl/api/v1/pipeline_pb";

// The API's client wants a browser; these components only render.
vi.mock("@/lib/api", () => ({ errorMessage: String }));

const failed = create(CheckItemSchema, {
  name: "net.connect",
  kind: "connection",
  status: "failed",
  title: "Reach {address}",
  message: "Can't reach {address}.",
  fix: "Check the address in the config, and the network path from the runner.",
  field: "host",
  code: "TRANSIENT",
  reason: "unreachable",
  detail: "dial tcp 10.0.4.12:5432: connect: no route to host\n<b>second line</b>",
  description: "Opens a TCP connection to {address}.",
  params: { address: "db.example.com:5432" },
});
const passed = create(CheckItemSchema, {
  name: "net.dns",
  kind: "connection",
  status: "passed",
  title: "Resolve {host}",
  message: "Resolves to {ips}.",
  fix: "Not shown",
  params: { host: "db", ips: "10.0.4.12" },
});
const skipped = create(CheckItemSchema, { name: "db.connect", status: "skipped", title: "Sign in", message: "needs net.connect, which failed" });

const html = (n: React.ReactNode) => renderToStaticMarkup(<>{n}</>);
const text = (h: string) => h.replace(/<[^>]+>/g, "").replace(/&#x27;/g, "'");

describe("Template", () => {
  it("sets a value apart, as text", () => {
    expect(html(<Template text="Can't reach {host}" params={{ host: "<b>db</b>" }} />)).toBe(
      'Can&#x27;t reach <span class="font-mono text-[0.92em] text-foreground">&lt;b&gt;db&lt;/b&gt;</span>',
    );
  });
});

describe("CheckItemList", () => {
  it("shows a failed item's message, detail, fix, field, code and reason, rendered", () => {
    const h = html(<CheckItemList items={[failed]} />);
    const t = text(h);
    for (const s of [
      "Failed: Reach db.example.com:5432",
      "Connection",
      "Can't reach db.example.com:5432.",
      "Fix: Check the address",
      "Fieldhost",
      "CodeTRANSIENT",
      "Reasonunreachable",
    ])
      expect(t).toContain(s);
    expect(h).toContain("no route to host\n&lt;b&gt;second line&lt;/b&gt;</pre>");
    expect(h).not.toContain("{address}");
    // The description waits behind a button that says whether it's open.
    expect(h).toMatch(/<p id="[^"]+-about" hidden="".*>Opens a TCP connection to/);
    expect(h).toMatch(/aria-label="What it checks"[^>]*aria-expanded="false"/);
  });
  it("shows a passed item's message and a skipped item's reason, and no fix", () => {
    const t = text(html(<CheckItemList items={[passed, skipped]} />));
    expect(t).toContain("Passed: Resolve db");
    expect(t).toContain("Resolves to 10.0.4.12.");
    expect(t).toContain("Skipped: Sign in");
    expect(t).toContain("needs net.connect, which failed");
    expect(t).not.toContain("Not shown");
  });
  it("shows a status it doesn't know as failed", () => {
    const odd = create(CheckItemSchema, { name: "net.dns", status: "exploded", title: "Odd" });
    const t = text(html(<CheckItemList items={[passed, odd]} />));
    expect(t).toContain("Passed: Resolve db");
    expect(t).toContain("Failed: Odd");
  });
});

describe("CheckResultView", () => {
  it("fails from its items whatever ok says", () => {
    const t = text(html(<CheckResultView result={create(CheckResultSchema, { ok: true, items: [passed, failed] })} />));
    expect(t).toContain("The check failed: 1 passed, 1 failed.");
  });
  it("shows its own code and message when no item failed", () => {
    const r = create(CheckResultSchema, { ok: false, code: "TRANSIENT", message: "the checks ran out of time", items: [passed, skipped] });
    const t = text(html(<CheckResultView result={r} />));
    expect(t).toContain("The check failed: TRANSIENT");
    expect(t).toContain("the checks ran out of time");
    expect(t).toContain("1 passed, 1 skipped.");
  });
  it("leaves its own code and message to the failed item they come from", () => {
    const message = "Reach db.example.com:5432: Can't reach db.example.com:5432: dial tcp 10.0.4.12:5432: connect: no route to host\n<b>second line</b>";
    const t = text(html(<CheckResultView result={create(CheckResultSchema, { ok: false, code: "TRANSIENT", message, items: [failed] })} />));
    expect(t).not.toContain("The check failed: TRANSIENT");
    expect(t).toContain("The check failed: 1 failed.");
  });
  it("shows its own code and message beside a failed item when they say more", () => {
    const auth = create(CheckItemSchema, { name: "auth", status: "failed", title: "Sign in", message: "The password is wrong.", code: "AUTH_INVALID" });
    for (const [code, message] of [
      ["TRANSIENT", "the checks ran out of time"],
      ["AUTH_INVALID", "the checks ran out of time"],
      ["TRANSIENT", "Sign in: The password is wrong."],
    ]) {
      const t = text(html(<CheckResultView result={create(CheckResultSchema, { ok: false, code, message, items: [passed, auth] })} />));
      expect(t).toContain(`The check failed: ${code}${message}`);
      expect(t).toContain("The check failed: 1 passed, 1 failed.");
    }
  });
  it("passes with warnings", () => {
    const warning = create(CheckItemSchema, { name: "w", status: "warning", title: "W" });
    expect(text(html(<CheckResultView result={create(CheckResultSchema, { ok: true, items: [passed, warning] })} />))).toContain(
      "The check passed: 1 passed, 1 warning.",
    );
  });
});

describe("PreflightView", () => {
  const part = (o: Parameters<typeof create<typeof PreflightPartSchema>>[1]) =>
    create(PreflightPartSchema, { resource: "workspaces/default/sources/orders", ...o });
  it("shows a failed part with no failed item, and a skipped part's reason", () => {
    const r = create(PreflightResultSchema, {
      ok: false,
      parts: [
        part({ role: "source", stage: "check", ok: false, code: "TRANSIENT", message: "the connector didn't answer", items: [passed] }),
        part({ role: "source", stage: "preflight", ok: false, skipped: "the source's check failed" }),
        part({ role: "sync", stage: "plan", ok: false, code: "CONFIG_INVALID", message: "a sync can't be planned" }),
      ],
    });
    const t = text(html(<PreflightView result={r} />));
    expect(t).toContain("Failed: Source orders: connection");
    expect(t).toContain("Failed: TRANSIENTthe connector didn't answer");
    expect(t).toContain("1 passed.");
    expect(t).toContain("Skipped: Source orders: pipeline's streams");
    expect(t).toContain("Skipped: the source's check failed.");
    expect(t).toContain("Failed: CONFIG_INVALIDa sync can't be planned");
  });
  it("fails a part from its items", () => {
    const r = create(PreflightResultSchema, { ok: true, parts: [part({ role: "source", stage: "preflight", ok: true, items: [failed] })] });
    const t = text(html(<PreflightView result={r} />));
    expect(t).toContain("Failed: Source orders: pipeline's streams");
    expect(t).toContain("1 failed.");
  });
  it("shows a part's own code and message beside a failed item when they say more", () => {
    const own = part({ role: "source", stage: "check", ok: false, code: "TRANSIENT", message: "the source's check didn't finish within 1m0s", items: [failed] });
    const t = text(html(<PreflightView result={create(PreflightResultSchema, { ok: false, parts: [own] })} />));
    expect(t).toContain("Failed: TRANSIENTthe source's check didn't finish within 1m0s");
    expect(t).toContain("1 failed.");
    // A plan's problem, as the API makes the part's code and message from it.
    const problem = create(CheckItemSchema, { name: "plan.problem.1", status: "failed", title: "The plan", message: 'no stream "s"', code: "CONFIG_INVALID" });
    const plan = part({ role: "sync", stage: "plan", ok: false, code: "CONFIG_INVALID", message: 'The plan: no stream "s"', items: [problem] });
    expect(text(html(<PreflightView result={create(PreflightResultSchema, { ok: false, parts: [plan] })} />))).not.toContain("Failed: CONFIG_INVALID");
  });
});
