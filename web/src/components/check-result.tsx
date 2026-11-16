import { useId, useState } from "react";
import { CircleCheck, CircleMinus, CircleX, Info, TriangleAlert, type LucideIcon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import type { CheckItem, CheckResult } from "@/gen/openetl/api/v1/connection_pb";
import { checkCounts, checkFailed, failureSaysMore, itemFailed, itemStatus, templateParts, type ItemStatus } from "@/lib/checks";
import { cn } from "@/lib/utils";

type Mark = { icon: LucideIcon; className: string; label: string };

const marks: Record<ItemStatus, Mark> = {
  passed: { icon: CircleCheck, className: "text-healthy", label: "Passed" },
  warning: { icon: TriangleAlert, className: "text-degraded", label: "Warning" },
  failed: { icon: CircleX, className: "text-failed", label: "Failed" },
  skipped: { icon: CircleMinus, className: "text-muted-foreground", label: "Skipped" },
};

/** A template from a connector, filled from params, each value set off from the words around it. */
export function Template({ text, params }: { text: string; params: Record<string, string> }) {
  return (
    <>
      {templateParts(text, params).map((p, i) =>
        typeof p === "string" ? (
          p
        ) : (
          <span key={i} className="font-mono text-[0.92em] text-foreground">
            {p.value}
          </span>
        ),
      )}
    </>
  );
}

/** The result's or the part's own code and message, when no failed item says as much. */
export function FailureNote({ title, code, message }: { title: string; code: string; message: string }) {
  return (
    <Alert variant="destructive">
      <CircleX aria-hidden />
      <AlertTitle>
        {title}
        {code && `: ${code}`}
      </AlertTitle>
      {message && <AlertDescription className="break-words whitespace-pre-wrap">{message}</AlertDescription>}
    </Alert>
  );
}

/** Items of a check or a preflight, one a row. */
export function CheckItemList({ items }: { items: CheckItem[] }) {
  return (
    <ul className="divide-y rounded-lg border px-4">
      {items.map((it, i) => (
        // A connector can send two items with the same name.
        <CheckItemRow key={`${i}:${it.name}`} item={it} />
      ))}
    </ul>
  );
}

function CheckItemRow({ item }: { item: CheckItem }) {
  const [open, setOpen] = useState(false);
  const id = useId();
  const status = itemStatus(item);
  const mark = marks[status];
  const Icon = mark.icon;
  const loud = status === "failed" || status === "warning";
  const p = item.params;
  return (
    <li className="flex gap-3 py-2.5">
      <Icon className={cn("mt-0.5 size-4 shrink-0", mark.className)} aria-hidden />
      <div className="grid min-w-0 flex-1 gap-1.5">
        <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
          <span className="text-sm font-medium break-words">
            <span className="sr-only">{mark.label}: </span>
            {item.title ? <Template text={item.title} params={p} /> : item.name}
          </span>
          {item.kind && (
            <Badge variant="outline" className="text-[0.6875rem] font-normal text-muted-foreground">
              {item.kind.charAt(0).toUpperCase() + item.kind.slice(1)}
            </Badge>
          )}
        </div>
        {item.description && (
          <p id={`${id}-about`} hidden={!open} className="text-xs break-words text-muted-foreground">
            <Template text={item.description} params={p} />
          </p>
        )}
        {item.message && (
          <p className="text-sm break-words text-muted-foreground">
            <Template text={item.message} params={p} />
          </p>
        )}
        {loud && item.detail && (
          <div className="grid gap-1">
            <p id={`${id}-detail`} className="text-xs text-muted-foreground">
              Detail
            </p>
            <pre
              tabIndex={0}
              aria-labelledby={`${id}-detail`}
              className="max-h-48 overflow-auto rounded-md bg-muted px-3 py-2 font-mono text-xs break-words whitespace-pre-wrap"
            >
              {item.detail}
            </pre>
          </div>
        )}
        {loud && item.fix && (
          <p className="rounded-md border px-3 py-2 text-sm break-words">
            <span className="font-medium">Fix: </span>
            <Template text={item.fix} params={p} />
          </p>
        )}
        {loud && (item.field || item.code || item.reason) && (
          <dl className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
            {[
              ["Field", item.field],
              ["Code", item.code],
              ["Reason", item.reason],
            ].map(
              ([label, value]) =>
                value && (
                  <div key={label} className="flex min-w-0 gap-1.5">
                    <dt>{label}</dt>
                    <dd className="font-mono break-all text-foreground">{value}</dd>
                  </div>
                ),
            )}
          </dl>
        )}
      </div>
      {item.description && (
        <Button
          variant="ghost"
          size="icon-xs"
          className="text-muted-foreground aria-expanded:bg-accent aria-expanded:text-foreground"
          aria-label="What it checks"
          title="What it checks"
          aria-expanded={open}
          aria-controls={`${id}-about`}
          onClick={() => setOpen(!open)}
        >
          <Info aria-hidden />
        </Button>
      )}
    </li>
  );
}

const sentence = (s: string) => `${s.charAt(0).toUpperCase()}${s.slice(1)}.`;

/** A connector's check: what it looked at, item by item, or its one result when it reports no items. */
export function CheckResultView({ result }: { result: CheckResult }) {
  const failed = checkFailed(result);
  const items = result.items;
  if (failed && !items.some(itemFailed)) {
    return (
      <div className="grid gap-2">
        <FailureNote title="The check failed" code={result.code} message={result.message} />
        {items.length > 0 && (
          <>
            <p className="text-sm text-muted-foreground">{sentence(checkCounts(items))}</p>
            <CheckItemList items={items} />
          </>
        )}
      </div>
    );
  }
  if (items.length === 0) {
    return (
      <Alert>
        <CircleCheck className="text-healthy" aria-hidden />
        <AlertTitle>The check passed</AlertTitle>
        <AlertDescription>The connector reached it with this config. It doesn't say what it looked at.</AlertDescription>
      </Alert>
    );
  }
  return (
    <div className="grid gap-2">
      {failed && failureSaysMore(result) && <FailureNote title="The check failed" code={result.code} message={result.message} />}
      <p className="text-sm text-muted-foreground">
        {failed ? "The check failed" : "The check passed"}: {checkCounts(items)}.
      </p>
      <CheckItemList items={items} />
    </div>
  );
}
