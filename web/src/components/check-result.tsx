import { CircleCheck, CircleMinus, CircleX, TriangleAlert, type LucideIcon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import type { CheckItem, CheckResult } from "@/gen/openetl/api/v1/connection_pb";
import { cn } from "@/lib/utils";

type Mark = { icon: LucideIcon; className: string; label: string };

const skipped: Mark = { icon: CircleMinus, className: "text-muted-foreground", label: "Skipped" };
const marks: Record<string, Mark> = {
  passed: { icon: CircleCheck, className: "text-healthy", label: "Passed" },
  warning: { icon: TriangleAlert, className: "text-degraded", label: "Warning" },
  failed: { icon: CircleX, className: "text-failed", label: "Failed" },
  skipped,
};

/** How many items ended each way, for the result's summary: "4 passed, 1 failed". */
export function checkCounts(items: CheckItem[]): string {
  return ["passed", "warning", "failed", "skipped"]
    .map((s) => [s, items.filter((i) => i.status === s).length] as const)
    .filter(([, n]) => n > 0)
    .map(([s, n]) => `${n} ${s === "warning" ? (n === 1 ? "warning" : "warnings") : s}`)
    .join(", ");
}

/** Items of a check or a preflight, one a row. */
export function CheckItemList({ items }: { items: CheckItem[] }) {
  return (
    <ul className="divide-y rounded-lg border px-4">
      {items.map((it) => (
        <CheckItemRow key={it.name} item={it} />
      ))}
    </ul>
  );
}

function CheckItemRow({ item }: { item: CheckItem }) {
  const mark = marks[item.status] ?? skipped;
  const Icon = mark.icon;
  const loud = item.status === "failed" || item.status === "warning";
  return (
    <li className="flex gap-3 py-2.5">
      <Icon className={cn("mt-0.5 size-4 shrink-0", mark.className)} aria-label={mark.label} />
      <div className="grid min-w-0 gap-1">
        <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
          <span className="text-sm font-medium">{item.title}</span>
          {item.code && <Badge variant="outline" className="font-mono text-[0.6875rem]">{item.code}</Badge>}
          {loud && item.field && (
            <Badge variant="secondary" className="font-mono text-[0.6875rem] font-normal">
              {item.field}
            </Badge>
          )}
        </div>
        {item.message && <p className="text-sm break-words text-muted-foreground">{item.message}</p>}
        {loud && item.fix && (
          <p className="text-sm break-words">
            <span className="font-medium">Fix: </span>
            {item.fix}
          </p>
        )}
      </div>
    </li>
  );
}

/** A connector's check: what it looked at, item by item, or its one result when it reports no items. */
export function CheckResultView({ result }: { result: CheckResult }) {
  if (result.items.length === 0) {
    return result.ok ? (
      <Alert>
        <CircleCheck className="text-healthy" aria-hidden />
        <AlertTitle>The check passed</AlertTitle>
        <AlertDescription>The connector reached it with this config. It doesn't say what it looked at.</AlertDescription>
      </Alert>
    ) : (
      <Alert variant="destructive">
        <CircleX aria-hidden />
        <AlertTitle>The check failed{result.code && `: ${result.code}`}</AlertTitle>
        <AlertDescription className="break-words">{result.message}</AlertDescription>
      </Alert>
    );
  }
  return (
    <div className="grid gap-2">
      <p className="text-sm text-muted-foreground">
        {result.ok ? "The check passed" : "The check failed"}: {checkCounts(result.items)}.
      </p>
      <CheckItemList items={result.items} />
    </div>
  );
}
