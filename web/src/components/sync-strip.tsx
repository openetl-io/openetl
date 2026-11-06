import { Link } from "@tanstack/react-router";
import type { Sync } from "@/gen/openetl/api/v1/sync_pb";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { absolute, compact, dateOf, duration, relative } from "@/lib/format";
import { lastSegment } from "@/lib/names";
import { heldLabel, rows, statusLabel, syncStatus } from "@/lib/status";
import { cn } from "@/lib/utils";
import { statusFill } from "./status";

// Each sync is a round-ended capsule, like the streams in the mark: its
// color is its status, its length grows with how long it took, and time runs
// left to right.
function width(ms: number, size: "sm" | "lg") {
  const [lo, hi] = size === "lg" ? [18, 56] : [10, 26];
  const t = Math.min(1, Math.log10(Math.max(ms, 1000) / 1000) / 3);
  return lo + t * (hi - lo);
}

export function SyncStrip({
  syncs,
  workspace,
  pipeline,
  size = "sm",
  className,
}: {
  /** Newest first, as the API lists them. */
  syncs: Sync[];
  workspace: string;
  pipeline: string;
  size?: "sm" | "lg";
  className?: string;
}) {
  const now = Date.now();
  return (
    <ol
      aria-label="Recent syncs, oldest first"
      className={cn("flex items-center", size === "lg" ? "gap-1" : "gap-[3px]", className)}
    >
      {[...syncs].reverse().map((s) => {
        const status = syncStatus(s);
        const start = dateOf(s.startTime)!;
        const end = dateOf(s.endTime);
        const ms = (end?.getTime() ?? now) - start.getTime();
        const r = rows(s);
        return (
          <li key={s.name} className="flex">
            <Tooltip>
              <TooltipTrigger asChild>
                <Link
                  to="/workspaces/$workspace/pipelines/$pipeline/syncs/$sync"
                  params={{ workspace, pipeline, sync: lastSegment(s.name) }}
                  onClick={(e) => e.stopPropagation()}
                  aria-label={`${statusLabel[status]} sync, ${relative(start, now)}`}
                  className={cn(
                    "block rounded-full transition-[filter] hover:brightness-110 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring",
                    size === "lg" ? "h-2.5" : "h-1.5",
                    statusFill[status],
                    status === "running" && "animate-pulse",
                  )}
                  style={{ width: width(ms, size) }}
                />
              </TooltipTrigger>
              <TooltipContent side="top" className="grid gap-0.5 text-xs">
                <span className="font-medium">
                  {statusLabel[status]} {end ? `in ${duration(ms)}` : `for ${duration(ms)}`}
                </span>
                <span className="opacity-80">{absolute(start)}</span>
                <span className="opacity-80 tabular">
                  {compact(r.all)} rows{r.quarantined > 0n ? `, ${compact(r.quarantined)} quarantined` : ""}
                  {r.held > 0 ? `, ${heldLabel(r.held)}` : ""}
                </span>
              </TooltipContent>
            </Tooltip>
          </li>
        );
      })}
    </ol>
  );
}
