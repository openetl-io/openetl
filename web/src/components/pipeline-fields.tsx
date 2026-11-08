import type { Pipeline } from "@/gen/openetl/api/v1/pipeline_pb";
import { SyncState } from "@/gen/openetl/api/v1/sync_pb";
import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { interval } from "@/lib/format";
import { lastUpdate, scheduleLabel, stale } from "@/lib/pipeline";
import { cn } from "@/lib/utils";
import { RelativeTime } from "./relative-time";

/** When the pipeline's data last changed, marked when it's stale. */
export function Freshness({ pipeline }: { pipeline: Pipeline }) {
  const t = lastUpdate(pipeline);
  if (!t) return <span className="text-muted-foreground">Never</span>;
  if (!stale(pipeline)) return <RelativeTime date={t} />;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="text-degraded">
          <RelativeTime date={t} />
        </span>
      </TooltipTrigger>
      <TooltipContent>Older than two schedule intervals</TooltipContent>
    </Tooltip>
  );
}

/** When syncs start: heal's override of how often, if it set one, or the spec's schedule. */
export function Schedule({ pipeline, className }: { pipeline: Pipeline; className?: string }) {
  const cron = pipeline.spec?.schedule?.trigger.case === "cron";
  const every = pipeline.overrides.find((o) => o.field === "schedule.every");
  if (every && !cron) {
    return (
      <span className={className} title="Heal widened it after syncs kept failing rate limited">
        Every {interval(parseFloat(JSON.parse(every.value)) * 1000)}, by heal
      </span>
    );
  }
  return <span className={cn(cron && "font-mono text-[0.8125rem]", className)}>{scheduleLabel(pipeline.spec)}</span>;
}

/** A running sync that retries failed attempts, such as one whose destination refuses it. */
export function FailedAttempts({ pipeline, className }: { pipeline: Pipeline; className?: string }) {
  const last = pipeline.status?.lastSync;
  if (last?.state !== SyncState.RUNNING || last.failedAttempts <= 0) return null;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Badge variant="outline" className={cn("font-normal text-degraded", className)}>
          {last.failedAttempts === 1 ? "1 failed attempt" : `${last.failedAttempts} failed attempts`}
        </Badge>
      </TooltipTrigger>
      {last.lastAttemptError && <TooltipContent className="max-w-md font-mono whitespace-pre-wrap">{last.lastAttemptError.split("\n")[0]}</TooltipContent>}
    </Tooltip>
  );
}

export function IncidentCount({ count, className }: { count: number; className?: string }) {
  if (count <= 0) return null;
  return (
    <Badge variant="outline" className={cn("font-normal text-muted-foreground", className)}>
      {count === 1 ? "1 incident" : `${count} incidents`}
    </Badge>
  );
}
