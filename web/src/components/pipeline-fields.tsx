import type { Pipeline } from "@/gen/openetl/api/v1/pipeline_pb";
import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
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

export function Schedule({ pipeline, className }: { pipeline: Pipeline; className?: string }) {
  const cron = pipeline.spec?.schedule?.trigger.case === "cron";
  return <span className={cn(cron && "font-mono text-[0.8125rem]", className)}>{scheduleLabel(pipeline.spec)}</span>;
}

export function IncidentCount({ count, className }: { count: number; className?: string }) {
  if (count <= 0) return null;
  return (
    <Badge variant="outline" className={cn("font-normal text-muted-foreground", className)}>
      {count === 1 ? "1 incident" : `${count} incidents`}
    </Badge>
  );
}
