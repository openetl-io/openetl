import { Pause, Play, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { PipelineMode, PipelineService, type Pipeline } from "@/gen/openetl/api/v1/pipeline_pb";
import { SyncService } from "@/gen/openetl/api/v1/sync_pb";
import { useAction } from "@/lib/action";
import { useCan } from "@/lib/caller";
import { pipelineStatus } from "@/lib/status";

const refresh = [PipelineService, SyncService];

/** Sync now, and pause or resume, each shown only to a role that may. */
export function PipelineActions({ workspace, pipeline }: { workspace: string; pipeline: Pipeline }) {
  const can = useCan(workspace);
  const continuous = pipeline.spec?.mode === PipelineMode.CONTINUOUS;
  const sync = useAction(PipelineService.method.syncPipeline, { done: "Sync started", failed: "Couldn't start a sync", refresh });
  const pause = useAction(PipelineService.method.pausePipeline, {
    done: continuous ? "Paused. Its job stops." : "Paused. A sync that's running carries on.",
    failed: "Couldn't pause",
    refresh,
  });
  const resume = useAction(PipelineService.method.resumePipeline, { done: "Resumed", failed: "Couldn't resume", refresh });
  const name = { name: pipeline.name };
  const running = pipelineStatus(pipeline) === "running";

  return (
    <>
      {can("pipelines.sync") && !continuous && !pipeline.paused && (
        <Tooltip open={running ? undefined : false}>
          <TooltipTrigger asChild>
            {/* A disabled button gets no pointer events, so the tooltip hangs on its wrapper. */}
            <span tabIndex={running ? 0 : -1}>
              <Button size="sm" disabled={running || sync.isPending} onClick={() => sync.mutate(name)}>
                <RefreshCw aria-hidden />
                Sync now
              </Button>
            </span>
          </TooltipTrigger>
          <TooltipContent>A sync is running</TooltipContent>
        </Tooltip>
      )}
      {can("pipelines.pause") &&
        (pipeline.paused ? (
          <Button size="sm" disabled={resume.isPending} onClick={() => resume.mutate(name)}>
            <Play aria-hidden />
            Resume
          </Button>
        ) : (
          <Button size="sm" variant="outline" disabled={pause.isPending} onClick={() => pause.mutate(name)}>
            <Pause aria-hidden />
            Pause
          </Button>
        ))}
    </>
  );
}
