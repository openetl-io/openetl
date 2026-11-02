import { useInfiniteQuery } from "@connectrpc/connect-query";
import { Link } from "@tanstack/react-router";
import { CircleCheckBig, Zap } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Empty, ErrorNote } from "@/components/empty";
import { RelativeTime } from "@/components/relative-time";
import { PipelineService } from "@/gen/openetl/api/v1/pipeline_pb";
import { errorMessage } from "@/lib/api";
import { compact, dateOf, full } from "@/lib/format";
import { lastSegment, pipelineName } from "@/lib/names";

export function CheckpointsTab({ workspace, pipeline, running }: { workspace: string; pipeline: string; running: boolean }) {
  const cps = useInfiniteQuery(
    PipelineService.method.listCheckpoints,
    { parent: pipelineName(workspace, pipeline), pageSize: 50, pageToken: "" },
    { pageParamKey: "pageToken", getNextPageParam: (r) => r.nextPageToken || undefined, refetchInterval: running ? 3_000 : 30_000 },
  );
  if (cps.error) return <ErrorNote title="Couldn't list checkpoints" message={errorMessage(cps.error)} />;
  if (!cps.data) return <Skeleton className="h-40" />;
  const all = cps.data.pages.flatMap((p) => p.checkpoints);
  if (all.length === 0) {
    return (
      <Empty title="No checkpoints in the last 30 days">
        Each epoch a sync commits adds a checkpoint. The control plane keeps 30 days of them.
      </Empty>
    );
  }
  return (
    <div className="grid gap-3">
      <p className="text-muted-foreground">
        Each epoch a sync committed, newest first. A forced barrier was placed without a resume point; a complete stream finished
        its read in that epoch.
      </p>
      <div className="overflow-x-auto rounded-lg border bg-card">
        <table className="w-full min-w-[820px] border-collapse">
          <thead>
            <tr className="border-b text-left text-[0.8125rem] text-muted-foreground">
              <th scope="col" className="px-4 py-2 font-normal">Epoch</th>
              <th scope="col" className="px-4 py-2 font-normal">Generation</th>
              <th scope="col" className="px-4 py-2 font-normal">Committed</th>
              <th scope="col" className="px-4 py-2 text-right font-normal">Rows</th>
              <th scope="col" className="w-full px-4 py-2 font-normal">Streams</th>
              <th scope="col" className="px-4 py-2 font-normal">Sync</th>
            </tr>
          </thead>
          <tbody>
            {all.map((c) => {
              const total = c.streams.reduce((n, s) => n + s.rows, 0n);
              return (
                <tr key={`${c.generation}.${c.epoch}`} className="border-b last:border-b-0">
                  <td className="h-row px-4 py-2 font-mono text-[0.8125rem] tabular">{c.epoch.toString()}</td>
                  <td className="px-4 py-2 font-mono text-[0.8125rem] text-muted-foreground tabular">{c.generation.toString()}</td>
                  <td className="px-4 py-2">{c.commitTime && <RelativeTime date={dateOf(c.commitTime)!} />}</td>
                  <td className="px-4 py-2 text-right font-mono text-[0.8125rem] tabular" title={full(total)}>
                    {compact(total)}
                  </td>
                  <td className="px-4 py-2">
                    <ul className="flex flex-wrap gap-1.5">
                      {c.streams.map((s) => (
                        <li key={s.name} className="inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-[0.8125rem]">
                          <span>{s.name}</span>
                          <span className="font-mono text-muted-foreground tabular">{compact(s.rows)}</span>
                          {s.quarantinedRows > 0n && (
                            <span className="font-mono text-degraded tabular">{compact(s.quarantinedRows)} quarantined</span>
                          )}
                          {s.schemaVersion > 0 && <span className="font-mono text-muted-foreground">v{s.schemaVersion}</span>}
                          {s.forced && (
                            <Tooltip>
                              <TooltipTrigger aria-label="Forced barrier">
                                <Zap className="size-3.5 text-degraded" />
                              </TooltipTrigger>
                              <TooltipContent>Forced barrier, without a resume point</TooltipContent>
                            </Tooltip>
                          )}
                          {s.complete && (
                            <Tooltip>
                              <TooltipTrigger aria-label="Read complete">
                                <CircleCheckBig className="size-3.5 text-healthy" />
                              </TooltipTrigger>
                              <TooltipContent>The stream's read ended in this epoch</TooltipContent>
                            </Tooltip>
                          )}
                        </li>
                      ))}
                    </ul>
                  </td>
                  <td className="px-4 py-2 font-mono text-[0.8125rem]">
                    {c.sync ? (
                      <Link
                        to="/workspaces/$workspace/pipelines/$pipeline/syncs/$sync"
                        params={{ workspace, pipeline, sync: lastSegment(c.sync) }}
                        className="hover:text-signal"
                      >
                        {lastSegment(c.sync).slice(-8)}
                      </Link>
                    ) : (
                      <span className="text-muted-foreground">—</span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {cps.hasNextPage && (
        <Button variant="outline" size="sm" className="w-fit" disabled={cps.isFetchingNextPage} onClick={() => cps.fetchNextPage()}>
          {cps.isFetchingNextPage ? "Loading…" : "Load older checkpoints"}
        </Button>
      )}
    </div>
  );
}
