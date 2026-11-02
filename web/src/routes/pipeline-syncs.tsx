import { useInfiniteQuery } from "@connectrpc/connect-query";
import { Link } from "@tanstack/react-router";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Empty, ErrorNote } from "@/components/empty";
import { RelativeTime } from "@/components/relative-time";
import { StatusBadge } from "@/components/status";
import { SyncService } from "@/gen/openetl/api/v1/sync_pb";
import { errorMessage } from "@/lib/api";
import { compact, dateOf, duration, errorSummary, full } from "@/lib/format";
import { lastSegment, pipelineName, shortDigest } from "@/lib/names";
import { rows, syncStatus } from "@/lib/status";

export function SyncsTab({ workspace, pipeline, running }: { workspace: string; pipeline: string; running: boolean }) {
  const syncs = useInfiniteQuery(
    SyncService.method.listSyncs,
    { parent: pipelineName(workspace, pipeline), pageSize: 50, pageToken: "" },
    { pageParamKey: "pageToken", getNextPageParam: (r) => r.nextPageToken || undefined, refetchInterval: running ? 3_000 : 15_000 },
  );
  if (syncs.error) return <ErrorNote title="Couldn't list syncs" message={errorMessage(syncs.error)} />;
  if (!syncs.data) return <Skeleton className="h-40" />;
  const all = syncs.data.pages.flatMap((p) => p.syncs);
  if (all.length === 0) {
    return <Empty title="No syncs yet" command={`openetl sync ${pipeline}`}>The schedule starts them, or you can start one now.</Empty>;
  }
  const now = Date.now();
  return (
    <div className="grid gap-3">
      <div className="overflow-x-auto rounded-lg border bg-card">
        <table className="w-full min-w-[820px] border-collapse">
          <thead>
            <tr className="border-b text-left text-[0.8125rem] text-muted-foreground">
              <th scope="col" className="px-4 py-2 font-normal">Status</th>
              <th scope="col" className="px-4 py-2 font-normal">Started</th>
              <th scope="col" className="px-4 py-2 text-right font-normal">Took</th>
              <th scope="col" className="px-4 py-2 text-right font-normal">Rows</th>
              <th scope="col" className="px-4 py-2 font-normal">Version</th>
              <th scope="col" className="w-full px-4 py-2 font-normal">Error</th>
            </tr>
          </thead>
          <tbody>
            {all.map((s) => {
              const start = dateOf(s.startTime)!;
              const end = dateOf(s.endTime);
              const r = rows(s);
              return (
                <tr key={s.name} className="relative border-b last:border-b-0 focus-within:bg-accent/60 hover:bg-accent/60">
                  <td className="h-row px-4 py-2">
                    <Link
                      to="/workspaces/$workspace/pipelines/$pipeline/syncs/$sync"
                      params={{ workspace, pipeline, sync: lastSegment(s.name) }}
                      className="after:absolute after:inset-0 focus-visible:outline-none"
                      aria-label={`Sync ${lastSegment(s.name)}`}
                    >
                      <StatusBadge status={syncStatus(s)} />
                    </Link>
                  </td>
                  <td className="px-4 py-2">
                    <RelativeTime date={start} />
                  </td>
                  <td className="px-4 py-2 text-right font-mono text-[0.8125rem] tabular">
                    {duration((end?.getTime() ?? now) - start.getTime())}
                  </td>
                  <td className="px-4 py-2 text-right font-mono text-[0.8125rem] tabular">
                    <span title={full(r.all)}>{compact(r.all)}</span>
                    {r.quarantined > 0n && <span className="text-degraded"> ({compact(r.quarantined)} quarantined)</span>}
                  </td>
                  <td className="px-4 py-2 font-mono text-[0.8125rem] text-muted-foreground">{shortDigest(s.version, 10)}</td>
                  <td className="max-w-0 px-4 py-2">
                    {s.error && (
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <span className="relative z-10 block truncate font-mono text-[0.8125rem] text-failed">
                            {errorSummary(s.error)}
                          </span>
                        </TooltipTrigger>
                        <TooltipContent className="max-w-lg font-mono whitespace-pre-wrap">{s.error}</TooltipContent>
                      </Tooltip>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {syncs.hasNextPage && (
        <Button variant="outline" size="sm" className="w-fit" disabled={syncs.isFetchingNextPage} onClick={() => syncs.fetchNextPage()}>
          {syncs.isFetchingNextPage ? "Loading…" : "Load older syncs"}
        </Button>
      )}
    </div>
  );
}
