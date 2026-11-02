import { Code } from "@connectrpc/connect";
import { useQuery } from "@connectrpc/connect-query";
import { Link, getRouteApi, useNavigate } from "@tanstack/react-router";
import { ArrowLeft } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Empty, ErrorNote } from "@/components/empty";
import { RelativeTime } from "@/components/relative-time";
import { StatusBadge } from "@/components/status";
import { SyncStrip } from "@/components/sync-strip";
import { DestinationService, SourceService } from "@/gen/openetl/api/v1/connection_pb";
import { PipelineMode, PipelineService } from "@/gen/openetl/api/v1/pipeline_pb";
import { SyncService } from "@/gen/openetl/api/v1/sync_pb";
import { errorCode, errorMessage } from "@/lib/api";
import { dateOf } from "@/lib/format";
import { destinationName, pipelineName, shortDigest, sourceName } from "@/lib/names";
import { lag, lastUpdate, scheduleLabel, stale } from "@/lib/pipeline";
import { pipelineStatus } from "@/lib/status";
import { Overview } from "./pipeline-overview";
import { SyncsTab } from "./pipeline-syncs";
import { SchemasTab } from "./pipeline-schemas";
import { CheckpointsTab } from "./pipeline-checkpoints";
import { VersionsTab } from "./pipeline-versions";
import { tabs, type Tab } from "./tabs";

const tabLabel: Record<Tab, string> = {
  overview: "Overview",
  syncs: "Syncs",
  schemas: "Schemas",
  checkpoints: "Checkpoints",
  versions: "Versions",
};

const route = getRouteApi("/_app/workspaces/$workspace/pipelines/$pipeline");

/** The syncs every tab shares: the header's strip, the overview and the Syncs tab. */
export const recentSyncs = 50;

function Fact({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="grid min-w-0 gap-0.5">
      <dt className="text-[0.8125rem] text-muted-foreground">{label}</dt>
      <dd className="min-w-0 truncate">{children}</dd>
    </div>
  );
}

export function PipelinePage() {
  const { workspace, pipeline } = route.useParams();
  const { tab = "overview" } = route.useSearch();
  const navigate = useNavigate({ from: "/workspaces/$workspace/pipelines/$pipeline" });
  const name = pipelineName(workspace, pipeline);
  const p = useQuery(PipelineService.method.getPipeline, { name }, { refetchInterval: (q) => (q.state.data?.pipeline && pipelineStatus(q.state.data.pipeline) === "running" ? 3_000 : 15_000) });
  const running = p.data?.pipeline ? pipelineStatus(p.data.pipeline) === "running" : false;
  const syncs = useQuery(
    SyncService.method.listSyncs,
    { parent: name, pageSize: recentSyncs },
    { refetchInterval: running ? 3_000 : 15_000 },
  );
  const spec = p.data?.pipeline?.spec;
  const source = useQuery(
    SourceService.method.getSource,
    { name: sourceName(workspace, spec?.source?.ref ?? "") },
    { enabled: !!spec?.source?.ref },
  );
  const destination = useQuery(
    DestinationService.method.getDestination,
    { name: destinationName(workspace, spec?.destination?.ref ?? "") },
    { enabled: !!spec?.destination?.ref },
  );

  if (p.error) {
    return errorCode(p.error) === Code.NotFound ? (
      <Empty title={`There's no pipeline ${pipeline} in ${workspace}`} command={`openetl apply -f ${pipeline}.yaml`} />
    ) : (
      <ErrorNote title="Couldn't load the pipeline" message={errorMessage(p.error)} />
    );
  }
  const pl = p.data?.pipeline;
  const status = pl ? pipelineStatus(pl) : undefined;
  const lastSuccess = pl && lastUpdate(pl);
  const behind = spec?.mode === PipelineMode.CONTINUOUS ? lag(pl?.status?.lastSync) : undefined;

  return (
    <div className="grid gap-6">
      <div className="grid gap-4">
        <Link
          to="/workspaces/$workspace/pipelines"
          params={{ workspace }}
          search={{ q: undefined, show: undefined }}
          className="inline-flex w-fit items-center gap-1 text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-4" aria-hidden />
          Pipelines
        </Link>
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
          <h1 className="text-2xl font-semibold tracking-tight break-all">{pipeline}</h1>
          {status ? <StatusBadge status={status} /> : <Skeleton className="h-5 w-20" />}
        </div>
        {pl ? (
          <dl className="grid grid-cols-2 gap-x-8 gap-y-3 sm:grid-cols-3 lg:flex lg:flex-wrap">
            <Fact label="Source">
              {spec?.source?.ref}
              {source.data?.source?.spec?.connector && (
                <span className="text-muted-foreground"> ({source.data.source.spec.connector})</span>
              )}
            </Fact>
            <Fact label="Destination">
              {spec?.destination?.ref}
              {destination.data?.destination?.spec?.connector && (
                <span className="text-muted-foreground"> ({destination.data.destination.spec.connector})</span>
              )}
              {spec?.destination?.namespace && <span className="text-muted-foreground">, schema {spec.destination.namespace}</span>}
            </Fact>
            <Fact label="Schedule">
              {pl.paused ? "Paused" : <span className={spec?.schedule?.trigger.case === "cron" ? "font-mono text-[0.8125rem]" : undefined}>{scheduleLabel(spec)}</span>}
            </Fact>
            <Fact label={spec?.mode === PipelineMode.CONTINUOUS ? "Last commit" : "Last success"}>
              {lastSuccess ? (
                <span className={stale(pl) ? "text-degraded" : undefined}>
                  <RelativeTime date={lastSuccess} />
                </span>
              ) : (
                "Never"
              )}
            </Fact>
            {behind && <Fact label="Behind source">{behind}</Fact>}
            <Fact label="Version">
              <Tooltip>
                <TooltipTrigger asChild>
                  <button
                    onClick={() => navigate({ search: { tab: "versions", version: pl.version } })}
                    className="font-mono text-[0.8125rem] hover:text-signal"
                  >
                    {shortDigest(pl.version)}
                  </button>
                </TooltipTrigger>
                <TooltipContent className="font-mono">{pl.version}</TooltipContent>
              </Tooltip>
            </Fact>
          </dl>
        ) : (
          <Skeleton className="h-10 w-full max-w-3xl" />
        )}
        <div className="grid gap-2 rounded-lg border bg-card px-4 py-3">
          <div className="flex items-baseline justify-between gap-4 text-[0.8125rem] text-muted-foreground">
            <span>Last {Math.min(syncs.data?.syncs.length ?? 0, recentSyncs)} syncs, oldest on the left</span>
            {syncs.data?.syncs[0] && (
              <span>
                Latest started <RelativeTime date={dateOf(syncs.data.syncs[0].startTime)!} />
              </span>
            )}
          </div>
          {syncs.data ? (
            syncs.data.syncs.length ? (
              <SyncStrip syncs={syncs.data.syncs} workspace={workspace} pipeline={pipeline} size="lg" className="min-h-2.5 flex-wrap" />
            ) : (
              <p className="text-muted-foreground">
                No syncs yet. The schedule starts one, or{" "}
                <code className="font-mono text-[0.8125rem] text-foreground">openetl sync {pipeline}</code> does now.
              </p>
            )
          ) : (
            <Skeleton className="h-2.5 w-full" />
          )}
        </div>
      </div>

      <Tabs value={tab} onValueChange={(t) => navigate({ search: { tab: t === "overview" ? undefined : (t as Tab) } })}>
        <TabsList className="w-full justify-start overflow-x-auto sm:w-fit">
          {tabs.map((t) => (
            <TabsTrigger key={t} value={t}>
              {tabLabel[t]}
            </TabsTrigger>
          ))}
        </TabsList>
        <TabsContent value="overview" className="pt-4">
          {pl && <Overview workspace={workspace} pipeline={pl} syncs={syncs.data?.syncs ?? []} connectors={{ source: source.data?.source?.spec?.connector, destination: destination.data?.destination?.spec?.connector }} />}
        </TabsContent>
        <TabsContent value="syncs" className="pt-4">
          <SyncsTab workspace={workspace} pipeline={pipeline} running={running} />
        </TabsContent>
        <TabsContent value="schemas" className="pt-4">
          <SchemasTab workspace={workspace} pipeline={pipeline} />
        </TabsContent>
        <TabsContent value="checkpoints" className="pt-4">
          <CheckpointsTab workspace={workspace} pipeline={pipeline} running={running} />
        </TabsContent>
        <TabsContent value="versions" className="pt-4">
          <VersionsTab workspace={workspace} pipeline={pipeline} current={pl?.version} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
