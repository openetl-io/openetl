import { Code } from "@connectrpc/connect";
import { useQuery } from "@connectrpc/connect-query";
import { getRouteApi, useNavigate } from "@tanstack/react-router";
import { Workflow } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { ConnectorLogo } from "@/components/connector";
import { DescriptionItem, DetailHeader } from "@/components/detail-header";
import { ErrorNote } from "@/components/empty";
import { EmptyState } from "@/components/empty-state";
import { PipelineActions } from "@/components/pipeline-actions";
import { Freshness, Schedule } from "@/components/pipeline-fields";
import { RelativeTime } from "@/components/relative-time";
import { ResourceLink } from "@/components/resource-link";
import { StatusBadge } from "@/components/status";
import { SyncStrip } from "@/components/sync-strip";
import { DestinationService, SourceService } from "@/gen/openetl/api/v1/connection_pb";
import { PipelineMode, PipelineService } from "@/gen/openetl/api/v1/pipeline_pb";
import { SyncService } from "@/gen/openetl/api/v1/sync_pb";
import { errorCode, errorMessage } from "@/lib/api";
import { dateOf } from "@/lib/format";
import { clusterName, destinationName, pipelineName, shortDigest, sourceName } from "@/lib/names";
import { lag } from "@/lib/pipeline";
import { sizeAmounts, sizeLabel, sizeReasonLabel } from "@/lib/sizes";
import { pipelineStatus } from "@/lib/status";
import { connectorName } from "@/lib/connectors";
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

export function PipelinePage() {
  const { workspace, pipeline } = route.useParams();
  const { tab = "overview" } = route.useSearch();
  const navigate = useNavigate({ from: "/workspaces/$workspace/pipelines/$pipeline" });
  const name = pipelineName(workspace, pipeline);
  const p = useQuery(PipelineService.method.getPipeline, { name }, { refetchInterval: (q) => (q.state.data?.pipeline && pipelineStatus(q.state.data.pipeline) === "running" ? 3_000 : 15_000) });
  const running = p.data?.pipeline ? pipelineStatus(p.data.pipeline) === "running" : false;
  const syncs = useQuery(SyncService.method.listSyncs, { parent: name, pageSize: recentSyncs }, { refetchInterval: running ? 3_000 : 15_000 });
  const spec = p.data?.pipeline?.spec;
  const source = useQuery(SourceService.method.getSource, { name: sourceName(workspace, spec?.source?.ref ?? "") }, { enabled: !!spec?.source?.ref });
  const destination = useQuery(
    DestinationService.method.getDestination,
    { name: destinationName(workspace, spec?.destination?.ref ?? "") },
    { enabled: !!spec?.destination?.ref },
  );

  if (p.error) {
    return errorCode(p.error) === Code.NotFound ? (
      <EmptyState icon={Workflow} title={`There's no pipeline ${pipeline} in ${workspace}`} command={`openetl apply -f ${pipeline}.yaml`} />
    ) : (
      <ErrorNote title="Couldn't load the pipeline" message={errorMessage(p.error)} />
    );
  }
  const pl = p.data?.pipeline;
  const src = source.data?.source;
  const dst = destination.data?.destination;
  const continuous = spec?.mode === PipelineMode.CONTINUOUS;
  const behind = continuous ? lag(pl?.status?.lastSync) : undefined;
  const latest = syncs.data?.syncs[0];
  const sized = latest?.attempts.at(-1);

  return (
    <div className="grid gap-6">
      <DetailHeader
        title={pipeline}
        status={pl && <StatusBadge status={pipelineStatus(pl)} />}
        actions={pl && <PipelineActions workspace={workspace} pipeline={pl} />}
      >
        {pl && (
          <>
            <DescriptionItem label="Source">
              <span className="inline-flex items-center gap-2">
                <ConnectorLogo connector={connectorName(src?.spec?.connector)} version={src?.connector} />
                <ResourceLink name={sourceName(workspace, spec?.source?.ref ?? "")} />
              </span>
            </DescriptionItem>
            <DescriptionItem label="Destination">
              <span className="inline-flex items-center gap-2">
                <ConnectorLogo connector={connectorName(dst?.spec?.connector)} version={dst?.connector} />
                <ResourceLink name={destinationName(workspace, spec?.destination?.ref ?? "")} />
                {spec?.destination?.namespace && <span className="text-muted-foreground">schema {spec.destination.namespace}</span>}
              </span>
            </DescriptionItem>
            {spec?.transform?.engine.case && (
              <DescriptionItem label="Transform">
                {spec.transform.engine.case === "spark" ? "Spark" : "Flink"} on{" "}
                <ResourceLink name={clusterName(workspace, spec.transform.engine.case, spec.transform.engine.value.cluster)} />
              </DescriptionItem>
            )}
            <DescriptionItem label="Schedule">{pl.paused ? "Paused" : <Schedule pipeline={pl} />}</DescriptionItem>
            <DescriptionItem label={continuous ? "Last commit" : "Last success"}>
              <Freshness pipeline={pl} />
            </DescriptionItem>
            {behind && <DescriptionItem label="Behind source">{behind}</DescriptionItem>}
            {sized && (
              <DescriptionItem label="Size">
                <span title={sizeAmounts(sized.size)}>
                  {sizeLabel(sized.size)}, {sizeReasonLabel[sized.sizeReason]}
                  {sized.lanes && `, ${sized.lanes.lanes} ${sized.lanes.lanes === 1 ? "lane" : "lanes"}`}
                </span>
              </DescriptionItem>
            )}
            {pl.status?.scaling?.note === "destination_bound" && (
              <DescriptionItem label="Scaling">Falling behind while its lanes wait for the destination, so more lanes won't help</DescriptionItem>
            )}
            <DescriptionItem label="Version">
              <Tooltip>
                <TooltipTrigger asChild>
                  <button onClick={() => navigate({ search: { tab: "versions", version: pl.version } })} className="font-mono text-[0.8125rem] hover:underline">
                    {shortDigest(pl.version)}
                  </button>
                </TooltipTrigger>
                <TooltipContent className="font-mono">{pl.version}</TooltipContent>
              </Tooltip>
            </DescriptionItem>
          </>
        )}
      </DetailHeader>

      <div className="grid gap-2 rounded-lg border px-4 py-3">
        <div className="flex items-baseline justify-between gap-4 text-[0.8125rem] text-muted-foreground">
          <span>Last {Math.min(syncs.data?.syncs.length ?? 0, recentSyncs)} syncs, oldest on the left</span>
          {latest && (
            <span>
              Latest started <RelativeTime date={dateOf(latest.startTime)!} />
            </span>
          )}
        </div>
        {syncs.data ? (
          syncs.data.syncs.length ? (
            <SyncStrip syncs={syncs.data.syncs} workspace={workspace} pipeline={pipeline} size="lg" className="min-h-2.5 flex-wrap" />
          ) : (
            <p className="text-muted-foreground">
              No syncs yet. The schedule starts one, or <code className="font-mono text-[0.8125rem] text-foreground">openetl sync {pipeline}</code> does now.
            </p>
          )
        ) : (
          <Skeleton className="h-2.5 w-full" />
        )}
      </div>

      <Tabs value={tab} onValueChange={(t) => navigate({ search: { tab: t === "overview" ? undefined : (t as Tab) } })}>
        <TabsList variant="line" className="w-full justify-start overflow-x-auto contain-inline-size">
          {tabs.map((t) => (
            <TabsTrigger key={t} value={t} className="flex-none">
              {tabLabel[t]}
            </TabsTrigger>
          ))}
        </TabsList>
        <TabsContent value="overview" className="pt-4">
          {pl && (
            <Overview
              workspace={workspace}
              pipeline={pl}
              syncs={syncs.data?.syncs ?? []}
              connectors={{
                source: { ref: src?.spec?.connector, version: src?.connector },
                destination: { ref: dst?.spec?.connector, version: dst?.connector },
              }}
            />
          )}
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
