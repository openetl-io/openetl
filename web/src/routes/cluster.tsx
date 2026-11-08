import { useCallback, useMemo } from "react";
import { Code } from "@connectrpc/connect";
import { useQuery } from "@connectrpc/connect-query";
import { getRouteApi } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { Server } from "lucide-react";
import { stringify } from "yaml";
import { ClusterSettings } from "@/components/cluster-fields";
import { CodeBlock } from "@/components/code-block";
import { DescriptionItem, DetailHeader, Section } from "@/components/detail-header";
import { ErrorNote } from "@/components/empty";
import { EmptyState } from "@/components/empty-state";
import { PipelinesTable } from "@/components/pipelines-table";
import { RelativeTime } from "@/components/relative-time";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { FlinkClusterService } from "@/gen/openetl/api/v1/flink_pb";
import type { Pipeline } from "@/gen/openetl/api/v1/pipeline_pb";
import { SparkClusterService } from "@/gen/openetl/api/v1/spark_pb";
import { errorCode, errorMessage } from "@/lib/api";
import { clusterUse, flinkTarget, secretRef, sparkTarget, type ClusterKind, type ClusterTarget } from "@/lib/clusters";
import { dateOf } from "@/lib/format";
import { clusterName, shortDigest } from "@/lib/names";

const routes = {
  spark: getRouteApi("/_app/workspaces/$workspace/clusters/spark/$cluster"),
  flink: getRouteApi("/_app/workspaces/$workspace/clusters/flink/$cluster"),
};

const kindLabel = { spark: "Spark", flink: "Flink" };

/** What one cluster has in common with the other kind's, once read. */
type Shown = { target: ClusterTarget; version: string; usedBy: string[]; updated?: Date };

/** The pipelines whose current versions use the cluster, and how each does. */
function Users({ workspace, kind, cluster, usedBy }: { workspace: string; kind: ClusterKind; cluster: string; usedBy: string[] }) {
  const match = useCallback((p: Pipeline) => usedBy.includes(p.name), [usedBy]);
  const use = useMemo<ColumnDef<Pipeline>>(
    () => ({
      id: "use",
      header: "Runs",
      cell: ({ row }) => clusterUse(row.original, kind, cluster) ?? <span className="text-muted-foreground">—</span>,
      meta: { className: "hidden md:table-cell" },
    }),
    [kind, cluster],
  );
  return <PipelinesTable workspace={workspace} match={match} column={use} empty={`No pipeline's current version uses this ${kindLabel[kind]} cluster.`} />;
}

function ClusterPage({ kind }: { kind: ClusterKind }) {
  const { workspace, cluster } = routes[kind].useParams();
  const name = clusterName(workspace, kind, cluster);
  const spark = useQuery(SparkClusterService.method.getSparkCluster, { name }, { enabled: kind === "spark" });
  const flink = useQuery(FlinkClusterService.method.getFlinkCluster, { name }, { enabled: kind === "flink" });
  const q = kind === "spark" ? spark : flink;
  const s = spark.data?.sparkCluster;
  const f = flink.data?.flinkCluster;
  const c = kind === "spark" ? s : f;
  const shown: Shown | undefined = c && {
    target: kind === "spark" ? sparkTarget(s?.spec) : flinkTarget(f?.spec),
    version: c.version,
    usedBy: c.usedBy,
    updated: dateOf(c.updateTime),
  };

  if (q.error) {
    return errorCode(q.error) === Code.NotFound ? (
      <EmptyState icon={Server} title={`There's no ${kindLabel[kind]} cluster ${cluster} in ${workspace}`} command={`openetl apply -f ${cluster}.yaml`} />
    ) : (
      <ErrorNote title={`Couldn't load the ${kindLabel[kind]} cluster`} message={errorMessage(q.error)} />
    );
  }

  return (
    <div className="grid gap-8">
      <DetailHeader title={cluster}>
        {shown && (
          <>
            <DescriptionItem label="Kind">{kindLabel[kind]}</DescriptionItem>
            <DescriptionItem label="Runs on">{shown.target.label}</DescriptionItem>
            <DescriptionItem label="Version">
              <Tooltip>
                <TooltipTrigger className="font-mono text-[0.8125rem]">{shortDigest(shown.version)}</TooltipTrigger>
                <TooltipContent className="max-w-xs">
                  <span className="font-mono break-all">{shown.version}</span>
                  <br />
                  New pipeline versions pin it; older ones keep the settings they pinned.
                </TooltipContent>
              </Tooltip>
            </DescriptionItem>
            {shown.updated && (
              <DescriptionItem label="Updated">
                <RelativeTime date={shown.updated} />
              </DescriptionItem>
            )}
          </>
        )}
      </DetailHeader>
      {shown && (
        <>
          <Section title="Settings">
            <ClusterSettings workspace={workspace} settings={shown.target.settings} />
          </Section>
          <Section title="Pipelines">
            <Users workspace={workspace} kind={kind} cluster={cluster} usedBy={shown.usedBy} />
          </Section>
          {shown.target.conf && Object.keys(shown.target.conf).length > 0 && (
            <Section title={kind === "spark" ? "Spark properties" : "Flink configuration"}>
              <p className="text-muted-foreground">
                {kind === "spark"
                  ? "For every application. A pipeline's own properties take precedence."
                  : "For every job. A pipeline's own configuration takes precedence, and options the runner sets can't be set here."}
                {Object.values(shown.target.conf).some((v) => secretRef(v)) && " A value of {secret: NAME} is that secret's value."}
              </p>
              <CodeBlock className="max-w-4xl">{stringify(shown.target.conf)}</CodeBlock>
            </Section>
          )}
        </>
      )}
    </div>
  );
}

export const SparkClusterPage = () => <ClusterPage kind="spark" />;
export const FlinkClusterPage = () => <ClusterPage kind="flink" />;
