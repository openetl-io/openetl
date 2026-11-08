import { useCallback, useMemo } from "react";
import { Code } from "@connectrpc/connect";
import { useQuery } from "@connectrpc/connect-query";
import { getRouteApi } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { Database, Warehouse } from "lucide-react";
import { stringify } from "yaml";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { CodeBlock } from "@/components/code-block";
import { ConnectorLogo } from "@/components/connector";
import { DescriptionItem, DetailHeader, Section } from "@/components/detail-header";
import { ErrorNote } from "@/components/empty";
import { EmptyState } from "@/components/empty-state";
import { PipelinesTable } from "@/components/pipelines-table";
import { RelativeTime } from "@/components/relative-time";
import { ResourceLink } from "@/components/resource-link";
import { DestinationService, SourceService } from "@/gen/openetl/api/v1/connection_pb";
import type { Pipeline } from "@/gen/openetl/api/v1/pipeline_pb";
import { errorCode, errorMessage } from "@/lib/api";
import { dateOf } from "@/lib/format";
import { shortDigest } from "@/lib/names";
import { connectorName } from "@/lib/connectors";
import type { Kind } from "./connections";

const routes = {
  source: getRouteApi("/_app/workspaces/$workspace/sources/$source"),
  destination: getRouteApi("/_app/workspaces/$workspace/destinations/$destination"),
};

/** The pipelines that read from or load into one source or destination. */
function UsedByTable({ workspace, kind, id }: { workspace: string; kind: Kind; id: string }) {
  const match = useCallback((p: Pipeline) => (kind === "source" ? p.spec?.source?.ref : p.spec?.destination?.ref) === id, [kind, id]);
  const other = useMemo<ColumnDef<Pipeline>>(
    () => ({
      id: kind === "source" ? "destination" : "source",
      header: kind === "source" ? "Destination" : "Source",
      cell: ({ row }) => {
        const ref = kind === "source" ? row.original.spec?.destination?.ref : row.original.spec?.source?.ref;
        return ref && <ResourceLink name={`workspaces/${workspace}/${kind === "source" ? "destinations" : "sources"}/${ref}`} />;
      },
      meta: { className: "hidden md:table-cell" },
    }),
    [kind, workspace],
  );
  return <PipelinesTable workspace={workspace} match={match} column={other} empty={`No pipeline uses this ${kind} yet.`} />;
}

function ConnectionPage({ kind }: { kind: Kind }) {
  const params = routes[kind].useParams() as { workspace: string; source?: string; destination?: string };
  const { workspace } = params;
  const id = (kind === "source" ? params.source : params.destination) ?? "";
  const name = `workspaces/${workspace}/${kind}s/${id}`;
  const source = useQuery(SourceService.method.getSource, { name }, { enabled: kind === "source" });
  const destination = useQuery(DestinationService.method.getDestination, { name }, { enabled: kind === "destination" });
  const q = kind === "source" ? source : destination;
  const x = kind === "source" ? source.data?.source : destination.data?.destination;

  if (q.error) {
    return errorCode(q.error) === Code.NotFound ? (
      <EmptyState icon={kind === "source" ? Database : Warehouse} title={`There's no ${kind} ${id} in ${workspace}`} command={`openetl apply -f ${id}.yaml`} />
    ) : (
      <ErrorNote title={`Couldn't load the ${kind}`} message={errorMessage(q.error)} />
    );
  }
  const v = x?.connector;
  const updated = dateOf(x?.updateTime);

  return (
    <div className="grid gap-8">
      <DetailHeader title={id}>
        {x && (
          <>
            <DescriptionItem label="Connector">
              <span className="inline-flex items-center gap-2">
                <ConnectorLogo connector={connectorName(x.spec?.connector)} version={v} />
                {connectorName(x.spec?.connector)}
              </span>
            </DescriptionItem>
            <DescriptionItem label="Version">
              {!v?.version ? (
                <Tooltip>
                  <TooltipTrigger className="text-muted-foreground">Not pinned</TooltipTrigger>
                  <TooltipContent className="max-w-xs">No connector version by digest: a built-in connector, or the runner's own build.</TooltipContent>
                </Tooltip>
              ) : v.releaseUrl ? (
                <a href={v.releaseUrl} target="_blank" rel="noreferrer noopener" className="hover:underline">
                  {v.version}
                </a>
              ) : (
                v.version
              )}
            </DescriptionItem>
            {v?.digest && (
              <DescriptionItem label="Digest">
                <span className="font-mono text-[0.8125rem]" title={v.digest}>{shortDigest(v.digest)}</span>
              </DescriptionItem>
            )}
            {v?.author && <DescriptionItem label="Author">{v.author}</DescriptionItem>}
            {updated && <DescriptionItem label="Updated"><RelativeTime date={updated} /></DescriptionItem>}
          </>
        )}
      </DetailHeader>
      <Section title="Pipelines">
        <UsedByTable workspace={workspace} kind={kind} id={id} />
      </Section>
      {x?.spec?.config && Object.keys(x.spec.config).length > 0 && (
        <Section title="Config">
          <CodeBlock className="max-w-4xl">{stringify(x.spec.config)}</CodeBlock>
        </Section>
      )}
    </div>
  );
}

export const SourcePage = () => <ConnectionPage kind="source" />;
export const DestinationPage = () => <ConnectionPage kind="destination" />;
