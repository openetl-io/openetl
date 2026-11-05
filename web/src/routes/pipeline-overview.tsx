import { lazy, Suspense, useMemo, type ReactNode } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { Link } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import { Info, TriangleAlert } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { connectorLabel } from "@/components/connector";
import { DataTable } from "@/components/data-table/data-table";
import { useDataTable } from "@/components/data-table/use-data-table";
import { ErrorNote } from "@/components/empty";
import { Incidents } from "@/components/incidents";
import { IncidentService, IncidentState } from "@/gen/openetl/api/v1/incident_pb";
import {
  DeliveryLevel,
  PipelineService,
  WriteMode,
  DbtCommand,
  type Pipeline,
} from "@/gen/openetl/api/v1/pipeline_pb";
import { type ConnectorVersion, SyncMode } from "@/gen/openetl/api/v1/connection_pb";
import { SyncState, type Sync } from "@/gen/openetl/api/v1/sync_pb";
import { errorMessage } from "@/lib/api";
import { compact, enumLabel, full, sentence } from "@/lib/format";
import { lastSegment, schemaKey } from "@/lib/names";
import type { GraphStream } from "@/components/pipeline-graph";

const PipelineGraph = lazy(() => import("@/components/pipeline-graph"));

export const levelLabel: Record<DeliveryLevel, string> = {
  [DeliveryLevel.UNSPECIFIED]: "Unknown",
  [DeliveryLevel.AT_LEAST_ONCE]: "At least once",
  [DeliveryLevel.EFFECTIVELY_ONCE]: "Effectively once",
  [DeliveryLevel.EXACTLY_ONCE]: "Exactly once",
};

type Row = GraphStream & {
  mode?: SyncMode;
  write?: WriteMode;
  cursor?: string;
  reason?: string;
  warning?: string;
  quarantined?: bigint;
  held?: string[];
  collapsed?: bigint;
  schema?: number;
  schemaKey?: string;
};

const calloutTone = {
  problem: "flex gap-2 text-failed",
  warning: "flex gap-2 text-degraded",
  note: "flex gap-2 text-muted-foreground",
};

function Callout({ tone, children }: { tone: keyof typeof calloutTone; children: React.ReactNode }) {
  const Icon = tone === "note" ? Info : TriangleAlert;
  return (
    <p className={calloutTone[tone]}>
      <Icon className="mt-0.5 size-4 shrink-0" aria-hidden />
      <span>{children}</span>
    </p>
  );
}

type ConnectorRef = { ref?: string; version?: ConnectorVersion };

export function Overview({
  workspace,
  pipeline,
  syncs,
  connectors,
}: {
  workspace: string;
  pipeline: Pipeline;
  syncs: Sync[];
  connectors: { source: ConnectorRef; destination: ConnectorRef };
}) {
  const plan = useQuery(PipelineService.method.getPipelinePlan, { name: pipeline.name });
  const schemas = useQuery(PipelineService.method.listStreamSchemas, { parent: pipeline.name, pageSize: 500 });
  const spec = pipeline.spec;
  const last = syncs[0];
  const live = last?.state === SyncState.RUNNING;

  const streams = useMemo(() => {
    const m = new Map<string, Row>();
    const get = (name: string) => {
      let r = m.get(name);
      if (!r) m.set(name, (r = { name }));
      return r;
    };
    for (const s of plan.data?.plan?.streams ?? []) Object.assign(get(s.name), { level: s.level, reason: s.reason, write: s.write });
    for (const w of plan.data?.plan?.warnings ?? []) if (w.stream) get(w.stream).warning = w.message;
    for (const s of spec?.source?.streams ?? []) Object.assign(get(s.name), { mode: s.sync, cursor: s.cursor });
    for (const s of last?.streams ?? [])
      Object.assign(get(s.name), { rows: s.rows, quarantined: s.quarantinedRows, held: s.heldColumns, collapsed: s.collapsedRows });
    // A transform's output comes first, and is the destination's stream of
    // its name; an input of the same name is left to the schemas tab.
    for (const s of schemas.data?.streamSchemas ?? []) {
      const r = get(s.stream);
      if (r.schemaKey && r.schemaKey !== schemaKey(s)) continue;
      r.schema = Math.max(r.schema ?? 0, s.version);
      r.schemaKey = schemaKey(s);
    }
    return [...m.values()].sort((a, b) => a.name.localeCompare(b.name));
  }, [plan.data, spec, last, schemas.data]);

  const incidents = useQuery(IncidentService.method.listIncidents, { parent: pipeline.name, state: IncidentState.OPEN });
  const lastSync = pipeline.status?.lastSync;
  // An open incident that came from it says more than its error.
  const failed =
    lastSync?.state === SyncState.FAILED && !incidents.data?.incidents.some((i) => i.sync === lastSync.name) ? lastSync : undefined;
  const retrying = live && last.failedAttempts > 0 ? last : undefined;
  const problems = plan.data?.plan?.problems ?? [];
  const notes = plan.data?.plan?.notes ?? [];
  const warnings = (plan.data?.plan?.warnings ?? []).filter((w) => !w.stream).map((w) => w.message);

  return (
    <div className="grid gap-6">
      <Incidents workspace={workspace} pipeline={pipeline.name} live={live} />
      {retrying && (
        <Callout tone="warning">
          {retrying.failedAttempts === 1 ? "An attempt" : `${retrying.failedAttempts} attempts`} of this {retrying.continuous ? "run" : "sync"} failed,
          and it's retrying. The latest: <span className="font-mono text-[0.8125rem]">{retrying.lastAttemptError.split("\n")[0]}</span>
        </Callout>
      )}
      {failed && (
        <div className="grid gap-2">
          <ErrorNote title="The last sync failed" message={failed.error || "It ended without an error message."} />
          <Link
            to="/workspaces/$workspace/pipelines/$pipeline/syncs/$sync"
            params={{ workspace, pipeline: lastSegment(pipeline.name), sync: lastSegment(failed.name) }}
            className="w-fit text-signal underline-offset-4 hover:underline"
          >
            Read its log
          </Link>
        </div>
      )}

      <section aria-label="Graph" className="h-[340px] overflow-hidden rounded-lg border bg-card">
        <Suspense fallback={<Skeleton className="size-full rounded-none" />}>
          <PipelineGraph
            source={{ name: spec?.source?.ref ?? "", connector: connectorLabel(connectors.source.ref, connectors.source.version), logo: connectors.source.version?.logo }}
            destination={{
              name: spec?.destination?.ref ?? "",
              connector: connectorLabel(connectors.destination.ref, connectors.destination.version),
              logo: connectors.destination.version?.logo,
              detail: [spec?.destination?.namespace && `schema ${spec.destination.namespace}`, enumLabel(WriteMode[spec?.destination?.write ?? 0])]
                .filter(Boolean)
                .join(", "),
            }}
            streams={streams}
            steps={(spec?.postSync ?? []).flatMap((s) => {
              switch (s.step.case) {
                case "dbt":
                  return [{ name: s.step.value.project, detail: `dbt ${enumLabel(DbtCommand[s.step.value.command]).toLowerCase() || "build"}` }];
                case "spark": {
                  const file = s.step.value.code?.file?.value ?? "";
                  return [{ name: file.replace(/\?.*$/, "").split("/").pop() ?? "", detail: `Spark on ${s.step.value.cluster}` }];
                }
                default:
                  return [];
              }
            })}
            live={live}
          />
        </Suspense>
      </section>

      <section className="grid gap-3" aria-labelledby="streams-h">
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <h2 id="streams-h" className="text-base font-semibold">
            Streams
          </h2>
          <span className="text-[0.8125rem] text-muted-foreground">
            Rows are the {live ? "running" : "latest"} sync's
          </span>
        </div>
        {plan.error && <ErrorNote title="Couldn't plan the pipeline" message={errorMessage(plan.error)} />}
        {(problems.length > 0 || warnings.length > 0 || notes.length > 0) && (
          <div className="grid gap-1.5">
            {problems.map((p) => (
              <Callout key={p} tone="problem">
                {sentence(p)}
              </Callout>
            ))}
            {warnings.map((w) => (
              <Callout key={w} tone="warning">
                {sentence(w)}
              </Callout>
            ))}
            {notes.map((n) => (
              <Callout key={n} tone="note">
                {sentence(n)}
              </Callout>
            ))}
          </div>
        )}
        <StreamsTable
          workspace={workspace}
          pipeline={lastSegment(pipeline.name)}
          streams={streams}
          planned={!!plan.data?.plan?.streams.length}
          empty={
            <>
              No streams to show yet. They show once a sync commits rows, or once the plan can list them, after{" "}
              <code className="font-mono text-[0.8125rem] text-foreground">openetl discover {spec?.source?.ref}</code>.
            </>
          }
        />
      </section>
    </div>
  );
}

function StreamsTable({
  workspace,
  pipeline,
  streams,
  planned,
  empty,
}: {
  workspace: string;
  pipeline: string;
  streams: Row[];
  planned: boolean;
  empty: ReactNode;
}) {
  const columns = useMemo<ColumnDef<Row>[]>(
    () => [
      {
        id: "name",
        header: "Stream",
        cell: ({ row }) => <span className="font-medium">{row.original.name}</span>,
      },
      {
        id: "read",
        header: "Read",
        cell: ({ row }) => (
          <span className="text-muted-foreground">
            {enumLabel(SyncMode[row.original.mode ?? 0]) || "Source default"}
            {row.original.cursor && (
              <>
                {" "}on <code className="font-mono text-[0.8125rem] text-foreground">{row.original.cursor}</code>
              </>
            )}
          </span>
        ),
        meta: { className: "hidden md:table-cell" },
      },
      {
        id: "write",
        header: "Write",
        cell: ({ row }) => <span className="text-muted-foreground">{enumLabel(WriteMode[row.original.write ?? 0]) || "Unknown"}</span>,
        meta: { className: "hidden lg:table-cell" },
      },
      {
        id: "delivery",
        header: "Delivery",
        cell: ({ row }) => {
          const s = row.original;
          if (s.level === undefined) return <span className="text-muted-foreground">{planned ? "Not in the plan" : "Checked when it syncs"}</span>;
          return (
            <span className="inline-flex items-center gap-1.5">
              <Tooltip>
                <TooltipTrigger className="cursor-help underline decoration-muted-foreground/60 decoration-dotted underline-offset-4">
                  {levelLabel[s.level]}
                </TooltipTrigger>
                <TooltipContent className="max-w-xs">{sentence(s.reason ?? "")}.</TooltipContent>
              </Tooltip>
              {s.warning && (
                <Tooltip>
                  <TooltipTrigger aria-label={`Warning: ${s.name} ${s.warning}`}>
                    <TriangleAlert className="size-3.5 text-degraded" />
                  </TooltipTrigger>
                  <TooltipContent className="max-w-sm">{sentence(s.warning)}</TooltipContent>
                </Tooltip>
              )}
            </span>
          );
        },
      },
      {
        id: "schema",
        header: "Schema",
        cell: ({ row }) =>
          row.original.schema ? (
            <Link
              to="/workspaces/$workspace/pipelines/$pipeline"
              params={{ workspace, pipeline }}
              search={{ tab: "schemas", stream: row.original.schemaKey }}
              className="font-mono text-[0.8125rem] hover:underline"
            >
              v{row.original.schema}
            </Link>
          ) : (
            <span className="text-muted-foreground">—</span>
          ),
        meta: { className: "hidden sm:table-cell" },
      },
      {
        id: "rows",
        header: "Rows",
        cell: ({ row }) => <span className="tabular-nums">{row.original.rows !== undefined ? <span title={full(row.original.rows)}>{compact(row.original.rows)}</span> : "—"}</span>,
        meta: { align: "right" },
      },
      {
        id: "quarantined",
        header: "Quarantined",
        cell: ({ row }) =>
          row.original.quarantined ? (
            <span className="text-degraded tabular-nums">{full(row.original.quarantined)}</span>
          ) : (
            <span className="text-muted-foreground">0</span>
          ),
        meta: { align: "right", className: "hidden md:table-cell" },
      },
      {
        id: "held",
        header: "Held",
        cell: ({ row }) =>
          row.original.held?.length ? (
            <span className="text-degraded" title="Columns held at the last sync's latest commit, until they're released or discarded">
              {row.original.held.join(", ")}
            </span>
          ) : (
            <span className="text-muted-foreground">—</span>
          ),
        meta: { className: "hidden md:table-cell" },
      },
      {
        id: "collapsed",
        header: "Collapsed",
        cell: ({ row }) =>
          row.original.collapsed ? (
            <span className="tabular-nums" title="Rows not inserted because a row with the same _openetl_id was already in the epoch or the table">
              {full(row.original.collapsed)}
            </span>
          ) : (
            <span className="text-muted-foreground">0</span>
          ),
        meta: { align: "right", className: "hidden xl:table-cell" },
      },
    ],
    [workspace, pipeline, planned],
  );
  const table = useDataTable({ data: streams, columns, paged: false });
  return <DataTable table={table} empty={empty} />;
}
