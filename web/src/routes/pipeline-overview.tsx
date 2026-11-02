import { lazy, Suspense, useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { Link } from "@tanstack/react-router";
import { Info, TriangleAlert } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { ErrorNote } from "@/components/empty";
import {
  DeliveryLevel,
  PipelineService,
  SyncMode,
  WriteMode,
  DbtCommand,
  type Pipeline,
} from "@/gen/openetl/api/v1/pipeline_pb";
import { SyncState, type Sync } from "@/gen/openetl/api/v1/sync_pb";
import { errorMessage } from "@/lib/api";
import { compact, enumLabel, full, sentence } from "@/lib/format";
import { lastSegment } from "@/lib/names";
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
  cursor?: string;
  reason?: string;
  warning?: string;
  quarantined?: bigint;
  collapsed?: bigint;
  schema?: number;
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

export function Overview({
  workspace,
  pipeline,
  syncs,
  connectors,
}: {
  workspace: string;
  pipeline: Pipeline;
  syncs: Sync[];
  connectors: { source?: string; destination?: string };
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
    for (const s of plan.data?.plan?.streams ?? []) Object.assign(get(s.name), { level: s.level, reason: s.reason });
    for (const w of plan.data?.plan?.warnings ?? []) if (w.stream) get(w.stream).warning = w.message;
    for (const s of spec?.source?.streams ?? []) Object.assign(get(s.name), { mode: s.sync, cursor: s.cursor });
    for (const s of last?.streams ?? [])
      Object.assign(get(s.name), { rows: s.rows, quarantined: s.quarantinedRows, collapsed: s.collapsedRows });
    for (const s of schemas.data?.streamSchemas ?? []) get(s.stream).schema = Math.max(get(s.stream).schema ?? 0, s.version);
    return [...m.values()].sort((a, b) => a.name.localeCompare(b.name));
  }, [plan.data, spec, last, schemas.data]);

  const failed = pipeline.status?.lastSync?.state === SyncState.FAILED ? pipeline.status.lastSync : undefined;
  const problems = plan.data?.plan?.problems ?? [];
  const notes = plan.data?.plan?.notes ?? [];
  const warnings = (plan.data?.plan?.warnings ?? []).filter((w) => !w.stream).map((w) => w.message);

  return (
    <div className="grid gap-6">
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
            source={{ name: spec?.source?.ref ?? "", connector: connectors.source }}
            destination={{
              name: spec?.destination?.ref ?? "",
              connector: connectors.destination,
              detail: [spec?.destination?.namespace && `schema ${spec.destination.namespace}`, enumLabel(WriteMode[spec?.destination?.write ?? 0])]
                .filter(Boolean)
                .join(", "),
            }}
            streams={streams}
            steps={(spec?.postSync ?? []).flatMap((s) =>
              s.step.case === "dbt"
                ? [{ name: s.step.value.project, detail: `dbt ${enumLabel(DbtCommand[s.step.value.command]).toLowerCase() || "build"}` }]
                : [],
            )}
            live={live}
          />
        </Suspense>
      </section>

      <section className="grid gap-3" aria-labelledby="streams-h">
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <h2 id="streams-h" className="text-lg font-semibold tracking-tight">
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
        <div className="overflow-x-auto rounded-lg border bg-card">
          <table className="w-full min-w-[800px] border-collapse">
            <thead>
              <tr className="border-b text-left text-[0.8125rem] text-muted-foreground">
                <th scope="col" className="px-4 py-2 font-normal">Stream</th>
                <th scope="col" className="px-4 py-2 font-normal">Read</th>
                <th scope="col" className="px-4 py-2 font-normal">Delivery</th>
                <th scope="col" className="px-4 py-2 font-normal">Schema</th>
                <th scope="col" className="px-4 py-2 text-right font-normal">Rows</th>
                <th scope="col" className="px-4 py-2 text-right font-normal">Quarantined</th>
                <th scope="col" className="px-4 py-2 text-right font-normal">Collapsed</th>
              </tr>
            </thead>
            <tbody>
              {streams.map((s) => (
                <tr key={s.name} className="border-b last:border-b-0">
                  <td className="h-row px-4 py-2 font-medium">{s.name}</td>
                  <td className="px-4 py-2 text-muted-foreground">
                    {enumLabel(SyncMode[s.mode ?? 0]) || "Source default"}
                    {s.cursor && (
                      <>
                        {" "}on <code className="font-mono text-[0.8125rem] text-foreground">{s.cursor}</code>
                      </>
                    )}
                  </td>
                  <td className="px-4 py-2">
                    {s.level !== undefined ? (
                      <span className="inline-flex items-center gap-1.5">
                        <Tooltip>
                          <TooltipTrigger className="cursor-help underline decoration-dotted decoration-muted-foreground/60 underline-offset-4">
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
                    ) : (
                      <span className="text-muted-foreground">
                        {plan.data?.plan?.streams.length ? "Not in the plan" : "Checked when it syncs"}
                      </span>
                    )}
                  </td>
                  <td className="px-4 py-2">
                    {s.schema ? (
                      <Link
                        to="/workspaces/$workspace/pipelines/$pipeline"
                        params={{ workspace, pipeline: lastSegment(pipeline.name) }}
                        search={{ tab: "schemas", stream: s.name }}
                        className="font-mono text-[0.8125rem] hover:text-signal"
                      >
                        v{s.schema}
                      </Link>
                    ) : (
                      <span className="text-muted-foreground">—</span>
                    )}
                  </td>
                  <td className="px-4 py-2 text-right font-mono text-[0.8125rem] tabular">
                    {s.rows !== undefined ? <span title={full(s.rows)}>{compact(s.rows)}</span> : "—"}
                  </td>
                  <td className="px-4 py-2 text-right font-mono text-[0.8125rem] tabular">
                    {s.quarantined ? <span className="text-degraded">{full(s.quarantined)}</span> : <span className="text-muted-foreground">0</span>}
                  </td>
                  <td className="px-4 py-2 text-right font-mono text-[0.8125rem] tabular">
                    {s.collapsed ? (
                      <span title="Rows not inserted because a row with the same _openetl_id was already in the epoch or the table">
                        {full(s.collapsed)}
                      </span>
                    ) : (
                      <span className="text-muted-foreground">0</span>
                    )}
                  </td>
                </tr>
              ))}
              {streams.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-4 py-6 text-muted-foreground">
                    No streams to show yet. They show once a sync commits rows, or once the plan can list them, after{" "}
                    <code className="font-mono text-[0.8125rem] text-foreground">openetl discover {spec?.source?.ref}</code>.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
