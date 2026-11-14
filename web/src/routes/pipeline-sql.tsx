import { lazy, Suspense, useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { Skeleton } from "@/components/ui/skeleton";
import { Empty, ErrorNote } from "@/components/empty";
import { Section } from "@/components/detail-header";
import { SqlMessages, SqlStatement, SqlStreams, SqlView } from "@/components/sql-plan";
import type { Marks } from "@/components/code-view";
import { Passthrough, PipelineService, type Pipeline } from "@/gen/openetl/api/v1/pipeline_pb";
import { errorMessage } from "@/lib/api";
import { notDelivered, partOf, parts, sqlMessages } from "@/lib/sql-plan";

const CodeView = lazy(() => import("@/components/code-view"));

/** A pipeline's SQL transform: its statements as written, and how they plan against the source's latest catalog. */
export function SqlTab({ pipeline }: { pipeline: Pipeline }) {
  const plan = useQuery(PipelineService.method.getPipelinePlan, { name: pipeline.name });
  const engine = pipeline.spec?.transform?.engine;
  const sql = engine?.case === "sql" ? engine.value : undefined;
  const transform = plan.data?.plan?.transform;
  const messages = useMemo(() => sqlMessages(plan.data?.plan?.problems ?? [], plan.data?.plan?.notes ?? []), [plan.data]);
  const all = useMemo(() => (transform ? parts(transform) : []), [transform]);
  const marks = useMemo(() => {
    const m: Marks = new Map();
    for (const x of messages) if (x.line && m.get(x.line) !== "problem") m.set(x.line, x.tone);
    return m;
  }, [messages]);

  if (!sql) return <Empty title="This pipeline has no SQL transform" />;
  const placed = new Map(messages.map((m) => [m, partOf(all, m)]));
  const unplaced = messages.filter((m) => !placed.get(m));
  const of = (at: object) => messages.filter((m) => placed.get(m)?.at === at);
  const tables = pipeline.spec?.destination?.tables ?? {};
  const none = sql.passthrough === Passthrough.NONE;

  return (
    <div className="grid gap-6">
      {plan.error && <ErrorNote title="Couldn't plan the pipeline" message={errorMessage(plan.error)} />}
      <SqlMessages messages={unplaced} />
      <div className="grid items-start gap-6 xl:grid-cols-2">
        <Section title="Statements">
          <p className="text-muted-foreground">
            As the current version has them. Streams no statement writes {none ? "aren't delivered" : "pass through"}, as{" "}
            <code className="font-mono text-[0.8125rem] text-foreground">passthrough</code> is{" "}
            <code className="font-mono text-[0.8125rem] text-foreground">{none ? "none" : "all"}</code>.
          </p>
          <div className="overflow-hidden rounded-lg border bg-card">
            <Suspense fallback={<Skeleton className="h-60 rounded-none" />}>
              <CodeView value={sql.sql} label="The pipeline's SQL" language="sql" marks={marks} />
            </Suspense>
          </div>
        </Section>
        <Section title="Plan">
          <p className="text-muted-foreground">
            How the statements plan against the source's latest catalog, as{" "}
            <code className="font-mono text-[0.8125rem] text-foreground">openetl plan</code> shows it.
          </p>
          {!plan.data && !plan.error ? (
            <Skeleton className="h-60" />
          ) : transform ? (
            <div className="grid gap-4">
              {all.map((p) =>
                p.kind === "statement" ? (
                  <SqlStatement key={`s${p.at.line}:${p.at.column}`} statement={p.at} table={tables[p.at.output]} messages={of(p.at)} />
                ) : (
                  <SqlView key={`v${p.at.line}:${p.at.column}`} view={p.at} messages={of(p.at)} />
                ),
              )}
              <SqlStreams label="Passed through" streams={transform.passedThrough}>
                No statement writes them, so they reach the destination as they would without a transform.
              </SqlStreams>
              <SqlStreams label="Not delivered" streams={notDelivered(transform)}>
                No statement writes them, and passthrough is none.
              </SqlStreams>
            </div>
          ) : (
            <Empty title={messages.some((m) => m.tone === "problem") ? "The statements have errors" : "The statements weren't planned"}>
              {messages.some((m) => m.tone === "problem")
                ? "Each error is marked in the SQL. A sync of this version fails until a new version fixes them."
                : "The note above says why. Each sync plans them when it starts."}
            </Empty>
          )}
        </Section>
      </div>
    </div>
  );
}
