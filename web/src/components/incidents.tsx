import { useQuery } from "@connectrpc/connect-query";
import { Link } from "@tanstack/react-router";
import { TriangleAlert } from "lucide-react";
import {
  HealActionState,
  IncidentClass,
  IncidentService,
  IncidentSeverity,
  IncidentState,
  type Incident,
} from "@/gen/openetl/api/v1/incident_pb";
import { RelativeTime } from "@/components/relative-time";
import { dateOf, enumLabel } from "@/lib/format";
import { lastSegment } from "@/lib/names";
import { cn } from "@/lib/utils";

/** The pipeline's open incidents, each with what heal did and what to do. */
export function Incidents({ workspace, pipeline, live }: { workspace: string; pipeline: string; live: boolean }) {
  const q = useQuery(
    IncidentService.method.listIncidents,
    { parent: pipeline, state: IncidentState.OPEN },
    { refetchInterval: live ? 3_000 : 15_000 },
  );
  const open = q.data?.incidents ?? [];
  if (open.length === 0) return null;
  return (
    <section className="grid gap-3" aria-labelledby="incidents-h">
      <h2 id="incidents-h" className="text-lg font-semibold tracking-tight">
        Open incidents
      </h2>
      {open.map((i) => (
        <IncidentCard key={i.name} workspace={workspace} incident={i} />
      ))}
    </section>
  );
}

function IncidentCard({ workspace, incident: i }: { workspace: string; incident: Incident }) {
  const critical = i.severity === IncidentSeverity.CRITICAL;
  const last = dateOf(i.lastSeenTime);
  return (
    <article
      role={critical ? "alert" : undefined}
      className={cn(
        "grid gap-2 rounded-lg border p-4",
        critical ? "border-failed/40 bg-failed/5" : "border-degraded/40 bg-degraded/5",
      )}
    >
      <p className={cn("flex gap-2 font-medium", critical ? "text-failed" : "text-degraded")}>
        <TriangleAlert className="mt-0.5 size-4 shrink-0" aria-hidden />
        <span>{i.summary}</span>
      </p>
      <p className="text-[0.8125rem] text-muted-foreground">
        {enumLabel(IncidentClass[i.class])}
        {i.code && ` (${i.code})`} · seen {i.occurrences === 1 ? "once" : `${i.occurrences} times`}
        {last && (
          <>
            , last <RelativeTime date={last} />
          </>
        )}
      </p>
      {i.message && <p className="font-mono text-[0.8125rem] break-words whitespace-pre-wrap">{i.message.split("\n")[0]}</p>}
      {i.actions.map((a, n) => (
        <p key={n} className="text-[0.8125rem]">
          {a.state === HealActionState.APPLIED ? "Heal: " : "Heal proposes: "}
          {a.description}
        </p>
      ))}
      {i.nextSteps && <p className="max-w-prose">{i.nextSteps}</p>}
      {i.sync && (
        <Link
          to="/workspaces/$workspace/pipelines/$pipeline/syncs/$sync"
          params={{ workspace, pipeline: lastSegment(i.sync.slice(0, i.sync.indexOf("/syncs/"))), sync: lastSegment(i.sync) }}
          className="w-fit text-signal underline-offset-4 hover:underline"
        >
          Read the sync's log
        </Link>
      )}
    </article>
  );
}
