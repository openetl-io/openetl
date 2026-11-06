import { useQuery } from "@connectrpc/connect-query";
import { Link } from "@tanstack/react-router";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { HealActionState, IncidentService, IncidentSeverity, IncidentState, type Incident } from "@/gen/openetl/api/v1/incident_pb";
import { RelativeTime } from "@/components/relative-time";
import { dateOf } from "@/lib/format";
import { SeverityIcon, incidentIds, kindLabel } from "./incident-fields";

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
    <section className="grid gap-3" aria-label="Open incidents">
      {open.map((i) => (
        <IncidentAlert key={i.name} workspace={workspace} incident={i} />
      ))}
    </section>
  );
}

function IncidentAlert({ workspace, incident: i }: { workspace: string; incident: Incident }) {
  const critical = i.severity === IncidentSeverity.CRITICAL;
  const last = dateOf(i.lastSeenTime);
  return (
    <Alert variant={critical ? "destructive" : "default"} role={critical ? "alert" : undefined}>
      <SeverityIcon severity={i.severity} />
      <AlertTitle>
        <Link to="/workspaces/$workspace/incidents/$pipeline/$incident" params={{ workspace, ...incidentIds(i) }} className="hover:underline">
          {i.summary}
        </Link>
      </AlertTitle>
      <AlertDescription>
        <p>
          {kindLabel(i.class)}
          {i.code && ` (${i.code})`}, seen {i.occurrences === 1 ? "once" : `${i.occurrences} times`}
          {last && (
            <>
              , last <RelativeTime date={last} />
            </>
          )}
        </p>
        {i.actions.map((a, n) => (
          <p key={n}>
            {a.state === HealActionState.APPLIED ? "Heal: " : "Heal proposes: "}
            {a.description}
          </p>
        ))}
        {i.nextSteps && <p className="text-foreground">{i.nextSteps}</p>}
      </AlertDescription>
    </Alert>
  );
}
