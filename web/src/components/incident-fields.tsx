import { CircleCheck, OctagonAlert, TriangleAlert } from "lucide-react";
import { IncidentClass, IncidentSeverity, IncidentState, type Incident } from "@/gen/openetl/api/v1/incident_pb";
import { enumLabel } from "@/lib/format";
import { cn } from "@/lib/utils";

export const severityLabel = (s: IncidentSeverity) => (s === IncidentSeverity.CRITICAL ? "Critical" : "Warning");
export const stateLabel = (s: IncidentState) => (s === IncidentState.RESOLVED ? "Resolved" : "Open");
export const kindLabel = (c: IncidentClass) => enumLabel(IncidentClass[c]) || "Other";

/** The pipeline and incident IDs in an incident's name, workspaces/{w}/pipelines/{p}/incidents/{i}. */
export function incidentIds(i: Incident) {
  const parts = i.name.split("/");
  return { pipeline: parts[3] ?? "", incident: parts[5] ?? "" };
}

export function SeverityIcon({ severity }: { severity: IncidentSeverity }) {
  const critical = severity === IncidentSeverity.CRITICAL;
  const Icon = critical ? OctagonAlert : TriangleAlert;
  return <Icon className={cn("size-4 shrink-0", critical ? "text-failed" : "text-degraded")} aria-hidden />;
}

export function Severity({ severity }: { severity: IncidentSeverity }) {
  return (
    <span className="inline-flex items-center gap-1.5 whitespace-nowrap">
      <SeverityIcon severity={severity} />
      {severityLabel(severity)}
    </span>
  );
}

export function IncidentStateBadge({ state }: { state: IncidentState }) {
  const resolved = state === IncidentState.RESOLVED;
  return (
    <span className={cn("inline-flex items-center gap-1.5 whitespace-nowrap", resolved && "text-muted-foreground")}>
      {resolved ? (
        <CircleCheck className="size-4 shrink-0" aria-hidden />
      ) : (
        <span className="mx-1 size-2 shrink-0 rounded-full bg-failed" aria-hidden />
      )}
      {stateLabel(state)}
    </span>
  );
}
