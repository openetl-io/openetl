import { Code } from "@connectrpc/connect";
import { useQuery } from "@connectrpc/connect-query";
import { Link, getRouteApi } from "@tanstack/react-router";
import { Siren } from "lucide-react";
import { CodeBlock } from "@/components/code-block";
import { DescriptionItem, DescriptionList, DetailHeader, Section } from "@/components/detail-header";
import { ErrorNote } from "@/components/empty";
import { EmptyState } from "@/components/empty-state";
import { IncidentActions } from "@/components/incident-actions";
import { IncidentStateBadge, Severity, kindLabel } from "@/components/incident-fields";
import { RelativeTime } from "@/components/relative-time";
import { ResourceLink } from "@/components/resource-link";
import { Badge } from "@/components/ui/badge";
import { HealActionState, IncidentService } from "@/gen/openetl/api/v1/incident_pb";
import { errorCode, errorMessage } from "@/lib/api";
import { dateOf } from "@/lib/format";
import { lastSegment } from "@/lib/names";

const route = getRouteApi("/_app/workspaces/$workspace/incidents/$pipeline/$incident");

function When({ t }: { t?: Date }) {
  return t ? <RelativeTime date={t} /> : <span className="text-muted-foreground">—</span>;
}

export function IncidentPage() {
  const { workspace, pipeline, incident } = route.useParams();
  const name = `workspaces/${workspace}/pipelines/${pipeline}/incidents/${incident}`;
  const q = useQuery(IncidentService.method.getIncident, { name }, { refetchInterval: 15_000 });

  if (q.error) {
    return errorCode(q.error) === Code.NotFound ? (
      <EmptyState icon={Siren} title={`There's no incident ${incident} on ${pipeline}`} />
    ) : (
      <ErrorNote title="Couldn't load the incident" message={errorMessage(q.error)} />
    );
  }
  const i = q.data?.incident;
  const details = Object.entries(i?.details ?? {});

  return (
    <div className="grid gap-8">
      <DetailHeader
        title={i?.summary ?? incident}
        status={i && (
          <span className="flex items-center gap-3">
            <Severity severity={i.severity} />
            <IncidentStateBadge state={i.state} />
          </span>
        )}
        actions={i && <IncidentActions workspace={workspace} incident={i} />}
      >
        {i && (
          <>
            <DescriptionItem label="Pipeline">
              <ResourceLink name={`workspaces/${workspace}/pipelines/${pipeline}`} />
            </DescriptionItem>
            {i.stream && <DescriptionItem label="Stream"><span className="font-mono text-[0.8125rem]">{i.stream}</span></DescriptionItem>}
            <DescriptionItem label="Kind">
              {kindLabel(i.class)}
              {i.code && <span className="ml-1.5 font-mono text-xs text-muted-foreground">{i.code}</span>}
            </DescriptionItem>
            <DescriptionItem label="Seen">{i.occurrences === 1 ? "Once" : `${i.occurrences} times`}</DescriptionItem>
            <DescriptionItem label="First seen"><When t={dateOf(i.firstSeenTime)} /></DescriptionItem>
            <DescriptionItem label="Last seen"><When t={dateOf(i.lastSeenTime)} /></DescriptionItem>
            {i.resolveTime && <DescriptionItem label="Resolved"><When t={dateOf(i.resolveTime)} /></DescriptionItem>}
          </>
        )}
      </DetailHeader>

      {i && (
        <div className="grid max-w-4xl gap-8">
          {i.resolution && (
            <Section title="Resolution">
              <p>{i.resolution}</p>
            </Section>
          )}
          {i.message && (
            <Section
              title="What happened"
              actions={
                i.sync && (
                  <Link
                    to="/workspaces/$workspace/pipelines/$pipeline/syncs/$sync"
                    params={{ workspace, pipeline, sync: lastSegment(i.sync) }}
                    className="text-[0.8125rem] text-signal hover:underline"
                  >
                    Read the sync's log
                  </Link>
                )
              }
            >
              <CodeBlock>{i.message}</CodeBlock>
            </Section>
          )}
          {i.nextSteps && (
            <Section title="Next steps">
              <p className="max-w-prose">{i.nextSteps}</p>
            </Section>
          )}
          {i.actions.length > 0 && (
            <Section title="Heal">
              <ul className="grid gap-2">
                {i.actions.map((a, n) => (
                  <li key={n} className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                    <Badge variant={a.state === HealActionState.APPLIED ? "secondary" : "outline"} className="font-normal">
                      {a.state === HealActionState.APPLIED ? "Applied" : "Proposed"}
                    </Badge>
                    <span>{a.description}</span>
                    {dateOf(a.time) && <span className="text-[0.8125rem] text-muted-foreground"><RelativeTime date={dateOf(a.time)!} /></span>}
                  </li>
                ))}
              </ul>
            </Section>
          )}
          {details.length > 0 && (
            <Section title="Details">
              <DescriptionList className="lg:grid lg:grid-cols-3">
                {details.map(([k, v]) => (
                  <DescriptionItem key={k} label={k}>
                    <span className="font-mono text-[0.8125rem]">{v}</span>
                  </DescriptionItem>
                ))}
              </DescriptionList>
            </Section>
          )}
        </div>
      )}
    </div>
  );
}
