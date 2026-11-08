import { Link } from "@tanstack/react-router";
import { Play, Undo2 } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { RelativeTime } from "@/components/relative-time";
import { IncidentService } from "@/gen/openetl/api/v1/incident_pb";
import { PipelineService, type Pipeline, type PipelineOverride } from "@/gen/openetl/api/v1/pipeline_pb";
import { useAction } from "@/lib/action";
import { useCan } from "@/lib/caller";
import { dateOf, interval } from "@/lib/format";
import { lastSegment } from "@/lib/names";

const refresh = [PipelineService, IncidentService];

/** What an override sets, and what the spec has, in words. */
export function overrideText(o: PipelineOverride): { label: string; value: string; spec: string } {
  switch (o.field) {
    case "schedule.every": {
      const every = (v: string) => (v ? `Every ${interval(parseFloat(JSON.parse(v)) * 1000)}` : "Unset");
      return { label: "Schedule", value: every(o.value), spec: every(o.specValue) };
    }
    case "source.rate_limit_scale":
      return { label: "Source's rate limit", value: `${Number(o.value) * 100}% of what it declares`, spec: "What it declares" };
  }
  return { label: o.field, value: o.value, spec: o.specValue || "Unset" };
}

function IncidentLink({ workspace, incident }: { workspace: string; incident: string }) {
  const parts = incident.split("/");
  return (
    <Link
      to="/workspaces/$workspace/incidents/$pipeline/$incident"
      params={{ workspace, pipeline: parts[3] ?? "", incident: lastSegment(incident) }}
      className="text-signal underline-offset-4 hover:underline"
    >
      See the incident
    </Link>
  );
}

/** The streams paused on their own and heal's overrides, each with how to undo it; nothing when there are none. */
export function RunOverrides({ workspace, pipeline }: { workspace: string; pipeline: Pipeline }) {
  const can = useCan(workspace);
  const resume = useAction(PipelineService.method.resumePipeline, { done: "Resumed the stream", failed: "Couldn't resume the stream", refresh });
  const remove = useAction(PipelineService.method.removePipelineOverride, {
    done: "Removed. The spec has its say again.",
    failed: "Couldn't remove the override",
    refresh,
  });
  if (pipeline.pausedStreams.length === 0 && pipeline.overrides.length === 0) return null;
  return (
    <section className="grid gap-3" aria-labelledby="overrides-h">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 id="overrides-h" className="text-base font-semibold">
          Not as the spec says
        </h2>
        <span className="text-[0.8125rem] text-muted-foreground">Until someone undoes it</span>
      </div>
      {pipeline.pausedStreams.map((s) => (
        <article key={s.stream} className="grid gap-2 rounded-lg border bg-card p-4" aria-label={`Stream ${s.stream}`}>
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5">
            <span className="font-mono text-[0.8125rem] font-medium">{s.stream}</span>
            <Badge variant="outline" className="font-normal">
              Paused
            </Badge>
            <span className="text-[0.8125rem] text-muted-foreground sm:ml-auto">
              {dateOf(s.createTime) && <RelativeTime date={dateOf(s.createTime)!} />}
            </span>
          </div>
          <p>
            Syncs skip it, and keep where it was for when it's resumed. {s.reason}
          </p>
          <div className="flex flex-wrap items-center gap-3">
            {can("pipelines.pause") && (
              <Button size="sm" variant="outline" disabled={resume.isPending} onClick={() => resume.mutate({ name: pipeline.name, stream: s.stream })}>
                <Play aria-hidden />
                Resume stream
              </Button>
            )}
            {s.incident && <IncidentLink workspace={workspace} incident={s.incident} />}
          </div>
        </article>
      ))}
      {pipeline.overrides.map((o) => {
        const t = overrideText(o);
        return (
          <article key={o.field} className="grid gap-2 rounded-lg border bg-card p-4" aria-label={t.label}>
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5">
              <span className="font-medium">{t.label}</span>
              <Badge variant="outline" className="font-normal">
                Changed by heal
              </Badge>
              <span className="text-[0.8125rem] text-muted-foreground sm:ml-auto">
                {dateOf(o.updateTime) && <RelativeTime date={dateOf(o.updateTime)!} />}
              </span>
            </div>
            <p>
              {t.value}, where the spec has {t.spec.charAt(0).toLowerCase() + t.spec.slice(1)}. {o.reason}
            </p>
            <div className="flex flex-wrap items-center gap-3">
              {can("pipelines.override") && (
                <Button size="sm" variant="outline" disabled={remove.isPending} onClick={() => remove.mutate({ name: pipeline.name, field: o.field })}>
                  <Undo2 aria-hidden />
                  Use the spec's
                </Button>
              )}
              {o.incident && <IncidentLink workspace={workspace} incident={o.incident} />}
            </div>
          </article>
        );
      })}
    </section>
  );
}
