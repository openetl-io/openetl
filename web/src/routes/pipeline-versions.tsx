import { lazy, Suspense } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { getRouteApi, useNavigate } from "@tanstack/react-router";
import { Copy } from "lucide-react";
import { toast } from "sonner";
import { Skeleton } from "@/components/ui/skeleton";
import { Empty, ErrorNote } from "@/components/empty";
import { RelativeTime } from "@/components/relative-time";
import { PipelineService, type PinnedConfig } from "@/gen/openetl/api/v1/pipeline_pb";
import { Button } from "@/components/ui/button";
import { pipelineYAML } from "@/lib/spec-yaml";
import { errorMessage } from "@/lib/api";
import { dateOf } from "@/lib/format";
import { lastSegment, pipelineName, shortDigest } from "@/lib/names";
import { cn } from "@/lib/utils";

const CodeView = lazy(() => import("@/components/code-view"));
const route = getRouteApi("/_app/workspaces/$workspace/pipelines/$pipeline");

function Digest({ value }: { value: string }) {
  if (!value) return <span className="text-muted-foreground">Built in</span>;
  return (
    <button
      onClick={() => void navigator.clipboard.writeText(value).then(() => toast("Copied the digest"))}
      className="inline-flex items-center gap-1 font-mono text-[0.8125rem] hover:text-signal"
      title={value}
    >
      {shortDigest(value)}
      <Copy className="size-3" aria-label="Copy" />
    </button>
  );
}

function Pinned({ label, pin }: { label: string; pin?: PinnedConfig }) {
  return (
    <div className="grid gap-0.5">
      <dt className="text-[0.8125rem] text-muted-foreground">{label}</dt>
      <dd className="grid gap-0.5">
        <span>
          {pin?.name ? lastSegment(pin.name) : <span className="text-muted-foreground">Deleted since</span>}
          {pin?.spec?.connector && <span className="text-muted-foreground"> ({pin.spec.connector})</span>}
        </span>
        <span className="text-[0.8125rem] text-muted-foreground">
          version <span className="font-mono">{shortDigest(pin?.version ?? "")}</span>, connector <Digest value={pin?.connector ?? ""} />
        </span>
      </dd>
    </div>
  );
}

export function VersionsTab({ workspace, pipeline, current }: { workspace: string; pipeline: string; current?: string }) {
  const { version } = route.useSearch();
  const navigate = useNavigate({ from: "/workspaces/$workspace/pipelines/$pipeline" });
  const list = useQuery(PipelineService.method.listPipelineVersions, { parent: pipelineName(workspace, pipeline), pageSize: 100 });
  if (list.error) return <ErrorNote title="Couldn't list versions" message={errorMessage(list.error)} />;
  if (!list.data) return <Skeleton className="h-60" />;
  const versions = list.data.pipelineVersions;
  if (versions.length === 0) return <Empty title="No versions" />;
  const shown = versions.find((v) => lastSegment(v.name) === version) ?? versions.find((v) => v.current) ?? versions[0]!;
  const doc = shown.spec ? pipelineYAML(pipeline, workspace, shown.spec) : "";

  return (
    <div className="grid gap-6 md:grid-cols-[260px_1fr]">
      <nav aria-label="Versions" className="grid content-start gap-0.5">
        {versions.map((v) => {
          const id = lastSegment(v.name);
          return (
            <button
              key={v.name}
              onClick={() => navigate({ search: (q) => ({ ...q, version: id }) })}
              aria-current={v === shown ? "true" : undefined}
              className={cn(
                "grid gap-0.5 rounded-md px-3 py-1.5 text-left hover:bg-accent focus-visible:outline-2 focus-visible:outline-ring",
                v === shown && "bg-secondary",
              )}
            >
              <span className="flex items-center gap-2">
                <span className="font-mono text-[0.8125rem]">{shortDigest(id)}</span>
                {(v.current || id === current) && <span className="rounded-full bg-signal-soft px-1.5 text-xs text-signal">Current</span>}
              </span>
              <span className="text-[0.8125rem] text-muted-foreground">
                <RelativeTime date={dateOf(v.createTime)!} />
              </span>
            </button>
          );
        })}
      </nav>
      <div className="grid min-w-0 content-start gap-4">
        <dl className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          <div className="grid gap-0.5">
            <dt className="text-[0.8125rem] text-muted-foreground">Applied from</dt>
            <dd className="truncate font-mono text-[0.8125rem]">{shown.provenance || "—"}</dd>
          </div>
          <Pinned label="Source" pin={shown.source} />
          <Pinned label="Destination" pin={shown.destination} />
          {Object.entries(shown.dbt).map(([project, digest]) => (
            <div key={project} className="grid gap-0.5">
              <dt className="text-[0.8125rem] text-muted-foreground">dbt project {project}</dt>
              <dd>
                <Digest value={digest} />
              </dd>
            </div>
          ))}
        </dl>
        <div className="grid gap-2">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <p className="text-muted-foreground">
              {shown.current
                ? "The document that applies this version."
                : "Applying this document makes this version current again."}
            </p>
            <Button
              variant="outline"
              size="sm"
              onClick={() => void navigator.clipboard.writeText(doc).then(() => toast("Copied the YAML"))}
            >
              <Copy />
              Copy YAML
            </Button>
          </div>
          <div className="overflow-hidden rounded-lg border bg-card">
            <Suspense fallback={<Skeleton className="h-60 rounded-none" />}>
              <CodeView value={doc} label={`The document of version ${shortDigest(lastSegment(shown.name))}`} />
            </Suspense>
          </div>
        </div>
      </div>
    </div>
  );
}
