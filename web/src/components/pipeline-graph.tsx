import { useMemo, useState } from "react";
import {
  BaseEdge,
  Handle,
  Position,
  ReactFlow,
  getBezierPath,
  type Edge,
  type EdgeProps,
  type Node,
  type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/base.css";
import { Database, Hammer, Waypoints } from "lucide-react";
import type { DeliveryLevel } from "@/gen/openetl/api/v1/pipeline_pb";
import { compact } from "@/lib/format";
import { usePrefs } from "@/lib/prefs";
import { cn } from "@/lib/utils";

export type GraphStream = { name: string; rows?: bigint; level?: DeliveryLevel };
type End = { name: string; connector?: string; detail?: string; logo?: string };

type EndNode = Node<{ kind: "source" | "destination" | "step"; title: string; detail?: string; logo?: string }, "end">;
type StreamNode = Node<{ name: string; rows?: bigint; live: boolean }, "stream">;

const kindIcon = { source: Database, destination: Database, step: Hammer };
const kindLabel = { source: "Source", destination: "Destination", step: "After each sync" };

function EndView({ data }: NodeProps<EndNode>) {
  const Icon = kindIcon[data.kind];
  const [logoFailed, setLogoFailed] = useState(false);
  return (
    <div className="w-[220px] rounded-lg border bg-card px-3 py-2.5 shadow-[0_1px_0_var(--border)]">
      {data.kind !== "source" && <Handle type="target" position={Position.Left} className="!size-1.5 !border-0 !bg-border" />}
      <div className="flex items-center gap-2 text-[0.75rem] text-muted-foreground">
        {data.logo && !logoFailed ? (
          <img src={data.logo} alt="" referrerPolicy="no-referrer" onError={() => setLogoFailed(true)} className="size-3.5 rounded-sm object-contain" />
        ) : (
          <Icon className="size-3.5" aria-hidden />
        )}
        {kindLabel[data.kind]}
      </div>
      <div className="mt-1 truncate font-medium">{data.title}</div>
      {data.detail && <div className="truncate text-[0.8125rem] text-muted-foreground">{data.detail}</div>}
      <Handle type="source" position={Position.Right} className="!size-1.5 !border-0 !bg-border" />
    </div>
  );
}

function StreamView({ data }: NodeProps<StreamNode>) {
  return (
    <div
      className={cn(
        "flex h-9 w-[220px] items-center gap-2 rounded-full border bg-card pr-3 pl-2.5",
        data.live && "border-signal/50",
      )}
    >
      <Handle type="target" position={Position.Left} className="!size-1.5 !border-0 !bg-border" />
      <Waypoints className={cn("size-3.5 shrink-0 text-muted-foreground", data.live && "text-signal")} aria-hidden />
      <span className="truncate text-[0.8125rem] font-medium">{data.name}</span>
      <span className="ml-auto font-mono text-[0.75rem] text-muted-foreground tabular">
        {data.rows !== undefined ? compact(data.rows) : ""}
      </span>
      <Handle type="source" position={Position.Right} className="!size-1.5 !border-0 !bg-border" />
    </div>
  );
}

/** While a sync runs, a beam travels along each edge. */
function FlowEdge({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data }: EdgeProps<Edge<{ live: boolean }>>) {
  const [path] = getBezierPath({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition });
  const reduced = typeof window !== "undefined" && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  return (
    <>
      <BaseEdge path={path} style={{ stroke: data?.live ? "var(--signal)" : "var(--border)", strokeWidth: 1.5, opacity: data?.live ? 0.55 : 1 }} />
      {data?.live && !reduced && (
        <circle r={2.5} fill="var(--signal)">
          <animateMotion dur="1.8s" repeatCount="indefinite" path={path} />
        </circle>
      )}
    </>
  );
}

const nodeTypes = { end: EndView, stream: StreamView };
const edgeTypes = { flow: FlowEdge };

export default function PipelineGraph({
  source,
  destination,
  streams,
  steps,
  live,
}: {
  source: End;
  destination: End;
  streams: GraphStream[];
  steps: { name: string; detail: string }[];
  live: boolean;
}) {
  const { dark } = usePrefs();
  const { nodes, edges } = useMemo(() => {
    const gap = 52;
    const height = Math.max(streams.length, 1) * gap;
    const mid = height / 2 - 30;
    const nodes: Node[] = [
      { id: "source", type: "end", position: { x: 0, y: mid }, data: { kind: "source", title: source.name, detail: source.connector, logo: source.logo } },
      { id: "destination", type: "end", position: { x: 640, y: mid }, data: { kind: "destination", title: destination.name, detail: [destination.connector, destination.detail].filter(Boolean).join(", "), logo: destination.logo } },
    ];
    const edges: Edge[] = [];
    streams.forEach((s, i) => {
      const id = `stream:${s.name}`;
      nodes.push({ id, type: "stream", position: { x: 320, y: i * gap }, data: { name: s.name, rows: s.rows, live } });
      edges.push({ id: `a:${s.name}`, source: "source", target: id, type: "flow", data: { live } });
      edges.push({ id: `b:${s.name}`, source: id, target: "destination", type: "flow", data: { live } });
    });
    if (streams.length === 0) edges.push({ id: "direct", source: "source", target: "destination", type: "flow", data: { live } });
    let prev = "destination";
    steps.forEach((s, i) => {
      const id = `step:${i}`;
      nodes.push({ id, type: "end", position: { x: 940 + i * 280, y: mid }, data: { kind: "step", title: s.name, detail: s.detail } });
      edges.push({ id: `c:${i}`, source: prev, target: id, type: "flow", data: { live: false } });
      prev = id;
    });
    return { nodes, edges };
  }, [source, destination, streams, steps, live]);

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      edgeTypes={edgeTypes}
      colorMode={dark ? "dark" : "light"}
      fitView
      fitViewOptions={{ padding: 0.15, maxZoom: 1.1 }}
      nodesDraggable={false}
      nodesConnectable={false}
      elementsSelectable={false}
      zoomOnScroll={false}
      panOnScroll
      aria-label="How data flows through the pipeline"
    />
  );
}
