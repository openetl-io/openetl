import { CircleCheck, CircleDashed, CirclePause, CircleStop, CircleX, LoaderCircle, TriangleAlert, type LucideIcon } from "lucide-react";
import { statusLabel, type Status } from "@/lib/status";
import { cn } from "@/lib/utils";

const icons: Record<Status, LucideIcon> = {
  healthy: CircleCheck,
  running: LoaderCircle,
  degraded: TriangleAlert,
  failed: CircleX,
  paused: CirclePause,
  stopped: CircleStop,
  idle: CircleDashed,
};

export const statusText: Record<Status, string> = {
  healthy: "text-healthy",
  running: "text-running",
  degraded: "text-degraded",
  failed: "text-failed",
  paused: "text-paused",
  stopped: "text-muted-foreground",
  idle: "text-muted-foreground",
};

export const statusFill: Record<Status, string> = {
  healthy: "bg-healthy",
  running: "bg-running",
  degraded: "bg-degraded",
  failed: "bg-failed",
  paused: "bg-paused",
  stopped: "bg-idle",
  idle: "bg-idle",
};

export function StatusIcon({ status, className }: { status: Status; className?: string }) {
  const Icon = icons[status];
  return (
    <Icon
      className={cn("size-4 shrink-0", statusText[status], status === "running" && "animate-spin [animation-duration:1.6s]", className)}
      aria-hidden
    />
  );
}

/** Status is always an icon and a label, never color alone. Only the icon takes the status's color, except for a failure. */
export function StatusBadge({ status, label, className }: { status: Status; label?: string; className?: string }) {
  return (
    <span className={cn("inline-flex items-center gap-1.5 whitespace-nowrap", status === "failed" && "text-failed", className)}>
      <StatusIcon status={status} />
      <span>{label ?? statusLabel[status]}</span>
    </span>
  );
}
