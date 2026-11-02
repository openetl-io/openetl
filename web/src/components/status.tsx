import { CircleCheck, CircleDashed, CirclePause, CircleX, LoaderCircle, TriangleAlert, type LucideIcon } from "lucide-react";
import { statusLabel, type Status } from "@/lib/status";
import { cn } from "@/lib/utils";

const icons: Record<Status, LucideIcon> = {
  healthy: CircleCheck,
  running: LoaderCircle,
  degraded: TriangleAlert,
  failed: CircleX,
  paused: CirclePause,
  idle: CircleDashed,
};

export const statusText: Record<Status, string> = {
  healthy: "text-healthy",
  running: "text-running",
  degraded: "text-degraded",
  failed: "text-failed",
  paused: "text-paused",
  idle: "text-muted-foreground",
};

export const statusFill: Record<Status, string> = {
  healthy: "bg-healthy",
  running: "bg-running",
  degraded: "bg-degraded",
  failed: "bg-failed",
  paused: "bg-paused",
  idle: "bg-idle",
};

/** Status is always an icon and a label, never color alone. */
export function StatusBadge({ status, label, className }: { status: Status; label?: string; className?: string }) {
  const Icon = icons[status];
  return (
    <span className={cn("inline-flex items-center gap-1.5 font-medium whitespace-nowrap", statusText[status], className)}>
      <Icon className={cn("size-4", status === "running" && "animate-spin [animation-duration:1.6s]")} aria-hidden />
      <span>{label ?? statusLabel[status]}</span>
    </span>
  );
}
