import { CircleCheck, CircleDashed, CirclePause, CircleX, TriangleAlert } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { DeliveryState, WebhookState, type Webhook } from "@/gen/openetl/api/v1/webhook_pb";
import { cn } from "@/lib/utils";

/** Failing: enabled, but its deliveries have failed since failing_since. */
export function webhookState(w: Webhook) {
  if (w.state === WebhookState.DISABLED) return "Disabled";
  return w.failingSince ? "Failing" : "Enabled";
}

export function WebhookStateBadge({ webhook }: { webhook: Webhook }) {
  const s = webhookState(webhook);
  const Icon = s === "Disabled" ? CirclePause : s === "Failing" ? TriangleAlert : CircleCheck;
  return (
    <span className="inline-flex items-center gap-1.5 whitespace-nowrap">
      <Icon className={cn("size-4 shrink-0", s === "Disabled" ? "text-paused" : s === "Failing" ? "text-degraded" : "text-healthy")} aria-hidden />
      {s}
    </span>
  );
}

const delivery = {
  [DeliveryState.UNSPECIFIED]: { label: "Unknown", icon: CircleDashed, tone: "text-muted-foreground" },
  [DeliveryState.PENDING]: { label: "Retrying", icon: CircleDashed, tone: "text-degraded" },
  [DeliveryState.SUCCEEDED]: { label: "Delivered", icon: CircleCheck, tone: "text-healthy" },
  [DeliveryState.FAILED]: { label: "Failed", icon: CircleX, tone: "text-failed" },
};

export function DeliveryStateBadge({ state }: { state: DeliveryState }) {
  const d = delivery[state];
  return (
    <span className={cn("inline-flex items-center gap-1.5 whitespace-nowrap", state === DeliveryState.FAILED && "text-failed")}>
      <d.icon className={cn("size-4 shrink-0", d.tone)} aria-hidden />
      {d.label}
    </span>
  );
}

/** A webhook's event filters, the first few as badges. */
export function Events({ events, max = 3 }: { events: string[]; max?: number }) {
  return (
    <span className="inline-flex flex-wrap gap-1">
      {events.slice(0, max).map((e) => (
        <Badge key={e} variant="secondary" className="font-mono font-normal">
          {e}
        </Badge>
      ))}
      {events.length > max && <span className="text-[0.8125rem] text-muted-foreground">+{events.length - max}</span>}
    </span>
  );
}
