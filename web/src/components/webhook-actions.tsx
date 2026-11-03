import { Ban, Power, Send } from "lucide-react";
import { Button } from "@/components/ui/button";
import { WebhookService, WebhookState, type Webhook } from "@/gen/openetl/api/v1/webhook_pb";
import { useAction } from "@/lib/action";
import { useCan } from "@/lib/caller";
import { lastSegment } from "@/lib/names";
import { Confirm } from "./confirm";

const refresh = [WebhookService];

/** Sending a test event, and disabling or enabling, each shown only to a role that may. */
export function WebhookActions({ workspace, webhook }: { workspace: string; webhook: Webhook }) {
  const can = useCan(workspace);
  const test = useAction(WebhookService.method.testWebhook, { done: "Test event sent. Its delivery is listed below.", failed: "Couldn't send a test event", refresh });
  const disable = useAction(WebhookService.method.disableWebhook, { done: "Disabled", failed: "Couldn't disable", refresh });
  const enable = useAction(WebhookService.method.enableWebhook, { done: "Enabled", failed: "Couldn't enable", refresh });
  const name = { name: webhook.name };
  const disabled = webhook.state === WebhookState.DISABLED;

  return (
    <>
      {can("webhooks.test") && !disabled && (
        <Button size="sm" variant="outline" disabled={test.isPending} onClick={() => test.mutate(name)}>
          <Send aria-hidden />
          Send test event
        </Button>
      )}
      {can("webhooks.update") &&
        (disabled ? (
          <Button size="sm" disabled={enable.isPending} onClick={() => enable.mutate(name)}>
            <Power aria-hidden />
            Enable
          </Button>
        ) : (
          <Confirm
            title={`Disable ${lastSegment(webhook.name)}?`}
            description="Its pending deliveries fail, and events aren't delivered to it until it's enabled again."
            confirm={disable.isPending ? "Disabling…" : "Disable"}
            destructive
            pending={disable.isPending}
            onConfirm={(close) => disable.mutate(name, { onSuccess: close })}
          >
            <Button size="sm" variant="outline">
              <Ban aria-hidden />
              Disable
            </Button>
          </Confirm>
        ))}
    </>
  );
}
