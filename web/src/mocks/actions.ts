import { create } from "@bufbuild/protobuf";
import { DurationSchema, timestampNow } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError } from "@connectrpc/connect";
import { IncidentState, type Incident } from "@/gen/openetl/api/v1/incident_pb";
import { PipelineMode, type Pipeline } from "@/gen/openetl/api/v1/pipeline_pb";
import { SyncSchema, SyncState } from "@/gen/openetl/api/v1/sync_pb";
import { DeliveryState, WebhookDeliverySchema, WebhookState, type Webhook } from "@/gen/openetl/api/v1/webhook_pb";
import { syncs } from "./fixtures";
import { deliveries } from "./workspace";
import { user } from "./session";

// The calls that change something, on the fixtures in place, with the
// server's refusals. A started sync succeeds after 20 seconds, and a test
// event is delivered after 3.

const refuse = (message: string) => new ConnectError(message, Code.FailedPrecondition);
const id = () => Math.floor(Math.random() * 0xffffff).toString(16);

export function setPaused(p: Pipeline, paused: boolean) {
  p.paused = paused;
  return { pipeline: p };
}

export function startSync(p: Pipeline) {
  if (p.paused) throw refuse(`${p.name} is paused; resume it first`);
  if (p.spec?.mode === PipelineMode.CONTINUOUS) throw refuse(`${p.name} is continuous, so it has no syncs to start`);
  if (p.status?.lastSync?.state === SyncState.RUNNING) throw refuse(`a sync of ${p.name} is running`);
  const s = create(SyncSchema, {
    name: `${p.name}/syncs/${id()}`,
    version: p.version,
    state: SyncState.RUNNING,
    startTime: timestampNow(),
    streams: p.spec?.source?.streams.map((st) => ({ name: st.name, rows: 0n })),
    dataPlane: "default",
  });
  syncs.set(p.name, [s, ...(syncs.get(p.name) ?? [])]);
  p.status!.lastSync = s;
  setTimeout(() => {
    s.state = SyncState.SUCCEEDED;
    s.endTime = s.lastCommitTime = timestampNow();
    for (const st of s.streams) st.rows = 1_200n;
    p.status!.lastSuccessTime = s.endTime;
  }, 20_000);
  return {};
}

export function resolve(i: Incident, comment: string) {
  if (i.state !== IncidentState.OPEN) throw refuse(`${i.name} is resolved already`);
  i.state = IncidentState.RESOLVED;
  i.resolveTime = timestampNow();
  i.resolution = `resolved by ${user.email}${comment.trim() ? `: ${comment.trim()}` : ""}`;
  return { incident: i };
}

export function setDisabled(w: Webhook, disabled: boolean) {
  w.state = disabled ? WebhookState.DISABLED : WebhookState.ENABLED;
  w.disabledReason = disabled ? `disabled by ${user.name}` : "";
  w.updateTime = timestampNow();
  if (disabled) for (const d of deliveries.get(w.name) ?? []) if (d.state === DeliveryState.PENDING) d.state = DeliveryState.FAILED;
  return { webhook: w };
}

export function sendTest(w: Webhook) {
  if (w.state === WebhookState.DISABLED) throw refuse(`${w.name} is disabled; enable it first`);
  const d = create(WebhookDeliverySchema, {
    name: `${w.name}/deliveries/${id()}`,
    eventId: `evt_${id()}`,
    eventType: "webhook.test",
    state: DeliveryState.PENDING,
    createTime: timestampNow(),
  });
  deliveries.set(w.name, [d, ...(deliveries.get(w.name) ?? [])]);
  setTimeout(() => {
    d.state = DeliveryState.SUCCEEDED;
    d.attempts = 1;
    d.responseStatus = 200;
    d.latency = create(DurationSchema, { nanos: 120_000_000 });
    d.lastAttemptTime = timestampNow();
  }, 3_000);
  return { delivery: d.name };
}
