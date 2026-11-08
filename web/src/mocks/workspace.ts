import { create, fromJson } from "@bufbuild/protobuf";
import { ValueSchema, timestampFromMs } from "@bufbuild/protobuf/wkt";
import {
  DataPlaneSchema,
  KeySchema,
  PrincipalSchema,
  PrincipalType,
  Role,
  RunnerTokenSchema,
  type DataPlane,
  type Key,
  type Principal,
  type RunnerToken,
} from "@/gen/openetl/api/v1/access_pb";
import { AuditEntrySchema, type AuditEntry } from "@/gen/openetl/api/v1/audit_pb";
import { DbtProjectSchema, type DbtProject } from "@/gen/openetl/api/v1/dbt_pb";
import { FlinkClusterSchema, type FlinkCluster } from "@/gen/openetl/api/v1/flink_pb";
import { SparkClusterSchema, type SparkCluster } from "@/gen/openetl/api/v1/spark_pb";
import {
  HealActionKind,
  HealActionState,
  IncidentClass,
  IncidentSchema,
  IncidentSeverity,
  IncidentState,
  type Incident,
} from "@/gen/openetl/api/v1/incident_pb";
import { SecretSchema, type Secret } from "@/gen/openetl/api/v1/secret_pb";
import {
  DeliveryState,
  WebhookDeliverySchema,
  WebhookSchema,
  WebhookState,
  type Webhook,
  type WebhookDelivery,
} from "@/gen/openetl/api/v1/webhook_pb";
import { syncs } from "./fixtures";

const ws = "workspaces/default";
const now = Date.now();
const min = 60_000;
const ago = (m: number) => timestampFromMs(Math.round(now - m * min));
const latestSync = (p: string) => syncs.get(`${ws}/pipelines/${p}`)?.[0]?.name ?? "";

export const incidents: Incident[] = [
  create(IncidentSchema, {
    name: `${ws}/pipelines/billing-to-warehouse/incidents/7f2c`,
    class: IncidentClass.SYNC_FAILED,
    code: "WRITE_FAILED",
    state: IncidentState.OPEN,
    severity: IncidentSeverity.CRITICAL,
    summary: "Syncs fail to write to warehouse",
    message: 'destination warehouse: copy into staging "_openetl_stage_invoices": ERROR: permission denied for schema analytics (SQLSTATE 42501)',
    nextSteps: "Grant the destination's user USAGE and CREATE on schema analytics, then start a sync.",
    occurrences: 4,
    sync: latestSync("billing-to-warehouse"),
    firstSeenTime: ago(4 * 60 + 10),
    lastSeenTime: ago(52),
    actions: [{ kind: HealActionKind.PAUSE, state: HealActionState.PROPOSED, description: "Pause the pipeline until the grant is fixed", time: ago(52) }],
  }),
  create(IncidentSchema, {
    name: `${ws}/pipelines/products-to-warehouse/incidents/91ab`,
    class: IncidentClass.AUTH,
    code: "AUTH_INVALID",
    state: IncidentState.OPEN,
    severity: IncidentSeverity.CRITICAL,
    summary: "The source rejects its credentials",
    message: 'source inventory-db: password authentication failed for user "openetl"',
    nextSteps: "Update the secret inventory-db-password, then start a sync.",
    occurrences: 1,
    sync: latestSync("products-to-warehouse"),
    firstSeenTime: ago(70),
    lastSeenTime: ago(70),
  }),
  create(IncidentSchema, {
    name: `${ws}/pipelines/products-to-warehouse/incidents/91ac`,
    class: IncidentClass.SCHEMA_BLOCKED,
    code: "SCHEMA_INCOMPATIBLE",
    stream: "public.products",
    state: IncidentState.OPEN,
    severity: IncidentSeverity.WARNING,
    summary: "public.products.price changed from numeric(10,2) to text",
    message: "The schema policy blocks a column type that narrows.",
    nextSteps: "Change the column back, or allow the change in the pipeline's schema policy.",
    occurrences: 2,
    firstSeenTime: ago(3 * 60),
    lastSeenTime: ago(2 * 60),
  }),
  create(IncidentSchema, {
    name: `${ws}/pipelines/events-to-lake/incidents/3d10`,
    class: IncidentClass.SCHEMA_BLOCKED,
    code: "QUARANTINED",
    stream: "orders.v1",
    state: IncidentState.OPEN,
    severity: IncidentSeverity.WARNING,
    summary: "61 rows in orders.v1 quarantined",
    message: "amount: value 1.0E21 out of range for decimal(18,2)",
    nextSteps: "Widen the column, then replay the quarantined rows.",
    occurrences: 1,
    sync: latestSync("events-to-lake"),
    firstSeenTime: ago(9),
    lastSeenTime: ago(9),
  }),
  create(IncidentSchema, {
    name: `${ws}/pipelines/orders-to-warehouse/incidents/1a02`,
    class: IncidentClass.RATE_LIMITED,
    code: "TRANSIENT",
    state: IncidentState.RESOLVED,
    severity: IncidentSeverity.WARNING,
    summary: "warehouse refused connections for 3 syncs",
    message: "too many connections for role openetl",
    occurrences: 3,
    firstSeenTime: ago(26 * 60),
    lastSeenTime: ago(25 * 60),
    resolveTime: ago(24 * 60),
    resolution: "A sync succeeded.",
  }),
  create(IncidentSchema, {
    name: `${ws}/pipelines/orders-cdc/incidents/0c9e`,
    class: IncidentClass.POSITION_LOST,
    code: "POSITION_LOST",
    state: IncidentState.RESOLVED,
    severity: IncidentSeverity.CRITICAL,
    summary: "The replication slot fell behind its retention",
    occurrences: 1,
    firstSeenTime: ago(4 * 24 * 60),
    lastSeenTime: ago(4 * 24 * 60),
    resolveTime: ago(4 * 24 * 60 - 40),
    resolution: "Resolved by hand by priya@example.com.",
  }),
];

export const webhooks: Webhook[] = [
  create(WebhookSchema, {
    name: `${ws}/webhooks/pagerduty`,
    spec: { url: "https://events.pagerduty.com/integration/2b9f/enqueue", events: ["incident.*"], secrets: ["pagerduty-signing"] },
    state: WebhookState.ENABLED,
    createTime: ago(30 * 24 * 60),
    updateTime: ago(30 * 24 * 60),
  }),
  create(WebhookSchema, {
    name: `${ws}/webhooks/slack-data-team`,
    spec: { url: "https://hooks.example.com/services/T02/B07/data-team", events: ["sync.failed", "pipeline.lagging", "schema.*"], pipelines: [`${ws}/pipelines/orders-cdc`, `${ws}/pipelines/events-to-lake`], secrets: ["slack-signing"] },
    state: WebhookState.ENABLED,
    failingSince: ago(35),
    createTime: ago(12 * 24 * 60),
    updateTime: ago(2 * 24 * 60),
  }),
  create(WebhookSchema, {
    name: `${ws}/webhooks/legacy-ingest`,
    spec: { url: "https://ingest.old.example.com/openetl", events: ["*"], secrets: ["legacy-signing"] },
    state: WebhookState.DISABLED,
    disabledReason: "The endpoint answered 410 Gone.",
    createTime: ago(90 * 24 * 60),
    updateTime: ago(6 * 24 * 60),
  }),
];

const events = ["sync.succeeded", "sync.failed", "incident.opened", "pipeline.lagging", "schema.changed"];
export const deliveries = new Map<string, WebhookDelivery[]>(
  webhooks.map((w, wi) => [
    w.name,
    Array.from({ length: 24 }, (_, i) => {
      const failing = wi === 1 && i < 4;
      const state = failing ? (i === 0 ? DeliveryState.PENDING : DeliveryState.FAILED) : DeliveryState.SUCCEEDED;
      return create(WebhookDeliverySchema, {
        name: `${w.name}/deliveries/${(0x9a00 + i * 31).toString(16)}`,
        eventId: `evt_${(0x51f3 + i * 97).toString(16)}`,
        eventType: events[(i + wi) % events.length],
        state,
        attempts: failing ? 3 + (i % 3) : 1,
        responseStatus: failing ? (i % 2 ? 503 : 0) : 200,
        error: failing ? (i % 2 ? "503 Service Unavailable" : "dial tcp: lookup hooks.example.com: no such host") : "",
        latency: { nanos: (80 + ((i * 37) % 300)) * 1_000_000 },
        lastAttemptTime: ago(i * 47 + 3),
        nextAttemptTime: state === DeliveryState.PENDING ? ago(-6) : undefined,
        createTime: ago(i * 47 + 5),
      });
    }),
  ]),
);

export const secrets: Secret[] = [
  ["orders-db-password", ["sources/orders-db"], 60],
  ["billing-db-password", ["sources/billing-db"], 60],
  ["inventory-db-password", ["sources/inventory-db"], 2],
  ["warehouse-password", ["destinations/warehouse"], 45],
  ["kafka-sasl", ["sources/events", "destinations/events-out"], 20],
  ["aws-lake", ["destinations/lake-glue", "destinations/raw-archive", "sources/clickstream"], 120],
  ["pagerduty-signing", ["webhooks/pagerduty"], 30],
  ["slack-signing", ["webhooks/slack-data-team"], 12],
  ["legacy-signing", [], 90],
  ["lake-spark-s3", ["sparkClusters/lake-spark"], 18],
  ["flink-kubeconfig", ["flinkClusters/flink-k8s"], 16],
  ["flink-proxy-token", ["flinkClusters/flink-session"], 9],
].map(([id, used, days]) =>
  create(SecretSchema, {
    name: `${ws}/secrets/${id}`,
    ref: id === "aws-lake" ? "aws-sm://prod/openetl/lake?region=eu-west-1" : "",
    usedBy: (used as string[]).map((u) => `${ws}/${u}`),
    createTime: ago((days as number) * 24 * 60),
    updateTime: ago((days as number) * 24 * 60),
  }),
);

export const dbtProjects: DbtProject[] = [
  create(DbtProjectSchema, {
    name: `${ws}/dbtProjects/analytics`,
    digest: "sha256:c41f7e9a2b8d3c6e5f4a1b0c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1d0e",
    usedBy: [`${ws}/pipelines/orders-to-warehouse`, `${ws}/pipelines/customers-to-warehouse`, `${ws}/pipelines/shipments-to-warehouse`],
    createTime: ago(20 * 24 * 60),
    updateTime: ago(3 * 60),
  }),
  create(DbtProjectSchema, {
    name: `${ws}/dbtProjects/finance`,
    digest: "sha256:8e2d4a6c1f3b5e7d9a0c2e4f6b8d0a1c3e5f7b9d1a3c5e7f9b1d3a5c7e9f1b3d",
    usedBy: [`${ws}/pipelines/billing-to-warehouse`],
    createTime: ago(14 * 24 * 60),
    updateTime: ago(5 * 24 * 60),
  }),
];

const secretRef = (name: string) => fromJson(ValueSchema, { secret: name });
const version = (seed: number) => `sha256:${(0x5e1d00 + seed * 7919).toString(16).padEnd(64, "a")}`;

export const sparkClusters: SparkCluster[] = [
  create(SparkClusterSchema, {
    name: `${ws}/sparkClusters/lake-spark`,
    spec: {
      target: {
        case: "submit",
        value: {
          master: "spark://spark-master.data.internal:7077",
          conf: {
            "spark.executor.memory": "8g",
            "spark.executor.cores": "4",
            "spark.hadoop.fs.s3a.access.key": "AKIA4EXAMPLE7Q2LAKE",
            "spark.hadoop.fs.s3a.secret.key": { secret: "lake-spark-s3" },
          },
        },
      },
    },
    version: version(1),
    usedBy: [`${ws}/pipelines/events-to-lake`],
    createTime: ago(18 * 24 * 60),
    updateTime: ago(26 * 60),
  }),
  create(SparkClusterSchema, {
    name: `${ws}/sparkClusters/emr-nightly`,
    spec: { target: { case: "emr", value: { clusterName: "nightly-batch", region: "eu-west-1" } } },
    version: version(2),
    usedBy: [`${ws}/pipelines/inventory-snapshot`],
    createTime: ago(30 * 24 * 60),
    updateTime: ago(6 * 24 * 60),
  }),
  create(SparkClusterSchema, {
    name: `${ws}/sparkClusters/emr-adhoc`,
    spec: { target: { case: "emr", value: { clusterId: "j-2AXXXXXXGAPLF", region: "eu-west-1" } } },
    version: version(3),
    createTime: ago(9 * 24 * 60),
    updateTime: ago(9 * 24 * 60),
  }),
];

export const flinkClusters: FlinkCluster[] = [
  create(FlinkClusterSchema, {
    name: `${ws}/flinkClusters/flink-k8s`,
    spec: {
      target: {
        case: "kubernetes",
        value: {
          namespace: "flink-jobs",
          image: "ghcr.io/acme/flink:1.20-s3",
          kubeconfig: secretRef("flink-kubeconfig"),
          jobManager: { cpu: 1, memory: "2g" },
          taskManager: { cpu: 2, memory: "4g" },
        },
      },
      conf: { "state.checkpoints.num-retained": "3", "taskmanager.numberOfTaskSlots": "2" },
    },
    version: version(4),
    usedBy: [`${ws}/pipelines/clickstream-to-lake`],
    createTime: ago(16 * 24 * 60),
    updateTime: ago(2 * 24 * 60),
  }),
  create(FlinkClusterSchema, {
    name: `${ws}/flinkClusters/flink-session`,
    spec: {
      target: {
        case: "rest",
        value: {
          url: "https://flink.data.internal",
          auth: secretRef("flink-proxy-token"),
        },
      },
    },
    version: version(5),
    usedBy: [`${ws}/pipelines/events-fanout`],
    createTime: ago(9 * 24 * 60),
    updateTime: ago(9 * 24 * 60),
  }),
];

export const principals: Principal[] = [
  create(PrincipalSchema, { name: "principals/admin", type: PrincipalType.SERVICE_ACCOUNT, displayName: "admin", roleBindings: [{ role: Role.OWNER }], createTime: ago(40 * 24 * 60) }),
  create(PrincipalSchema, { name: "principals/priya", type: PrincipalType.USER, displayName: "Priya Shah", email: "priya@example.com", roleBindings: [{ role: Role.ADMIN }], createTime: ago(38 * 24 * 60) }),
  create(PrincipalSchema, { name: "principals/alex", type: PrincipalType.USER, displayName: "Alex Morgan", email: "alex@example.com", roleBindings: [{ role: Role.OPERATOR, workspace: ws }], createTime: ago(30 * 24 * 60) }),
  create(PrincipalSchema, { name: "principals/sam", type: PrincipalType.USER, displayName: "Sam Lee", email: "sam@example.com", roleBindings: [{ role: Role.VIEWER, workspace: ws }], disabled: true, createTime: ago(25 * 24 * 60) }),
  create(PrincipalSchema, { name: "principals/ci-deploy", type: PrincipalType.SERVICE_ACCOUNT, displayName: "ci-deploy", roleBindings: [{ role: Role.EDITOR, workspace: ws }], createTime: ago(20 * 24 * 60) }),
];

export const keys = new Map<string, Key[]>([
  ["principals/admin", [create(KeySchema, { name: "principals/admin/keys/k1", prefix: "oek_7Hc2", createTime: ago(40 * 24 * 60), lastUsedTime: ago(1) })]],
  ["principals/priya", [create(KeySchema, { name: "principals/priya/keys/k2", prefix: "oek_Qm9x", createTime: ago(10 * 24 * 60), lastUsedTime: ago(2 * 60), expireTime: ago(-80 * 24 * 60) })]],
  [
    "principals/ci-deploy",
    [
      create(KeySchema, { name: "principals/ci-deploy/keys/k3", prefix: "oek_Zr4p", createTime: ago(20 * 24 * 60), lastUsedTime: ago(3 * 60) }),
      create(KeySchema, { name: "principals/ci-deploy/keys/k4", prefix: "oek_Lb1d", createTime: ago(2 * 24 * 60) }),
    ],
  ],
]);

export const dataPlanes: DataPlane[] = [create(DataPlaneSchema, { name: "dataPlanes/default", createTime: ago(40 * 24 * 60) })];

export const runnerTokens = new Map<string, RunnerToken[]>([
  [
    "dataPlanes/default",
    [
      create(RunnerTokenSchema, { name: "dataPlanes/default/tokens/t1", prefix: "oert_A8f3", createTime: ago(40 * 24 * 60), lastUsedTime: ago(0.2) }),
      create(RunnerTokenSchema, { name: "dataPlanes/default/tokens/t2", prefix: "oert_K2m7", createTime: ago(6 * 24 * 60), lastUsedTime: ago(0.5) }),
    ],
  ],
]);

const calls: [string, string, string[], string?][] = [
  ["principals/ci-deploy", "/openetl.api.v1.ApplyService/Apply", [`${ws}/pipelines/orders-to-warehouse`]],
  ["principals/alex", "/openetl.api.v1.PipelineService/SyncPipeline", [`${ws}/pipelines/billing-to-warehouse`]],
  ["principals/alex", "/openetl.api.v1.IncidentService/ResolveIncident", [`${ws}/pipelines/orders-cdc/incidents/0c9e`]],
  ["principals/priya", "/openetl.api.v1.SecretService/UpdateSecret", [`${ws}/secrets/inventory-db-password`]],
  ["principals/admin", "/openetl.api.v1.AccessService/CreateKey", ["principals/ci-deploy"]],
  ["principals/sam", "/openetl.api.v1.PipelineService/PausePipeline", [`${ws}/pipelines/payments-to-archive`], "permission_denied"],
  ["principals/priya", "/openetl.api.v1.AccessService/DisablePrincipal", ["principals/sam"]],
  ["principals/ci-deploy", "/openetl.api.v1.ApplyService/Apply", [`${ws}/pipelines/events-to-lake`, `${ws}/sources/events`]],
  ["principals/priya", "/openetl.api.v1.PipelineService/PausePipeline", [`${ws}/pipelines/payments-to-archive`]],
  ["principals/admin", "/openetl.api.v1.WebhookService/DisableWebhook", [`${ws}/webhooks/legacy-ingest`]],
  ["dataPlanes/default", "/openetl.dataplane.v1.DataPlaneService/GetSyncSpec", [`${ws}/pipelines/orders-to-warehouse`, `${ws}/secrets/orders-db-password`]],
];
export const auditEntries: AuditEntry[] = [...Array.from({ length: 40 }, (_, i) => {
  const [actor, method, resources, code] = calls[i % calls.length]!;
  return create(AuditEntrySchema, {
    name: `auditEntries/${(0x3f00 + i).toString(16)}`,
    time: ago(i * 37 + 4),
    endTime: ago(i * 37 + 4 - 0.01),
    actor,
    credential: `${actor}/keys/k${(i % 4) + 1}`,
    method,
    resources,
    peer: `10.0.${i % 4}.${20 + (i % 9)}`,
    userAgent: i % 3 ? "openetl-cli/0.1.0" : "Mozilla/5.0",
    code: code ?? "ok",
    error: code ? "principals/sam needs pipelines.pause (operator or above) on workspace default" : "",
  });
}),
  create(AuditEntrySchema, {
    name: "auditEntries/3e00",
    time: ago(40 * 24 * 60),
    actor: "openetld",
    method: "openetld bootstrap",
    resources: ["principals/admin", "principals/admin/keys/k1"],
    code: "ok",
  }),
];
