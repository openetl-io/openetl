import { create } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import { DestinationSchema, SourceSchema, SyncMode, type Destination, type Source } from "@/gen/openetl/api/v1/connection_pb";
import { PipelineMode, PipelineSchema, type Pipeline } from "@/gen/openetl/api/v1/pipeline_pb";
import { JobSizeReason, SyncAttemptSchema, SyncSchema, SyncState, type Sync } from "@/gen/openetl/api/v1/sync_pb";

// A workspace with a pipeline in every state the UI shows, for working on the
// UI without an API (pnpm dev:mock).

const workspace = "workspaces/default";
const now = Date.now();
const min = 60_000;

const ts = (ms: number) => timestampFromMs(Math.round(ms));
const logo = (slug: string) => `https://cdn.simpleicons.org/${slug}`;

function connector(name: string, logoSlug?: string) {
  return {
    name: `connectors/openetl/${name}/versions/0.1.0`,
    version: "0.1.0",
    digest: "sha256:9f3c2a41d0b7e6c58a1f4e2d3c6b9a8170f5e4d3c2b1a09f8e7d6c5b4a392817",
    author: "OpenETL",
    logo: logoSlug ? logo(logoSlug) : "",
    releaseTag: `${name}/v0.1.0`,
    repository: "https://github.com/unsuitable001/openetl-connectors",
  };
}

const sourceDefs: [string, string, string?][] = [
  ["orders-db", "postgres", "postgresql"],
  ["billing-db", "postgres", "postgresql"],
  ["inventory-db", "postgres", "postgresql"],
  ["events", "kafka", "apachekafka"],
  ["clickstream", "kinesis"],
];
const destinationDefs: [string, string, string?][] = [
  ["warehouse", "postgres", "postgresql"],
  ["lake", "iceberg"],
  ["lake-glue", "iceberg"],
  ["raw-archive", "s3"],
  ["events-out", "kafka", "apachekafka"],
];

const configs: Record<string, (id: string) => Record<string, string | number>> = {
  postgres: (id) => ({ host: `${id}.internal`, port: 5432, database: id.replace(/-db$/, ""), user: "openetl", password: `secret://${id}-password` }),
  kafka: () => ({ brokers: "kafka-1.internal:9092,kafka-2.internal:9092", sasl_mechanism: "SCRAM-SHA-512", sasl_password: "secret://kafka-sasl" }),
  kinesis: () => ({ region: "eu-west-1", stream: "clickstream-prod", credentials: "secret://aws-lake" }),
  iceberg: (id): Record<string, string> =>
    id === "lake-glue" ? { catalog: "glue", region: "eu-west-1", warehouse: "s3://acme-lake/warehouse" } : { catalog: "rest", uri: "https://catalog.internal/iceberg", warehouse: "s3://acme-lake/rest" },
  s3: () => ({ bucket: "acme-raw-archive", region: "eu-west-1", format: "parquet", credentials: "secret://aws-lake" }),
};
const updated = (i: number) => ts(Date.now() - (3 + i * 5) * 24 * 3_600_000);

export const sources: Source[] = sourceDefs.map(([id, c, slug], i) =>
  create(SourceSchema, { name: `${workspace}/sources/${id}`, spec: { connector: c, config: configs[c]?.(id) }, connector: slug ? connector(c, slug) : undefined, updateTime: updated(i) }),
);
export const destinations: Destination[] = destinationDefs.map(([id, c, slug], i) =>
  create(DestinationSchema, { name: `${workspace}/destinations/${id}`, spec: { connector: c, config: configs[c]?.(id) }, connector: slug ? connector(c, slug) : undefined, updateTime: updated(i + 1) }),
);

type Def = {
  name: string;
  source: string;
  destination: string;
  every?: number;
  cron?: string;
  continuous?: boolean;
  outcome: "succeeded" | "running" | "retrying" | "failed" | "quarantined" | "stopped" | "never";
  paused?: boolean;
  rows: number;
  incidents?: number;
  streams: string[];
  /** How many of the latest syncs failed. */
  failing?: number;
};

const defs: Def[] = [
  { name: "orders-to-warehouse", source: "orders-db", destination: "warehouse", every: 15, outcome: "succeeded", rows: 1_284_019, streams: ["public.orders", "public.order_items", "public.customers"] },
  { name: "orders-cdc", source: "orders-db", destination: "lake", continuous: true, outcome: "running", rows: 48_211_906, streams: ["public.orders", "public.order_items"] },
  { name: "billing-to-warehouse", source: "billing-db", destination: "warehouse", every: 60, outcome: "failed", failing: 4, incidents: 1, rows: 0, streams: ["public.invoices", "public.payments"] },
  { name: "inventory-snapshot", source: "inventory-db", destination: "raw-archive", cron: "0 2 * * *", outcome: "succeeded", rows: 3_904_112, streams: ["public.stock", "public.warehouses"] },
  { name: "clickstream-to-lake", source: "clickstream", destination: "lake-glue", continuous: true, outcome: "running", rows: 912_408_337, streams: ["page_views"] },
  { name: "events-to-lake", source: "events", destination: "lake", every: 30, outcome: "quarantined", incidents: 1, rows: 2_118_440, streams: ["orders.v1", "payments.v1", "shipments.v1"] },
  { name: "customers-to-warehouse", source: "orders-db", destination: "warehouse", every: 30, outcome: "retrying", rows: 88_402, streams: ["public.customers"] },
  { name: "payments-to-archive", source: "billing-db", destination: "raw-archive", every: 360, outcome: "succeeded", paused: true, rows: 640_221, streams: ["public.payments"] },
  { name: "events-fanout", source: "events", destination: "events-out", continuous: true, outcome: "stopped", rows: 7_220_118, streams: ["orders.v1"] },
  { name: "legacy-orders", source: "orders-db", destination: "raw-archive", outcome: "never", rows: 0, streams: ["public.orders_2019"] },
  { name: "products-to-warehouse", source: "inventory-db", destination: "warehouse", every: 60, outcome: "failed", failing: 1, incidents: 2, rows: 0, streams: ["public.products", "public.categories"] },
  { name: "audit-log-archive", source: "billing-db", destination: "lake-glue", cron: "30 1 * * *", outcome: "succeeded", rows: 214_903, streams: ["audit.entries"] },
  { name: "shipments-to-warehouse", source: "inventory-db", destination: "warehouse", every: 15, outcome: "succeeded", rows: 52_887, streams: ["public.shipments", "public.carriers"] },
  { name: "refunds-to-warehouse", source: "billing-db", destination: "warehouse", every: 60, outcome: "succeeded", rows: 4_410, streams: ["public.refunds"] },
];

// The same pseudo-random history on every load.
function random(seed: string) {
  let h = 2166136261;
  for (const c of seed) h = Math.imul(h ^ c.charCodeAt(0), 16777619);
  return () => {
    h = Math.imul(h ^ (h >>> 15), 2246822507);
    h = Math.imul(h ^ (h >>> 13), 3266489909);
    return ((h ^= h >>> 16) >>> 0) / 4294967296;
  };
}

function split(total: number, streams: string[], quarantined = 0) {
  return streams.map((name, i) => {
    const rows = i === streams.length - 1 ? total - Math.floor(total / streams.length) * i : Math.floor(total / streams.length);
    return { name, rows: BigInt(Math.max(rows, 0)), quarantinedRows: BigInt(i === 0 ? quarantined : 0) };
  });
}

const failure =
  "WRITE_FAILED: destination warehouse: copy into staging \"_openetl_stage_invoices\": ERROR: permission denied for schema analytics (SQLSTATE 42501)";

const pipelineVersion = "sha256:4e1a9c27b8d03f6e5a2c1b9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1d0e9f";

const presets: Record<string, [bigint, bigint]> = { s: [1000n, 2n << 30n], m: [2000n, 4n << 30n] };

function attempt(n: number, preset: string, why: JobSizeReason, seconds: number, gib: number, code = "") {
  const [cpuMillis, memoryBytes] = presets[preset]!;
  return create(SyncAttemptSchema, {
    attempt: n,
    size: { preset, cpuMillis, memoryBytes },
    sizeReason: why,
    usage: { cpuSeconds: seconds * 0.6, peakMemoryBytes: BigInt(Math.round(gib * 2 ** 30)), runnerCpuSeconds: seconds * 0.05, runnerMemoryBytes: 80n << 20n, seconds },
    code,
  });
}

function history(d: Def): Sync[] {
  const parent = `${workspace}/pipelines/${d.name}`;
  const rnd = random(d.name);
  const syncName = (i: number) => `${parent}/syncs/${(0x1f3a00 + i * 7919).toString(16)}`;
  if (d.outcome === "never") return [];
  if (d.continuous) {
    const start = now - (2 * 24 * 60 + rnd() * 600) * min;
    const current = create(SyncSchema, {
      name: syncName(0),
      version: pipelineVersion,
      state: d.outcome === "stopped" ? SyncState.STOPPED : SyncState.RUNNING,
      continuous: true,
      startTime: ts(start),
      endTime: d.outcome === "stopped" ? ts(now - 3 * min) : undefined,
      lastCommitTime: ts(d.outcome === "stopped" ? now - 3 * min : now - 4_000),
      streams: split(d.rows, d.streams),
      progress: [{ lag: { seconds: 2n } }],
      dataPlane: "default",
      attempts: [attempt(1, "m", JobSizeReason.HISTORY, 2 * 24 * 3600, 2.6)],
    });
    const earlier = [1, 2, 3].map((i) =>
      create(SyncSchema, {
        name: syncName(i),
        version: pipelineVersion,
        state: SyncState.STOPPED,
        continuous: true,
        startTime: ts(start - i * 3 * 24 * 60 * min),
        endTime: ts(start - (i - 1) * 3 * 24 * 60 * min - 5 * min),
        streams: split(Math.round(d.rows * (0.8 + rnd() * 0.4)), d.streams),
        dataPlane: "default",
      }),
    );
    return [current, ...earlier];
  }
  const every = (d.every ?? 24 * 60) * min;
  // The latest sync started long enough ago to have ended, unless it's running.
  const last = d.paused ? now - 3 * 24 * 60 * min : d.outcome === "running" || d.outcome === "retrying" ? now - 2 * min : now - 5 * min - rnd() * every * 0.8;
  return Array.from({ length: 40 }, (_, i) => {
    const start = last - i * every;
    const took = (40 + rnd() * 200) * 1000;
    let state = SyncState.SUCCEEDED;
    let quarantined = 0;
    if (i === 0 && (d.outcome === "running" || d.outcome === "retrying")) state = SyncState.RUNNING;
    else if (i < (d.failing ?? 0)) state = SyncState.FAILED;
    else if (i === 0 && d.outcome === "quarantined") quarantined = 61;
    else if (rnd() < 0.04) state = SyncState.FAILED;
    const rows = state === SyncState.FAILED ? 0 : Math.round(d.rows * (0.7 + rnd() * 0.6));
    return create(SyncSchema, {
      name: syncName(i),
      version: pipelineVersion,
      state,
      error: state === SyncState.FAILED ? failure : "",
      failedAttempts: state === SyncState.FAILED ? 3 : i === 0 && d.outcome === "retrying" ? 2 : 0,
      lastAttemptError: i === 0 && d.outcome === "retrying" ? "runner: open destination: INTERNAL: postgres: connect: too many connections for role \"openetl\" (SQLSTATE 53300)" : "",
      startTime: ts(start),
      endTime: state === SyncState.RUNNING ? undefined : ts(start + took),
      lastCommitTime: state === SyncState.SUCCEEDED ? ts(start + took) : undefined,
      streams: split(rows, d.streams, quarantined),
      dataPlane: "default",
      attempts:
        i === 0 && d.outcome === "retrying"
          ? [attempt(1, "s", JobSizeReason.HISTORY, 40, 1.9, "RESOURCE_EXHAUSTED"), attempt(2, "m", JobSizeReason.OUT_OF_MEMORY, 30, 2.1)]
          : [attempt(1, "s", JobSizeReason.HISTORY, took / 1000, 0.6 + rnd())],
    });
  });
}

export const syncs = new Map<string, Sync[]>(defs.map((d) => [`${workspace}/pipelines/${d.name}`, history(d)]));

export const pipelines: Pipeline[] = defs.map((d) => {
  const name = `${workspace}/pipelines/${d.name}`;
  const h = syncs.get(name) ?? [];
  const lastSuccess = h.find((s) => s.state === SyncState.SUCCEEDED);
  return create(PipelineSchema, {
    name,
    version: pipelineVersion,
    paused: d.paused ?? false,
    createTime: ts(now - 40 * 24 * 60 * min),
    updateTime: ts(now - 2 * 24 * 60 * min),
    spec: {
      mode: d.continuous ? PipelineMode.CONTINUOUS : PipelineMode.SCHEDULED,
      schedule: d.cron
        ? { trigger: { case: "cron", value: d.cron } }
        : d.every
          ? { trigger: { case: "every", value: { seconds: BigInt(d.every * 60) } }, jitter: { seconds: 120n } }
          : undefined,
      dataPlane: "default",
      source: { ref: d.source, streams: d.streams.map((s) => ({ name: s, sync: d.continuous ? SyncMode.INCREMENTAL : SyncMode.FULL_REFRESH })) },
      destination: { ref: d.destination },
    },
    status: { lastSync: h[0], lastSuccessTime: lastSuccess?.endTime, openIncidents: d.incidents ?? 0 },
  });
});
