import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { durationFromMs, timestampFromMs } from "@bufbuild/protobuf/wkt";
import {
  CheckpointSchema,
  DeliveryLevel,
  HoldCause_Kind,
  HoldSchema,
  HoldWayOut_Action,
  OperatorChange,
  PipelinePlanSchema,
  PipelineVersionSchema,
  QuarantineFileSchema,
  StreamSchemaSchema,
  TransformPlanSchema,
  WriteMode,
  type Checkpoint,
  type Hold,
  type Pipeline,
  type PipelinePlan,
  type PipelineVersion,
  type QuarantineFile,
  type StreamSchema,
} from "@/gen/openetl/api/v1/pipeline_pb";
import { CheckItemSchema, CheckResultSchema, type CheckResult } from "@/gen/openetl/api/v1/connection_pb";
import { PreflightPartSchema, PreflightResultSchema, type PreflightResult } from "@/gen/openetl/api/v1/pipeline_pb";
import { LogEntrySchema, SyncState, type LogEntry, type Sync } from "@/gen/openetl/api/v1/sync_pb";
import { destinations, pipelines, syncs } from "./fixtures";
import { flinkClusters, sparkClusters } from "./workspace";

// What a pipeline's tabs and a sync's page read, made up from the pipeline
// and its syncs.

const ts = (ms: number) => timestampFromMs(Math.round(ms));
const ms = (t?: { seconds: bigint; nanos: number }) => (t ? Number(t.seconds) * 1000 + t.nanos / 1e6 : Date.now());

const find = (name: string) => pipelines.find((p) => p.name === name);

const cols = (spec: string) =>
  spec.split("; ").map((c) => {
    const [name, ...type] = c.split(" ");
    return { name: name!, type: type.join(" "), nullable: true };
  });

/** What planning a fixture's SQL gives, as a runner of its data plane would answer. */
function sqlPlan(p: Pipeline): MessageInitShape<typeof PipelinePlanSchema> {
  if (p.name.endsWith("/invoices-cleaned")) {
    return {
      problems: ["transform.sql:9:10: GROUP BY without a window: transforms don't run updating statements yet (tier 5)"],
    };
  }
  const ts = "timestamp[us, tz=UTC]";
  return {
    transform: create(TransformPlanSchema, {
      views: [{ name: "paid_orders", line: 1, column: 13, inputs: ["public.orders"], primaryKey: ["id"],
        columns: cols(`id int64; customer_id int64; amount decimal(12, 2); details extension<arrow.json>; tags list<item: utf8, nullable>; created_at ${ts}`) }],
      statements: [
        { line: 6, column: 1, output: "order_facts", class: "row_wise", inputs: ["public.orders"], views: ["paid_orders"], primaryKey: ["order_id"],
          columns: cols(`order_id int64; customer_id int64; amount decimal(12, 2); coupon utf8; created_at ${ts}`) },
        { line: 11, column: 1, output: "order_tags", class: "row_wise", inputs: ["public.orders"], views: ["paid_orders"], columns: cols("order_id int64; tag utf8") },
        { line: 15, column: 1, output: "public.customers", replaces: true, class: "row_wise", inputs: ["public.customers"], primaryKey: ["id"],
          columns: cols(`id int64; email utf8; country utf8; updated_at ${ts}`) },
        { line: 20, column: 1, output: "revenue_by_minute", class: "group_window", inputs: ["public.orders"], views: ["paid_orders"],
          primaryKey: ["window_start", "customer_id"], exchange: ["customer_id"], operator: "window-29faae33810aefa5",
          columns: cols(`window_start ${ts}; customer_id int64; revenue decimal(22, 2); orders int64`) },
      ],
      operators: [
        { id: "window-29faae33810aefa5", output: "revenue_by_minute", line: 20, column: 1, class: "group_window", input: "public.orders", eventTime: "created_at",
          window: "tumble", size: durationFromMs(60_000), keys: ["customer_id"], aggregates: ["count(*)", "sum(amount)"],
          lateness: durationFromMs(30_000), retention: durationFromMs(90_000), change: OperatorChange.KEPT },
        { id: "window-b20660707787f280", output: "revenue_by_hour", change: OperatorChange.DROPPED },
      ],
      passedThrough: ["public.orders"],
      withheld: ["public.customers"],
    }),
    streams: ["public.orders", "order_facts", "order_tags", "public.customers"].map((s) => ({
      name: s,
      level: DeliveryLevel.AT_LEAST_ONCE,
      write: WriteMode.APPEND,
      reason: s === "public.orders" ? "superset replay, and the destination can't dedupe" : "a transform's output over superset inputs, which isn't deduped unless written with merge",
    })),
    notes: [
      "transform.sql:15:1: public.customers is a changelog: a row that an update brings into the WHERE without sending a column an output column needs, as Postgres leaves out a large value that didn't change, lands without that column's current value",
    ],
  };
}

export function plan(name: string): PipelinePlan | undefined {
  const p = find(name);
  if (!p) return undefined;
  if (p.spec?.transform?.engine.case === "sql") return create(PipelinePlanSchema, { pipeline: name, version: p.version, ...sqlPlan(p) });
  const dst = p.spec?.destination?.ref ?? "";
  if (!destinations.find((d) => d.name.endsWith(`/destinations/${dst}`))?.connector) {
    return create(PipelinePlanSchema, {
      pipeline: name,
      version: p.version,
      notes: [`the control plane doesn't know destination "${dst}"'s capabilities, so only the runner checks delivery, before each sync`],
    });
  }
  const merge = dst === "warehouse";
  const kafka = p.spec?.source?.ref === "events" || p.spec?.destination?.ref === "events-out";
  return create(PipelinePlanSchema, {
    pipeline: name,
    version: p.version,
    streams: (p.spec?.source?.streams ?? []).map((s) => ({
      name: s.name,
      level: kafka ? DeliveryLevel.AT_LEAST_ONCE : merge ? DeliveryLevel.EFFECTIVELY_ONCE : DeliveryLevel.EXACTLY_ONCE,
      write: merge ? WriteMode.MERGE : WriteMode.APPEND,
      reason: kafka
        ? "the destination acknowledges records but can't dedupe them"
        : merge
          ? "merge on the primary key makes a repeated record land once"
          : "the destination commits each epoch with its checkpoint",
    })),
    notes: p.spec?.destination?.ref === "raw-archive" ? ["each sync writes a new set of Parquet files under the run's prefix"] : [],
  });
}

const columnsFor = (stream: string) => [
  { name: "id", type: "int64", nullable: false, source: "" },
  { name: stream.split(".").pop()!.replace(/s$/, "") + "_ref", type: "utf8", nullable: true, source: "ref" },
  { name: "amount", type: "decimal128(18, 2)", nullable: true, source: "" },
  { name: "updated_at", type: "timestamp[us, tz=UTC]", nullable: false, source: "" },
  { name: "_openetl_id", type: "extension<arrow.uuid>", nullable: false, source: "" },
];

export function schemas(name: string): StreamSchema[] {
  const p = find(name);
  if (!p) return [];
  return (p.spec?.source?.streams ?? []).flatMap((s, i) => {
    const v1 = columnsFor(s.name);
    const table = `${p.spec?.destination?.ref === "warehouse" ? "analytics" : "raw"}.${s.name.split(".").pop()!.replace(/\./g, "_")}`;
    const first = create(StreamSchemaSchema, { stream: s.name, version: 1, table, columns: v1, generation: 1n, epoch: 1n, createTime: ts(Date.now() - 30 * 86_400_000) });
    if (i > 0) return [first];
    const v2 = [...v1.slice(0, 2), { ...v1[2]!, type: "decimal128(20, 4)" }, v1[3]!, { name: "currency", type: "utf8", nullable: true, source: "" }, v1[4]!];
    return [first, create(StreamSchemaSchema, { stream: s.name, version: 2, table, columns: v2, generation: 1n, epoch: 412n, createTime: ts(Date.now() - 6 * 86_400_000) })];
  });
}

export function checkpoints(name: string): Checkpoint[] {
  const committed = (syncs.get(name) ?? []).filter((s) => s.state === SyncState.SUCCEEDED || s.lastCommitTime);
  // audit-log-archive's barrier was forced at epoch 894, and the next attempt restored from it.
  const restored = name.endsWith("/pipelines/audit-log-archive");
  const gen = (i: number) => (restored && i <= 5 ? 2n : 1n);
  return committed.slice(0, 30).map((s, i) =>
    create(CheckpointSchema, {
      generation: gen(i),
      epoch: BigInt(900 - i),
      previousGeneration: gen(i + 1),
      previousEpoch: BigInt(899 - i),
      sync: s.name,
      commitTime: s.lastCommitTime ?? s.endTime,
      streams: s.streams.map((st, j) => {
        const reread = restored && j === 0 && (i === 4 || i === 5);
        const forced = restored && j === 0 && (i === 5 || i === 6);
        return {
          name: st.name,
          rows: st.rows,
          quarantinedRows: st.quarantinedRows,
          schemaVersion: j === 0 ? 2 : 1,
          complete: !s.continuous && !forced,
          forced,
          level: reread ? DeliveryLevel.AT_LEAST_ONCE : DeliveryLevel.UNSPECIFIED,
          plannedLevel: reread ? DeliveryLevel.EXACTLY_ONCE : DeliveryLevel.UNSPECIFIED,
        };
      }),
    }),
  );
}

export function holds(name: string): Hold[] {
  if (!name.endsWith("/pipelines/events-to-lake")) return [];
  const fix = "ALTER TABLE lake.orders_v1 ALTER COLUMN amount TYPE bigint";
  return [
    create(HoldSchema, {
      stream: "orders.v1",
      column: "amount",
      whole: true,
      reason:
        "amount changed from int32 to int64 (int to long in the destination), and the destination refused it: it doesn't change a column's type.",
      sinceGeneration: 1n,
      sinceEpoch: 812n,
      startTime: ts(Date.now() - 29 * 3_600_000),
      cause: {
        kind: HoldCause_Kind.REFUSED,
        sourceColumn: "amount",
        from: "int32",
        to: "int64",
        fromType: "int",
        toType: "long",
        refusal: "it doesn't change a column's type",
        fix,
      },
      refused: { reason: "the destination refused it again" },
      waysOut: [
        {
          action: HoldWayOut_Action.RELEASE,
          first: "Run this on the destination, so it takes the change.",
          statement: fix,
          effect: "Patches its held values in, and delivers the column again from the next run.",
        },
        {
          action: HoldWayOut_Action.RELEASE_NEW_VERSION,
          effect: "Sends its held values, and those after, to a new column, amount__v2, and leaves amount as the destination has it.",
        },
        { action: HoldWayOut_Action.DISCARD, effect: "Drops its held values; the column stays as the destination has it." },
      ],
    }),
    create(HoldSchema, {
      stream: "payments.v1",
      column: "fee",
      cells: 7n,
      reason: `7 values of fee don't fit its type (range: column "fee" is out of range for int16).`,
      sinceGeneration: 1n,
      sinceEpoch: 840n,
      startTime: ts(Date.now() - 3 * 3_600_000),
      cause: { kind: HoldCause_Kind.VALUE, value: `range: column "fee" is out of range for int16` },
      pending: true,
    }),
  ];
}

/** The clusters of a kind that a spec uses, by the version of their settings. */
function pinned(spec: Pipeline["spec"], clusters: { name: string; version: string }[], kind: "spark" | "flink") {
  const used = new Set<string>();
  if (spec?.transform?.engine.case === kind) used.add(spec.transform.engine.value.cluster);
  if (kind === "spark") for (const s of spec?.postSync ?? []) if (s.step.case === "spark") used.add(s.step.value.cluster);
  return Object.fromEntries(clusters.filter((c) => used.has(c.name.split("/").pop()!)).map((c) => [c.name.split("/").pop()!, c.version]));
}

export function versions(p: Pipeline): PipelineVersion[] {
  return [0, 1, 2].map((i) =>
    create(PipelineVersionSchema, {
      name: `${p.name}/versions/${i === 0 ? p.version : `sha256:${(0x7a3c19 + i * 4099).toString(16).padEnd(64, "0")}`}`,
      spec: p.spec,
      sparkClusters: pinned(p.spec, sparkClusters, "spark"),
      flinkClusters: pinned(p.spec, flinkClusters, "flink"),
      dataPlane: "default",
      provenance: i === 0 ? "git: acme/data-pipelines@4e1a9c2 by ci-deploy" : `openetl apply by priya`,
      createTime: ts(Date.now() - (2 + i * 9) * 86_400_000),
      current: i === 0,
    }),
  );
}

export function log(s: Sync): LogEntry[] {
  const start = ms(s.startTime);
  const end = s.endTime ? ms(s.endTime) : Date.now();
  const lines: [string, string, Record<string, string | number>][] = [
    ["INFO", "attempt started", { component: "runner", data_plane: s.dataPlane }],
    ["INFO", "engine ready", { component: "runner", engine: "0.1.0" }],
    ["INFO", "connected", { component: "source", host: "orders-db.internal" }],
    ...s.streams.map((st): [string, string, Record<string, string | number>] => ["INFO", "stream read", { component: "source", stream: st.name, rows: Number(st.rows) }]),
    ["DEBUG", "epoch staged", { component: "engine", epoch: 900 }],
  ];
  if (s.state === SyncState.FAILED) {
    lines.push(["WARN", "write failed, retrying", { component: "destination", attempt: 1 }]);
    lines.push(["ERROR", s.error, { component: "destination" }]);
  } else if (s.state !== SyncState.RUNNING) {
    if (s.streams.some((st) => st.quarantinedRows > 0n)) lines.push(["WARN", "rows quarantined", { component: "engine", stream: s.streams[0]!.name, rows: Number(s.streams[0]!.quarantinedRows) }]);
    lines.push(["INFO", "epoch committed", { component: "destination", epoch: 900 }]);
    lines.push(["INFO", "attempt finished", { component: "runner" }]);
  }
  return lines.map(([level, message, attrs], i) =>
    create(LogEntrySchema, { time: ts(start + ((end - start) * i) / lines.length), level, message, attempt: 1, attrs }),
  );
}

// Where the data plane keeps a pipeline's quarantine files: its store, then
// the org's and the pipeline's IDs.
const quarantineDir = (p: string) => `s3://acme-openetl/checkpoints/0192f3a4-0000-7000-8000-000000000001/${p.split("/").pop()}/_quarantine`;

/** The quarantine files of a pipeline's committed epochs, newest first, made up from its checkpoints' quarantined rows. */
export function quarantineFiles(parent: string, stream?: string): QuarantineFile[] {
  return checkpoints(parent).flatMap((c) =>
    c.streams
      .filter((st) => st.quarantinedRows > 0n && (!stream || st.name === stream))
      .flatMap((st) => {
        const n = st.quarantinedRows;
        const file = (path: string, rows: bigint, causes: { code: string; column?: string; notHeld?: string; rows: bigint }[]) =>
          create(QuarantineFileSchema, {
            stream: st.name,
            path,
            url: `${quarantineDir(parent)}/${path}`,
            rows,
            causes,
            generation: c.generation,
            epoch: c.epoch,
            sync: c.sync,
            commitTime: c.commitTime,
          });
        const at = `${c.generation}/${c.epoch}/${st.name}`;
        if (n < 20n) return [file(`${at}.jsonl`, n, [{ code: "type", column: "amount", rows: n }])];
        // Two lanes' files: values that don't fit, and records that aren't objects. The stream holds cells, so each line says
        // why its row wasn't held.
        return [
          file(`${at}.jsonl`, n - 3n, [
            { code: "type", column: "amount", rows: n - 9n },
            { code: "precision", column: "amount", rows: 4n },
            { code: "type", column: "id", notHeld: "key", rows: 2n },
          ]),
          file(`${at}.1.jsonl`, 3n, [{ code: "object", notHeld: "record", rows: 3n }]),
        ];
      }),
  );
}

/** A file's first lines, as a runner reads them; undefined for a file the pipeline doesn't have. */
export function quarantineLines(parent: string, path: string, limit: number) {
  const f = quarantineFiles(parent).find((x) => x.path === path);
  if (!f) return undefined;
  const line = (code: string, column: string, notHeld: string, reason: string, record: string) =>
    JSON.stringify({ stream: f.stream, code, ...(column && { column }), ...(notHeld && { notHeld }), reason, record });
  const lines: string[] = [];
  for (const c of f.causes) {
    for (let i = 0; i < Number(c.rows) && lines.length < 100; i++) {
      const id = 7 + lines.length;
      const [reason, record] =
        c.code === "object"
          ? [`row ${i} is not a JSON object`, `[${id}]`]
          : c.code === "precision"
            ? [`precision: column "${c.column}" doesn't fit decimal(18, 2)`, `{"id": ${id}, "amount": "1204.12345"}`]
            : c.column === "id"
              ? [`type: column "id" isn't a number`, `{"id": "${id}a", "amount": "3.25"}`]
              : [`type: column "${c.column}" isn't a number`, `{"id": ${id}, "amount": "12.5O", "currency": "EUR"}`];
      lines.push(line(c.code, c.column, c.notHeld, reason, record));
    }
  }
  const n = Math.min(limit || 20, 100);
  return { lines: lines.slice(0, n), more: Number(f.rows) > n };
}

/** A check's result: Kinesis says what it looked at, and one of its streams can't be read; the others report only a result. */
export function checkResult(connector: string): CheckResult {
  if (connector !== "kinesis") return create(CheckResultSchema, { ok: true, version: "0.2.0" });
  const item = (name: string, title: string, status: string, o: { message?: string; code?: string; fix?: string; field?: string } = {}) =>
    create(CheckItemSchema, { name, title, status, ...o });
  return create(CheckResultSchema, {
    ok: false,
    code: "PERMISSION_DENIED",
    message: "kinesis: clickstream-prod: GetRecords: AccessDeniedException",
    version: "0.2.0",
    items: [
      item("connect", "Reach Kinesis in eu-west-1", "passed", { message: "Listed 14 streams." }),
      item("stream.clickstream-prod.describe", "Describe clickstream-prod", "passed", { message: "Active, 4 open shards, 24 hours' retention, KMS encrypted." }),
      item("stream.clickstream-prod.read", "Read clickstream-prod", "failed", {
        code: "PERMISSION_DENIED",
        message: "GetRecords: AccessDeniedException",
        fix: "Allow kinesis:GetShardIterator and kinesis:GetRecords on arn:aws:kinesis:eu-west-1:*:stream/clickstream-prod.",
        field: "streams",
      }),
      item("stream.clickstream-prod.retention", "Retention of clickstream-prod", "warning", {
        message: "24 hours: a scheduled read more than a day apart loses records.",
        fix: "Raise the stream's retention, or schedule reads more often than daily.",
      }),
      item("consumer", "Enhanced fan-out consumer", "skipped", { message: "consumer.type is polling." }),
    ],
  });
}

/** A pipeline's preflight: the Kinesis source's streams fail to read and its schedule outlasts retention; other pipelines pass. */
export function preflightResult(pipeline: Pipeline): PreflightResult {
  const item = (name: string, title: string, status: string, o: { message?: string; code?: string; fix?: string; field?: string } = {}) =>
    create(CheckItemSchema, { name, title, status, ...o });
  const ws = pipeline.name.slice(0, pipeline.name.indexOf("/pipelines/"));
  const src = `${ws}/sources/${pipeline.spec?.source?.ref ?? ""}`;
  const dst = `${ws}/destinations/${pipeline.spec?.destination?.ref ?? ""}`;
  const part = (role: string, stage: string, o: Partial<{ ok: boolean; code: string; message: string; skipped: string; items: ReturnType<typeof item>[] }> = {}) =>
    create(PreflightPartSchema, { role, stage, resource: role === "source" ? src : role === "destination" ? dst : "", ok: true, ...o });
  const kinesis = pipeline.spec?.source?.ref === "clickstream";
  const parts = [
    part("source", "check", { items: kinesis ? [item("connect", "Reach Kinesis in eu-west-1", "passed", { message: "Listed 14 streams." })] : [] }),
    part("destination", "check"),
    kinesis
      ? part("source", "preflight", {
          ok: false,
          code: "PERMISSION_DENIED",
          message: "kinesis: clickstream-prod: GetRecords: AccessDeniedException",
          items: [
            item("stream.clickstream-prod.describe", "Describe clickstream-prod", "passed", { message: "Active, 4 open shards, 24 hours' retention, KMS encrypted." }),
            item("stream.clickstream-prod.read", "Read clickstream-prod", "failed", {
              code: "PERMISSION_DENIED",
              message: "GetRecords: AccessDeniedException",
              fix: "Allow kinesis:GetShardIterator and kinesis:GetRecords on arn:aws:kinesis:eu-west-1:*:stream/clickstream-prod.",
              field: "streams",
            }),
            item("stream.clickstream-prod.start", "Where clickstream-prod's first read starts", "warning", {
              message: "startTimestamp is 3 days ago, older than the stream's 24 hours' retention: the first read starts at the oldest record still held.",
              fix: "Set startTimestamp within the last 24 hours, or startPosition to earliest.",
              field: "startTimestamp",
            }),
            item("stream.clickstream-prod.consumer", "Enhanced fan-out consumer", "skipped", { message: "consumer.type is polling." }),
          ],
        })
      : part("source", "preflight"),
    part("destination", "preflight"),
    part("sync", "plan", {
      items: [item("plan.warning.1", "Stream clickstream-prod", "warning", { message: "Rows with the same content become one row within each epoch." })],
    }),
  ];
  return create(PreflightResultSchema, { ok: parts.every((p) => p.ok || p.skipped), version: pipeline.version, parts });
}
