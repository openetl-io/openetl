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
import { CheckItemSchema, CheckResultSchema, type CheckItem, type CheckResult } from "@/gen/openetl/api/v1/connection_pb";
import { PreflightPartSchema, PreflightResultSchema, type PreflightResult } from "@/gen/openetl/api/v1/pipeline_pb";
import { LogEntrySchema, SyncState, type LogEntry, type Sync } from "@/gen/openetl/api/v1/sync_pb";
import { itemError, itemFailed } from "@/lib/checks";
import { destinations, pipelines, sources, syncs } from "./fixtures";
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
    warnings: p.name.endsWith("/orders-to-warehouse")
      ? [{ stream: "public.order_items", message: "each attempt reads it on one of its 4 workers: it has no cursor to split its reads by" }]
      : [],
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
      operators: name.endsWith("/pipelines/shop-to-lake") ? windowStats(i, s.lastCommitTime ?? s.endTime) : [],
    }),
  );
}

/** shop-to-lake's group window at its i-th newest checkpoint, committed at commit: a sync's read ends 3 minutes behind it. */
function windowStats(i: number, commit?: { seconds: bigint; nanos: number }) {
  const rows = BigInt(18_000 - i * 211);
  return [
    {
      operator: "window-29faae33810aefa5",
      rowsIn: rows,
      inserts: 58n,
      updates: i === 0 ? 3n : 0n,
      lateRows: i === 0 ? 2n : 0n,
      windows: BigInt(412 + i),
      watermark: ts(ms(commit) - 3 * 60_000),
      memoryBytes: 6_815_744n,
      storedBytes: 48_234_496n,
      checkpointBytes: 18_432n,
      barrierRequests: { count: 4n, bytes: 1_310_720n },
      readRequests: { count: 37n, bytes: 151_552n },
      restoreRequests: { count: i === 0 ? 28n : 0n, bytes: i === 0 ? 57_344n : 0n },
      mergeWriteRequests: { count: 1n, bytes: 262_144n },
      mergeReadRequests: { count: 3n, bytes: 393_216n },
      diskReads: { count: 212n, bytes: 868_352n },
    },
  ];
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

type ItemInit = Partial<Pick<CheckItem, "message" | "code" | "fix" | "field" | "description" | "reason" | "detail" | "params">>;

// Items as a connector sends them: title, message, fix and description are templates that params fill.
const item = (name: string, kind: string, status: string, title: string, o: ItemInit = {}) => create(CheckItemSchema, { name, kind, status, title, ...o });

const aws = (region: string) =>
  item("aws.identity", "authentication", "passed", "Sign in to AWS in {region}", {
    message: "Signed in as {identity}.",
    description: "Calls sts:GetCallerIdentity with the configured credentials, because every other call needs them.",
    params: { region, identity: "arn:aws:sts::123456789012:assumed-role/openetl-runner/i-0a1b2c3d" },
  });

// What the runner answers for a check: its items, and a code and message only for what failed besides them.
type Answer = { items: CheckItem[]; code?: string; message?: string };

const checks: Record<string, (id: string, destination: boolean) => Answer> = {
  // Passes, with a warning.
  postgres: (id, destination) => {
    const host = `${id}.internal`;
    const params = { host, address: `${host}:5432`, database: id.replace(/-db$/, ""), user: "openetl" };
    return {
      items: [
        item("net.dns", "connection", "passed", "Resolve {host}", { message: "{host} resolves to {ips}.", params: { ...params, ips: "10.0.4.12" } }),
        item("net.connect", "connection", "passed", "Reach {address}", { params }),
        item("db.connect", "authentication", "passed", "Sign in to {database} as {user}", { message: "PostgreSQL {version}.", params: { ...params, version: "16.4" } }),
        item("pg.certificate", "connection", "warning", "Verify the certificate of {address}", {
          message: "The certificate of {address} expires on {expires}.",
          fix: "Renew the certificate.",
          code: "TRANSIENT",
          reason: "certificate_expiring",
          description: "Checks the certificate the server presents, because a sync can't connect once it has expired.",
          params: { ...params, expires: "2026-11-30" },
        }),
        ...(destination
          ? [
              item("pg.create", "permission", "passed", "Create tables in {schema}", {
                description: "Checks that the connector's user may create and alter the tables a sync writes.",
                params: { ...params, schema: "analytics" },
              }),
            ]
          : [
              item("pg.select", "permission", "passed", "Read the tables in {schema}", {
                message: "{user} can read {count} tables.",
                description: "Checks that the connector's user may SELECT from the schema's tables.",
                params: { ...params, schema: "public", count: "12" },
              }),
              item("pg.replication", "permission", "skipped", "Read the write-ahead log", { message: "the source reads with queries, not CDC" }),
            ]),
      ],
    };
  },
  // Fails, with a detail, a fix, a field and a reason.
  kafka: () => {
    const params = { brokers: "kafka-1.internal:9092, kafka-2.internal:9092", mechanism: "SCRAM-SHA-512", user: "openetl" };
    return {
      items: [
        item("net.connect", "connection", "passed", "Reach {brokers}", { params }),
        item("kafka.auth", "authentication", "failed", "Sign in to the brokers as {user}", {
          message: "The upstream rejected the credentials.",
          fix: "Check the credentials in the config and the secrets they name.",
          field: "sasl_password",
          code: "AUTH_INVALID",
          reason: "credentials_rejected",
          detail:
            "kafka: client has run out of available brokers to talk to: 2 errors occurred:\n" +
            "\t* kafka server: SASL Authentication failed: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512\n" +
            "\t* kafka server: SASL Authentication failed: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512",
          description: "Signs in with SASL {mechanism}, because the connector reads and writes as this user.",
          params,
        }),
        item("kafka.topics", "permission", "skipped", "Describe the topics", { message: "needs kafka.auth, which failed" }),
      ],
    };
  },
  kinesis: () => ({
    items: [aws("eu-west-1"), item("kinesis.list", "connection", "passed", "Reach Kinesis in {region}", { message: "Listed {count} streams.", params: { region: "eu-west-1", count: "14" } })],
  }),
  // Failed, and no item says why: the checks ran out of time.
  s3: () => ({
    code: "TRANSIENT",
    message: "the checks ran out of time: context deadline exceeded",
    items: [
      aws("eu-west-1"),
      item("s3.bucket", "resource", "skipped", "Find the bucket {bucket}", {
        message: "the checks stopped before it ran: context deadline exceeded",
        params: { bucket: "acme-raw-archive" },
      }),
      item("s3.write", "permission", "skipped", "Write to {bucket}", {
        message: "the checks stopped before it ran: context deadline exceeded",
        params: { bucket: "acme-raw-archive" },
      }),
    ],
  }),
};

/** As the API settles a result: failed with a code or a failed item, whose code and text it takes when it has no code of its own. */
function verdict(code: string, message: string, items: CheckItem[]): { ok: boolean; code: string; message: string } {
  const f = items.find(itemFailed);
  if (f && !code) return { ok: false, code: f.code || "INTERNAL", message: itemError(f) };
  return { ok: !f && !code, code, message };
}

const answer = (r: { name: string; spec?: { connector: string } }): Answer =>
  checks[r.spec?.connector ?? ""]?.(r.name.slice(r.name.lastIndexOf("/") + 1), r.name.includes("/destinations/")) ?? { items: [] };

/** A check's result: a Postgres source passes with a warning, Kafka can't sign in, S3 runs out of time; others report only a result. */
export function checkResult(r: { name: string; spec?: { connector: string } }): CheckResult {
  const a = answer(r);
  return create(CheckResultSchema, { version: "0.2.0", items: a.items, ...verdict(a.code ?? "", a.message ?? "", a.items) });
}

/** The plan's problems and warnings as the API makes them items, of the kind configuration; or one item that passed. */
function planItems(pl: PipelinePlan | undefined): CheckItem[] {
  const kind = "configuration";
  const items = [
    ...(pl?.problems ?? []).map((message, i) =>
      item(`plan.problem.${i + 1}`, kind, "failed", "The plan", { message, code: "CONFIG_INVALID", reason: "config_invalid" }),
    ),
    ...(pl?.warnings ?? []).map((w, i) =>
      w.stream
        ? item(`plan.warning.${i + 1}`, kind, "warning", "Stream {stream}", { message: w.message, params: { stream: w.stream } })
        : item(`plan.warning.${i + 1}`, kind, "warning", "The plan", { message: w.message }),
    ),
  ];
  return items.length > 0 ? items : [item("plan", kind, "passed", "The plan", { message: `${pl?.streams.length ?? 0} streams, no problems` })];
}

/**
 * A pipeline's preflight as the runner runs it and the API settles it: the Kinesis source can't read its stream; a Kafka source fails
 * its check, so its preflight and the plan are skipped, and the destination's preflight too, as a sync can't be planned; the S3
 * destination's check runs out of time, so its preflight is skipped.
 */
export function preflightResult(pipeline: Pipeline): PreflightResult {
  const ws = pipeline.name.slice(0, pipeline.name.indexOf("/pipelines/"));
  const src = `${ws}/sources/${pipeline.spec?.source?.ref ?? ""}`;
  const dst = `${ws}/destinations/${pipeline.spec?.destination?.ref ?? ""}`;
  // A part as the API settles it: ok only without a code, a failed item or a skip.
  const part = (role: string, stage: string, o: { code?: string; message?: string; skipped?: string; items?: CheckItem[] } = {}) => {
    const items = o.items ?? [];
    const v = verdict(o.code ?? "", o.message ?? "", items);
    const resource = role === "source" ? src : role === "destination" ? dst : "";
    return create(PreflightPartSchema, { role, stage, resource, skipped: o.skipped ?? "", items, ...v, ok: v.ok && !o.skipped });
  };
  const srcCheck = part("source", "check", answer(sources.find((s) => s.name === src) ?? { name: src }));
  const dstCheck = part("destination", "check", answer(destinations.find((d) => d.name === dst) ?? { name: dst }));
  const kinesis = pipeline.spec?.source?.ref === "clickstream";
  const stream = { stream: "clickstream-prod" };
  // The runner settles what a sync reads and writes only after the source's check passed; invoices-cleaned's SQL can't be settled.
  const unsettled = pipeline.name.endsWith("/invoices-cleaned")
    ? { code: "CONFIG_INVALID", message: "transform.sql:9:10: GROUP BY without a window: transforms don't run updating statements yet (tier 5)" }
    : undefined;
  const planned = srcCheck.ok && !unsettled;
  // The parts in the order they ran. The API adds the plan's problems and warnings to a plan part that passed.
  const parts = [
    srcCheck,
    dstCheck,
    !srcCheck.ok
      ? part("sync", "plan", { skipped: "the source's check failed" })
      : unsettled
        ? part("sync", "plan", unsettled)
        : part("sync", "plan", { items: planItems(plan(pipeline.name)) }),
    !srcCheck.ok
      ? part("source", "preflight", { skipped: "the source's check failed" })
      : !planned
        ? part("source", "preflight", { skipped: "a sync can't be planned" })
        : kinesis
          ? part("source", "preflight", {
              items: [
                item("stream.describe", "resource", "passed", "Describe {stream}", {
                  message: "Active, {shards} open shards, {retention} retention.",
                  params: { ...stream, shards: "4", retention: "24 hours" },
                }),
                item("stream.read", "permission", "failed", "Read {stream}", {
                  message: "{principal} has no {privilege} on {object}.",
                  fix: "Grant {privilege} on {object} to {principal}.",
                  field: "stream",
                  code: "PERMISSION_DENIED",
                  reason: "privilege_missing",
                  detail:
                    "operation error Kinesis: GetRecords, https response error StatusCode: 400, RequestID: 4c1e9f2a-7b3d-4e8f-a6c5-0d9e8f7a6b5c,\n" +
                    "AccessDeniedException: User: arn:aws:sts::123456789012:assumed-role/openetl-runner/i-0a1b2c3d is not authorized to perform: " +
                    "kinesis:GetRecords on resource: arn:aws:kinesis:eu-west-1:123456789012:stream/clickstream-prod because no identity-based " +
                    "policy allows the kinesis:GetRecords action",
                  description: "Reads a record from each shard as the connector's role, because a sync reads them the same way.",
                  params: {
                    ...stream,
                    principal: "openetl-runner",
                    privilege: "kinesis:GetRecords",
                    object: "arn:aws:kinesis:eu-west-1:123456789012:stream/clickstream-prod",
                  },
                }),
                item("stream.start", "configuration", "warning", "Where {stream}'s first read starts", {
                  message: "startTimestamp is {start}, older than the stream's {retention} retention: the first read starts at the oldest record still held.",
                  fix: "Set startTimestamp within the last {retention}, or startPosition to earliest.",
                  field: "startTimestamp",
                  reason: "start_outside_retention",
                  params: { ...stream, start: "3 days ago", retention: "24 hours" },
                }),
                item("stream.consumer", "resource", "skipped", "Register an enhanced fan-out consumer", { message: "consumer.type is {type}.", params: { type: "polling" } }),
              ],
            })
          : part("source", "preflight"),
    !dstCheck.ok
      ? part("destination", "preflight", { skipped: "the destination's check failed" })
      : planned
        ? part("destination", "preflight")
        : part("destination", "preflight", { skipped: "a sync can't be planned" }),
  ];
  return create(PreflightResultSchema, { ok: parts.every((p) => p.ok), version: pipeline.version, parts });
}
