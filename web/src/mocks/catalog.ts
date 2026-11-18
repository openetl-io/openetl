import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError } from "@connectrpc/connect";
import {
  CatalogPackageSchema,
  CatalogRoleSchema,
  ConfigFieldSchema,
  ConfigGroupSchema,
  ConnectorSourceSchema,
  type CatalogPackage,
  type CatalogRole,
  type ConnectorSource,
} from "@/gen/openetl/api/v1/connector_pb";

// The connector catalog: the official namespace, a GitHub repository of a
// third party's, a private registry that fails to list, with an openetl/
// package it shouldn't have, and a package applied by hand that no source
// lists.

const now = Date.now();
const ago = (m: number) => timestampFromMs(Math.round(now - m * 60_000));
const simpleIcon = (slug: string) => `https://cdn.simpleicons.org/${slug}`;
const svg = (body: string) => `data:image/svg+xml,${encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24">${body}</svg>`)}`;

const official = "oci://ghcr.io/openetl-io/openetl-connectors";
const acme = "https://github.com/acme/openetl-connectors";
const internal = "oci://registry.internal:5000/data/connectors";

export const connectorSources: ConnectorSource[] = [
  create(ConnectorSourceSchema, { name: "connectorSources/official", spec: { url: official }, official: true, refreshTime: ago(4), packages: 5 }),
  create(ConnectorSourceSchema, { name: "connectorSources/acme", spec: { url: acme, secret: "workspaces/default/secrets/github-token" }, refreshTime: ago(6), packages: 2 }),
  create(ConnectorSourceSchema, {
    name: "connectorSources/internal",
    spec: { url: internal, username: "ci-bot", secret: "workspaces/default/secrets/registry" },
    refreshTime: ago(2 * 24 * 60),
    error:
      'Get "https://registry.internal:5000/v2/data/connectors/index/tags/list": dial tcp 10.0.4.12:5000: connect: connection refused',
    errorTime: ago(3),
    packages: 2,
  }),
];

type Def = Partial<Omit<CatalogPackage, "$typeName">> & { package: string; source: string; version: string; roles: string[] };

const defs: Def[] = [
  {
    package: "openetl/postgres", source: "official", title: "PostgreSQL", version: "0.2.0", roles: ["destination", "source"], appliedVersion: "0.2.0",
    description: "Full, incremental and CDC reads from PostgreSQL, and merges into it with column holds.", logo: simpleIcon("postgresql"),
    tags: ["database", "cdc"], license: "Apache-2.0", docs: "https://github.com/openetl-io/openetl-connectors/tree/main/postgres",
  },
  {
    package: "openetl/kafka", source: "official", title: "Apache Kafka", version: "0.2.0", roles: ["destination", "source"], appliedVersion: "0.1.0",
    description: "Reads topics from their committed offsets and writes records with the schema registry's Avro, Protobuf or JSON.",
    logo: simpleIcon("apachekafka"), tags: ["streaming"], license: "Apache-2.0",
  },
  {
    package: "openetl/iceberg", source: "official", title: "Apache Iceberg", version: "0.2.0", roles: ["destination"],
    description: "Commits each checkpoint to Iceberg tables through a REST or Glue catalog.", tags: ["lakehouse", "parquet"], license: "Apache-2.0",
  },
  {
    package: "openetl/s3", source: "official", title: "Amazon S3", version: "0.2.0", roles: ["destination", "source"],
    description: "Reads and writes Parquet, CSV and JSON Lines files on S3 or an S3-compatible store; by default, a full read replaces a table's files when it ends.",
    tags: ["files", "parquet"], license: "Apache-2.0",
  },
  {
    package: "openetl/kinesis", source: "official", title: "Amazon Kinesis", version: "0.2.0", roles: ["destination", "source"],
    description: "Reads shards from their sequence numbers, parents first, and puts records with their IDs.", tags: ["streaming", "aws"], license: "Apache-2.0",
  },
  {
    package: "acme/zendesk", source: "acme", title: "Zendesk", version: "1.4.0", roles: ["source"], author: "Acme Data Team",
    description: "Tickets, users and organizations from the Zendesk Support API, incrementally by their cursor.",
    logo: svg('<rect width="24" height="24" rx="4" fill="#03363d"/><path d="M6 7h12l-12 10h12" stroke="#fff" stroke-width="2" fill="none"/>'),
    tags: ["saas", "support", "tickets"], license: "MIT", reference: "github.com/acme/openetl-connectors/zendesk@1.4.0",
  },
  {
    package: "acme/files", source: "acme", title: "Files", version: "0.3.0-rc.1", roles: ["destination", "source"], author: "Acme Data Team",
    description: "CSV and JSON lines files on SFTP servers.", tags: ["files"], reference: "github.com/acme/openetl-connectors/files@0.3.0-rc.1",
  },
  {
    package: "openetl/postgres", source: "internal", title: "PostgreSQL (patched)", version: "9.9.0", roles: ["source"], impostor: true,
    description: "Our fork with a faster snapshot.", author: "data-platform",
  },
  {
    package: "internal/ledger", source: "internal", title: "Ledger", version: "2.1.0", roles: ["source"], author: "data-platform",
    description: "The finance ledger's journal entries, from its gRPC API.", tags: ["finance"],
  },
];

const url = (source: string) => (source === "official" ? official : source === "internal" ? internal : acme);

export const catalogPackages: CatalogPackage[] = defs.map((d) =>
  create(CatalogPackageSchema, {
    ...d,
    name: `catalog/${d.source}/${d.package}`,
    source: `connectorSources/${d.source}`,
    official: d.source === "official" && d.package.startsWith("openetl/"),
    updateAvailable: !!d.appliedVersion && d.appliedVersion !== d.version,
    author: d.author ?? (d.source === "official" ? "OpenETL" : ""),
    repository: d.repository ?? (d.source === "official" ? "https://github.com/openetl-io/openetl-connectors" : d.source === "acme" ? acme : ""),
    reference: d.reference ?? `${url(d.source)}/${d.package.split("/")[1]}:${d.version}`,
  }),
);

catalogPackages.push(
  create(CatalogPackageSchema, {
    name: "catalog/-/finance/netsuite",
    package: "finance/netsuite",
    title: "NetSuite",
    version: "3.2.0",
    appliedVersion: "3.2.0",
    manual: true,
    roles: ["source"],
    author: "finance-eng",
    description: "Saved searches and records from NetSuite's SuiteTalk API.",
    tags: ["finance", "saas"],
  }),
);

type FieldInit = MessageInitShape<typeof ConfigFieldSchema>;
const field = (name: string, type: string, o: FieldInit = {}) => create(ConfigFieldSchema, { name, path: name, type, ...o });
const opt = (value: string, title: string, description = "") => ({ value: JSON.stringify(value), title, description });

// The Kinesis source's form, as the API reads it from the schema's x- keywords.
const kinesisGroups = [
  create(ConfigGroupSchema, { id: "connection", title: "Connection", description: "Where the streams are, and the credentials to reach them." }),
  create(ConfigGroupSchema, { id: "streams", title: "Streams", description: "Which Kinesis streams to offer, and where a first read starts." }),
  create(ConfigGroupSchema, { id: "reading", title: "Reading", description: "How shards are read." }),
];
const kinesisSource = [
  field("region", "string", { group: "connection", description: "The streams' AWS region. Unset: the AWS default config's.", examples: ['"eu-west-1"'] }),
  field("accessKeyId", "string", { group: "connection", secret: true, description: "An AWS access key. Without one, the role the runner gives." }),
  field("secretAccessKey", "string", { group: "connection", secret: true, description: "The access key's secret." }),
  field("endpoint", "string", { group: "connection", advanced: true, format: "uri", description: "A VPC endpoint, or an emulator's." }),
  field("streams", "array", { group: "streams", items: "string", description: "The streams to offer. Empty: every stream the credentials can list." }),
  field("startPosition", "string", {
    group: "streams",
    default: '"earliest"',
    description: "Where a stream's first read starts.",
    options: [opt("earliest", "Earliest", "Each shard's oldest record."), opt("latest", "Latest"), opt("timestamp", "At a time")],
  }),
  field("startTimestamp", "string", {
    group: "streams",
    format: "date-time",
    description: "The first read starts at records that arrived at or after this.",
    when: [{ field: "startPosition", values: ['"timestamp"'] }],
  }),
  field("consumer", "object", {
    group: "reading",
    description: "How shards are read.",
    fields: [
      field("type", "string", {
        path: "consumer.type",
        group: "reading",
        default: '"polling"',
        description: "Polling shares each shard's 2 MB/s; enhanced fan-out gives this pipeline its own.",
        options: [opt("polling", "Polling"), opt("efo", "Enhanced fan-out")],
      }),
      field("name", "string", { path: "consumer.name", group: "reading", description: "The consumer's name. Default: openetl- and the pipeline's.", when: [{ field: "consumer.type", values: ['"efo"'] }] }),
    ],
  }),
  field("polling", "object", {
    group: "reading",
    advanced: true,
    description: "How often each shard is polled.",
    when: [{ field: "consumer.type", values: ['"polling"'] }],
    fields: [
      field("interval", "string", { path: "polling.interval", group: "reading", format: "duration", default: '"200ms"', description: "Between GetRecords calls on a shard." }),
      field("maxRecords", "integer", { path: "polling.maxRecords", group: "reading", minimum: 1, maximum: 10000, default: "10000", description: "Records a call returns at most." }),
    ],
  }),
];

const configs: Record<string, ReturnType<typeof field>[]> = {
  postgres: [
    field("host", "string", { required: true, title: "Host" }),
    field("database", "string", { required: true }),
    field("user", "string", { required: true }),
    field("password", "string", { required: true, secret: true, description: "A secret's name; the runner reads its value." }),
    field("port", "integer", { description: "Default 5432." }),
    field("sslmode", "string", { title: "TLS", description: "disable, require, verify-ca or verify-full." }),
    field("publication", "string", { description: "For CDC: the publication to read. Default: openetl." }),
  ],
  kafka: [
    field("brokers", "array", { required: true, description: "host:port pairs." }),
    field("sasl", "object", { description: "mechanism, user and password." }),
    field("schema_registry", "object", { title: "Schema registry" }),
    field("tls", "boolean"),
  ],
  zendesk: [
    field("subdomain", "string", { required: true, description: "acme for acme.zendesk.com." }),
    field("api_token", "string", { required: true, secret: true }),
    field("email", "string", { required: true }),
    field("start_date", "string", { description: "RFC 3339. Default: everything." }),
  ],
};

const roles = (p: CatalogPackage): CatalogRole[] => {
  const short = p.package.split("/")[1] ?? "";
  const fields = configs[short] ?? [field("endpoint", "string", { required: true }), field("credentials", "string", { secret: true }), field("options", "object")];
  if (short === "kinesis") return p.roles.map((role) => create(CatalogRoleSchema, { role, config: kinesisSource, groups: kinesisGroups }));
  return p.roles.map((role) => create(CatalogRoleSchema, { role, config: fields }));
};

/** Its roles' config fields; a package of the failing source can't be fetched. */
export function catalogPackage(name: string) {
  const pkg = catalogPackages.find((p) => p.name === name);
  if (!pkg) throw new ConnectError(`${name} isn't in the catalog`, Code.NotFound);
  if (pkg.source === "connectorSources/internal") {
    throw new ConnectError(
      `${pkg.reference}'s descriptor: Get "https://registry.internal:5000/v2/data/connectors/${pkg.package.split("/")[1]}/manifests/${pkg.version}": dial tcp 10.0.4.12:5000: connect: connection refused`,
      Code.Unavailable,
    );
  }
  return { package: pkg, roles: roles(pkg) };
}
