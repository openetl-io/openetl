---
title: openetld settings
description: Every setting of openetld, as a flag and an environment variable, with its default.
---

Every setting of `openetld` is a flag and an environment variable: `--database-url` is also `OPENETL_DATABASE_URL`. A flag overrides the variable, and the variable overrides the default. Every role takes every setting and uses the ones that concern it. `openetld ROLE -h` lists them, with the defaults the environment gives.

`openetld dev` has defaults of its own: `--auth` is `none`, `--checkpoints` is `.openetl/checkpoints` and `--log-format` is `text`.

This page is generated from [`internal/platform/config/config.go`](../../internal/platform/config/config.go), where the settings are defined.

## Database and API

The server and the orchestrators keep OpenETL's records in the metadata database, which `openetld bootstrap-key` writes to as well. Runners don't connect to it: they reach the API at `--api-url`.

| Flag | Variable | Default | What it sets |
|---|---|---|---|
| `--database-url` | `OPENETL_DATABASE_URL` | `postgres://openetl:openetl@localhost:5432/openetl` | metadata database URL |
| `--listen` | `OPENETL_LISTEN` | `localhost:8080` | API listen address |
| `--api-url` | `OPENETL_API_URL` | `http://localhost:8080` | API URL, for runners |
| `--data-plane` | `OPENETL_DATA_PLANE` | `default` | data plane ID |

## Orchestration

What runs workflows, for the server and the orchestrators: DBOS, which keeps them in Postgres, or Temporal.

| Flag | Variable | Default | What it sets |
|---|---|---|---|
| `--orchestrator` | `OPENETL_ORCHESTRATOR` | `dbos` | what runs workflows: dbos, which needs nothing but Postgres, or temporal |
| `--dbos-database-url` | `OPENETL_DBOS_DATABASE_URL` |  | the database DBOS keeps workflows in, which it creates if it can; not through a transaction pooler (default: on the metadata database's server, named after it with \_dbos) |
| `--executor-id` | `OPENETL_EXECUTOR_ID` |  | which orchestrator this is, unique among them and the same across restarts, such as its pod's name; on DBOS only (default: the host name) |
| `--temporal-address` | `OPENETL_TEMPORAL_ADDRESS` | `localhost:7233` | Temporal frontend host:port; on Temporal only |
| `--temporal-namespace` | `OPENETL_TEMPORAL_NAMESPACE` | `default` | Temporal namespace; on Temporal only |

## Console and sign-in

The console the server serves, and how people sign in to it.

| Flag | Variable | Default | What it sets |
|---|---|---|---|
| `--web` | `OPENETL_WEB` |  | a directory holding the built UI (web/dist) for the API server to serve at /; empty: no UI |
| `--auth` | `OPENETL_AUTH` | `password` | how browsers sign in: password, or none, which makes every call without a credential an owner's (loopback only, unless --auth-none-allow-remote) |
| `--auth-none-allow-remote` | `OPENETL_AUTH_NONE_ALLOW_REMOTE` | `false` | let --auth none serve on an address that isn't loopback, such as a private network or behind an authenticating proxy, and answer to any Host |
| `--public-url` | `OPENETL_PUBLIC_URL` |  | the URL browsers use, which password links start with; an https one makes the session cookie Secure (default: http:// and the listen address) |
| `--session-idle-timeout` | `OPENETL_SESSION_IDLE_TIMEOUT` | `12h` | how long a UI session lasts unused, as a Go duration |
| `--session-max-age` | `OPENETL_SESSION_MAX_AGE` | `168h` | how long a UI session lasts in all, as a Go duration |
| `--sign-in-rate` | `OPENETL_SIGN_IN_RATE` | `20` | sign-ins a minute each client address may make; behind a proxy, every user shares its address |

## Secrets and keys

The key that protects secrets, and the owner key the server writes when it creates the org.

| Flag | Variable | Default | What it sets |
|---|---|---|---|
| `--secrets-key` | `OPENETL_SECRETS_KEY` | `.openetl/secrets.key` | the API server's key-encryption key for secrets: a file holding a base64-encoded 32-byte key, created if missing; or base64key://KEY, awskms://…, gcpkms://… or azurekeyvault://… |
| `--bootstrap-key-file` | `OPENETL_BOOTSTRAP_KEY_FILE` | `.openetl/admin.key` | where the server writes the owner key of the org it creates on first start, readable only by its owner; and where openetld bootstrap-key writes a new one |

## Events and the audit log

When a continuous pipeline counts as lagging, and how long the audit log keeps entries.

| Flag | Variable | Default | What it sets |
|---|---|---|---|
| `--lagging-after` | `OPENETL_LAGGING_AFTER` | `5m` | how far behind its source a continuous pipeline's read gets before the server sends pipeline.lagging to webhooks |
| `--audit-retention-days` | `OPENETL_AUDIT_RETENTION_DAYS` | `90` | days the audit log keeps entries at least: a month's entries go once the month ended that long ago; 0 keeps every entry |

## Network and registries

What connectors, registries and webhooks may reach, and where connector packages come from.

| Flag | Variable | Default | What it sets |
|---|---|---|---|
| `--egress-allow` | `OPENETL_EGRESS_ALLOW` |  | comma-separated CIDR prefixes connectors, registries and webhooks may reach besides public addresses, such as a hybrid data plane's own networks |
| `--official-connectors` | `OPENETL_OFFICIAL_CONNECTORS` | `oci://ghcr.io/openetl-io/openetl-connectors` | the official connectors' registry namespace, connected as a source of the catalog when the org is created; empty: none |
| `--registry-plain-http` | `OPENETL_REGISTRY_PLAIN_HTTP` |  | comma-separated registry hosts, host:port, reached over plain HTTP besides loopback ones |
| `--github-api` | `OPENETL_GITHUB_API` |  | GitHub's REST API, for connector sources and releases (default: https://api.github.com) |

## Runners

Where a runner keeps checkpoints and its cache, the programs it runs, the pool it serves, the room it has, and its token.

| Flag | Variable | Default | What it sets |
|---|---|---|---|
| `--checkpoints` | `OPENETL_CHECKPOINTS` |  | checkpoint store, which a runner needs (openetld dev: .openetl/checkpoints): a directory, which every runner and any Spark or Flink cluster must see at the same path, so a container needs it mounted, s3://bucket (with region=… to name the bucket's region rather than ask S3, endpoint=… for another S3-compatible store, and anonymous=true for unsigned requests), gs://bucket, or azblob://container?account=…&endpoint=…; a path after the bucket puts everything under it; the kubernetes launcher needs a bucket |
| `--engine` | `OPENETL_ENGINE` |  | openetl-engine executable (default: next to openetld, then on PATH) |
| `--connectors` | `OPENETL_CONNECTORS` |  | directory of external connector executables, named source-NAME and destination-NAME (default: connectors/ next to openetld) |
| `--cache` | `OPENETL_CACHE` | `.openetl/cache` | where runners cache artifacts, such as connector versions and dbt projects, and connectors' executables |
| `--dbt` | `OPENETL_DBT` |  | dbt executable for pipelines' post-sync steps, with the destinations' adapters installed (default: dbt on PATH) |
| `--spark-submit` | `OPENETL_SPARK_SUBMIT` |  | Spark 3.5's spark-submit, for pipelines' Spark steps on self-managed clusters (default: spark-submit on PATH) |
| `--bridge-listen` | `OPENETL_BRIDGE_LISTEN` |  | where the runner serves the bridge to pipelines' Flink jobs, such as :7710; empty: the runner doesn't run Flink pipelines |
| `--bridge-advertise` | `OPENETL_BRIDGE_ADVERTISE` |  | the host:port Flink jobs reach this runner's bridge at, which must reach this runner and not a load balancer (default: the listen address) |
| `--pool` | `OPENETL_POOL` | `default` | the pool this runner serves |
| `--capacity` | `OPENETL_CAPACITY` |  | what this runner places on itself, as CPU,MEMORY such as 4,8Gi (default: its cgroup's or host's, less --reserve) |
| `--reserve` | `OPENETL_RESERVE` | `0.5,512Mi` | what this runner keeps for itself, as CPU,MEMORY |
| `--cgroup-root` | `OPENETL_CGROUP_ROOT` |  | a cgroup v2 directory delegated to this runner alone, for a cgroup per job on Linux 5.7 or later, which holds a job to its size's CPU and memory; empty: its threads and a memory watchdog do |
| `--report-interval` | `OPENETL_REPORT_INTERVAL` | `10s` | how often this runner reports what it runs to the API, as a Go duration; keep it well under its work's heartbeat timeouts |
| `--token-file` | `OPENETL_RUNNER_TOKEN_FILE` |  | a file holding the runner's token, which openetl dataplane token create makes (default: the OPENETL\_RUNNER\_TOKEN variable) |
|  | `OPENETL_RUNNER_TOKEN` |  | the runner's token, when --token-file names no file; it has no flag, so the token isn't in a process listing |

## Pod launchers

A runner with `--launcher` set is a pod launcher: it makes a pod, or a process, for each attempt placed on it, instead of running the attempt itself.

| Flag | Variable | Default | What it sets |
|---|---|---|---|
| `--launcher` | `OPENETL_LAUNCHER` |  | makes this runner a pod launcher for its pool, which runs light operations itself and makes a pod for each attempt placed on it: kubernetes, or process, which runs each in a process of its own, for development and tests |
| `--pod-min-size` | `OPENETL_POD_MIN_SIZE` | `m` | a launcher's pool sends jobs this size or larger in CPU or memory to pods, and those no runner of the pool could take: xs to xl, or CPU,MEMORY such as 0,0 for every job |
| `--pod-max-size` | `OPENETL_POD_MAX_SIZE` |  | the largest pod a launcher makes, as a preset or CPU,MEMORY (default: any size) |
| `--max-pods` | `OPENETL_MAX_PODS` | `20` | how many pods a launcher has at once; 0: no limit |
| `--max-pod-cpu` | `OPENETL_MAX_POD_CPU` | `64` | the vCPUs a launcher's pods may have between them; 0: no limit |
| `--pod-template` | `OPENETL_POD_TEMPLATE` |  | the pod the kubernetes launcher makes each attempt's from, in a YAML file or as podtemplate/NAME, a PodTemplate in the pods' namespace: its container runs openetld's image with the API's URL, the runner token and the checkpoint store, and the launcher sets the rest |
| `--pod-namespace` | `OPENETL_POD_NAMESPACE` |  | the namespace the kubernetes launcher makes pods in (default: the kubeconfig context's, or the launcher's own) |
| `--kubeconfig` | `OPENETL_KUBECONFIG` |  | a kubeconfig file for the kubernetes launcher (default: its service account; it never reads one it isn't given) |
| `--pod-stream-spot` | `OPENETL_POD_STREAM_SPOT` | `prefer` | whether the kubernetes launcher's stream pods keep off spot nodes: prefer, so they run on spot only where there's nothing else, or avoid, so they never do |
| `--pod-scratch` | `OPENETL_POD_SCRATCH` | `10Gi` | each pod's ephemeral storage, for its runner's scratch; 0: none set |
| `--attempt` | `OPENETL_ATTEMPT` |  | the one attempt this runner runs before it stops, as a pod launcher's pod; set by the launcher |
| `--placement` | `OPENETL_PLACEMENT` | `0` | which placement of --attempt this pod is for; set by the launcher |

## Logs

What each role logs, and where.

| Flag | Variable | Default | What it sets |
|---|---|---|---|
| `--log-level` | `OPENETL_LOG_LEVEL` | `info` | debug, info, warn or error |
| `--log-format` | `OPENETL_LOG_FORMAT` | `json` | json or text |
| `--log-output` | `OPENETL_LOG_OUTPUT` | `stderr` | stderr, stdout, or a file, rotated by size |
| `--log-max-size` | `OPENETL_LOG_MAX_SIZE` | `100` | megabytes a log file reaches before it's rotated |
| `--log-max-files` | `OPENETL_LOG_MAX_FILES` | `5` | rotated log files kept |

## Traces and metrics

Traces and metrics are set with OpenTelemetry's standard variables rather than flags. A signal is exported only once its exporter (`OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`) or an OTLP endpoint (`OTEL_EXPORTER_OTLP_ENDPOINT`, or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`) is set, and `OTEL_SDK_DISABLED=true` turns both off. `OTEL_METRICS_EXPORTER=prometheus` serves a Prometheus endpoint instead, on `OTEL_EXPORTER_PROMETHEUS_HOST` and `OTEL_EXPORTER_PROMETHEUS_PORT`, `localhost` and `9464` unless they're set.

Each process reports as the service `openetld`, unless `OTEL_SERVICE_NAME` names another, with its role and data plane as `openetl.role` and `openetl.data_plane`, and `OTEL_RESOURCE_ATTRIBUTES` adds to them.
