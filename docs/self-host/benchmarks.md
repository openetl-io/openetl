---
title: Benchmarks
description: Measured times for three full copies from Postgres into Iceberg tables on AWS, with the machines, specs and steps to repeat them.
---

This page reports how long OpenETL took to copy three Postgres databases into Iceberg tables in AWS Glue and S3. Every run used the release `v0.2.0-alpha`, on AWS in `ap-southeast-2`. Each workload's section gives every run and the spread, the machines, the data and the pipeline spec. It then gives what limited the runs, how the result was checked, and what a run costs. [Repeat the runs](#repeat-the-runs) has the steps. [What these numbers do not show](#what-these-numbers-do-not-show) lists the limits of the results.

## Results

| Workload | Rows | Runner machine | Runs | Sync time | Rows/s |
|---|---:|---|---:|---|---:|
| [24 tables derived from TPC-DS](#24-tables-derived-from-tpc-ds), scale factor 10 | 191,496,628 | 8 vCPUs, 16 GiB | 3 | Mean 64.20 s, from 63.68 to 64.47 s | 2.98 million |
| [8 tables derived from TPC-H](#8-tables-derived-from-tpc-h), scale factor 10 | 86,586,082 | 2 vCPUs, 4 GiB | 3 | Mean 145.79 s, from 145.10 to 146.14 s | 594,000 |
| [8 tables derived from TPC-H](#8-tables-derived-from-tpc-h), scale factor 50 | 433,005,841 | 2 vCPUs, 4 GiB | 1 | 727.20 s | 595,000 |
| [New York City taxi trips](#new-york-city-taxi-trips), 2 tables | 318,879,363 | 8 vCPUs, 16 GiB | 3 | Mean 171.56 s, from 162.89 to 176.66 s | 1.86 million |

Rows/s is the rows over the mean sync time.

This workload is derived from the TPC-DS Benchmark and is not comparable to published TPC-DS Benchmark results, as this implementation does not comply with all requirements of the TPC-DS Benchmark.

This workload is derived from the TPC-H Benchmark and is not comparable to published TPC-H Benchmark results, as this implementation does not comply with all requirements of the TPC-H Benchmark.

## What ran

Each workload ran OpenETL's whole [Docker Compose](./compose.md) stack on one EC2 instance: its Postgres, the API server, an orchestrator and a runner with the engine and the connectors. The orchestrator is DBOS, the Compose file's default. All three workloads ran the same files:

- **Compose file and CLI.** The `compose.yml` of the release `v0.2.0-alpha`, SHA-256 `02715bb4abe7c1fbc12b3de86730334f966394d2d6012305543da8d7182f1d1c`, and its `openetl-cli_0.2.0-alpha_linux_arm64.tar.gz`, SHA-256 `27e385d3199c723a093294c7352877c9936fbbdf50cb23293cb605825ceba0ac`. Both match the release's `checksums.txt`.
- **Image.** The image that `compose.yml` names, `ghcr.io/openetl-io/openetl@sha256:c56bab36a46755781959a276e86e0c20275b69176b756046a2a19203ed1381f7`. Docker pulled its `linux/arm64` image, whose revision label is `9ce8bbc1eb24c4658a7bcbfe02cb0bcf2e2bab4f`, the commit of the tag `v0.2.0-alpha`.
- **OpenETL's own Postgres.** `postgres:18`, as `compose.yml` names it, at `postgres@sha256:64c8b0d31e4ab6ea665e8426ea406a94bf52de3c05b3a4a8bf713704eefa9328`.
- **Connectors.** The [`postgres`](https://github.com/openetl-io/openetl-connectors/blob/main/postgres/README.md) source and the [`iceberg`](https://github.com/openetl-io/openetl-connectors/blob/main/iceberg/README.md) destination at version `0.2.0-alpha`, applied from the official registry with `openetl connector apply`. Their package manifests are `sha256:35b04f9206332d59ed54729415b1ebc6d9cb8df5560752732d9422e70704a44b` (`postgres`) and `sha256:5cd3b6061736a9f39676a8dcced5c9d65de349bdc5dad738a975643bf4d58d5e` (`iceberg`). The runner downloaded their `linux/arm64` executables at the first sync. On the taxi workload's machine, these were `source-postgres` at `sha256:69a4843e82f46b89f33b37e37de1dd22e7124904e0bfaa6a4d0c2f44d4ccc4d8` and `destination-iceberg` at `sha256:9065ce1da89663bff577c78372a3ecf2483390967b6404e650691a0103e6126d`.
- **Settings.** `.env` held the two lines that the Compose page writes, and these:

  ```sh
  OPENETL_RESERVE=0,1Gi
  AWS_REGION=ap-southeast-2
  AWS_DEFAULT_REGION=ap-southeast-2
  OPENETL_CONNECTOR_AWS_CREDENTIALS=inherit
  OTEL_METRICS_EXPORTER=prometheus
  OTEL_EXPORTER_PROMETHEUS_HOST=0.0.0.0
  OTEL_EXPORTER_PROMETHEUS_PORT=9464
  ```

`OPENETL_RESERVE=0,1Gi` makes the runner keep 1 GiB of memory and no CPU for itself, so that a job can use every vCPU of the machine. The default reserve is 0.5 vCPU and 512 MiB. `OPENETL_CONNECTOR_AWS_CREDENTIALS=inherit` gives the connectors the runner's credentials, here the instance's role. [Credentials for connectors](./aws.md#credentials-for-connectors) describes this setting and the alternatives. The Prometheus exporter serves the runner's metrics, which were read once after each run.

Every source was an RDS for PostgreSQL instance in the runner's availability zone, `ap-southeast-2a`, reached with TLS (`sslmode=require`). Every destination table was an Iceberg table of format version 2, unpartitioned and unsorted, in Parquet with zstd at level 1. These are the `iceberg` connector's defaults for the tables that it creates. The catalog was AWS Glue, and the data was in an S3 general-purpose bucket in the same region. Each pipeline commits each table on its own (`delivery.commit.scope: stream`).

## How a run is timed

Each run has its own pipeline and its own Glue database. For each run:

1. Apply the pipeline with its source pointed at a seed schema, which has one row in each table. Applying the pipeline starts its first sync. This sync creates the Iceberg tables. The first time, the runner also downloads the connectors.
2. Apply the pipeline pointed at the full schema with `openetl apply --no-check`. This apply starts no sync.
3. Leave the stack idle for 120 seconds.
4. Run `openetl sync PIPELINE`. Wait until `openetl syncs PIPELINE` shows the sync as `succeeded`.

The time of a run is the sync's own record: its `endTime` minus its `startTime`. Read both from the API:

```sh
curl -s -H "Authorization: Bearer $OPENETL_API_KEY" "$OPENETL_API_URL/v1/workspaces/default/pipelines/PIPELINE/syncs?pageSize=1"
```

The time includes placing the job on the runner, starting the connectors and the engine, planning, reading, writing and the commit of every table. It does not include:

- downloading or generating the data, and loading it into Postgres;
- starting the stack, and downloading the connectors;
- the seed sync, which creates the tables;
- the 120 seconds of idle time;
- the time from `openetl sync` to the sync's `startTime`, which was under a second in every run.

The sync log's `job started` and `job finished` lines divide the time into three parts: from the start to `job started`, the job itself, and from `job finished` to the end. The tables of runs show the three parts.

During each run, scripts sampled the runner's machine every second with `vmstat`, `/proc/net/dev` and `free`, and every 5 seconds with `pidstat` by process. On the two workloads derived from the TPC benchmarks, they also read the network interface's allowance counters (`ethtool -S`) before and after each run. CloudWatch gave the RDS instance's metrics by minute. A CPU average on this page is over the 1-second samples between the sync log's `job started` and `job finished`.

The usage figures come from the sync's record. The job's CPU and peak memory are those of its engine and connector processes. The runner's share is its estimate of its own work for the job. Lane seconds are summed over the job's lanes: busy is time spent working, backpressured is time spent waiting for the writers to take rows, and idle is time spent waiting for rows.

Every timed sync succeeded at its first attempt, with no line at `WARN` or `ERROR` in its sync log or in the runner's log. Athena, which shares no code with the connectors, read each run's tables back. Its counts and sums were compared with Postgres's. A cost on this page is the on-demand list price in `ap-southeast-2`, multiplied by the time that each resource existed. The prices come from the AWS Price List API when the runs were made. A cost is not a bill.

## 24 tables derived from TPC-DS

This workload copies all 24 tables of a database derived from TPC-DS at scale factor 10: 191,496,628 rows. The runner has 8 vCPUs and 16 GiB.

This workload is derived from the TPC-DS Benchmark and is not comparable to published TPC-DS Benchmark results, as this implementation does not comply with all requirements of the TPC-DS Benchmark.

The workload differs from the benchmark that it is derived from in these ways:

- It uses only the generated data.
- It runs none of the benchmark's queries.
- What it times is a load: a copy from Postgres into Iceberg tables.
- It reports no metric of that benchmark.

### Result

The mean of three runs is 64.20 s, from 63.68 to 64.47 s. The spread, slowest minus fastest, is 0.79 s, and the standard deviation is 0.45 s. The rate is 2.98 million rows a second.

| Run | Sync time | Rows/s | Start to `job started` | Job | `job finished` to end | Attempts |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 63.680 s | 3,007,178 | 3.91 s | 59.70 s | 0.07 s | 1 |
| 2 | 64.468 s | 2,970,394 | 4.13 s | 60.06 s | 0.28 s | 1 |
| 3 | 64.458 s | 2,970,879 | 3.96 s | 60.26 s | 0.25 s | 1 |
| Mean | 64.202 s | | 4.00 s | 60.01 s | 0.20 s | |

This workload is derived from the TPC-DS Benchmark and is not comparable to published TPC-DS Benchmark results, as this implementation does not comply with all requirements of the TPC-DS Benchmark.

### Machines

| Role | Machine |
|---|---|
| Runner, with OpenETL's whole Compose stack | EC2 `c8g.2xlarge`: 8 vCPUs, which are 8 Graviton4 (Neoverse-V2) cores without SMT, and 16 GiB of memory, 15,664 MiB usable, without swap. Ubuntu 24.04.5, kernel 7.0.0-1018-aws, Docker 29.1.3, Compose 2.40.3. Root disk: 300 GB of gp3 at 16,000 IOPS and 1,000 MB/s. |
| Source | RDS for PostgreSQL 18.6, `db.r8g.2xlarge`: 8 vCPUs, 64 GiB, Single-AZ. Storage: 500 GB of gp3 at 12,000 IOPS and 500 MB/s, not encrypted. Backup retention 0, so `wal_level` is `minimal`. `shared_buffers` is 2,014,030 pages of 8 kB (15.4 GiB), and `max_connections` is 5000. No Performance Insights or Enhanced Monitoring. |

### Data

The data is AWS's public set of files derived from TPC-DS, version 2.13, at scale factor 10: `s3://redshift-downloads/TPC-DS/2.13/10GB/`, with one file for each table. The files total 12 GiB, as `du -sh` reports them. The download with `aws s3 cp --no-sign-request --recursive` took 1 minute 47 seconds.

The load used `psql` and `vacuumdb` on the runner's machine, against database `tpcds` of the RDS instance:

1. Create the 24 tables in schema `tpcds_sf10`, as the `tpcds.sql` file of the benchmark's 2.13 toolkit defines them, without their `primary key` clauses. The [TPC](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) distributes the toolkit. The 24 tables have 425 columns in all.
2. Load each file with `\copy TABLE from 'FILE' with (format csv, delimiter '|', null '', quote E'\x01', encoding 'UTF8')`, 8 tables at a time. This took 80 seconds.
3. Add each table's primary key, as the table below lists them, 6 tables at a time. Set `maintenance_work_mem` to `4GB`. This took 68 seconds.
4. Run `vacuumdb --analyze -j 8 --schema=tpcds_sf10 -d tpcds`. This took 27 seconds.
5. Make the seed schema `tpcds_sf10_seed`. For each table, run `CREATE TABLE tpcds_sf10_seed.TABLE (LIKE tpcds_sf10.TABLE INCLUDING ALL)`, and copy one row into it.

The database is 20 GiB with the primary keys (`pg_database_size`). The 24 tables are 15 GiB without indexes: the sum of `pg_table_size`, which counts the heap, TOAST, the free space map and the visibility map. `pg_size_pretty` reports both sizes. The sync's record counts 12,684,588,574 bytes that the source emitted.

| Table | Rows | Primary key |
|---|---:|---|
| `call_center` | 24 | `cc_call_center_sk` |
| `catalog_page` | 12,000 | `cp_catalog_page_sk` |
| `catalog_returns` | 1,439,749 | `cr_item_sk, cr_order_number` |
| `catalog_sales` | 14,401,261 | `cs_item_sk, cs_order_number` |
| `customer` | 500,000 | `c_customer_sk` |
| `customer_address` | 250,000 | `ca_address_sk` |
| `customer_demographics` | 1,920,800 | `cd_demo_sk` |
| `date_dim` | 73,049 | `d_date_sk` |
| `household_demographics` | 7,200 | `hd_demo_sk` |
| `income_band` | 20 | `ib_income_band_sk` |
| `inventory` | 133,110,000 | `inv_date_sk, inv_item_sk, inv_warehouse_sk` |
| `item` | 102,000 | `i_item_sk` |
| `promotion` | 500 | `p_promo_sk` |
| `reason` | 45 | `r_reason_sk` |
| `ship_mode` | 20 | `sm_ship_mode_sk` |
| `store` | 102 | `s_store_sk` |
| `store_returns` | 2,875,432 | `sr_item_sk, sr_ticket_number` |
| `store_sales` | 28,800,991 | `ss_item_sk, ss_ticket_number` |
| `time_dim` | 86,400 | `t_time_sk` |
| `warehouse` | 10 | `w_warehouse_sk` |
| `web_page` | 200 | `wp_web_page_sk` |
| `web_returns` | 719,217 | `wr_item_sk, wr_order_number` |
| `web_sales` | 7,197,566 | `ws_item_sk, ws_order_number` |
| `web_site` | 42 | `web_site_sk` |
| **Total** | **191,496,628** | |

### Pipeline

These are the documents as run 1 applied them, with the bucket's name replaced by `BUCKET`. [`examples/benchmark-24-tables.yaml`](../../examples/benchmark-24-tables.yaml) holds the same documents.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: tpcds-l8
spec:
  connector: postgres@0.2.0-alpha
  config:
    dsn: { secret: tpcds-dsn }
    schema: tpcds_sf10
    maxConnections: 8
---
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: tpcds-seed
spec:
  connector: postgres@0.2.0-alpha
  config:
    dsn: { secret: tpcds-dsn }
    schema: tpcds_sf10_seed
    maxConnections: 8
---
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: glue-lake
spec:
  connector: iceberg@0.2.0-alpha
  config:
    catalog: glue
    region: ap-southeast-2
    warehouse: s3://BUCKET/iceberg
    namespace: oetl_tpc_ds_r1
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: tpcds-r1
spec:
  schedule: { cron: "0 0 1 1 *" }
  resources:
    size: { cpu: 8, memory: 13Gi }
  delivery:
    commit: { scope: stream }
  source:
    ref: tpcds-l8
  destination:
    ref: glue-lake
    namespace: oetl_tpc_ds_r1
```

- The seed sync applies the same pipeline with `source.ref: tpcds-seed`.
- Runs 2 and 3 use `tpcds-r2` and `tpcds-r3` as the pipeline's name, and `oetl_tpc_ds_r2` and `oetl_tpc_ds_r3` as its namespace.
- The cron schedule fires once a year, so only `openetl apply` and `openetl sync` start syncs.
- The job size is 8 vCPUs and 13 GiB. The job ran 8 lanes and 8 writer lanes for 24 streams, as the sync log's `job started` line shows. The source opens at most 8 connections.
- The plan reads every table with `full_refresh`, writes it with `overwrite`, and delivers it exactly once (`exact replay`). Each table is split by its primary key to the writer, with no exchange.

### What limited the runs

| | Run 1 | Run 2 | Run 3 |
|---|---:|---:|---:|
| Job CPU | 357.8 s | 357.8 s | 358.9 s |
| Job peak memory | 7,775 MiB | 8,029 MiB | 7,046 MiB |
| Runner's share: CPU, memory | 30.6 s, 1,849 MiB | 30.8 s, 1,568 MiB | 31.1 s, 2,219 MiB |
| Lane seconds: busy, backpressured, idle (8 lanes) | 109.0, 35.0, 333.6 | 107.6, 25.3, 347.6 | 106.5, 39.5, 336.1 |
| CPU seconds by process (`pidstat`): destination, source, engine, runner | 173, 118, 65, 32 | 172, 119, 64, 32 | 174, 118, 64, 33 |
| Runner machine's CPU busy, first 50 s of the job | 95.5% | 94.1% | 95.6% |
| Runner machine's CPU busy, whole job | 83.4% | 82.8% | 82.5% |
| Runner machine's memory: most used, least available | 9,716 MiB, 5,948 MiB | 10,033 MiB, 5,631 MiB | 9,868 MiB, 5,796 MiB |
| Network in: job average, highest second | 393 MiB/s, 597 MiB/s | 393 MiB/s, 574 MiB/s | 393 MiB/s, 603 MiB/s |
| Network out: job average, highest second | 111 MiB/s, 574 MiB/s | 112 MiB/s, 551 MiB/s | 112 MiB/s, 332 MiB/s |
| Packets over the outbound allowance (`bw_out_allowance_exceeded`) | 235,942 | 441,934 | 132,546 |
| Written to S3 | 6.609 GB: 158 Parquet files, 216 metadata files | 6.608 GB: 172, 216 | 6.611 GB: 177, 216 |
| RDS CPU, busiest minute | 40.3% | 69.8% | 46.0% |
| RDS CPU above idle, over the run's minutes | about 329 vCPU-seconds | about 335 vCPU-seconds | about 332 vCPU-seconds |
| RDS read IOPS, connections, least freeable memory | 0, 8, 43 GiB | 0, 8, 43 GiB | 0, 8, 43 GiB |
| RDS network out, over the run's minutes | 24.86 GB | 24.86 GB | 24.85 GB |

This workload is derived from the TPC-DS Benchmark and is not comparable to published TPC-DS Benchmark results, as this implementation does not comply with all requirements of the TPC-DS Benchmark.

- **The runner machine's CPU is the limit while data moves.** It is 94.1 to 95.6% busy over the first 50 seconds of each job. The destination connector uses the most CPU, then the source connector, then the engine.
- **The whole-job average is lower, 82.5 to 83.4%.** In the last seconds of each job, the 1-second samples show the CPU near idle. In this commit tail, the last files and table metadata go to S3 and Glue.
- **The source is not the limit.** RDS served every read from memory, with 0 read IOPS. It used about 330 vCPU-seconds above idle in each run, with 8 connections.
- **Network.** Data came in at 393 MiB/s on average. Uploads to S3 come in bursts, and in every run the network interface counted packets over its outbound allowance. The counters do not show how much time this shaping took. The lanes spent 25 to 40 lane-seconds backpressured, of about 480 lane-seconds in each job.
- RDS metrics are 1-minute buckets, which do not line up with the runs. Most of run 2 fell into one bucket, so its busiest minute is higher. The vCPU-seconds and bytes over each run's minutes compare better.

### Checks

Athena read all 24 tables of each run's Glue database. In each run, every table's count equals Postgres's, 191,496,628 rows in all. These sums and non-null counts also equal Postgres's, which were read once after the load:

| Column | Sum | Rows that are not null |
|---|---:|---:|
| `inventory.inv_quantity_on_hand` | 63,223,996,520 | 126,456,148 |
| `store_sales.ss_net_paid` | 47,364,095,902.84 | 27,504,268 |
| `catalog_sales.cs_net_paid` | 32,876,899,525.18 | 14,329,516 |
| `web_sales.ws_net_paid` | 16,503,965,102.64 | 7,195,779 |
| `length(customer.c_email_address)` | 13,250,009 | 482,463 |

### Cost

At on-demand list prices in `ap-southeast-2`:

| Resource | Price |
|---|---:|
| RDS `db.r8g.2xlarge` | $1.148 an hour |
| RDS storage, 500 GB of gp3 | $0.138 a GB-month |
| EC2 `c8g.2xlarge` | $0.4152 an hour |
| EBS, 300 GB of gp3 at 16,000 IOPS and 1,000 MB/s | $0.204 an hour |
| Public IPv4 address | $0.005 an hour |

The two machines together cost about $1.87 an hour. At that rate, a timed sync of 64.2 seconds costs about $0.033. With its seed sync and the 120 seconds of idle time, a run costs about $0.11. S3 storage and requests, Athena and Glue for the whole workload cost under $0.03.

## 8 tables derived from TPC-H

This workload copies the 8 tables of a database derived from TPC-H, at two scale factors:

- scale factor 10, 86,586,082 rows, in three runs;
- scale factor 50, 433,005,841 rows, in one run.

The runner has 2 vCPUs and 4 GiB, and also holds OpenETL's whole Compose stack. The run at scale factor 50 shows how the rate and the memory change when the data grows five times.

This workload is derived from the TPC-H Benchmark and is not comparable to published TPC-H Benchmark results, as this implementation does not comply with all requirements of the TPC-H Benchmark.

The workload differs from the benchmark that it is derived from in these ways:

- It uses only the generated data.
- It runs none of the benchmark's queries.
- What it times is a load: a copy from Postgres into Iceberg tables.
- It reports no metric of that benchmark.
- The data at scale factor 50 comes from DuckDB's `tpch` extension, not from the TPC's `dbgen` program.

### Result

At scale factor 10, the mean of three runs is 145.79 s (2 minutes 25.8 seconds), from 145.10 to 146.14 s. The spread is 1.05 s, and the standard deviation is 0.60 s. The rate is 594,000 rows a second.

At scale factor 50, the run took 727.20 s (12 minutes 7.2 seconds), at 595,000 rows a second. The rate and the peak memory at scale factor 50 match those at scale factor 10.

| Run | Scale factor | Sync time | Rows/s | Start to `job started` | Job | `job finished` to end | Attempts | Runner machine's memory: most used, least available |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 10 | 146.121 s | 592,563 | 1.72 s | 144.22 s | 0.18 s | 1 | 3,623 MiB, 176 MiB |
| 2 | 10 | 145.095 s | 596,755 | 1.55 s | 143.43 s | 0.12 s | 1 | 3,493 MiB, 306 MiB |
| 3 | 10 | 146.143 s | 592,477 | 1.57 s | 144.40 s | 0.18 s | 1 | 3,661 MiB, 138 MiB |
| Mean | 10 | 145.786 s | | 1.61 s | 144.02 s | 0.16 s | | |
| 1 | 50 | 727.196 s | 595,446 | 1.69 s | 725.36 s | 0.14 s | 1 | 3,617 MiB, 182 MiB |

Against the database's size, 15.40 GB and 76.97 GB, the rate is 380 GB an hour at scale factor 10, and 381 GB an hour at scale factor 50. Against the tables without their indexes, it is 332 and 333 GB an hour.

This workload is derived from the TPC-H Benchmark and is not comparable to published TPC-H Benchmark results, as this implementation does not comply with all requirements of the TPC-H Benchmark.

### Job size and lanes

With `OPENETL_RESERVE=0,1Gi`, the runner offers jobs 2 vCPUs and 2,775 MiB. The preset `m`, 2 vCPUs and 4 GiB, is larger, and `openetl plan` shows this problem for it:

```text
the job's size, m, is larger than any runner of data plane "default" and pool "default" can take: the largest has 2 vCPU, 2775 MiB
```

The preset `s` has 1 vCPU. So the pipelines set the size `{ cpu: 2, memory: 2560Mi }`, which gives the job 2 lanes. Two runs copied `lineitem` alone, 59,986,052 rows, to compare 2 lanes with 1. Each run wrote into new tables after the 120 seconds of idle time.

| Job size | Sync time | Lane seconds: busy, backpressured, idle | Runner machine's CPU busy, whole job | Runner machine's memory: most used, least available | Job peak memory | RDS reads, busiest minute |
|---|---:|---|---:|---|---:|---|
| `{ cpu: 2, memory: 2560Mi }`, 2 lanes | 117.258 s | 56.7, 117.9, 58.3 | 88.9% | 3,585 MiB, 214 MiB | 1,718 MiB | 86.7 MB/s, 1,182 IOPS |
| `{ cpu: 1, memory: 2560Mi }`, 1 lane | 129.634 s | 33.5, 73.5, 21.7 | 79.8% | 2,756 MiB, 1,043 MiB | 1,168 MiB | 7.4 MB/s, 251 IOPS |

One lane took 10.6% longer than two. The run with 2 lanes read `lineitem` from disk, and the run with 1 lane read it from the cache that the first run left.

This workload is derived from the TPC-H Benchmark and is not comparable to published TPC-H Benchmark results, as this implementation does not comply with all requirements of the TPC-H Benchmark.

### Machines

| Role | Machine |
|---|---|
| Runner, with OpenETL's whole Compose stack | EC2 `c8g.large`: 2 vCPUs, which are 2 Graviton4 (Neoverse-V2) cores without SMT, and 4 GiB of memory, 3,799 MiB usable, without swap. Ubuntu 24.04.5, kernel 7.0.0-1018-aws, Docker 29.1.3, Compose 2.40.3. Disk: 40 GB of gp3 at 3,000 IOPS and 125 MB/s. Network: 0.937 Gbit/s baseline, up to 12.5 Gbit/s in bursts. Idle with the stack up, it used 777 MiB, with 3,022 MiB available. |
| Source | RDS for PostgreSQL 18.6, `db.m8g.xlarge`: 4 vCPUs, 16 GiB, Single-AZ. Storage: 200 GB of gp3 at 3,000 IOPS and 125 MiB/s, which RDS sets for gp3 below 400 GiB on PostgreSQL. Not encrypted. Backup retention 0, so `wal_level` is `minimal`. The default parameter group: `shared_buffers` 3.78 GiB, `effective_cache_size` 7.56 GiB, `work_mem` 4 MB, `max_connections` 1702. No Performance Insights or Enhanced Monitoring. |
| Data loaders, not timed | Scale factor 10: the `c8g.2xlarge` of the workload derived from TPC-DS, before that workload's runs. Scale factor 50: another `c8g.2xlarge`, with 150 GB of gp3 at 6,000 IOPS and 500 MB/s, removed before the run at scale factor 50. Neither loader ran on the runner's machine. |

### Data

At scale factor 10, the data is AWS's public set of files derived from TPC-H, version 2.18: `s3://redshift-downloads/TPC-H/2.18/10GB/`. It has one `.tbl` file of `dbgen`'s output for each table, 11.1 GB in all. The download took 24 seconds.

AWS's public files have no set at scale factor 50. DuckDB v1.4.1's `tpch` extension generated it in 16 parts, each with a share of every table: `CALL dbgen(sf=50, children=16, step=N)` for each `N` from 0 to 15, 8 parts at a time. The generation took 87 seconds.

These are the tables, in schema `tpch_sf10` or `tpch_sf50` of database `tpch`, with the types of `dbgen`'s `dss.ddl` in Postgres's names:

```sql
CREATE TABLE nation   (n_nationkey integer not null, n_name char(25) not null, n_regionkey integer not null, n_comment varchar(152));
CREATE TABLE region   (r_regionkey integer not null, r_name char(25) not null, r_comment varchar(152));
CREATE TABLE part     (p_partkey integer not null, p_name varchar(55) not null, p_mfgr char(25) not null, p_brand char(10) not null,
                       p_type varchar(25) not null, p_size integer not null, p_container char(10) not null,
                       p_retailprice decimal(15,2) not null, p_comment varchar(23) not null);
CREATE TABLE supplier (s_suppkey integer not null, s_name char(25) not null, s_address varchar(40) not null, s_nationkey integer not null,
                       s_phone char(15) not null, s_acctbal decimal(15,2) not null, s_comment varchar(101) not null);
CREATE TABLE partsupp (ps_partkey integer not null, ps_suppkey integer not null, ps_availqty integer not null,
                       ps_supplycost decimal(15,2) not null, ps_comment varchar(199) not null);
CREATE TABLE customer (c_custkey integer not null, c_name varchar(25) not null, c_address varchar(40) not null, c_nationkey integer not null,
                       c_phone char(15) not null, c_acctbal decimal(15,2) not null, c_mktsegment char(10) not null, c_comment varchar(117) not null);
CREATE TABLE orders   (o_orderkey integer not null, o_custkey integer not null, o_orderstatus char(1) not null, o_totalprice decimal(15,2) not null,
                       o_orderdate date not null, o_orderpriority char(15) not null, o_clerk char(15) not null, o_shippriority integer not null,
                       o_comment varchar(79) not null);
CREATE TABLE lineitem (l_orderkey integer not null, l_partkey integer not null, l_suppkey integer not null, l_linenumber integer not null,
                       l_quantity decimal(15,2) not null, l_extendedprice decimal(15,2) not null, l_discount decimal(15,2) not null,
                       l_tax decimal(15,2) not null, l_returnflag char(1) not null, l_linestatus char(1) not null, l_shipdate date not null,
                       l_commitdate date not null, l_receiptdate date not null, l_shipinstruct char(25) not null, l_shipmode char(10) not null,
                       l_comment varchar(44) not null);
```

The load, for each scale factor:

1. Create the tables, without keys.
2. Load each table with one `COPY ... FREEZE` in the transaction that truncates it, all 8 tables at once:

   ```sh
   psql -c "BEGIN" -c "TRUNCATE SCHEMA.TABLE" -c "\copy SCHEMA.TABLE from pstdin with (OPTIONS)" -c "COMMIT"
   ```

   At scale factor 10, `sed 's/|$//'` removes the last `|` of each line of a `.tbl` file. `OPTIONS` is `format csv, delimiter '|', quote E'\x01', freeze`. This took 113 seconds. At scale factor 50, DuckDB writes each table of the 16 parts as CSV, with `|` as the delimiter. `OPTIONS` is `format csv, delimiter '|', freeze`. This took 10 minutes 39 seconds. With `wal_level` at `minimal`, these rows are not written to the write-ahead log.
3. Add the primary keys, all at once, with `maintenance_work_mem` set to `1GB`: `lineitem (l_orderkey, l_linenumber)`, `orders (o_orderkey)`, `partsupp (ps_partkey, ps_suppkey)`, `part (p_partkey)`, `customer (c_custkey)`, `supplier (s_suppkey)`, `nation (n_nationkey)` and `region (r_regionkey)`. This took 132 seconds at scale factor 10, and 12 minutes 43 seconds at scale factor 50.
4. Run `vacuumdb --analyze -j 4`. This took 15 seconds at scale factor 10, and 45 seconds at scale factor 50.
5. Make the seed schema, `tpch_sf10_seed` or `tpch_sf50_seed`, with one row in each table.

Before the load at scale factor 50, the schemas of scale factor 10 were dropped, so the database held scale factor 50 alone.

| Table | Rows, scale factor 10 | Tables and indexes, scale factor 10 | Rows, scale factor 50 | Tables and indexes, scale factor 50 |
|---|---:|---|---:|---|
| `lineitem` | 59,986,052 | 9.22 GB, 1.35 GB | 300,005,811 | 46.11 GB, 6.74 GB |
| `orders` | 15,000,000 | 2.14 GB, 0.34 GB | 75,000,000 | 10.71 GB, 1.68 GB |
| `partsupp` | 8,000,000 | 1.44 GB, 0.18 GB | 40,000,000 | 7.18 GB, 0.90 GB |
| `part` | 2,000,000 | 0.34 GB, 0.04 GB | 10,000,000 | 1.68 GB, 0.22 GB |
| `customer` | 1,500,000 | 0.29 GB, 0.03 GB | 7,500,000 | 1.47 GB, 0.17 GB |
| `supplier` | 100,000 | 18 MB, 2 MB | 500,000 | 91 MB, 11 MB |
| `nation` | 25 | 40,960 bytes, 16,384 bytes | 25 | 40,960 bytes, 16,384 bytes |
| `region` | 5 | 40,960 bytes, 16,384 bytes | 5 | 40,960 bytes, 16,384 bytes |
| **Total rows** | **86,586,082** | | **433,005,841** | |
| Tables without indexes | | 13.45 GB (13,445,242,880 bytes) | | 67.24 GB (67,237,773,312 bytes) |
| Primary-key indexes | | 1.95 GB (1,945,034,752 bytes) | | 9.73 GB (9,726,205,952 bytes) |
| Database (`pg_database_size`) | | 15.40 GB (15,398,532,799 bytes) | | 76.97 GB (76,972,562,111 bytes) |

A table's size is its `pg_table_size`: the heap, TOAST, the free space map and the visibility map. An index's size is the table's `pg_indexes_size`. GB is 10^9 bytes, and MB 10^6 bytes. The sync's record counts 11,286,959,995 bytes that the source emitted at scale factor 10, and 56,445,142,586 at scale factor 50.

### Pipeline

These are the documents as run 1 at each scale factor applied them, with the bucket's name replaced by `BUCKET`. [`examples/benchmark-8-tables.yaml`](../../examples/benchmark-8-tables.yaml) holds the same documents.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: tpch-sf10
spec:
  connector: postgres@0.2.0-alpha
  config:
    dsn: { secret: tpch-dsn }
    schema: tpch_sf10
    maxConnections: 8
---
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: tpch-sf10-seed
spec:
  connector: postgres@0.2.0-alpha
  config:
    dsn: { secret: tpch-dsn }
    schema: tpch_sf10_seed
    maxConnections: 8
---
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: tpch-sf50
spec:
  connector: postgres@0.2.0-alpha
  config:
    dsn: { secret: tpch-dsn }
    schema: tpch_sf50
    maxConnections: 8
---
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: tpch-sf50-seed
spec:
  connector: postgres@0.2.0-alpha
  config:
    dsn: { secret: tpch-dsn }
    schema: tpch_sf50_seed
    maxConnections: 8
---
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: glue-lake
spec:
  connector: iceberg@0.2.0-alpha
  config:
    catalog: glue
    region: ap-southeast-2
    warehouse: s3://BUCKET/iceberg
    namespace: oetl_tpc_h_cal1
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: tpch-r1
spec:
  schedule: { cron: "0 0 1 1 *" }
  resources:
    size: { cpu: 2, memory: 2560Mi }
  delivery:
    commit: { scope: stream }
  source:
    ref: tpch-sf10
  destination:
    ref: glue-lake
    namespace: oetl_tpc_h_r1
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: tpch-sf50
spec:
  schedule: { cron: "0 0 1 1 *" }
  resources:
    size: { cpu: 2, memory: 2560Mi }
  delivery:
    commit: { scope: stream }
  source:
    ref: tpch-sf50
  destination:
    ref: glue-lake
    namespace: oetl_tpc_h_sf50
```

- Each seed sync applies the same pipeline with `source.ref: tpch-sf10-seed` or `tpch-sf50-seed`.
- Runs 2 and 3 at scale factor 10 use `tpch-r2` and `tpch-r3` as the pipeline's name, and `oetl_tpc_h_r2` and `oetl_tpc_h_r3` as its namespace.
- The two runs of [Job size and lanes](#job-size-and-lanes) use the pipelines `tpch-cal1` and `tpch-cal2`, into the namespaces `oetl_tpc_h_cal1` and `oetl_tpc_h_cal2`. Each has `source.streams: [{ name: lineitem }]`, and `tpch-cal2` has `cpu: 1`.
- The job ran 2 lanes and 2 writer lanes for 8 streams. The plan reads every table with `full_refresh`, writes it with `overwrite`, and delivers it exactly once (`exact replay`). Each table is split by its primary key to the writer, with no exchange.

### What limited the runs

| | Run 1, scale factor 10 | Run 2, scale factor 10 | Run 3, scale factor 10 | Run 1, scale factor 50 |
|---|---:|---:|---:|---:|
| Job CPU | 236.7 s | 235.4 s | 237.4 s | 1,202.7 s |
| Job peak memory | 1,801 MiB | 1,823 MiB | 1,813 MiB | 1,845 MiB |
| Runner's share: CPU, memory | 22.1 s, 885 MiB | 21.8 s, 891 MiB | 22.1 s, 997 MiB | 110.3 s, 1,195 MiB |
| Lane seconds: busy, backpressured, idle (2 lanes) | 68.1, 159.5, 60.7 | 69.5, 112.0, 105.4 | 69.5, 173.4, 45.9 | 322.2, 714.4, 414.1 |
| CPU seconds by process (`pidstat`): destination, source, engine, runner | 135, 73, 29, 25 | 137, 70, 29, 25 | 138, 70, 29, 25 | 697, 361, 143, 125 |
| Highest resident memory by process (`pidstat`, 5 s): runner, destination, engine, source | 1,322, 1,275, 390, 99 MiB | 1,321, 1,293, 357, 95 MiB | 1,327, 1,282, 358, 91 MiB | 1,390, 1,300, 366, 105 MiB |
| Runner machine's CPU busy: first 50 s, whole job | 97.2%, 94.7% | 97.2%, 94.5% | 97.7%, 95.7% | 95.6%, 95.8% |
| Runner machine's memory, of 3,799 MiB: most used, least available | 3,623 MiB, 176 MiB | 3,493 MiB, 306 MiB | 3,661 MiB, 138 MiB | 3,617 MiB, 182 MiB |
| Network in: job average, highest second | 113 MiB/s, 181 MiB/s | 114 MiB/s, 187 MiB/s | 113 MiB/s, 202 MiB/s | 114 MiB/s, 203 MiB/s |
| Network out: job average, highest second | 28 MiB/s, 121 MiB/s | 28 MiB/s, 116 MiB/s | 28 MiB/s, 159 MiB/s | 28 MiB/s, 136 MiB/s |
| Packets over the allowance: inbound, outbound, packets a second | 0, 53,675, 0 | 0, 33,087, 0 | 0, 75,186, 3 | 0, 112,915, 33 |
| Written to S3 | 4.035 GB: 62 Parquet files, 72 metadata files | 4.033 GB: 64, 72 | 4.035 GB: 64, 72 | 20.403 GB: 217, 72 |
| RDS CPU: busiest minute, above idle over the run's minutes | 20.7%, about 108 vCPU-seconds | 20.2%, about 106 vCPU-seconds | 21.3%, about 107 vCPU-seconds | 29.9%, about 704 vCPU-seconds |
| RDS reads, busiest minute | 681 IOPS, 74.9 MB/s | 999 IOPS, 64.6 MB/s | 869 IOPS, 70.6 MB/s | 1,088 IOPS, 121.4 MB/s |
| RDS network out, over the run's minutes; connections | 17.42 GB; 2 | 17.41 GB; 2 | 17.42 GB; 2 | 91.78 GB; 2 |

This workload is derived from the TPC-H Benchmark and is not comparable to published TPC-H Benchmark results, as this implementation does not comply with all requirements of the TPC-H Benchmark.

- **The runner machine's CPU is the limit.** At scale factor 10, it is 97.2 to 97.7% busy over the first 50 seconds, and 94.5 to 95.7% over the whole job. The destination connector uses about 58% of the job's CPU, the source connector 30% and the engine 12%. The lanes spend more time backpressured than busy, because the writers set the pace.
- **The source is not the limit.** At scale factor 10, RDS's CPU is near 21% in the busiest minute, with 2 connections. It reads up to 75 MB/s from disk, below the volume's 125 MiB/s.
- **The network is at its baseline, but it is not the limit.** The runner machine takes 113 to 114 MiB/s (0.95 Gbit/s) on average, near the `c8g.large`'s baseline of 0.937 Gbit/s. The network interface counted no inbound packet over its allowance in any run, also in the 12 minutes at scale factor 50. With 1 lane, the copy of `lineitem` took 93 MiB/s, with the CPU about 80% busy. So the rate follows the CPU.
- **Memory has the smallest margin on 4 GiB.** At its lowest, the runner machine had 138 to 306 MiB available in the runs at scale factor 10, without swap. The runner process, `openetld`, held up to 1,327 MiB, the destination connector up to 1,293 MiB, and the engine up to 390 MiB. The job's own peak, up to 1,823 MiB, is within its size of 2,560 MiB. The kernel stopped no process for lack of memory: `dmesg` has no out-of-memory line.
- **Memory does not grow with the data.** At scale factor 50, with five times the rows over five times as long, the machine used at most 3,617 MiB. The job's peak was 1,845 MiB, and the runner process's 1,390 MiB.
- **At scale factor 50, the source reads from disk.** The 67 GB of tables do not fit in the instance's 16 GiB. RDS read about 100 MB/s for most of the run, below the volume's 131 MB/s, and its CPU stayed under 30%. The runner machine's CPU, 95.8% busy, is still the limit.

### Checks

Athena read every table of each run:

- In each run at scale factor 10, all 8 counts equal Postgres's, 86,586,082 rows in all. `sum(l_extendedprice)` is 2,293,813,156,773.36 over 59,986,052 rows, and `sum(o_totalprice)` 2,266,298,190,748.43 over 15,000,000 rows, equal to Postgres's.
- At scale factor 50, all 8 counts equal Postgres's, 433,005,841 rows in all. `sum(l_extendedprice)` is 11,471,074,750,766.66 over 300,005,811 rows, and `sum(o_totalprice)` 11,333,446,554,268.71 over 75,000,000 rows, equal to Postgres's.
- In each of the two runs of `lineitem` alone, the count, 59,986,052, equals Postgres's.

Postgres's counts and sums were read once after each load.

### Cost

At on-demand list prices in `ap-southeast-2`:

| Resource | Price |
|---|---:|
| RDS `db.m8g.xlarge` | $0.468 an hour |
| RDS storage, 200 GB of gp3 | $0.138 a GB-month |
| EC2 `c8g.large` | $0.1038 an hour |
| EBS, 40 GB of gp3 | $0.0053 an hour |
| Public IPv4 address | $0.005 an hour |

The runner machine and the RDS instance together cost about $0.62 an hour. At that rate:

- a timed sync at scale factor 10, 145.8 seconds, costs about $0.025;
- a run at scale factor 10, with its seed sync and the 120 seconds of idle time, costs about $0.05;
- the timed sync at scale factor 50, 727.2 seconds, costs about $0.125;
- each run of `lineitem` alone costs about $0.02.

The data at scale factor 50 cost more than its run. Its generation and load kept the second `c8g.2xlarge` for 56 minutes ($0.46), and the RDS instance for 32 minutes ($0.27). That `c8g.2xlarge` costs $0.4152 an hour. Its 150 GB of gp3 at 6,000 IOPS and 500 MB/s costs $0.069 an hour, and its public IPv4 address $0.005 an hour.

## New York City taxi trips

This workload is a full load of 318,879,363 rows of New York City taxi trip records, in two wide tables: `trips`, with 29 columns, and `fhv_trips`, with 27. The runner has 8 vCPUs and 16 GiB.

### Result

The mean of three runs is 171.562 s, from 162.887 to 176.663 s. The spread is 13.8 s, 8% of the mean. The rate is 1,858,683 rows a second, which is 232,335 rows a second for each vCPU of the runner.

| Run | Sync time | Rows/s | Rows/s for each vCPU | Heap MB/s | Start to `job started` | Job | `job finished` to end | Attempts |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 176.663 s | 1,805,018 | 225,627 | 283.7 | 0.98 s | 175.44 s | 0.24 s | 1 |
| 2 | 162.887 s | 1,957,670 | 244,709 | 307.7 | 0.84 s | 161.96 s | 0.09 s | 1 |
| 3 | 175.136 s | 1,820,752 | 227,594 | 286.1 | 0.82 s | 174.03 s | 0.29 s | 1 |
| Mean | 171.562 s | 1,858,683 | 232,335 | 292.1 | | | | |

Each run copies 209,789,604 rows of `trips` and 109,089,759 rows of `fhv_trips` into new tables. Heap MB/s is the two tables' `pg_relation_size`, 32,579,829,760 and 17,534,124,032 bytes (50.11 GB in all), over the sync time.

### Machines

| Role | Machine |
|---|---|
| Runner, with OpenETL's whole Compose stack | EC2 `c8g.2xlarge`: 8 vCPUs, which are 8 Graviton4 (Neoverse-V2) cores without SMT, and 16 GiB of memory, 15.3 GiB usable. Ubuntu 24.04.5, kernel 7.0.0-1018-aws, Docker 29.1.3, Compose 2.40.3. Root disk: 150 GB of gp3 at 6,000 IOPS and 500 MB/s, which also held the 9.8 GB of Parquet files to load. |
| Source | RDS for PostgreSQL 18.7, `db.r8g.2xlarge`: 8 vCPUs, 64 GiB, Single-AZ. Storage: 400 GB of gp3 at 12,000 IOPS and 500 MB/s, not encrypted. A parameter group with `rds.logical_replication=1`, so `wal_level` is `logical`, and the family's defaults for the rest: `max_wal_size` 6 GB, `wal_compression` `zstd`, data checksums on. `shared_buffers` is 2,014,030 pages of 8 kB (15.4 GiB). No automated backups. Performance Insights at its free tier, and Enhanced Monitoring every 5 seconds. |

### Data

The data is the New York City Taxi and Limousine Commission's [trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page): the first 15 months of yellow taxi trips from January 2009, and the first 5 months of high-volume for-hire vehicle trips from February 2019. The Parquet files come from `https://d37ci6vzurychx.cloudfront.net/trip-data/FILE.parquet`. Their download took 54 seconds.

| Files | Table | Rows | Bytes |
|---|---|---:|---:|
| `yellow_tripdata_2009-01` to `yellow_tripdata_2009-12` | `trips` | 170,896,055 | 5,717,475,607 |
| `yellow_tripdata_2010-01` to `yellow_tripdata_2010-03` | `trips` | 38,893,549 | 1,282,838,704 |
| `fhvhv_tripdata_2019-02` to `fhvhv_tripdata_2019-06` | `fhv_trips` | 109,089,759 | 2,790,578,768 |
| **All** | | **318,879,363** | **9,790,893,079** |

The SHA-256 of each file, as `sha256sum` printed it:

```text
b3efb77e26acf7926bb4d20ff3f4f441cb5d8debca76ce92bde94e4116cc24d2  fhvhv_tripdata_2019-02.parquet
53098177e9021a77d7a38f2bf205864ce6a03c253e451641d5f525967f21b000  fhvhv_tripdata_2019-03.parquet
cc1cf8773e5af3acbd8e1e8c248fed96ac5841545f4a22fea8b2d9845440c1d9  fhvhv_tripdata_2019-04.parquet
b5bd02285256d6fd610476d2a219f60de05d292057acb710b7d60a39aa1e8455  fhvhv_tripdata_2019-05.parquet
49b297586e6a2770161a347b417da3bdcd0950f52a830d9875a9cc7b8cdf3330  fhvhv_tripdata_2019-06.parquet
cb53301e6b350779a6f396da7c149034bd89e261e1c7b1b8550831ddf65ebede  yellow_tripdata_2009-01.parquet
f94594fb3d1900cd173a02eee43fc38f2d763e05c460f0fe62291d76d2e3f6f6  yellow_tripdata_2009-02.parquet
28cf320c9438f1d7f6491ce8182ee4c576133eb7eceb271e600e7da45ddcb545  yellow_tripdata_2009-03.parquet
a66ab349e8e3d77902d4aef9a25e0ab8f0bd8ee4dd1b20361da4e06336a40dda  yellow_tripdata_2009-04.parquet
18744ac5d5a6706533a908e59f578e5f578fa5946ff3445fd3be8891680db4d8  yellow_tripdata_2009-05.parquet
3f0ec58a6137769e5b05856c5934e384f2e653434fe955ca26936af821887ee5  yellow_tripdata_2009-06.parquet
3bc26d7e0e1a7fd83eeaf66d6e8d64d01480cbd984a383ffafdb48ac9a5fa9d9  yellow_tripdata_2009-07.parquet
9afde8d6be45dde17ab5292ddd75ef31cde716085e76087c4c738aa9029323ad  yellow_tripdata_2009-08.parquet
02c6959c9c70776fb79b6213a59a856d6f70fdc1a4d8abd09b178454630e0685  yellow_tripdata_2009-09.parquet
b9fc7e48c04c0fccf16677f06104290598fe3503d3427764bce470f77b36fdf6  yellow_tripdata_2009-10.parquet
5bbc1b1e2edb0ecf4955257d1390e287ae7e9f83d127445d373a184a6c07fdb4  yellow_tripdata_2009-11.parquet
ffaab8baededd6c880b5101afbd45c1b24c978fdf656e2f2aade2e5ab3d59dce  yellow_tripdata_2009-12.parquet
8ce0046912d268ab6306dafeca7af6d03fa632ee4c6ad83ec3fa1583a1cc54bf  yellow_tripdata_2010-01.parquet
654343434a1548ca605ef26d5c69b17c28437ba8afa86deeb900e9958e29c4fc  yellow_tripdata_2010-02.parquet
89637c07e9885b1d2536719873958a0d656aeb48e7b77fe1fd87b29be9679c44  yellow_tripdata_2010-03.parquet
```

These are the tables, in schema `taxi` of database `taxi`. The primary keys on `id` are added after the load.

```sql
CREATE TABLE taxi.trips (
  id bigserial,
  cab_type_id integer,
  vendor_id integer,
  pickup_datetime timestamp without time zone,
  dropoff_datetime timestamp without time zone,
  store_and_fwd_flag boolean,
  rate_code_id integer,
  pickup_longitude numeric,
  pickup_latitude numeric,
  dropoff_longitude numeric,
  dropoff_latitude numeric,
  passenger_count integer,
  trip_distance numeric,
  fare_amount numeric,
  extra numeric,
  mta_tax numeric,
  tip_amount numeric,
  tolls_amount numeric,
  ehail_fee numeric,
  improvement_surcharge numeric,
  congestion_surcharge numeric,
  airport_fee numeric,
  total_amount numeric,
  payment_type integer,
  trip_type integer,
  pickup_nyct2010_gid integer,
  dropoff_nyct2010_gid integer,
  pickup_location_id integer,
  dropoff_location_id integer
);
CREATE TABLE taxi.fhv_trips (
  id bigserial,
  hvfhs_license_num text,
  dispatching_base_num text,
  originating_base_num text,
  request_datetime timestamp without time zone,
  on_scene_datetime timestamp without time zone,
  pickup_datetime timestamp without time zone,
  dropoff_datetime timestamp without time zone,
  pickup_location_id integer,
  dropoff_location_id integer,
  trip_miles numeric,
  trip_time numeric,
  base_passenger_fare numeric,
  tolls numeric,
  black_car_fund numeric,
  sales_tax numeric,
  congestion_surcharge numeric,
  airport_fee numeric,
  tips numeric,
  driver_pay numeric,
  shared_request boolean,
  shared_match boolean,
  access_a_ride boolean,
  wav_request boolean,
  wav_match boolean,
  legacy_shared_ride integer,
  affiliated_base_num text
);
```

DuckDB 1.4.4 reads each Parquet file, maps its columns to the table's, and writes CSV to `psql`'s `\copy`, 8 files at a time. For each file:

```sh
duckdb -batch <<'SQL' | PGOPTIONS="-c synchronous_commit=off" psql -c "\copy taxi.TABLE from stdin with (format csv)"
SET threads = 2; SET memory_limit = '1200MB'; SET preserve_insertion_order = false;
COPY (SELECT ROWS_BEFORE + file_row_number + 1 AS id, MAPPED_COLUMNS
      FROM read_parquet('FILE', file_row_number = true))
  TO '/dev/stdout' (FORMAT csv, HEADER false);
SQL
```

`ROWS_BEFORE` is the number of rows in the files before `FILE` that go to the same table, in the order of the files above. `MAPPED_COLUMNS` are the expressions of the mapping, in the order of the table's columns after `id`. For each file, the load checked that every command of the pipe exited with status 0. It also checked that `psql` printed `COPY` with the file's row count, from the Parquet file's metadata. A copy whose input ends early commits the rows that it read, so these checks stop a short load. The mapping:

- `id` is the row's place in the order of the files above, then of the rows in its file, from 1.
- In `trips`, the yellow files' 18 columns map by their position in the file. `cab_type_id` is 1. `vendor_id` is 1 for `CMT`, 2 for `VTS` and 3 for `DDS`. `payment_type` is 1 for credit, 2 for cash, 3 for no charge and 4 for dispute, from each spelling that the files use. `store_and_fwd_flag` is true for `Y` or `1`, and false for `N` or `0`. A coordinate of 0 becomes NULL.
- `ehail_fee`, `improvement_surcharge`, `congestion_surcharge`, `airport_fee`, `trip_type`, `pickup_nyct2010_gid`, `dropoff_nyct2010_gid`, `pickup_location_id` and `dropoff_location_id` of `trips` are NULL.
- In `fhv_trips`, the files' 24 columns map by their position in the file. The 5 flag columns become booleans: `Y` is true and `N` is false, and an empty `access_a_ride_flag` is false. The license and base numbers are trimmed and in capitals. `legacy_shared_ride` and `affiliated_base_num` are NULL.
- An empty string becomes NULL. A floating-point value that holds a whole number is written as an integer. The 2010 files' extra index column, `__index_level_0__`, is not loaded.

The load:

1. Create the tables, without the primary keys.
2. Copy the 20 files, 8 at a time, with `synchronous_commit` off. This took 226 seconds, at 1.41 million rows a second, with RDS writing at its 500 MB/s in the busiest minutes.
3. Add the two primary keys at once, each with `maintenance_work_mem` set to `4GB` and 3 parallel workers, and set the `id` sequences. This took 232 seconds.
4. Run `vacuum (freeze, analyze)` on both tables at once, so that no run sets hint bits or freezes rows. This took 220 seconds.
5. Make the seed schema `taxi_seed`, with the row of `id` 1 in each table and the same primary keys.

| | `trips` | `fhv_trips` |
|---|---:|---:|
| Rows | 209,789,604 | 109,089,759 |
| Heap, `pg_relation_size` | 32,579,829,760 bytes (30.3 GiB) | 17,534,124,032 bytes (16.3 GiB) |
| Heap bytes a row: `pg_relation_size` over the row estimate | 155.3 | 160.7 |
| Bytes a row: mean `pg_column_size` of the row, in a 1% sample | 147.4 | 150.7 |
| Bytes a row: sum of `pg_stats.avg_width` | 134 | 119 |
| Primary-key index | 4.71 GB | 2.45 GB |
| Correlation of `id` with the heap's order (`pg_stats`) | 0.786 | 0.609 |

The 8 parallel copies interleave each table's ids in runs of pages, so `id` does not follow the heap's order exactly. The sync's record counts 57.48 GB that the source emitted in each run.

### Pipeline

These are the documents as run 1 applied them, with the bucket's name replaced by `BUCKET`. [`examples/benchmark-taxi-trips.yaml`](../../examples/benchmark-taxi-trips.yaml) holds the same documents.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: taxi-full
spec:
  connector: postgres@0.2.0-alpha
  config:
    dsn: { secret: taxi-dsn }
    schema: taxi
    tables: [trips, fhv_trips]
    maxConnections: 8
---
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: taxi-seed
spec:
  connector: postgres@0.2.0-alpha
  config:
    dsn: { secret: taxi-dsn }
    schema: taxi_seed
    tables: [trips, fhv_trips]
    maxConnections: 8
---
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: glue-lake
spec:
  connector: iceberg@0.2.0-alpha
  config:
    catalog: glue
    region: ap-southeast-2
    warehouse: s3://BUCKET/iceberg
    namespace: oetl_taxi_default
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: taxi-full-r1
spec:
  schedule: { cron: "0 0 1 1 *" }
  resources:
    size: { cpu: 8, memory: 13Gi }
  delivery:
    commit: { scope: stream }
  source:
    ref: taxi-full
    streams:
      - name: trips
        sync: full_refresh
        columns:
          pickup_longitude: { type: float64 }
          pickup_latitude: { type: float64 }
          dropoff_longitude: { type: float64 }
          dropoff_latitude: { type: float64 }
          trip_distance: { type: float64 }
          fare_amount: { type: float64 }
          extra: { type: float64 }
          mta_tax: { type: float64 }
          tip_amount: { type: float64 }
          tolls_amount: { type: float64 }
          total_amount: { type: float64 }
      - name: fhv_trips
        sync: full_refresh
  destination: { ref: glue-lake, namespace: oetl_taxi_full_r1 }
```

- The spec reads 11 columns of `trips` as `float64`, because they are unconstrained `numeric` columns that hold floating-point values.
- The seed sync applies the same pipeline with `source.ref: taxi-seed`.
- Runs 2 and 3 use `taxi-full-r2` and `taxi-full-r3` as the pipeline's name, and `oetl_taxi_full_r2` and `oetl_taxi_full_r3` as its namespace.
- The job ran 8 lanes and 8 writer lanes for 2 streams. The plan writes both tables with `overwrite`, delivers them exactly once (`exact replay`), and splits each by `id` to the writer.

### What limited the runs

| | Run 1 | Run 2 | Run 3 |
|---|---:|---:|---:|
| Job CPU | 1,046.9 s | 1,037.8 s | 1,050.3 s |
| Job peak memory | 7.29 GiB | 7.54 GiB | 7.61 GiB |
| Lane seconds: busy, backpressured, idle (8 lanes) | 288.9, 244.4, 870.2 | 286.5, 267.9, 741.3 | 270.7, 263.9, 857.6 |
| CPU seconds by process (`pidstat`): destination, source, engine, runner | 527, 361, 159, 109 | 517, 362, 159, 111 | 524, 367, 159, 114 |
| Runner machine's CPU busy, whole job | 84.1% | 90.6% | 85.8% |
| Runner machine's CPU busy, median from `job started` to 20 s before `job finished` | 98% | 99% | 98% |
| Runner machine's memory, most used | 10,691 MiB | 10,752 MiB | 10,928 MiB |
| Network in: job average, highest second | 427 MiB/s, 761 MiB/s | 465 MiB/s, 782 MiB/s | 432 MiB/s, 694 MiB/s |
| Network out: job average, highest second | 92 MiB/s, 296 MiB/s | 100 MiB/s, 302 MiB/s | 93 MiB/s, 304 MiB/s |
| Parquet written to S3 | 15.94 GB, 610 files | 15.92 GB, 401 files | 15.93 GB, 513 files |
| RDS CPU, busiest minute | 42.9% | 43.3% | 43.3% |
| RDS reads, busiest minute | 10,310 IOPS, 394.9 MB/s | 9,140 IOPS, 371.3 MB/s | 12,778 IOPS, 491.1 MB/s |

- **The runner machine's CPU is the limit.** The 8 lanes keep it busy from the job's first seconds until about 20 seconds before its end. Over that span, the median of the 1-second samples is 98 to 99% busy. The lowest samples are 9%, 68% and 56% in runs 1, 2 and 3. Averaged over the whole job, it is 84.1, 90.6 and 85.8% busy.
- **The commit tail.** In about the last 20 seconds, the CPU is near idle. In this time, the last files and table metadata go to S3 and Glue.
- **CPU by process.** The destination connector uses the most CPU, then the source connector, the engine and the runner. The job uses 1,038 to 1,050 CPU-seconds a run.
- **The source comes near its storage's limits, but does not set the pace.** RDS's CPU is 42.9 to 43.3% in the busiest minute. Its reads reach 371 to 491 MB/s and 9,140 to 12,778 IOPS in the busiest minute, against gp3's 500 MB/s and 12,000 IOPS. The tables and their indexes, 57 GB, do not all fit in the instance's 64 GiB. So each run reads part of them from disk, and how much depends on what the run before left in the cache. Run 2, the fastest, had the fewest reads in its busiest minute.
- **Network.** Data comes into the runner machine at 427 to 465 MiB/s on average, with peaks of 694 to 782 MiB/s.
- **Memory.** The runner machine uses at most 10,691 to 10,928 MiB (10.44 to 10.67 GiB). The job's peak is 7.29 to 7.61 GiB of its 13 GiB.

### Checks

Athena read both tables of each run. Postgres's values were read once after the load. For each table, the checks compare these values:

- the rows, the distinct ids, and the lowest and highest id;
- the sums of two numeric columns, rounded to 9 decimal places, with their non-null counts;
- the sum of an integer column.

Every value is equal in all three runs:

| | `trips` | `fhv_trips` |
|---|---:|---:|
| Rows | 209,789,604 | 109,089,759 |
| Distinct ids | 209,789,604 | 109,089,759 |
| Ids | 1 to 209,789,604 | 1 to 109,089,759 |
| First sum, rounded to 9 places | `trip_distance`: 1,090,230,366.102 | `trip_miles`: 514,540,240.972 |
| Second sum, rounded to 9 places | `total_amount`: 2,235,172,852.42 | `base_passenger_fare`: 1,823,773,933.85 |
| Sum of an integer column | `passenger_count`: 353,387,377 | `pickup_location_id`: 15,126,851,574 |

The summed columns have no NULLs, so each non-null count equals the rows. `trip_distance` and `total_amount` arrive as `float64`, so Athena sums `CAST(x AS DECIMAL(38, 9))` and Postgres sums `round(x, 9)`. The `fhv_trips` columns arrive as `decimal(38, 9)`, and compare as they are.

### Cost

At on-demand list prices in `ap-southeast-2`:

| Resource | Price |
|---|---:|
| RDS `db.r8g.2xlarge` | $1.148 an hour |
| RDS storage, 400 GB of gp3 | $0.138 a GB-month |
| EC2 `c8g.2xlarge` | $0.4152 an hour |
| EBS, 150 GB of gp3 at 6,000 IOPS and 500 MB/s | $0.069 an hour |
| Public IPv4 address | $0.005 an hour |

The two machines together cost about $1.713 an hour. At that rate, a timed sync costs about $0.08. With its seed sync and the 120 seconds of idle time, a run takes 4.9 to 5.2 minutes and costs $0.14 to $0.15.

## Repeat the runs

These steps make one workload's machines, load its data, and time its runs. They create resources that cost money until you delete them.

`VERSION` is the release to measure, and `TAG` is its tag. For the results on this page, `VERSION` is `0.2.0-alpha` and `TAG` is `v0.2.0-alpha`. To measure the newest release, use `{{version}}` and `v{{version}}`.

### Before you start

- An AWS account, with the AWS CLI configured for a region that has the instance types above.
- A VPC subnet in one availability zone, and an RDS subnet group that includes it.
- An SSH key pair for the instance.

### Create the AWS resources

1. Create an S3 bucket in the region, and block public access to it.
2. Create an IAM role that EC2 can assume, with this policy, and an instance profile with the role. Replace `BUCKET`, `REGION` and `ACCOUNT`. The Glue databases on this page start with `oetl_`.

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       { "Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"], "Resource": "arn:aws:s3:::BUCKET" },
       {
         "Effect": "Allow",
         "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
         "Resource": "arn:aws:s3:::BUCKET/*"
       },
       {
         "Effect": "Allow",
         "Action": ["glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase", "glue:DeleteDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable"],
         "Resource": ["arn:aws:glue:REGION:ACCOUNT:catalog", "arn:aws:glue:REGION:ACCOUNT:database/oetl_*", "arn:aws:glue:REGION:ACCOUNT:table/oetl_*/*"]
       }
     ]
   }
   ```

3. Create two security groups: one for the instance that allows SSH from your address, and one for RDS that allows port 5432 from the instance's group.
4. Create the RDS instance in the instance's availability zone. For the workload derived from TPC-DS:

   ```sh
   aws rds create-db-instance --db-instance-identifier SOURCE --db-instance-class db.r8g.2xlarge \
     --engine postgres --engine-version 18.6 --master-username openetl --master-user-password PASSWORD \
     --db-name tpcds --allocated-storage 500 --storage-type gp3 --iops 12000 --storage-throughput 500 \
     --availability-zone ZONE --no-multi-az --db-subnet-group-name SUBNET_GROUP \
     --vpc-security-group-ids RDS_GROUP --no-publicly-accessible --backup-retention-period 0 \
     --no-enable-performance-insights --monitoring-interval 0
   ```

   For the workload derived from TPC-H, use `--db-instance-class db.m8g.xlarge --db-name tpch --allocated-storage 200 --storage-type gp3`, without `--iops` and `--storage-throughput`. For the taxi trips, use `--engine-version 18.7 --db-name taxi --allocated-storage 400 --storage-type gp3`, also without `--iops` and `--storage-throughput`. Add `--db-parameter-group-name` with a parameter group of the `postgres18` family that sets `rds.logical_replication` to `1`.
5. Launch the instance from the Ubuntu 24.04 `arm64` image that the SSM parameter `/aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id` names. Require IMDSv2, and set the metadata hop limit to 2, so that the runner's container can use the instance's role:

   ```sh
   aws ec2 run-instances --instance-type c8g.2xlarge --image-id AMI --key-name KEY \
     --security-group-ids INSTANCE_GROUP --subnet-id SUBNET --associate-public-ip-address \
     --iam-instance-profile Name=PROFILE \
     --metadata-options "HttpTokens=required,HttpPutResponseHopLimit=2" \
     --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=300,Iops=16000,Throughput=1000,VolumeType=gp3,DeleteOnTermination=true}"
   ```

   For the workload derived from TPC-H, use `c8g.large` with `VolumeSize=40,Iops=3000,Throughput=125`. For the taxi trips, use `VolumeSize=150,Iops=6000,Throughput=500`.
6. On the instance, install the tools. Ubuntu 24.04 has no package for the AWS CLI, so install AWS CLI v2 from its archive.

   ```sh
   sudo apt-get update
   sudo apt-get install -y docker.io docker-compose-v2 jq unzip curl postgresql-client sysstat python3 zstd ethtool
   sudo usermod -aG docker "$USER"
   ```

   Sign out and sign in again, so that your shell has the `docker` group before you start the stack.

   For the taxi trips, also install the DuckDB 1.4.4 CLI. For scale factor 50 of the workload derived from TPC-H, the loader needs the DuckDB 1.4.1 CLI.

### Install OpenETL

1. Download the release's Compose file, its CLI and its checksums, and check them:

   ```sh
   mkdir -p ~/rel ~/bin ~/stack && cd ~/rel
   curl -fsSLO https://github.com/openetl-io/openetl/releases/download/TAG/checksums.txt
   curl -fsSLO https://github.com/openetl-io/openetl/releases/download/TAG/compose.yml
   curl -fsSLO https://github.com/openetl-io/openetl/releases/download/TAG/openetl-cli_VERSION_linux_arm64.tar.gz
   grep -E " (compose.yml|openetl-cli_VERSION_linux_arm64.tar.gz)$" checksums.txt | sha256sum -c
   tar -xzf openetl-cli_VERSION_linux_arm64.tar.gz --strip-components=1 -C ~/bin openetl-cli_VERSION_linux_arm64/openetl
   cp compose.yml ~/stack/
   ```

   `sha256sum` prints `OK` for each file.
2. In `~/stack`, write `.env` with the two lines of [Install with Docker Compose](./compose.md#install), and add the lines of [What ran](#what-ran).
3. Start the stack, and give the CLI the API's URL and the owner key:

   ```sh
   cd ~/stack
   docker compose up -d
   export OPENETL_API_URL=http://127.0.0.1:8080
   export OPENETL_API_KEY="$(docker compose exec -T server cat .openetl/admin.key)"
   ```

4. Apply the two connectors:

   ```sh
   ~/bin/openetl connector apply oci://ghcr.io/openetl-io/openetl-connectors/postgres:VERSION
   ~/bin/openetl connector apply oci://ghcr.io/openetl-io/openetl-connectors/iceberg:VERSION
   ```

### Load the data

Give `psql` and `vacuumdb` the RDS instance, the user and the database: `tpcds`, `tpch` or `taxi`.

```sh
export PGHOST=RDS_ENDPOINT PGUSER=openetl PGDATABASE=DATABASE PGSSLMODE=require
```

Put the password in `~/.pgpass`, in a line `RDS_ENDPOINT:5432:*:openetl:PASSWORD`. If the password has a `:` or a `\`, put a `\` before each of them in that line. Then run `chmod 600 ~/.pgpass`, so that only you can read it.

Then follow the steps of the workload's Data section:

- [24 tables derived from TPC-DS](#data)
- [8 tables derived from TPC-H](#data-1)
- [New York City taxi trips](#data-2)

Add the primary keys before you apply the connections, so that `openetl discover` finds them and the plan splits each table by its key.

### Apply the connections

1. Store the source's URL as the secret that the documents name, `tpcds-dsn`, `tpch-dsn` or `taxi-dsn`. Enter `postgres://openetl:PASSWORD@HOST:5432/DATABASE?sslmode=require` at the prompt:

   ```sh
   ~/bin/openetl secret set tpcds-dsn
   ```

2. Save the workload's example file, such as [`examples/benchmark-24-tables.yaml`](../../examples/benchmark-24-tables.yaml), with your bucket's name and region. Split it into the connections and the pipeline. Apply the connections, then discover both sources:

   ```sh
   ~/bin/openetl apply -f connections.yaml
   ~/bin/openetl discover tpcds-l8
   ~/bin/openetl discover tpcds-seed
   ```

3. Check the plan of the pipeline. It shows the job's size and `split by` with each table's primary key:

   ```sh
   ~/bin/openetl plan -f pipeline.yaml
   ```

### Time the runs

For each run, use a new pipeline name and a new namespace.

1. Apply the pipeline with the seed source, such as `source.ref: tpcds-seed`, in `seed.yaml`. This starts the seed sync, which creates the tables. Wait until `openetl syncs PIPELINE` shows it as `succeeded`.
2. Apply the pipeline with the full source, without its checks, and wait 120 seconds:

   ```sh
   ~/bin/openetl apply --no-check -f pipeline.yaml
   sleep 120
   ```

3. Start the sync, and wait until it succeeds:

   ```sh
   ~/bin/openetl sync PIPELINE
   ~/bin/openetl syncs PIPELINE
   ```

4. Read the sync's `startTime` and `endTime`, as [How a run is timed](#how-a-run-is-timed) shows. The run's time is `endTime` minus `startTime`.

To sample the machine as these runs did, start `vmstat -t -w 1` and `pidstat -u -r -h 5` before step 3. Stop them after step 4.

### Check the result

Run the checks in Athena, against the run's Glue database. For each table, compare `count(*)` with Postgres's. Compare the sums and non-null counts of each Checks section with the same query in Postgres, for example:

```sql
SELECT CAST(sum(ss_net_paid) AS varchar) || '/' || CAST(count(ss_net_paid) AS varchar) FROM store_sales
```

For the taxi trips, Athena sums `CAST(x AS DECIMAL(38, 9))`, and Postgres sums `round(x, 9)`.

### Delete the resources

Delete the RDS instance without a final snapshot, terminate the instance, and delete the bucket's objects and the bucket. Delete the Glue databases, the IAM role, its policy and instance profile, the security groups, the key pair, and the RDS parameter group if you made one.

## What these numbers do not show

These results describe these workloads on these machines, and nothing more:

- **One route.** Every run reads Postgres with the `postgres` connector and writes Iceberg tables in Glue and S3 with the `iceberg` connector. Other sources, destinations and catalogs have other rates.
- **Full refresh only.** Every run reads each table in full (`full_refresh`) and replaces the destination table (`overwrite`). Incremental reads, CDC and continuous pipelines work differently, and these numbers do not describe them.
- **No transforms.** No run has a SQL, Spark or Flink step.
- **One job on one runner.** No run uses `resources.workers`.
- **One region.** The runner, the source and the bucket are in `ap-southeast-2`, and the runner and the source in one availability zone. A longer network path between them gives other times.
- **One kind of processor.** Every machine is a Graviton4 `arm64` instance.
- **The source's cache.** In the 8-vCPU runs derived from TPC-DS, the source served every read from memory. In the taxi runs, it read part of the data from disk, and how much depended on the run before. In the 2-vCPU runs derived from TPC-H, the source read part of the data from disk at scale factor 10. At scale factor 50, it read from disk throughout.
- **An idle source.** No other work ran on the source during a timed sync.
- **Tables that exist.** The seed sync creates the tables, and the time does not include it or the connectors' download.
- **A shared machine.** OpenETL's own Postgres, server and orchestrator run on the runner's machine, and share its CPU and memory.
- **Few runs.** Each workload has three runs, and scale factor 50 has one. The spread, and the standard deviation where this page gives it, are the only measures of variation.
- **List prices.** A cost is the on-demand list price multiplied by the uptime, not a bill.
- **Not TPC results.** The workloads derived from TPC-DS and derived from TPC-H use only the generated data. They run no queries, and they time a load.
- **One release.** Every run used `v0.2.0-alpha`. Other releases can give other numbers.

This workload is derived from the TPC-DS Benchmark and is not comparable to published TPC-DS Benchmark results, as this implementation does not comply with all requirements of the TPC-DS Benchmark.

This workload is derived from the TPC-H Benchmark and is not comparable to published TPC-H Benchmark results, as this implementation does not comply with all requirements of the TPC-H Benchmark.
