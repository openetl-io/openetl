---
title: Plan and apply changes
description: Review resource changes, delivery guarantees and SQL plans before you apply a pipeline.
---

Use this guide after you [define the resources](./define-pipeline.md). First, complete the [quickstart](../get-started/quickstart.md). The example uses its `faker` source.

## Plan, then apply

`openetl plan -f FILE` sends the documents to the API. The API checks them and reports proposed changes. The command does not change resources.

This example uses the quickstart’s `faker` source and the built-in `parquet` connector. It writes Parquet files on the runner’s disk at 06:00 each day:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: local-parquet
spec:
  connector: parquet
  config:
    url: file:///tmp/openetl-parquet
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: faker-to-parquet
spec:
  schedule: { cron: "0 6 * * *" }
  source: { ref: faker }
  destination: { ref: local-parquet }
```

Save the documents as `faker-to-parquet.yaml`. Review the proposed changes:

```sh
openetl plan -f faker-to-parquet.yaml
```

```text
would be created workspaces/default/destinations/local-parquet (version d10c3e1925e9)
would be created workspaces/default/pipelines/faker-to-parquet (version known after apply)

workspaces/default/pipelines/faker-to-parquet (version known after apply)
  job size: s (1 vCPU, 2 GiB), the default, in pool default
  streams:
    users      append  exactly_once  exact replay
    purchases  append  exactly_once  exact replay
  partitioning:
    users      unpartitioned -> exchange by key group on id -> writer, needs id
    purchases  unpartitioned -> exchange by key group on id -> writer, needs id
```

The plan has two parts:

- **A line per document** shows its planned state and version. Pipelines affected by a source or destination change also get new versions, even if the file does not define them. Their lines end with "which uses what changed".
- **A section per pipeline**, which can have:
  - `changes`: each field that changes, as `field: before -> after`.
  - `job size`: how much CPU and memory its jobs get, what set that, and the pool of runners it runs in.
  - `transform.sql`: for a pipeline with a SQL transform, each statement and its output. [A SQL transform in the plan](#a-sql-transform-in-the-plan) explains it.
  - `streams`: each stream's write mode, its [delivery guarantee](../get-started/concepts.md#delivery-guarantees), and the reason for that guarantee.
  - `partitioning`: how each stream's rows are spread over the job's lanes on their way to the destination, and where they are exchanged between lanes. [Partitioning in the plan](#partitioning-in-the-plan) explains it.
  - `problems`: what stops the pipeline from running. `plan` exits with an error, and `apply` refuses the whole file.
  - `warnings`: what is allowed but worth a second look.
  - `notes`: what the plan could not check, and why.

The streams section requires two sources of information:

- **The source catalog.** `openetl apply` and `openetl discover NAME` refresh it. Until discovery completes, the plan checks only delivery settings.
- **Destination capabilities.** The connector package declares them. For a local executable without a package, the plan explains that the runner checks guarantees before each sync.

A plan with problems looks like this one, for the same pipeline made continuous:

```text
workspaces/default/pipelines/faker-to-parquet (version known after apply)
  job size: s (1 vCPU, 2 GiB), the default, in pool default
  streams:
    users      append  exactly_once  exact replay
    purchases  append  exactly_once  exact replay
  partitioning:
    users      unpartitioned -> exchange by key group on id -> writer, needs id
    purchases  unpartitioned -> exchange by key group on id -> writer, needs id
  problems:
    stream "users" is read with full_refresh, whose reads end, and a continuous pipeline needs streams read with cdc or stream
    stream "purchases" is read with full_refresh, whose reads end, and a continuous pipeline needs streams read with cdc or stream
openetl: the plan has problems, so apply would refuse it
```

### A SQL transform in the plan

For `transform.sql`, the control plane sends statements and stream schemas to a runner in the pipeline's data plane. The runner's `openetl-engine` plans the statements and returns the result.

Schemas come from the source catalog. If the catalog declares no columns for a stream, OpenETL uses schemas recorded by earlier syncs. Each sync plans the statements again at startup. It uses the source's declared columns and schemas from the last checkpoint.

This pipeline reads a source named `shop` whose catalog declares the columns of `customers` and `orders`. The source does not split either stream by key:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: shop-to-parquet
spec:
  schedule: { every: 1h }
  source: { ref: shop }
  destination:
    ref: local-parquet
    tables: { contacts: customer_contacts }
  transform:
    sql: |
      INSERT INTO customers
      SELECT id, lower(email) AS email, created_at
      FROM customers;

      INSERT INTO contacts
      SELECT id AS customer_id, email AS contact_email
      FROM customers
      WHERE email IS NOT NULL;
```

Its plan, without the job size:

```text
workspaces/default/pipelines/shop-to-parquet (version known after apply)
  transform.sql:
    1:1  INSERT INTO customers  row_wise  key id           no exchange  reads customers; replaces stream customers
    5:1  INSERT INTO contacts   row_wise  key customer_id  no exchange  reads customers
    output customers: id int64, email utf8, created_at timestamp[us, tz=UTC]
    output contacts: customer_id int64, contact_email utf8
    passed through: orders
  streams:
    orders                         append  exactly_once  exact replay
    customers                      append  exactly_once  a transform's output over exact inputs
    contacts -> customer_contacts  append  exactly_once  a transform's output over exact inputs
  partitioning:
    orders     unpartitioned -> exchange by key group on id -> writer, needs id
    customers  unpartitioned -> statement 1:1 -> exchange by key group on id -> writer, needs id
    contacts   unpartitioned -> statement 5:1 -> exchange by key group on customer_id -> writer, needs customer_id
```

The `transform.sql` section has:

- **A line per statement:** the `INSERT` position, output, statement class and output primary key. It also identifies lane exchanges and input streams and views. A `row_wise` statement runs on the lane that read its rows, without an exchange. A `group_window` statement exchanges rows by its group keys. An output without a primary key cannot use `merge`.
- **The columns of each output,** with their types.
- **The streams that pass through:** the source's streams that no statement writes. They reach the destination as they would without a transform.
- **The streams that are not delivered,** when `transform.passthrough` is `none`.

[Write a SQL transform](./sql-transforms.md) walks through a transform from its statements to what the destination gets.

A statement that writes a source stream's name replaces that stream: the output is written in its place. The output takes the stream's entry in `destination.tables` and its `require`. The `streams` section lists what the destination gets: the outputs, and the streams that pass through.

The output also takes the stream's `unfitValues`, `held`, `heldValue`, `groups` and `typeChanges`, and holds columns as the stream would without a transform. The `streams` section says `columns' types change in place` for each output whose `typeChanges` is `allow`. A setting that applies to nothing the destination gets is a problem, such as `collapse` on a stream that a statement replaces. [Settings and holds of outputs](../reference/specs/pipeline.md#settings-and-holds-of-outputs) lists these cases.

SQL errors appear under `problems`, with their line and column. Correct these errors before you apply the pipeline.

[SQL restrictions and errors](../reference/sql-errors.md) lists unsupported statements and operations.

If the statements cannot be planned, a note that starts with `transform.sql wasn't planned` says why, and the `streams` section is empty. This happens when:

- No runner of the pipeline's data plane takes the request within 30 seconds.
- The source has not been discovered.
- The catalog declares no columns for a stream that the SQL names, and no sync has recorded its schema yet.
- A runner takes the request and fails to plan it. This is a fault in OpenETL, so the plan also has a warning that starts with `OpenETL failed to plan transform.sql`. If `openetl apply` creates or changes the pipeline, it prints the warning too.

### Partitioning in the plan

A job reads, transforms and writes its streams in parallel **lanes**. The `partitioning` section has a line for each destination stream. Each line shows the distribution of rows across lanes. It also shows where rows move between lanes.

A line starts with what the source gives:

- `split by COLUMNS`: the source splits the stream by these columns. All rows with the same values are in one split, and a split's rows stay on one lane. The catalog says whether a source splits a stream by its primary key. A stream read with `cdc` is never split by key.
- `unpartitioned`: rows with the same values can be on any lane.

Then come the operators that the rows pass, in order. Each statement of `transform.sql` that writes the stream is shown by its line and column, and the destination's **writer** comes last. `needs COLUMNS` says that an operator needs all rows with the same values of these columns on one of its lanes. The writer of a stream with a primary key needs its key, so that one writer lane writes each key's rows, in order. A `row_wise` statement needs nothing.

If the rows do not meet an operator's need, an **exchange by key group** comes before the operator. The exchange sends each row to the lane that owns the row's key group, a hash of its values of the needed columns. On a job with [workers](./workers.md), that lane can be on another worker. The plan places an exchange only there. Rows spread by columns `K` meet a need for columns `J` if each column of `K` is in `J`. For example, rows split by `id` meet the writer's need for `id`, so the stream needs no exchange.

The line shows a new partitioning before an operator where it changes. A statement keeps a partitioning while its columns pass through unchanged, with their new names. In [Write a SQL transform](./sql-transforms.md#plan-it), `split by order_id` follows a statement that renames `id` to `order_id`. A statement that computes or drops one of the columns ends the partitioning. An exchange then comes before the writer only if the writer has a need: an output without a primary key needs nothing. `on one lane` means that the destination takes one writer lane, so every lane's rows go to it. This meets every need without an exchange.

Each writer lane owns a range of the pipeline's key groups. Each processing lane owns part of one writer lane's range. Thus, a lane always writes to the same writer lane.

### Apply the plan

When the plan is what you want, apply the same files:

```sh
openetl apply -f faker-to-parquet.yaml
```

An apply stores all documents together or stores none. Its output identifies each resource as "created", "updated" or "unchanged", followed by warnings.

After the apply, a runner runs a [preflight](../get-started/concepts.md#checks-and-preflights) of each pipeline that the apply created or changed. The preflight checks the pipeline's source and destination, plans the sync, and checks the streams that the pipeline reads and writes. The runner also checks each other source and destination that changed, and discovers those sources. The output shows `checked` with the names of those that passed. Under `the apply stands, but its checks found:`, it lists each failure and each warning, with its fix, as [`apply`](../reference/pipeline-cli.md#apply-and-plan) shows. A failure does not undo the apply: the applied documents stay, and a new pipeline still starts. To skip all of this, use `--no-check`. To run a pipeline's preflight at any time, use `openetl preflight NAME`.

A new scheduled pipeline starts its first sync immediately. An updated pipeline waits for its schedule or a manual `openetl sync`.

`openetl apply --dry-run` performs apply validation without storing changes. It does not print the plan; use `openetl plan` to review the planned changes.

## Versions

Each applied document is stored as an immutable version identified by its content hash. A pipeline version pins its source, destination and dbt project versions. Changing a source therefore creates a new version of every pipeline that uses it. Each sync runs one version.

```sh
openetl versions faker-to-postgres
```

```text
VERSION                                                           CURRENT  CREATED              PROVENANCE
edab40da6d76c1aba235c7cd81c5618db8af2206c759539c5baea80a584560b3           2026-11-08 12:30:15  cli:examples/faker-to-postgres.yaml#3
9e052e7e6881672dbdcfcc4356f04f592bdfbd9777622e2d2a877eb1530db134  *        2026-11-08 12:28:23  cli:examples/faker-to-postgres.yaml#3
```

A pipeline version with `transform.sql` also stores its SQL plan. The plan records:

- Each output's columns and primary key.
- The ID of each statement that keeps state.
- The IDs of earlier statements whose state this version drops.

The API returns this information in the version's `sql_plan`.

Provenance identifies the origin of a version. Here, it is the third document in a file applied with the CLI. Versions are identified by content. To restore an earlier pipeline definition, revert its git commit and apply the file. This does not undo changes already written to the destination.
