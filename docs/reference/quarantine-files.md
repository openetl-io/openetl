---
title: Quarantine files
description: Where OpenETL keeps a pipeline's quarantined rows, the format of each line, the codes that say why, and how long OpenETL keeps the files.
---

OpenETL quarantines a row that it cannot write. It sets the row aside with the reason, and writes the rest of the batch. The destination does not get the quarantined rows. OpenETL keeps them in files in the data plane's object storage, the store that `openetld runner --checkpoints` names. This page describes those files.

The path, the fields of a line, the codes and the values of `notHeld` are a contract. A change to any of them is a change to this page. You can read the files with your own tools. After you fix the cause, you can load the rows again yourself.

[Holds and quarantine](../get-started/concepts.md#holds-and-quarantine) explains when OpenETL holds a value back in a row that it writes, and when it quarantines the whole row.

## Where the files are

Each lane of a run writes one file for each stream that quarantined rows in an epoch:

```text
STORE/ORG/PIPELINE/_quarantine/GENERATION/EPOCH/STREAM.jsonl
STORE/ORG/PIPELINE/_quarantine/GENERATION/EPOCH/STREAM.LANE.jsonl
STORE/ORG/PIPELINE/_quarantine/_outputs/GENERATION/EPOCH/OUTPUT.jsonl
```

| Part | Meaning |
|---|---|
| `STORE` | The runner's `--checkpoints` store, with its path if it has one. |
| `ORG`, `PIPELINE` | The IDs of the organization and the pipeline. |
| `GENERATION` | The generation of the attempt that wrote the file. Each attempt of a sync or a continuous run has a new generation. |
| `EPOCH` | The epoch in which the run read the rows. |
| `STREAM` | The stream's name. Characters that are not safe in a path are percent-encoded: `public/orders` is `public%2Forders`. |
| `LANE` | The lane of the attempt that wrote the file, from 1. The file of the first lane has no number. |
| `_outputs/`, `OUTPUT` | A transform's output. An output's file has the rows that a SQL statement cannot evaluate, and the rows that do not fit the output's types. |

The API, the `openetl quarantine` command and the `stream.quarantined` event name a file by its **path**. The path is the part after `_quarantine/`, such as `3/17/orders.jsonl`. They give the file's full URL too.

## The lines

A file is JSON Lines in UTF-8: one JSON object for each quarantined row, each followed by a newline. In this example, a stream `orders` declares `id` as `int64` and `amount` as `decimal(10, 2)`. It reads three records, and its file has these lines:

```text
{"stream":"orders","code":"type","column":"amount","reason":"type: column \"amount\" isn't a number","record":"{\"id\": 7, \"amount\": \"12.5O\"}"}
{"stream":"orders","code":"type","column":"id","reason":"type: column \"id\" isn't a number","record":"{\"id\": \"8a\", \"amount\": \"3.25\"}"}
{"stream":"orders","code":"object","reason":"row 2 is not a JSON object","record":"[9]"}
```

With `unfitValues: hold`, OpenETL holds the value of `amount` and writes the first row. It cannot hold a value of `id`, because `id` is the primary key. So it quarantines the second row with `"notHeld":"key"`. The third record is not an object, so its line has `"notHeld":"record"`:

```text
{"stream":"orders","code":"type","column":"id","notHeld":"key","reason":"type: column \"id\" isn't a number","record":"{\"id\": \"8a\", \"amount\": \"3.25\"}"}
{"stream":"orders","code":"object","notHeld":"record","reason":"row 2 is not a JSON object","record":"[9]"}
```

| Field | Type | Present | Meaning |
|---|---|---|---|
| `stream` | string | Always | The stream, or the transform's output, that quarantined the row. |
| `code` | string | Always | What was wrong, from [the codes](#codes). |
| `column` | string | Where one column caused it | The column. In a stream's file, it is the column as the source names it. In an output's file, it is the output's column for `type`, `range`, `precision` and `nan`. It is the input stream's column for `unsent` and for `notHeld: transform`. |
| `notHeld` | string | Where the stream holds values that do not fit, and OpenETL quarantined the row instead | Why OpenETL did not hold the row's value, from [the values of notHeld](#why-a-value-was-not-held). |
| `reason` | string | Always | Why, as a sentence for people. Its words can change from one version to the next, so read `code`, `column` and `notHeld` in your tools instead. For the code `statement`, the reason is the SQL engine's message, which can quote values of the row, as `record` does. For every other code, the reason never contains a value. If the line has a `column`, the reason names it. |
| `record` | string | Always | The row, as text. [What record holds](#what-record-holds) describes it. |

Only `record`, and the `reason` of a `statement` line, hold values of the row. Ignore fields that this page does not list.

## Codes

| `code` | Meaning | `column` |
|---|---|---|
| `json` | The source sent a record that is not JSON. | None |
| `object` | The source sent JSON that is not an object. | None |
| `type` | A value is not of its column's type, such as text in an integer column, or a date in another format. | The column |
| `range` | A value is outside its type's range. This includes an infinite date or timestamp with `types.infinity: unfit`. | The column |
| `precision` | A decimal has more digits than its column takes. | The column |
| `nan` | A value is NaN, and its column's type cannot hold NaN. | The column |
| `statement` | A statement of the SQL transform cannot evaluate the row. Examples are a cast that fails, a division by zero, and arithmetic whose result does not fit its type. | None |
| `unsent` | A change from the source's log updated the row, but did not send a column that the statement needs. | The column that the change did not send |
| `late` | The row came after its group window closed, later than the window's allowed lateness. | None |
| `event_time` | The row's event time is null, so the row is in no group window. | None |
| `aggregate` | An aggregate of a group window does not fit its type, such as a sum past the range of `BIGINT`. | None |

## Why a value was not held

A stream with `unfitValues: hold` holds a value that does not fit its column, and writes the rest of the row. If OpenETL quarantines a row of such a stream all the same, the row's line has `notHeld`, which says why:

| `notHeld` | Meaning |
|---|---|
| `key` | The column is in the stream's primary key. |
| `cursor` | The column is the stream's cursor, which orders its rows. |
| `destination` | The destination cannot hold the column, such as a column that it partitions by. |
| `not_null` | The destination's column does not take a null, and the stream sets no `heldValue` for it. |
| `held_value` | The column's `heldValue` does not fit the column's type. |
| `record` | The record is not JSON, or is not an object, so it has no value to hold. |
| `transform` | A statement of the SQL transform reads the column, and cannot evaluate a held value. OpenETL quarantines the row from the statement's output. |

A stream without `unfitValues: hold` holds no values, so its lines have no `notHeld`.

## What record holds

`record` is a string. Where it is JSON, a tool can parse it, such as with `fromjson` in `jq`:

- **A source that sends JSON records:** the record exactly as the source sent it. With the code `json`, it is the text that is not JSON.
- **A source that sends Arrow batches:** a JSON object made from the row, with the source's column names. Numbers and booleans are JSON numbers and booleans, a null is `null`, and other values are text. A source that sends changes also sends the operation and the order of each change in columns, and the object has these columns too.
- **An output's file:** the row of the input stream that the statement read, as above. With the code `aggregate`, the record is the window's row. With the code `statement` from a step after a group window, it is the window's row too.

## How long OpenETL keeps the files

- A lane writes its files of an epoch before the epoch's checkpoint names them. So a file is complete when its epoch commits.
- OpenETL keeps only the files of committed epochs. If an attempt stops before its epoch commits, the next attempt deletes that epoch's files and reads its rows again.
- OpenETL keeps the files of committed epochs until the pipeline's `quarantine.retention` deletes them. A rule of your object store can delete them too. Without either, OpenETL keeps them.
- The control plane lists each file of a committed epoch, with its rows by cause. It lists a file until the file's retention passes, or until you delete the pipeline. The control plane never keeps the records.

### Delete files after a period

Set `quarantine.retention` in the pipeline's spec, as a Go duration:

```yaml
spec:
  quarantine:
    retention: 720h
```

When a run starts, it deletes the files of committed epochs that are older than `retention`. While it runs, it does this again every hour. The control plane stops listing those files by the same rule. If `retention` is not set, OpenETL deletes no files.

### Delete files with a lifecycle rule

Your object store can delete the files itself. On S3, this lifecycle rule deletes each file of one pipeline 30 days after OpenETL writes it:

```json
{
  "Rules": [
    {
      "ID": "openetl-quarantine-PIPELINE",
      "Status": "Enabled",
      "Filter": {"Prefix": "PREFIX/ORG/PIPELINE/_quarantine/"},
      "Expiration": {"Days": 30},
      "NoncurrentVersionExpiration": {"NoncurrentDays": 1}
    }
  ]
}
```

`PREFIX/` is the path of the `--checkpoints` store, if it has one.

If the bucket has versioning, `Expiration` does not remove a file's data. It makes the file a noncurrent version, which keeps the rows. `NoncurrentVersionExpiration` then deletes that version 1 day later. In a bucket without versioning, it does nothing.

> [!WARNING]
> The command in step 3 replaces every lifecycle rule of the bucket. The bucket loses each rule that is not in `lifecycle.json`.

1. Save the rule as `lifecycle.json`.
2. If the bucket has other lifecycle rules, add them to `Rules`.
3. Apply the rules to the bucket:

   ```sh
   aws s3api put-bucket-lifecycle-configuration --bucket BUCKET --lifecycle-configuration file://lifecycle.json
   ```

The control plane does not know what a lifecycle rule deletes. It lists the files until the pipeline's `quarantine.retention` passes.

## List and read the files

`openetl quarantine PIPELINE` lists a pipeline's files, newest first, and adds up their rows by stream and cause. Use `--stream STREAM` and `--since 7d` to list fewer files, and `--json` to print each file as a line of JSON. The API method is `ListQuarantine`, at `GET /v1/workspaces/WORKSPACE/pipelines/PIPELINE/quarantine`. It needs the viewer role.

`openetl quarantine show PIPELINE PATH` prints the first lines of a file, at most 100. A runner of the pipeline's data plane reads the file, and the control plane does not keep the lines. The API method is `ReadQuarantineFile`, at `GET /v1/workspaces/WORKSPACE/pipelines/PIPELINE/quarantine:read`. It needs the editor role, and the audit log records each read. In the console, **Show records** does the same.

After each committed epoch that quarantined rows, OpenETL sends a [`stream.quarantined`](./events.md#when-type-is-streamquarantined) event. The event has the epoch's files and their rows by cause.

To read a file from S3, print the records of one code:

```sh
aws s3 cp s3://BUCKET/PREFIX/ORG/PIPELINE/_quarantine/3/17/orders.jsonl - | jq -c 'select(.code == "type") | .record | fromjson'
```

For the first file above, the command prints:

```text
{"id":7,"amount":"12.5O"}
{"id":"8a","amount":"3.25"}
```
