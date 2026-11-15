---
title: Quarantine files
description: Where a pipeline's quarantined rows are kept, the format of each line, the codes that say why, and how long the files are kept.
---

OpenETL quarantines a row it cannot write: it sets the row aside, with why, and writes the rest of the batch. The quarantined rows are not in the destination. They are in files in the data plane's object storage, the store that `openetld runner --checkpoints` names. This page describes those files.

The path, the fields of a line, the codes and the values of `notHeld` are a contract. A change to any of them is a change to this page. You can read the files with your own tools, and load the rows again yourself after you fix their cause.

[Holds and quarantine](../get-started/concepts.md#holds-and-quarantine) explains when OpenETL holds a value back in a row it writes, and when it quarantines the whole row.

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
| `GENERATION` | The generation of the attempt that wrote the file. Each attempt of a sync or a continuous run has a new one. |
| `EPOCH` | The epoch the rows were read in. |
| `STREAM` | The stream's name, with characters that are not safe in a path percent-encoded: `public/orders` is `public%2Forders`. |
| `LANE` | The lane of the attempt that wrote the file, from 1. The first lane's file has no number. |
| `_outputs/`, `OUTPUT` | A transform's output. A row that a statement of a SQL transform cannot evaluate, or that does not fit the output's types, is in its output's file. |

The API, the `openetl quarantine` command and the `stream.quarantined` event name a file by its **path**, the part after `_quarantine/`, such as `3/17/orders.jsonl`. They give its full URL too.

## The lines

A file is JSON Lines in UTF-8: one JSON object for each quarantined row, each followed by a newline. These are the lines of a stream `orders` that declares `id` as `int64` and `amount` as `decimal(10, 2)`, and reads three records:

```text
{"stream":"orders","code":"type","column":"amount","reason":"type: column \"amount\" isn't a number","record":"{\"id\": 7, \"amount\": \"12.5O\"}"}
{"stream":"orders","code":"type","column":"id","reason":"type: column \"id\" isn't a number","record":"{\"id\": \"8a\", \"amount\": \"3.25\"}"}
{"stream":"orders","code":"object","reason":"row 2 is not a JSON object","record":"[9]"}
```

With `unfitValues: hold`, the stream holds `amount`'s value and writes the first row. It cannot hold a value of `id`, which is its primary key, so it quarantines the second row with `"notHeld":"key"`. The third record is not an object, so it has `"notHeld":"record"`:

```text
{"stream":"orders","code":"type","column":"id","notHeld":"key","reason":"type: column \"id\" isn't a number","record":"{\"id\": \"8a\", \"amount\": \"3.25\"}"}
{"stream":"orders","code":"object","notHeld":"record","reason":"row 2 is not a JSON object","record":"[9]"}
```

| Field | Type | Present | Meaning |
|---|---|---|---|
| `stream` | string | Always | The stream, or the transform's output, the row was quarantined from. |
| `code` | string | Always | What was wrong, from [the codes](#codes). |
| `column` | string | Where one column caused it | The column. In a stream's file, it is the column as the source names it. In an output's file, it is the output's column for `type`, `range`, `precision` and `nan`, and the input stream's column for `unsent` and `notHeld: transform`. |
| `notHeld` | string | Where the stream holds values that do not fit, and the row was quarantined instead | Why the row's value could not be held, from [the values of notHeld](#why-a-value-was-not-held). |
| `reason` | string | Always | Why, as a sentence for people. It names the column, and never contains the value. Its words can change from one version to the next, so read `code`, `column` and `notHeld` in your tools. |
| `record` | string | Always | The row, as text. [What record holds](#what-record-holds) describes it. |

Fields other than `record` never hold a value of the row. Ignore fields that this page does not list.

## Codes

| `code` | Meaning | `column` |
|---|---|---|
| `json` | The source sent a record that is not JSON. | None |
| `object` | The source sent JSON that is not an object. | None |
| `type` | A value is not of its column's type, such as text in an integer column, or a date in another format. | The column |
| `range` | A value is outside its type's range. This includes an infinite date or timestamp that `types.infinity` does not clamp. | The column |
| `precision` | A decimal has more digits than its column takes. | The column |
| `nan` | A value is NaN, and its column's type cannot hold NaN. | The column |
| `statement` | A statement of the SQL transform cannot evaluate the row, such as a cast that fails, a division by zero, or arithmetic whose result does not fit its type. | None |
| `unsent` | A change from the source's log updated the row but did not send a column that the statement needs. | The column it did not send |
| `late` | The row came after its group window closed, later than the window's allowed lateness. | None |
| `event_time` | The row's event time is null, so it is in no group window. | None |
| `aggregate` | An aggregate of a group window does not fit its type, such as a sum past the range of `BIGINT`. | None |

## Why a value was not held

A stream with `unfitValues: hold` holds a value that does not fit its column, and writes the rest of the row. A row it quarantines all the same has `notHeld`, which says why:

| `notHeld` | Meaning |
|---|---|
| `key` | The column is in the stream's primary key. |
| `cursor` | The column is the stream's cursor, which orders its rows. |
| `destination` | The destination cannot hold the column, such as a column it partitions by. |
| `not_null` | The destination's column does not take a null, and the stream sets no `heldValue` for it. |
| `held_value` | The column's `heldValue` does not fit the column's type. |
| `record` | The record is not JSON, or not an object, so it has no value to hold. |
| `transform` | A statement of the SQL transform reads the column, and cannot evaluate a value that is held. The row is quarantined from the statement's output. |

A stream without `unfitValues: hold` holds no value, so its lines have no `notHeld`.

## What record holds

`record` is a string, which a tool parses as JSON where it is JSON, such as with `fromjson` in `jq`:

- **A source that sends JSON records:** the record exactly as the source sent it. With `code` `json`, it is the text that is not JSON.
- **A source that sends Arrow batches:** a JSON object made from the row, with the source's column names. Numbers and booleans are JSON numbers and booleans, a null is `null`, and other values are text. A source that sends changes also sends the columns that carry each change's operation and order, and the object has them too.
- **An output's file:** the row of the input stream that the statement read, as above. With `code` `aggregate`, and with `statement` from a step after a group window, it is the window's row instead.

## How long the files are kept

- A lane writes its files of an epoch before the epoch's checkpoint names them, so a file is complete when its epoch commits.
- Only the files of committed epochs are kept. If an attempt stops before its epoch commits, the next attempt deletes that epoch's files and reads its rows again.
- The files of committed epochs are kept until the pipeline's `quarantine.retention` deletes them, or a rule of your object store does. Without either, they are kept.
- The control plane lists each file of a committed epoch, with its rows by cause, until its retention passes or the pipeline is deleted. It never keeps the records.

### Delete files after a period

Set `quarantine.retention` in the pipeline's spec, as a Go duration:

```yaml
spec:
  quarantine:
    retention: 720h
```

A run deletes the files of committed epochs written longer ago than `retention` when it starts, and every hour while it runs. The control plane stops listing them by the same rule. Unset, nothing is deleted.

### Delete files with a lifecycle rule

Your object store can delete the files itself. On S3, a lifecycle rule for one pipeline's files deletes each file 30 days after it is written:

```json
{
  "Rules": [
    {
      "ID": "openetl-quarantine-PIPELINE",
      "Status": "Enabled",
      "Filter": {"Prefix": "PREFIX/ORG/PIPELINE/_quarantine/"},
      "Expiration": {"Days": 30}
    }
  ]
}
```

`PREFIX/` is the path of the `--checkpoints` store, if it has one. Save the rule as `lifecycle.json` and apply it to the bucket:

```sh
aws s3api put-bucket-lifecycle-configuration --bucket BUCKET --lifecycle-configuration file://lifecycle.json
```

> [!WARNING]
> This command replaces every lifecycle rule the bucket has. Add the bucket's other rules to `Rules` first.

The control plane does not know what a lifecycle rule deletes, and lists the files until the pipeline's `quarantine.retention` passes.

## List and read the files

`openetl quarantine PIPELINE` lists a pipeline's files, newest first, and sums their rows by stream and cause. `--stream STREAM` and `--since 7d` narrow the list, and `--json` prints each file as a line of JSON. The API's method is `ListQuarantine`, at `GET /v1/workspaces/WORKSPACE/pipelines/PIPELINE/quarantine`, for viewers and above. Each committed epoch that quarantined rows also sends a [`stream.quarantined`](./events.md#when-type-is-streamquarantined) event, with its files and their rows by cause.

To read a file from S3, print the records of one code:

```sh
aws s3 cp s3://BUCKET/PREFIX/ORG/PIPELINE/_quarantine/3/17/orders.jsonl - | jq -c 'select(.code == "type") | .record | fromjson'
```

For the first file above, it prints:

```text
{"id":7,"amount":"12.5O"}
{"id":"8a","amount":"3.25"}
```
