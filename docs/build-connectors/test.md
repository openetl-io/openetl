---
title: Test connector conformance
description: Use the conformance kit to check source and destination protocol behavior.
---

Use the conformance kit after you build a connector. The example command uses the source from [Write a source in Go](./write-source.md).

## Test it with the conformance kit

The [conformance kit](../../sdk/go/conformance) tests connectors through the protocol. It supports connectors in any implementation language.

Source checks cover:

- Specifications and configuration schemas.
- Connection checks and their items.
- A preflight of every discovered stream.
- Discovery, reads and resumption.
- Reads that stop at a redo's `until` or complete at its `complete_at`.

Destination checks cover:

- A preflight of the suite's streams.
- Commits, recovery at each commit step and fencing.
- Write modes, schema changes, types and type-change refusal.
- Holds and writer lanes.

For a destination with lane commits, checks commit each lane independently. They settle each committable on a writer of its lane. They also rewrite a lost lane's part.

Run it from the CLI:

```sh
openetl connector test --config config.json ./bin/source-greetings
```

```text
PASS spec
PASS check
SKIP errors
    the suite has neither BadCredentials nor Unreachable
SKIP setup
    the connector has no setup
PASS discover
PASS preflight
PASS read/full
SKIP read/resume
    no stream sent a resume point
SKIP read/until
    the suite doesn't say the source stops at until
SKIP read/incremental
    the suite reads no stream incrementally
SKIP read/complete-at
    the suite doesn't say the source completes at complete_at
SKIP read/committed
    the source doesn't acknowledge, so the platform never calls Committed
SKIP read/continuous
    the suite reads no stream with cdc or stream
source-greetings: 13 checks, 5 passed, 0 failed, 8 skipped
```

`--bad-credentials` and `--unreachable` add configs for the errors check, and `--add-records-command` lets the incremental and complete-at checks add records upstream. A destination's checks that write need `--read-command`, which prints what the destination made visible; they are skipped without it. `openetl connector test -h` has the details.

The until check runs only for a source that stops at `until`: pass `--stops-at-until`, or set `StopsAtUntil` in Go. The check reads from a resume point, with the next resume point's state as `until`. It expects the records between the two and nothing past them, then a resume point and a completion at `until`. It also reads from the last resume point with no `until`, and expects a completion at the state the first read completed at.

The complete-at check runs only for a source that completes at `complete_at`: pass `--stops-at-completion` and `--add-records-command`, or set `StopsAtCompletion` and `AddRecords` in Go. For each stream that completed with a state, the check first adds records upstream. It leaves out a stream that gets none, since a source that ignores `complete_at` would pass there. It then reads from the stream's last resume point, with the state the stream completed at as `complete_at`. It expects the records between the two and none that the first read did not return. It then expects a completion at `complete_at`, and nothing after it.

The kit reads each stream whole, so the until and complete-at checks give a stream's `until` and `complete_at`, not a split's. A source that reads splits gets the same fields on each `Split`, and its own tests cover that.

In Go, a connector's own tests run the same checks with `conformance.SourceSuite` or `conformance.DestinationSuite` and `Test(t)`, as every official connector does in its `conformance_test.go`.
