---
title: Test architecture
description: Explain the unit, simulation, end-to-end, chaos and workflow replay tests.
---

This page is for contributors who assess test coverage. [CONTRIBUTING.md](../../CONTRIBUTING.md) lists setup requirements and commands.

## How it's tested

Each suite checks a different part of OpenETL's behavior.

### Unit tests

`make test` runs unit tests without Docker.

### Commit protocol simulation

[`test/sim`](../../test/sim) runs the real runner with fake sources and destinations. A seed selects fault timing, failures and zombie attempts. Checks cover data loss, exactly-once duplicates and final rows under effectively-once delivery.

The `workers` kind models SQL on several workers with an exchange by key group. This kind does not run runner code. Its model keeps keyed state as object-storage runs named by the checkpoint record. It covers state restoration onto different lanes and workers, and run retention.

The model also covers recovery of a lost writer lane for a destination with lane commits. It checks two row-wise outputs. One writes independently on each lane. The other exchanges rows by key group.

Each `-broken=NAME` variant breaks one part of the protocol. `TestBroken` checks that the simulation detects each variant.

Use `make sim SEEDS=10000` to run more seeds. Use `go test ./test/sim -run 'TestSimulation/kafka' -seed=42 -v` to replay one seed with its trace.

### End-to-end tests

[`test/e2e`](../../test/e2e) runs the whole stack against Compose services and official connectors. Tests run one at a time.

`TestRunningTotalFromKafkaMatchesDuckDB` runs `OVER` over a bounded Kafka topic, on one pod and on two workers. DuckDB's CLI runs the same query over the same records, written as CSV, and the output must match it row for row. The test finds the CLI through `OPENETL_DUCKDB` or the `PATH`, and skips without it, except on CI. `TestDedupesThroughAKilledEngine` runs a first-row dedupe with a retention and a latest-row dedupe from Kafka into Postgres. It kills the engine once a checkpoint holds their state. Each key's first row within each retention must be written once. A row a retention or more after the key's last first row must be written as a new first row. The merged output must hold each key's latest row.

### Chaos tests

The chaos suite in `test/e2e` runs randomized pipelines under failures. It kills runners, engines and connectors. It also interrupts API access and injects object-storage failures. The suite checks that each row is written once.

Each chaos test starts its runs after all runners and pod launchers of its pod data plane have reported. If no launcher has reported, the plane places a pod's attempt on its runner.

When a run deletes its pipeline, the suite waits for the pipeline's teardowns to finish before the run's runners stop. A run fails if its teardowns don't finish. At the end of each test, a replication slot whose pipeline is gone fails the test, and the suite drops the slot.

Some runs place their attempts in pods, which pod launchers start as processes. A fault at a pod, a worker's pod or a pod's connector finds the process through the launcher's output. The first line of a pod gives only its attempt ID, and on Temporal the attempt ID does not contain the pipeline ID. The suite connects the attempt to its pipeline through the line of the attempt's engine, which gives both IDs. A run in pods fails if no launcher's output connects a pod to its pipeline. `TestChaos` fails if it makes faults at pods and none of them finds the process to kill.

A crash point fires at one of a pipeline's checkpoint writes and kills the process that wrote it, or the worker that commits. Runs can share a runner, so another run's fault can kill that runner first. The write then continues as if no crash point were armed, and the crash point is armed again for a later write. The run's fault counts show each such miss as `re-armed`. In `TestChaosWindows`, `TestChaosDedupe`, `TestChaosOver` and `TestChaosWorkers`, a run fails on a crash point that never kills. It also fails if the point finds nothing to kill five times in a row.

The data check stops at the first duplicate or phantom row. Rows that are missing at that point can still arrive, so the failure says they were still settling.

When a run fails with missing rows, the failure also gives the dispatch state of the pipeline's newest attempt. The state is pending, placed on a runner or pod launcher, running or finished. For a placement on a pod launcher, the failure says if the launcher has claimed the attempt. It also gives the start or lease deadline, and when that runner or launcher last reported.

In `TestChaosChecks`, a run with a held plant writes to its source for 15 seconds. The 15 seconds start when the pipeline has written the rows that the source held before the run. The run fails if the pipeline does not write those rows within `-chaos.settle`.

`TestChaos` can also add faults to the control plane:

- `-chaos.postgres-restart` restarts or kills the Compose Postgres at about this interval. A restart that is in progress when the runs end waits up to two minutes for Postgres to accept connections before the next test starts.
- `-chaos.orchestrator-kill` runs two more orchestrators on DBOS, and kills one at about this interval. Then it starts that orchestrator again.

The extra orchestrators are the test binary, which takes the flags of `openetld orchestrator`. They use the suite's short timings, the same as the API server and the orchestrator in the test process. Because they are the same binary, they run the same DBOS application version, so each orchestrator takes new workflows. When the runs end, the suite stops the extra orchestrators. It then waits until the orchestrator in the test process runs DBOS's latest version. `TestChaos` fails if that takes more than two minutes.

Some runs use group windows over Kafka, on one pod or several workers. These runs kill a worker or the coordinator. Records arrive in event-time order within each partition, so these cases have no late records. The suite compares each window's row with a window model.

`TestChaosDedupe` and `TestChaosOver` run the same faults with a first-row dedupe and with `OVER`. A dedupe's run compares each key's first rows with a model of the retention. An `OVER` run compares each record's row, by its partition and offset, with its key's running count and sum, peers included. `TestChaosChecks` plants a lost and a changed row in each of their outputs, and fails unless the check finds it.

When a shard of `chaos.yml` fails, the workflow keeps two artifacts for seven days. `chaos-logs-SEED` has the output of each runner and pod launcher. `chaos-compose-SEED` has the state and logs of each Compose service, and the kernel's out-of-memory lines.

### Event-time property tests

[`event_time.rs`](../../crates/engine/src/dataflow/tests/event_time.rs) runs seeded group windows through the engine's dataflow. Cases use one engine or several engines connected through the worker mesh. They compare output, late rows and watermarks with a model.

Some cases kill a job at a barrier or between barriers. They restore the last committed checkpoint onto different numbers of lanes and engines. These cases also check the window counts in epoch operator reports. Counts must stay nonnegative and reach zero at the end of input.

### Workflow replay tests

Recorded histories in `internal/orchestration/workflows/testdata` replay against current workflows. A change that breaks a running Temporal workflow fails a replay test.
