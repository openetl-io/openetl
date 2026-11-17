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

### Chaos tests

The chaos suite in `test/e2e` runs randomized pipelines under failures. It kills runners, engines and connectors. It also interrupts API access and injects object-storage failures. The suite checks that each row is written once.

Some runs use group windows over Kafka, on one pod or several workers. These runs kill a worker or the coordinator. Records arrive in event-time order within each partition, so these cases have no late records. The suite compares each window's row with a window model.

### Event-time property tests

[`event_time.rs`](../../crates/engine/src/dataflow/tests/event_time.rs) runs seeded group windows through the engine's dataflow. Cases use one engine or several engines connected through the worker mesh. They compare output, late rows and watermarks with a model.

Some cases kill a job at a barrier or between barriers. They restore the last committed checkpoint onto different numbers of lanes and engines. These cases also check the window counts in epoch operator reports. Counts must stay nonnegative and reach zero at the end of input.

### Workflow replay tests

Recorded histories in `internal/orchestration/workflows/testdata` replay against current workflows. A change that breaks a running Temporal workflow fails a replay test.
