//! The versions of a keyed stream's records across syncs, through the
//! record and a restore: a version read again at one value of the cursor
//! with other values gets an ID of its own, and one read again unchanged
//! keeps its ID.

use std::path::Path;

use openetl_proto::engine::v1::{KeyGroupRange as ProtoRange, StartJobRequest, StateSnapshot};

use super::*;

const OP: &str = "versions.users";

/// What a sync restores: the record before it, if any.
#[derive(Clone, Default)]
struct Restored {
    snapshots: Vec<StateSnapshot>,
    schema: Vec<u8>,
}

impl Restored {
    fn of(r: &EpochReady) -> Self {
        Restored {
            snapshots: r.snapshots.clone(),
            schema: r.schemas.get("users").cloned().unwrap_or_default(),
        }
    }
}

/// Sync `n` of stream users, keyed by id and read with cursor at, on
/// `lanes` lanes and writer lanes: generation n, epoch n, its read starting
/// at `read`. Its record names runs, not changes.
fn users(dir: &Path, n: u64, read: i64, from: &Restored, lanes: u32) -> StartJobRequest {
    let mut req = with_runs(crate::plan::tests::lanes_request(&["users"], lanes, lanes));
    req.generation = n;
    req.first_epoch = n;
    req.state_url = format!("file://{}", dir.join("state").display());
    req.restored_snapshots = from.snapshots.clone();
    let s = &mut req.streams[0];
    s.primary_key = vec!["id".into()];
    s.cursor_field = "at".into();
    s.read_start_micros = read;
    s.versions_operator = OP.into();
    s.restored_schema = from.schema.clone();
    req
}

/// A row of users as a writer lane wrote it: its id and name, and its
/// record ID.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    id: i64,
    name: String,
    record: Vec<u8>,
}

fn written(outs: &[Vec<frame::Frame>]) -> Vec<Row> {
    let mut got = Vec::new();
    for f in outs.iter().flatten() {
        if f.kind == FrameKind::SchemaChange {
            let c = SchemaChange::decode(f.payload.as_slice()).unwrap();
            let s = ipc::decode_schema(&c.arrow_schema).unwrap();
            assert!(
                s.fields().iter().all(|f| !f.name().starts_with('\0')),
                "a schema change names a hidden column: {s:?}"
            );
        }
        if f.kind != FrameKind::Batch {
            continue;
        }
        let b = ipc::decode_batch(f.payload.clone()).unwrap();
        assert!(
            b.schema()
                .fields()
                .iter()
                .all(|f| !f.name().starts_with('\0')),
            "a hidden column reached the writer's edge: {:?}",
            b.schema()
        );
        let ids = b
            .column_by_name("_openetl_id")
            .unwrap()
            .as_fixed_size_binary();
        for ((id, name), record) in column_ints(&b, "id")
            .into_iter()
            .zip(column_strings(&b, "name"))
            .zip(ids.iter())
        {
            got.push(Row {
                id: id.unwrap(),
                name: name.unwrap_or_default(),
                record: record.unwrap().to_vec(),
            });
        }
    }
    got
}

fn epoch_barrier(generation: u64, n: u64) -> Vec<u8> {
    Barrier {
        epoch: Some(Epoch { generation, n }),
        ..Default::default()
    }
    .encode_to_vec()
}

/// Runs `req`, whose lane l reads `lanes[l]`, one epoch: what its writer
/// lanes wrote, and the epoch.
async fn run_sync(req: StartJobRequest, lanes: &[&[&str]]) -> (Vec<Row>, EpochReady) {
    let (generation, n) = (req.generation, req.first_epoch);
    let mut h = start_lanes(req);
    for (w, rs) in h.ins.iter_mut().zip(lanes) {
        if !rs.is_empty() {
            put(w, FrameKind::Rows, 0, rows(rs)).await;
        }
        put(w, FrameKind::Barrier, 0, epoch_barrier(generation, n)).await;
    }
    h.end_all(1).await;
    let (outs, mut ready) = h.finish().await;
    assert_eq!(ready.len(), 1);
    (written(&outs), ready.remove(0))
}

/// Sync n on one lane.
async fn sync(dir: &Path, n: u64, from: &Restored, rs: &[&str]) -> (Vec<Row>, Restored) {
    let (rows, ready) = run_sync(users(dir, n, n as i64 * 1000, from, 1), &[rs]).await;
    let named: Vec<_> = ready
        .snapshots
        .iter()
        .filter(|s| s.operator == OP)
        .collect();
    assert!(!named.is_empty(), "epoch {n} names no versions");
    assert!(
        rs.is_empty() || named.iter().any(|s| !s.runs.is_empty()),
        "epoch {n}'s versions name no runs: {named:?}"
    );
    (rows, Restored::of(&ready))
}

fn record(rows: &[Row], id: i64) -> Vec<u8> {
    let mut of = rows.iter().filter(|r| r.id == id);
    let r = of
        .next()
        .unwrap_or_else(|| panic!("no row {id} in {rows:?}"));
    assert!(of.next().is_none(), "two rows {id} in {rows:?}");
    r.record.clone()
}

#[tokio::test]
async fn a_second_version_at_one_cursor_value_gets_an_id_of_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let (first, r1) = sync(
        d,
        1,
        &Restored::default(),
        &[
            r#"{"id": 1, "name": "v1", "at": "2024-05-01"}"#,
            r#"{"id": 2, "name": "x", "at": "2024-05-01"}"#,
        ],
    )
    .await;
    let again = [
        r#"{"id": 1, "name": "v2", "at": "2024-05-01"}"#,
        r#"{"id": 2, "name": "x", "at": "2024-05-01"}"#,
    ];
    let (second, r2) = sync(d, 2, &r1, &again).await;
    assert_ne!(record(&second, 1), record(&first, 1), "v2 took v1's ID");
    assert_eq!(
        record(&second, 2),
        record(&first, 2),
        "an unchanged row read again changed its ID"
    );
    // Read again, each keeps the ID it had.
    let (third, _) = sync(d, 3, &r2, &again).await;
    assert_eq!(third, second);
}

/// A key the destination names otherwise, as Id becomes id, keeps the
/// versions from a new stream's first sync, before the stream has a schema.
#[tokio::test]
async fn a_renamed_key_keeps_the_versions_of_a_new_stream() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let renamed = |n: u64, from: &Restored| {
        let mut req = users(d, n, n as i64 * 1000, from, 1);
        req.streams[0].primary_key = vec!["Id".into()];
        req
    };
    let row = |name: &str| format!(r#"{{"Id": 1, "name": "{name}", "at": "2024-05-01"}}"#);
    let (first, ready) = run_sync(renamed(1, &Restored::default()), &[&[&row("v1")]]).await;
    let (second, _) = run_sync(renamed(2, &Restored::of(&ready)), &[&[&row("v2")]]).await;
    assert_ne!(record(&second, 1), record(&first, 1), "v2 took v1's ID");

    // A held cell's batch, which names the key as the rows do, too, in a
    // new stream's first sync.
    let holding = |n: u64, from: &Restored| {
        use arrow_schema::{Field, Schema};
        let mut req = renamed(n, from);
        req.held_url = format!("file://{}/held", d.display());
        let declared = Schema::new(vec![
            Field::new("Id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Decimal128(10, 2), true),
            Field::new("at", DataType::Utf8, true),
        ]);
        req.streams[0].arrow_schema = ipc::encode_schema(&Arc::new(declared)).unwrap();
        req.streams[0].holds = Some(openetl_proto::engine::v1::HoldPlan {
            cells: true,
            ..Default::default()
        });
        req
    };
    let priced = |name: &str| {
        format!(r#"{{"Id": 1, "name": "{name}", "price": "12345678901.5", "at": "2024-05-01"}}"#)
    };
    let (first, ready) = run_sync(holding(3, &Restored::default()), &[&[&priced("v1")]]).await;
    assert_eq!(ready.holds["users"].files.len(), 1, "no cell held");
    let (second, _) = run_sync(holding(4, &Restored::of(&ready)), &[&[&priced("v2")]]).await;
    assert_ne!(record(&second, 1), record(&first, 1), "v2 took v1's ID");
}

/// A row read again whose volatile columns alone changed keeps its ID; one
/// whose other columns changed gets an ID of its own.
#[tokio::test]
async fn a_volatile_column_doesnt_make_a_version() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let volatile = |n: u64, from: &Restored| {
        let mut req = users(d, n, n as i64 * 1000, from, 1);
        req.streams[0].volatile_columns = vec!["fetched".into()];
        req
    };
    let row = |name: &str, fetched: i64| {
        format!(r#"{{"id": 1, "name": "{name}", "fetched": {fetched}, "at": "2024-05-01"}}"#)
    };
    let (first, ready) = run_sync(volatile(1, &Restored::default()), &[&[&row("v1", 1)]]).await;
    let (again, ready) = run_sync(volatile(2, &Restored::of(&ready)), &[&[&row("v1", 2)]]).await;
    assert_eq!(
        record(&again, 1),
        record(&first, 1),
        "a fetch time made a version"
    );
    let (second, _) = run_sync(volatile(3, &Restored::of(&ready)), &[&[&row("v2", 3)]]).await;
    assert_ne!(record(&second, 1), record(&first, 1), "v2 took v1's ID");
}

/// A declarative read gives again the rows at the highest cursor of the
/// read before the last, which the last gave too: an entry the last read
/// gave, or that no read since gave a row of its key group, is live.
#[tokio::test]
async fn an_entry_lives_through_the_read_after_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let v1 = r#"{"id": 1, "name": "v1", "at": "2024-05-01"}"#;
    let v2 = r#"{"id": 1, "name": "v2", "at": "2024-05-01"}"#;
    let (first, r1) = sync(d, 1, &Restored::default(), &[v1]).await;
    let (_, r2) = sync(d, 2, &r1, &[v1]).await;
    let (lag, _) = sync(d, 3, &r2, &[v2]).await;
    assert_ne!(
        record(&lag, 1),
        record(&first, 1),
        "the entry didn't outlive a read"
    );
    let (_, quiet) = sync(d, 4, &r1, &[]).await;
    let (late, _) = sync(d, 5, &quiet, &[v2]).await;
    assert_ne!(
        record(&late, 1),
        record(&first, 1),
        "a read without the key group's rows expired it"
    );
}

/// An entry the last read didn't give, where it gave a row of the key
/// group, is gone: its version keeps its ID.
#[tokio::test]
async fn an_entry_the_last_read_didnt_give_expires() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let v1 = r#"{"id": 1, "name": "v1", "at": "2024-05-01"}"#;
    let v2 = r#"{"id": 1, "name": "v2", "at": "2024-05-01"}"#;
    let moved = r#"{"id": 1, "name": "v1", "at": "2024-05-02"}"#;
    let (first, r1) = sync(d, 1, &Restored::default(), &[v1]).await;
    let (_, r2) = sync(d, 2, &r1, &[moved]).await;
    let (late, _) = sync(d, 3, &r2, &[v2]).await;
    assert_eq!(
        record(&late, 1),
        record(&first, 1),
        "an entry the last read didn't give is live"
    );
}

#[tokio::test]
async fn a_widening_keeps_the_versions_and_a_new_column_starts_them_again() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let (first, r1) = sync(
        d,
        1,
        &Restored::default(),
        &[
            r#"{"id": 1, "name": "a", "n": 1, "at": "2024-05-01"}"#,
            r#"{"id": 2, "name": "b", "n": 2, "at": "2024-05-01"}"#,
        ],
    )
    .await;
    // n widens to a float: row 1 is as it was, and row 2 changed.
    let (second, r2) = sync(
        d,
        2,
        &r1,
        &[
            r#"{"id": 1, "name": "a", "n": 1, "at": "2024-05-01"}"#,
            r#"{"id": 2, "name": "b", "n": 2.5, "at": "2024-05-01"}"#,
        ],
    )
    .await;
    assert_eq!(
        record(&second, 1),
        record(&first, 1),
        "a widening changed an ID"
    );
    assert_ne!(record(&second, 2), record(&first, 2));
    // A column every row has now starts each entry again: the rows keep
    // their IDs, the changed one too, and a later change gets its own.
    let (third, r3) = sync(
        d,
        3,
        &r2,
        &[
            r#"{"id": 1, "name": "a2", "n": 1, "extra": 5, "at": "2024-05-01"}"#,
            r#"{"id": 2, "name": "b", "n": 2.5, "extra": 5, "at": "2024-05-01"}"#,
        ],
    )
    .await;
    assert_eq!(
        record(&third, 1),
        record(&first, 1),
        "the schema's version didn't count"
    );
    assert_eq!(record(&third, 2), record(&first, 2));
    let (fourth, _) = sync(
        d,
        4,
        &r3,
        &[r#"{"id": 1, "name": "a3", "n": 1, "extra": 5, "at": "2024-05-01"}"#],
    )
    .await;
    assert_ne!(record(&fourth, 1), record(&first, 1));
}

/// Rows reach the writer lane their lane feeds: one of a key group it
/// doesn't keep keeps its ID, as it would without versions, and isn't
/// given two.
#[tokio::test]
async fn a_row_of_a_key_group_its_writer_lane_doesnt_keep_keeps_its_id() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let plan = Plan::from_request(users(d, 1, 1000, &Restored::default(), 2)).unwrap();
    let theirs = |w: usize| {
        (1..100)
            .find(|&id| {
                let b = RecordBatch::try_from_iter(vec![(
                    "id",
                    Arc::new(arrow_array::Int64Array::from(vec![id])) as arrow_array::ArrayRef,
                )])
                .unwrap();
                let g =
                    openetl_normalize::key_groups(&b, &["id".into()], plan.key_groups).unwrap()[0];
                plan.layout.writer(w).contains(&g)
            })
            .unwrap()
    };
    let (mine, other) = (theirs(0), theirs(1));
    let at =
        |id: i64, name: &str| format!(r#"{{"id": {id}, "name": "{name}", "at": "2024-05-01"}}"#);
    let read = |name: &str| [at(mine, name), at(other, name)];
    let run = |n: u64, from: Restored, name: &'static str| async move {
        let rs = read(name);
        let lane0: Vec<&str> = rs.iter().map(String::as_str).collect();
        run_sync(users(d, n, n as i64 * 1000, &from, 2), &[&lane0, &[]]).await
    };
    let (first, r1) = run(1, Restored::default(), "v1").await;
    let (second, _) = run(2, Restored::of(&r1), "v2").await;
    assert_ne!(record(&second, mine), record(&first, mine));
    assert_eq!(record(&second, other), record(&first, other));
}

/// A redo writes no state, reads its key groups' whichever writer lane
/// its rows reach, and from the epoch's own record gives a row read again
/// unchanged the ID the epoch gave it, and one changed since an ID of its
/// own.
#[tokio::test]
async fn a_redo_reads_the_epochs_versions_and_writes_none() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let v1 = r#"{"id": 1, "name": "v1", "at": "2024-05-01"}"#;
    let v2 = r#"{"id": 1, "name": "v2", "at": "2024-05-01"}"#;
    let (first, r1) = sync(d, 1, &Restored::default(), &[v1]).await;
    let redo = |generation: u64| {
        let mut req = users(d, generation, 1000, &r1, 1);
        req.first_epoch = 1;
        req.state_read_only = true;
        let groups = Plan::from_request(req.clone()).unwrap().key_groups;
        req.streams[0].key_groups = Some(ProtoRange {
            first: 0,
            last: groups - 1,
        });
        req
    };
    for (generation, row, same) in [(2, v1, true), (3, v2, false)] {
        let (rows, ready) = run_sync(redo(generation), &[&[row]]).await;
        assert_eq!(record(&rows, 1) == record(&first, 1), same, "{row}");
        assert!(
            ready.snapshots.iter().all(|s| s.operator != OP),
            "a redo wrote versions: {:?}",
            ready.snapshots
        );
    }
}

/// A stream with versions whose job has no state is refused, as is a
/// version kept of a stream without a cursor.
#[test]
fn versions_need_the_jobs_state_and_a_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let mut req = users(dir.path(), 1, 1000, &Restored::default(), 1);
    req.state_url.clear();
    assert!(Plan::from_request(req).is_err());
    let mut req = users(dir.path(), 1, 1000, &Restored::default(), 1);
    req.streams[0].cursor_field.clear();
    assert!(Plan::from_request(req).is_err());
}

/// Rows of users `ids`, at one cursor value.
fn page(ids: std::ops::Range<usize>) -> Vec<u8> {
    let rs: Vec<String> = ids
        .map(|i| format!(r#"{{"id": {i}, "name": "n{i}", "at": "2024-05-01"}}"#))
        .collect();
    rows(&rs.iter().map(String::as_str).collect::<Vec<_>>())
}

/// Under the job's memory, a read's versions go to runs as they pass the
/// operator's part of it, before the barrier, rather than stay in memory
/// to it: the record names more runs than one.
#[tokio::test]
async fn a_reads_versions_go_to_runs_under_the_jobs_memory() {
    let dir = tempfile::tempdir().unwrap();
    let mut req = users(dir.path(), 1, 1000, &Restored::default(), 1);
    req.limits = Some(openetl_proto::engine::v1::Limits {
        memory_bytes: 1 << 20,
        ..Default::default()
    });
    let mut h = start_lanes(req);
    let mut ins = std::mem::take(&mut h.ins);
    let feed = tokio::spawn(async move {
        let w = &mut ins[0];
        for p in 0..20 {
            put(w, FrameKind::Rows, 0, page(p * 500..(p + 1) * 500)).await;
        }
        put(w, FrameKind::Barrier, 0, epoch_barrier(1, 1)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        w.shutdown().await.unwrap();
    });
    let (outs, ready) = h.finish().await;
    feed.await.unwrap();
    assert_eq!(written(&outs).len(), 10_000);
    let runs: usize = ready[0]
        .snapshots
        .iter()
        .filter(|s| s.operator == OP)
        .map(|s| s.runs.len())
        .sum();
    assert!(runs > 1, "the versions stayed in memory to the barrier");
}

/// Times a first read of `ROWS` rows of users (default 200,000), in pages
/// of 1,000 on one lane and one epoch, then the same rows read again, with
/// the stream's versions kept and without; `VERSIONS=on` or `off` for one
/// of them, `MEMORY` the job's memory in bytes, none by default, and
/// `READS=1` for the first read alone. A measure, not a check:
/// `cargo test -p openetl-engine first_read_times -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn first_read_times() {
    let n: usize = std::env::var("ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let pages: Vec<Vec<u8>> = (0..n.div_ceil(1000))
        .map(|p| page(p * 1000..((p + 1) * 1000).min(n)))
        .collect();
    let memory: u64 = std::env::var("MEMORY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let reads: u64 = std::env::var("READS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let modes = match std::env::var("VERSIONS").as_deref() {
        Ok("on") => vec![true],
        Ok("off") => vec![false],
        _ => vec![false, true],
    };
    for versions in modes {
        let dir = tempfile::tempdir().unwrap();
        let mut from = Restored::default();
        for sync in 1..=reads {
            let mut req = users(dir.path(), sync, sync as i64 * 1000, &from, 1);
            req.state_pending_bytes = None;
            if memory > 0 {
                req.limits = Some(openetl_proto::engine::v1::Limits {
                    memory_bytes: memory,
                    ..Default::default()
                });
            }
            if !versions {
                req.streams[0].versions_operator.clear();
                req.state_url.clear();
            }
            let started = std::time::Instant::now();
            let mut h = start_lanes(req);
            let mut ins = std::mem::take(&mut h.ins);
            let pages = pages.clone();
            let feed = tokio::spawn(async move {
                let w = &mut ins[0];
                for p in pages {
                    put(w, FrameKind::Rows, 0, p).await;
                }
                put(w, FrameKind::Barrier, 0, epoch_barrier(sync, sync)).await;
                put(w, FrameKind::EndOfStream, 0, vec![]).await;
                w.shutdown().await.unwrap();
            });
            let (outs, ready) = h.finish().await;
            feed.await.unwrap();
            let took = started.elapsed();
            assert_eq!(written(&outs).len(), n);
            fn size(p: &Path) -> u64 {
                std::fs::read_dir(p).map_or(0, |d| {
                    d.flatten()
                        .map(|e| match e.metadata() {
                            Ok(m) if m.is_dir() => size(&e.path()),
                            Ok(m) => m.len(),
                            Err(_) => 0,
                        })
                        .sum()
                })
            }
            eprintln!(
                "{n} rows, read {sync}, versions {versions}: {took:?}, state {} bytes",
                size(&dir.path().join("state"))
            );
            from = Restored::of(&ready[0]);
        }
    }
}
