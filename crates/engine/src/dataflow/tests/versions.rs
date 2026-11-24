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
/// gave, or that no read since gave a row of its writer lane, is live.
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
        "a read without rows expired it"
    );
}

/// An entry the last read didn't give, where it gave a row of the writer
/// lane, is gone: its version keeps its ID.
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

/// A read's entries leave the record two reads after it, whichever key
/// groups those give rows of, where the record carries a lane's changes
/// and where every barrier writes a run: the record names none of its runs,
/// and the next read still finds the last two reads' entries.
#[tokio::test]
async fn a_reads_runs_go_two_reads_after_it() {
    let all: Vec<String> = (0..20_000)
        .map(|i| format!(r#"{{"id": {i}, "name": "n{i}", "at": "2024-05-01"}}"#))
        .collect();
    let all: Vec<&str> = all.iter().map(String::as_str).collect();
    let one = r#"{"id": 1, "name": "n1", "at": "2024-05-01"}"#;
    let changed = r#"{"id": 1, "name": "changed", "at": "2024-05-01"}"#;
    for pending in [None, Some(0)] {
        let dir = tempfile::tempdir().unwrap();
        let mut from = Restored::default();
        let mut got = Vec::new();
        for (n, rs) in [(1, &all[..]), (2, &[one][..]), (3, &[one][..])] {
            let mut req = users(dir.path(), n, n as i64 * 1000, &from, 1);
            req.state_pending_bytes = pending;
            let (rows, ready) = run_sync(req, &[rs]).await;
            got.push(rows);
            from = Restored::of(&ready);
        }
        let first: Vec<&String> = from
            .snapshots
            .iter()
            .flat_map(|s| &s.runs)
            .filter(|r| r.contains("/g1/"))
            .collect();
        assert!(
            first.is_empty(),
            "pending {pending:?}: the record names the first read's {first:?}"
        );
        let mut req = users(dir.path(), 4, 4000, &from, 1);
        req.state_pending_bytes = pending;
        let (fourth, _) = run_sync(req, &[&[changed]]).await;
        assert_ne!(
            record(&fourth, 1),
            record(&got[0], 1),
            "pending {pending:?}: the last read's entry was lost"
        );
    }
}

/// An attempt that takes a read up again, from a record whose key groups
/// moved on to it, finds the entries of the read before it.
#[tokio::test]
async fn a_read_taken_up_again_finds_the_read_before_its_entries() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let v1 = r#"{"id": 1, "name": "v1", "at": "2024-05-01"}"#;
    let other = r#"{"id": 2, "name": "o", "at": "2024-05-01"}"#;
    let v2 = r#"{"id": 1, "name": "v2", "at": "2024-05-01"}"#;
    let (first, r1) = sync(d, 1, &Restored::default(), &[v1]).await;
    let (_, r2) = sync(d, 2, &r1, &[other]).await;
    let (again, _) = run_sync(users(d, 3, 2000, &r2, 1), &[&[v2]]).await;
    assert_ne!(record(&again, 1), record(&first, 1), "v2 took v1's ID");
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

/// Each row each of users' writer lanes wrote, as its name and record ID,
/// sorted: a batch may have no key column.
fn names_and_records(outs: &[Vec<frame::Frame>]) -> Vec<Vec<(String, Vec<u8>)>> {
    outs.iter()
        .map(|frames| {
            let mut got = Vec::new();
            for f in frames.iter().filter(|f| f.kind == FrameKind::Batch) {
                let b = ipc::decode_batch(f.payload.clone()).unwrap();
                let ids = b
                    .column_by_name("_openetl_id")
                    .unwrap()
                    .as_fixed_size_binary();
                for (name, id) in column_strings(&b, "name").into_iter().zip(ids.iter()) {
                    got.push((name.unwrap_or_default(), id.unwrap().to_vec()));
                }
            }
            got.sort();
            got
        })
        .collect()
}

fn all_lanes(lanes: &[Vec<(String, Vec<u8>)>]) -> Vec<(String, Vec<u8>)> {
    let mut all: Vec<_> = lanes.iter().flatten().cloned().collect();
    all.sort();
    all
}

/// One sync of users on `lanes` lanes, whose lane 0 sends `frames`: what
/// each writer lane wrote, as without versions where `versions` is false.
/// With versions on more than one writer lane, the stream is exchanged by
/// key group, as the runner plans it.
async fn sync_frames(
    lanes: u32,
    versions: bool,
    frames: &[(FrameKind, Vec<u8>)],
) -> Vec<Vec<(String, Vec<u8>)>> {
    let dir = tempfile::tempdir().unwrap();
    let mut req = users(dir.path(), 1, 1000, &Restored::default(), lanes);
    if !versions {
        req.streams[0].versions_operator = String::new();
    } else if lanes > 1 {
        req.streams[0].exchange = crate::plan::tests::exchange_by(&["id"]);
    }
    let mut h = start_lanes(req);
    for (kind, payload) in frames {
        put(&mut h.ins[0], *kind, 0, payload.clone()).await;
    }
    for w in &mut h.ins {
        put(w, FrameKind::Barrier, 0, epoch_barrier(1, 1)).await;
    }
    h.end_all(1).await;
    let (outs, _) = h.finish().await;
    names_and_records(&outs)
}

/// The IDs of `got`'s rows named `names`, in order.
fn ids_of(got: &[(String, Vec<u8>)], names: &[&str]) -> Vec<Vec<u8>> {
    names
        .iter()
        .map(|n| {
            let mut of = got.iter().filter(|(name, _)| name == n);
            let (_, id) = of.next().unwrap_or_else(|| panic!("no row {n} in {got:?}"));
            assert!(of.next().is_none(), "two rows {n} in {got:?}");
            id.clone()
        })
        .collect()
}

/// Syncs `frames`, whose rows `null` have a null key, with versions on one
/// writer lane and on two, which exchange the stream by key group: both
/// give each row the same ID, and on two, `null` reach one writer lane. The
/// null key is a record's, as in the rows' IDs, which `null` share without
/// versions: the first of them keeps that ID, and the second, another
/// version of the record in the same batch, gets one of its own.
async fn null_keys_are_one_record(frames: &[(FrameKind, Vec<u8>)], null: &[&str]) {
    let without = ids_of(&all_lanes(&sync_frames(1, false, frames).await), null);
    assert!(without.iter().all(|id| *id == without[0]), "{without:?}");
    let one = all_lanes(&sync_frames(1, true, frames).await);
    let two = sync_frames(2, true, frames).await;
    assert_eq!(all_lanes(&two), one, "on two writer lanes");
    assert!(
        two.iter()
            .any(|w| null.iter().all(|n| w.iter().any(|(name, _)| name == n))),
        "the null key's rows reached two writer lanes: {two:?}"
    );
    let ids = ids_of(&one, &null[..2]);
    assert_eq!(ids[0], without[0], "the null key's first row");
    assert_ne!(ids[1], without[0], "the null key's second version");
}

/// A page whose rows have no key values, as a new stream's first can: the
/// typer leaves out a column without values, so the page's batch has no key
/// column. Its rows' key is null, as is that of the row of the page after
/// it whose key is null, where another row has a key.
#[tokio::test]
async fn a_page_without_key_values_syncs() {
    let pages = [
        rows(&[
            r#"{"id": null, "name": "a", "at": "2024-05-01"}"#,
            r#"{"name": "b", "at": "2024-05-01"}"#,
        ]),
        rows(&[
            r#"{"id": 1, "name": "c", "at": "2024-05-01"}"#,
            r#"{"id": null, "name": "d", "at": "2024-05-01"}"#,
        ]),
    ];
    let frames: Vec<_> = pages.into_iter().map(|p| (FrameKind::Rows, p)).collect();
    null_keys_are_one_record(&frames, &["a", "b", "d"]).await;
    let keyed = async |lanes, versions| {
        ids_of(
            &all_lanes(&sync_frames(lanes, versions, &frames).await),
            &["c"],
        )
    };
    assert_eq!(keyed(2, true).await, keyed(1, false).await);
}

/// A typed batch's key column of type null is null in every row.
#[tokio::test]
async fn a_key_column_of_type_null_syncs() {
    use arrow_array::{ArrayRef, NullArray, StringArray};
    let batch = |names: &[&str]| {
        let id: ArrayRef = Arc::new(NullArray::new(names.len()));
        let at: ArrayRef = Arc::new(StringArray::from(vec!["2024-05-01"; names.len()]));
        let names: ArrayRef = Arc::new(StringArray::from(names.to_vec()));
        let b = RecordBatch::try_from_iter(vec![("id", id), ("name", names), ("at", at)]).unwrap();
        (FrameKind::Batch, ipc::encode_batch(&b).unwrap())
    };
    null_keys_are_one_record(&[batch(&["a", "b"]), batch(&["c"])], &["a", "b", "c"]).await;
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

/// Versions of a layout this engine doesn't keep, as a later engine's that
/// a rollback restores, start again rather than fail the sync: a second
/// version keeps its ID, as with no versions.
#[tokio::test]
async fn versions_of_another_layout_start_again() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let v1 = r#"{"id": 1, "name": "v1", "at": "2024-05-01"}"#;
    let (first, mut r1) = sync(d, 1, &Restored::default(), &[v1]).await;
    let ours = r1.snapshots.iter_mut().filter(|s| s.operator == OP);
    for s in ours {
        assert_ne!(s.layout, openetl_state::LAYOUT, "no layout of their own");
        s.layout += 1 << 16;
    }
    let v2 = r#"{"id": 1, "name": "v2", "at": "2024-05-01"}"#;
    let (second, _) = sync(d, 2, &r1, &[v2]).await;
    assert_eq!(record(&second, 1), record(&first, 1));
}

/// Versions whose runs are gone, as after a rollback whose runner, from
/// before them, restored their record and deleted what it didn't know,
/// forget them at the lookup that finds one, rather than fail the sync: a
/// second version keeps its ID, and a third, a read later, gets its own.
#[tokio::test]
async fn versions_whose_runs_are_gone_forget_them() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let v1 = r#"{"id": 1, "name": "v1", "at": "2024-05-01"}"#;
    let (first, r1) = sync(d, 1, &Restored::default(), &[v1]).await;
    for s in r1.snapshots.iter().filter(|s| s.operator == OP) {
        for run in &s.runs {
            std::fs::remove_file(d.join("state").join(run)).unwrap();
        }
    }
    let v2 = r#"{"id": 1, "name": "v2", "at": "2024-05-01"}"#;
    let (second, r2) = sync(d, 2, &r1, &[v2]).await;
    assert_eq!(record(&second, 1), record(&first, 1));
    let v3 = r#"{"id": 1, "name": "v3", "at": "2024-05-01"}"#;
    let (third, _) = sync(d, 3, &r2, &[v3]).await;
    assert_ne!(record(&third, 1), record(&second, 1), "not kept again");
}

/// A run of the versions that's there but can't be read fails the sync, as
/// other state's does, rather than drop the versions for good: only a run
/// that's gone is forgotten. The error names the stream and where the run
/// is; once a person deletes it, the next sync forgets it, and the row
/// whose entry it held keeps its ID once.
#[tokio::test]
async fn versions_whose_run_cant_be_read_fail_the_sync() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let v1 = r#"{"id": 1, "name": "v1", "at": "2024-05-01"}"#;
    let (first, r1) = sync(d, 1, &Restored::default(), &[v1]).await;
    let mut damaged = Vec::new();
    for s in r1.snapshots.iter().filter(|s| s.operator == OP) {
        for run in &s.runs {
            let p = d.join("state").join(run);
            let mut b = std::fs::read(&p).unwrap();
            b.fill(0);
            std::fs::write(&p, b).unwrap();
            damaged.push(p);
        }
    }
    let v2 = r#"{"id": 1, "name": "v2", "at": "2024-05-01"}"#;
    let mut h = start_lanes(users(d, 2, 2000, &r1, 1));
    put(&mut h.ins[0], FrameKind::Rows, 0, rows(&[v2])).await;
    put(&mut h.ins[0], FrameKind::Barrier, 0, epoch_barrier(2, 2)).await;
    h.end_all(1).await;
    let err = h.job.await.unwrap().unwrap_err();
    assert_eq!(err.code, ErrorCode::CheckpointCorrupt, "{}", err.message);
    assert_eq!(damaged.len(), 1);
    let at = format!("file://{}", damaged[0].display());
    assert!(err.message.contains(&at), "{} names no {at}", err.message);
    assert!(err.message.starts_with("stream users: "), "{}", err.message);
    assert!(
        err.message.contains("Delete it to go on"),
        "{}",
        err.message
    );
    std::fs::remove_file(&damaged[0]).unwrap();
    let (second, r2) = sync(d, 2, &r1, &[v2]).await;
    assert_eq!(record(&second, 1), record(&first, 1), "skipped once");
    let v3 = r#"{"id": 1, "name": "v3", "at": "2024-05-01"}"#;
    let (third, _) = sync(d, 3, &r2, &[v3]).await;
    assert_ne!(record(&third, 1), record(&second, 1), "not kept again");
}

/// Rows of a lane's key groups and the other's, the two syncs on two lanes
/// that version them, the first giving `mine` and the second `other`, and
/// the second's record, whose runs of the other lane's groups are gone.
async fn other_lanes_runs_gone(d: &Path) -> (Vec<i64>, i64, Vec<Row>, Vec<Row>, EpochReady) {
    let plan = Plan::from_request(users(d, 1, 1000, &Restored::default(), 2)).unwrap();
    let of = |w: usize, n: usize| -> Vec<i64> {
        (0..)
            .filter(|&id| {
                let b = RecordBatch::try_from_iter(vec![(
                    "id",
                    Arc::new(arrow_array::Int64Array::from(vec![id])) as arrow_array::ArrayRef,
                )])
                .unwrap();
                let g =
                    openetl_normalize::key_groups(&b, &["id".into()], plan.key_groups).unwrap()[0];
                plan.layout.writer(w).contains(&g)
            })
            .take(n)
            .collect()
    };
    let (mine, other) = (of(0, 2), of(1, 1)[0]);
    let firsts = [user(mine[0], "v1"), user(mine[1], "v1")];
    let firsts: Vec<&str> = firsts.iter().map(String::as_str).collect();
    let (first, r1) = run_sync(users(d, 1, 1000, &Restored::default(), 2), &[&firsts, &[]]).await;
    let theirs = user(other, "v1");
    let (second, r2) = run_sync(users(d, 2, 2000, &Restored::of(&r1), 2), &[&[], &[&theirs]]).await;
    for s in r2.snapshots.iter().filter(|s| s.operator == OP) {
        if s.key_groups.as_ref().is_some_and(|r| r.first > 0) {
            for run in &s.runs {
                std::fs::remove_file(d.join("state").join(run)).unwrap();
            }
        }
    }
    (mine, other, first, second, r2)
}

fn user(id: i64, name: &str) -> String {
    format!(r#"{{"id": {id}, "name": "{name}", "at": "2024-05-01"}}"#)
}

/// A run found gone mid-epoch is forgotten, and the lane reads on with the
/// rest: rows of key groups it didn't hold keep their versions, before it
/// and after, the record no longer names it, and a redo of the epoch gives
/// the same IDs.
#[tokio::test]
async fn a_run_found_gone_leaves_the_other_key_groups_versions() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let (mine, other, first, second, r2) = other_lanes_runs_gone(d).await;
    let mut h = start_lanes(users(d, 3, 3000, &Restored::of(&r2), 1));
    for r in [user(mine[0], "v2"), user(other, "v2"), user(mine[1], "v2")] {
        put(&mut h.ins[0], FrameKind::Rows, 0, rows(&[&r])).await;
    }
    put(&mut h.ins[0], FrameKind::Barrier, 0, epoch_barrier(3, 3)).await;
    h.end_all(1).await;
    let (outs, ready) = h.finish().await;
    let third = written(&outs);
    for &id in &mine {
        assert_ne!(
            record(&third, id),
            record(&first, id),
            "row {id} lost its versions"
        );
    }
    assert_eq!(
        record(&third, other),
        record(&second, other),
        "the gone entry's"
    );
    for s in ready[0].snapshots.iter().filter(|s| s.operator == OP) {
        for run in &s.runs {
            assert!(d.join("state").join(run).exists(), "the record names {run}");
        }
    }
    let mut req = users(d, 4, 3000, &Restored::of(&r2), 1);
    req.first_epoch = 3;
    req.state_read_only = true;
    let groups = Plan::from_request(req.clone()).unwrap().key_groups;
    req.streams[0].key_groups = Some(ProtoRange {
        first: 0,
        last: groups - 1,
    });
    let again = [user(mine[0], "v2"), user(other, "v2"), user(mine[1], "v2")];
    let again: Vec<&str> = again.iter().map(String::as_str).collect();
    let (redo, _) = run_sync(req, &[&again]).await;
    for id in [mine[0], other, mine[1]] {
        assert_eq!(record(&redo, id), record(&third, id), "the redo's {id}");
    }
}

/// Held values get the IDs their rows got where another sender's rows,
/// between them, find a run gone: a writer lane takes frames from every
/// lane, so the operator is given them in that order here.
#[tokio::test]
async fn held_values_after_a_run_found_gone_get_their_rows_ids() {
    use super::super::versions::Versions;
    use openetl_normalize::versions::{HASH_COLUMN, SCHEMA_KEY, schema_version};

    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let (mine, other, first, second, r2) = other_lanes_runs_gone(d).await;
    let plan = Arc::new(Plan::from_request(users(d, 3, 3000, &Restored::of(&r2), 1)).unwrap());
    let lanes = plan.lanes.clone();
    let mut h = start_plan(plan);
    let flow = loop {
        match lanes.0.get().and_then(std::sync::Weak::upgrade) {
            Some(f) => break f,
            None => tokio::task::yield_now().await,
        }
    };
    let schema = ipc::decode_schema(&r2.schemas["users"]).unwrap();
    let version = schema_version(&schema, &[]).to_string();
    let batch = |id: i64, record: Vec<u8>| {
        use arrow_schema::{Field, Schema};
        let hash = Field::new(HASH_COLUMN, DataType::UInt64, false)
            .with_metadata([(SCHEMA_KEY.to_owned(), version.clone())].into());
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_openetl_id", DataType::FixedSizeBinary(16), false),
            hash,
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![id])),
                Arc::new(
                    arrow_array::FixedSizeBinaryArray::try_from_iter([record].into_iter()).unwrap(),
                ),
                Arc::new(arrow_array::UInt64Array::from(vec![7])),
            ],
        )
        .unwrap()
    };
    let id_of = |out: Out| match out {
        Out::Batch(_, b) | Out::Held(_, b) => b
            .column_by_name("_openetl_id")
            .unwrap()
            .as_fixed_size_binary()
            .iter()
            .next()
            .flatten()
            .unwrap()
            .to_vec(),
        _ => panic!("not a batch"),
    };
    let base = record(&first, mine[0]);
    let mut v = Versions::open(&flow, 0).await.unwrap();
    let row = v.apply(&flow, Out::Batch(0, batch(mine[0], base.clone())));
    let row = id_of(row.await.unwrap());
    assert_ne!(row, base, "the row got no version");
    let theirs = batch(other, record(&second, other));
    v.apply(&flow, Out::Batch(0, theirs)).await.unwrap();
    let held = v.apply(&flow, Out::Held(0, batch(mine[0], base)));
    assert_eq!(id_of(held.await.unwrap()), row, "the held value's ID");
    drop(flow);
    h.end_all(1).await;
    h.finish().await;
}

/// A run found gone after an early flush of the epoch is forgotten alone,
/// where a lane holds key groups of two lanes before it, at different
/// reads, and only the second's runs are gone: the lane flushes early on,
/// and the record names its flushes.
#[tokio::test]
async fn a_run_found_gone_after_an_early_flush_leaves_the_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let plan = Plan::from_request(users(d, 1, 1000, &Restored::default(), 2)).unwrap();
    let of = |w: usize, n: usize| -> Vec<i64> {
        (0..)
            .filter(|&id| {
                let b = RecordBatch::try_from_iter(vec![(
                    "id",
                    Arc::new(arrow_array::Int64Array::from(vec![id])) as arrow_array::ArrayRef,
                )])
                .unwrap();
                let g =
                    openetl_normalize::key_groups(&b, &["id".into()], plan.key_groups).unwrap()[0];
                plan.layout.writer(w).contains(&g)
            })
            .take(n)
            .collect()
    };
    let (mine, other) = (of(0, 6000), of(1, 1));
    let at = |id: &i64| format!(r#"{{"id": {id}, "name": "n", "at": "2024-05-01"}}"#);
    let page = |ids: &[i64]| ids.iter().map(at).collect::<Vec<_>>();
    // Writer lane 0's key groups move on to read 1, and lane 1's to read 2.
    let first = page(&mine[..1]);
    let first: Vec<&str> = first.iter().map(String::as_str).collect();
    let (_, r1) = run_sync(users(d, 1, 1000, &Restored::default(), 2), &[&first, &[]]).await;
    let second = page(&other);
    let second: Vec<&str> = second.iter().map(String::as_str).collect();
    let (_, r2) = run_sync(users(d, 2, 2000, &Restored::of(&r1), 2), &[&[], &second]).await;
    for s in r2.snapshots.iter().filter(|s| s.operator == OP) {
        if s.key_groups.as_ref().is_some_and(|r| r.first > 0) {
            for run in &s.runs {
                std::fs::remove_file(d.join("state").join(run)).unwrap();
            }
        }
    }
    // On one lane, lane 0's rows flush early before lane 1's row finds its
    // run gone, and more of lane 0's flush early again.
    let mut req = users(d, 3, 3000, &Restored::of(&r2), 1);
    req.limits = Some(openetl_proto::engine::v1::Limits {
        memory_bytes: 1 << 20,
        ..Default::default()
    });
    let pages: Vec<Vec<u8>> = [&mine[..3000], &other[..], &mine[3000..]]
        .iter()
        .flat_map(|ids| ids.chunks(500))
        .map(|ids| {
            let rs = page(ids);
            rows(&rs.iter().map(String::as_str).collect::<Vec<_>>())
        })
        .collect();
    let mut h = start_lanes(req);
    let mut ins = std::mem::take(&mut h.ins);
    let feed = tokio::spawn(async move {
        let w = &mut ins[0];
        for p in pages {
            put(w, FrameKind::Rows, 0, p).await;
        }
        put(w, FrameKind::Barrier, 0, epoch_barrier(3, 3)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        w.shutdown().await.unwrap();
    });
    let (outs, ready) = h.finish().await;
    feed.await.unwrap();
    assert_eq!(written(&outs).len(), 6001);
    let runs: usize = ready[0]
        .snapshots
        .iter()
        .filter(|s| s.operator == OP)
        .map(|s| s.runs.len())
        .sum();
    assert!(runs > 1, "no early flush named");
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
/// of them, `MEMORY` the job's memory and `SCRATCH` its scratch, in bytes,
/// none by default, and `READS=1` for the first read alone. `CHANGED=k`
/// makes each read after the first give `k` rows spread over the table, each
/// changed, rather than all of them again, `PENDING` sets the job's
/// pending bytes, as tests do, and `EPOCHS=e` cuts each read into `e`
/// epochs of about as many rows. Each read prints its time, the runs its
/// record names and their bytes, and its storage requests: the lookups'
/// filter probes, the blocks they read and those that held nothing. A
/// measure, not a check:
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
    let scratch: u64 = std::env::var("SCRATCH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let reads: u64 = std::env::var("READS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let changed: usize = std::env::var("CHANGED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let pending: Option<u64> = std::env::var("PENDING").ok().and_then(|v| v.parse().ok());
    let epochs: u64 = std::env::var("EPOCHS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
        .max(1);
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
            req.first_epoch = 1 + (sync - 1) * epochs;
            req.state_pending_bytes = pending;
            if memory > 0 || scratch > 0 {
                req.limits = Some(openetl_proto::engine::v1::Limits {
                    memory_bytes: memory,
                    scratch_dir: match scratch {
                        0 => String::new(),
                        _ => dir.path().join("scratch").display().to_string(),
                    },
                    scratch_bytes: scratch,
                });
            }
            if !versions {
                req.streams[0].versions_operator.clear();
                req.state_url.clear();
            }
            let (pages, given) = match changed {
                k if k > 0 && sync > 1 => {
                    let rs: Vec<String> = (0..k)
                        .map(|j| j * (n / k) + sync as usize % (n / k))
                        .map(|i| format!(r#"{{"id": {i}, "name": "r{sync}", "at": "2024-05-01"}}"#))
                        .collect();
                    (
                        vec![rows(&rs.iter().map(String::as_str).collect::<Vec<_>>())],
                        k,
                    )
                }
                _ => (pages.clone(), n),
            };
            let started = std::time::Instant::now();
            let plan = Arc::new(Plan::from_request(req).unwrap());
            let mut h = start_plan(plan.clone());
            let mut ins = std::mem::take(&mut h.ins);
            let first = 1 + (sync - 1) * epochs;
            // The epochs' events, taken as they come: more than the
            // harness's channel holds would hold up the job.
            let (closed, none) = tokio::sync::mpsc::channel(1);
            drop(closed);
            let mut events = std::mem::replace(&mut h.events, none);
            let readies = tokio::spawn(async move { ready(&mut events).await });
            let feed = tokio::spawn(async move {
                let w = &mut ins[0];
                let per = pages.len().div_ceil(epochs as usize).max(1);
                let mut epoch = first;
                for (i, p) in pages.into_iter().enumerate() {
                    put(w, FrameKind::Rows, 0, p).await;
                    if (i + 1) % per == 0 && epoch + 1 < first + epochs {
                        put(w, FrameKind::Barrier, 0, epoch_barrier(sync, epoch)).await;
                        epoch += 1;
                    }
                }
                put(w, FrameKind::Barrier, 0, epoch_barrier(sync, epoch)).await;
                put(w, FrameKind::EndOfStream, 0, vec![]).await;
                w.shutdown().await.unwrap();
            });
            let (outs, _) = h.finish().await;
            feed.await.unwrap();
            let took = started.elapsed();
            let last = readies.await.unwrap().pop().unwrap();
            assert_eq!(written(&outs).len(), given);
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
            let named: u64 = last
                .snapshots
                .iter()
                .flat_map(|s| &s.runs)
                .map(|r| std::fs::metadata(dir.path().join("state").join(r)).map_or(0, |m| m.len()))
                .sum();
            let runs: usize = last.snapshots.iter().map(|s| s.runs.len()).sum();
            let carried: usize = last
                .snapshots
                .iter()
                .map(|s| s.data.len() + s.pending.len())
                .sum();
            eprintln!(
                "{n} rows, read {sync} of {given}, versions {versions}: {took:?}, state {} bytes, {runs} runs of {named} bytes, record {carried} bytes",
                size(&dir.path().join("state"))
            );
            if let Some(j) = &plan.state.job {
                let c = j.stores(OP).counts();
                eprintln!(
                    "filter probes {}, blocks read {} ({} cached, {} held nothing), merges {}",
                    c.filtered,
                    c.hits + c.false_positives + c.cached,
                    c.cached,
                    c.false_positives,
                    c.merges
                );
                eprintln!("counts: {c:?}");
            }
            from = Restored::of(&last);
        }
    }
}

/// How a job of many streams that keep versions went: a read's time, the
/// most the process allocated above where it started, sampled each
/// millisecond, the runs its barriers wrote, merges, the runs the record
/// names, its pending sets' bytes and the block caches'. `None` where the
/// streams keep no versions.
struct Many {
    took: std::time::Duration,
    peak: u64,
    base: u64,
    kept: Option<Kept>,
}

struct Kept {
    /// The runs each lane of each stream names, fewest first.
    by_lane: Vec<usize>,
    runs: u64,
    written: u64,
    merges: u64,
    named: usize,
    pending: usize,
    caches: usize,
}

/// Two reads of `streams` streams on `lanes` lanes and writer lanes, in a
/// job of `memory` bytes: `rows` rows of each, then `changed` of them with
/// other values, in `epochs` epochs a read, the streams keeping versions
/// or not.
async fn many_streams(
    streams: usize,
    lanes: u32,
    (rows_each, changed): (usize, usize),
    memory: u64,
    epochs: u64,
    versions: bool,
) -> Vec<Many> {
    let names: Vec<String> = (0..streams).map(|s| format!("t{s}")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let dir = tempfile::tempdir().unwrap();
    let mut from: Option<EpochReady> = None;
    let mut out = Vec::new();
    for sync in 1..=2u64 {
        let mut req = crate::plan::tests::lanes_request(&refs, lanes, lanes);
        req.generation = sync;
        req.first_epoch = 1 + (sync - 1) * epochs;
        req.state_url = format!("file://{}", dir.path().join("state").display());
        req.restored_snapshots = from.as_ref().map_or(Vec::new(), |r| r.snapshots.clone());
        req.limits = Some(openetl_proto::engine::v1::Limits {
            memory_bytes: memory,
            ..Default::default()
        });
        for s in &mut req.streams {
            s.restored_schema = from
                .as_ref()
                .and_then(|r| r.schemas.get(&s.name).cloned())
                .unwrap_or_default();
            s.primary_key = vec!["id".into()];
            s.cursor_field = "at".into();
            s.read_start_micros = sync as i64 * 1000;
            if versions {
                s.versions_operator = format!("versions.{}", s.name);
            }
            if lanes > 1 {
                s.exchange = Some(openetl_proto::engine::v1::Exchange {
                    columns: vec!["id".into()],
                });
            }
        }
        if !versions {
            req.state_url.clear();
        }
        let given = match sync {
            1 => rows_each,
            _ => changed,
        };
        let plan = Arc::new(Plan::from_request(req).unwrap());
        let base = crate::memory::held();
        let peak = Arc::new(std::sync::atomic::AtomicU64::new(base));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sampler = {
            let (peak, done) = (peak.clone(), done.clone());
            tokio::spawn(async move {
                while !done.load(std::sync::atomic::Ordering::Relaxed) {
                    peak.fetch_max(crate::memory::held(), std::sync::atomic::Ordering::Relaxed);
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
        };
        let started = std::time::Instant::now();
        let mut h = start_plan(plan.clone());
        let (closed, none) = tokio::sync::mpsc::channel(1);
        drop(closed);
        let mut events = std::mem::replace(&mut h.events, none);
        let readies = tokio::spawn(async move { ready(&mut events).await });
        let mut ins = std::mem::take(&mut h.ins);
        let first = 1 + (sync - 1) * epochs;
        let feed = tokio::spawn(async move {
            let per = given.div_ceil(epochs as usize).max(1);
            let mut at = 0;
            for e in 0..epochs {
                let upto = (at + per).min(given);
                for s in 0..streams {
                    let rs: Vec<String> = (at..upto)
                        .map(|i| format!(r#"{{"id": {i}, "name": "r{sync}", "at": "2024-05-01"}}"#))
                        .collect();
                    if rs.is_empty() {
                        continue;
                    }
                    let page = rows(&rs.iter().map(String::as_str).collect::<Vec<_>>());
                    let lane = s % ins.len();
                    put(&mut ins[lane], FrameKind::Rows, s as u32, page).await;
                }
                at = upto;
                for w in &mut ins {
                    for s in 0..streams {
                        let b = epoch_barrier(sync, first + e);
                        put(w, FrameKind::Barrier, s as u32, b).await;
                    }
                }
            }
            for w in &mut ins {
                for s in 0..streams {
                    put(w, FrameKind::EndOfStream, s as u32, vec![]).await;
                }
                w.shutdown().await.unwrap();
            }
        });
        let (outs, _) = h.finish().await;
        feed.await.unwrap();
        let took = started.elapsed();
        done.store(true, std::sync::atomic::Ordering::Relaxed);
        sampler.await.unwrap();
        let ready = readies.await.unwrap();
        assert_eq!(written(&outs).len(), streams * given);
        let last = ready.last().unwrap().clone();
        let kept = plan.state.job.as_ref().map(|job| {
            let (mut runs, mut written, mut merges) = (0, 0, 0);
            for n in &names {
                let c = job.stores(&format!("versions.{n}")).counts();
                runs += c.barrier.count;
                written += c.barrier.bytes;
                merges += c.merges;
            }
            let mut by_lane: Vec<usize> = last.snapshots.iter().map(|s| s.runs.len()).collect();
            by_lane.sort_unstable();
            Kept {
                by_lane,
                runs,
                written,
                merges,
                named: last.snapshots.iter().map(|s| s.runs.len()).sum(),
                pending: last.snapshots.iter().map(|s| s.pending.len()).sum(),
                caches: job.cache_bytes(),
            }
        });
        out.push(Many {
            took,
            peak: peak.load(std::sync::atomic::Ordering::Relaxed) - base,
            base,
            kept,
        });
        from = Some(last);
    }
    out
}

/// The versions of many streams, each with a few rows on each writer lane,
/// share the job's budget: a record carries as much of their pending sets
/// as it may, rather than each stream's small share of it, so far fewer
/// lanes write a run at each barrier than there are lanes with changes.
#[tokio::test(flavor = "multi_thread")]
async fn versions_of_many_streams_share_the_jobs_budget() {
    let reads = many_streams(300, 2, (100, 100), 768 << 20, 1, true).await;
    for r in &reads {
        let k = r.kept.as_ref().unwrap();
        println!("{} runs written, {} bytes pending", k.runs, k.pending);
        assert!(k.pending <= 1 << 20, "{} bytes pending", k.pending);
        assert!(k.runs < 500, "{} runs of 600 lanes with changes", k.runs);
    }
}

/// A job of `STREAMS` streams (default 300) that keep versions, on `LANES`
/// lanes and writer lanes (default 8), with `MEMORY` bytes (default 768
/// MiB, the smallest engine's), reads `ROWS` rows of each (default 10),
/// then each again with `CHANGED` of them changed (default all), in
/// `EPOCHS` epochs a read (default 1); `VERSIONS=off` keeps none, for
/// what the rest takes. Each read prints what [`Many`] has. A measure, not
/// a check:
/// `cargo test -p openetl-engine versions_of_many_streams -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn versions_of_many_streams() {
    let var = |name: &str, default: u64| -> u64 {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let (streams, lanes) = (var("STREAMS", 300) as usize, var("LANES", 8) as u32);
    let (rows_each, memory) = (var("ROWS", 10) as usize, var("MEMORY", 768 << 20));
    let changed = var("CHANGED", rows_each as u64) as usize;
    let epochs = var("EPOCHS", 1).max(1);
    let versions = std::env::var("VERSIONS").as_deref() != Ok("off");
    let reads = many_streams(
        streams,
        lanes,
        (rows_each, changed),
        memory,
        epochs,
        versions,
    );
    for (i, r) in reads.await.iter().enumerate() {
        let what = format!(
            "{streams} streams on {lanes} lanes, read {} of {rows_each} rows each in {epochs} epochs: {:?}, peak {} MiB above {} MiB",
            i + 1,
            r.took,
            r.peak >> 20,
            r.base >> 20
        );
        match &r.kept {
            None => eprintln!("{what}, no versions"),
            Some(k) => {
                let at = |q: usize| {
                    k.by_lane
                        .get(k.by_lane.len() * q / 100)
                        .copied()
                        .unwrap_or(0)
                };
                eprintln!(
                    "{what}, {} runs written ({} KiB), {} merges, {} runs named, a lane's: least {}, median {}, 99th {}, most {}, pending sets {} bytes, block caches {} KiB",
                    k.runs,
                    k.written >> 10,
                    k.merges,
                    k.named,
                    at(0),
                    at(50),
                    at(99),
                    k.by_lane.last().copied().unwrap_or(0),
                    k.pending,
                    k.caches >> 10
                )
            }
        }
    }
}

/// A stream that ends, whose versions were in line for merge room or
/// merging at its last barrier, holds none of the room after its end:
/// another stream's versions merge on for many epochs. Each stream's runs
/// of an epoch are about a sixth of the job's merge room, so a merge of four
/// fits, and two at once don't.
#[tokio::test]
async fn a_stream_that_ends_holds_no_merge_room() {
    const ROWS: usize = 2600;
    for a_first in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut req = crate::plan::tests::lanes_request(&["a", "b"], 1, 1);
        req.state_url = format!("file://{}", dir.path().join("state").display());
        req.state_pending_bytes = Some(0);
        req.limits = Some(openetl_proto::engine::v1::Limits {
            memory_bytes: 16 << 20,
            ..Default::default()
        });
        for s in &mut req.streams {
            s.primary_key = vec!["id".into()];
            s.cursor_field = "at".into();
            s.read_start_micros = 1000;
            s.versions_operator = format!("versions.{}", s.name);
        }
        let plan = Arc::new(Plan::from_request(req).unwrap());
        let mut h = start_plan(plan.clone());
        let (closed, none) = tokio::sync::mpsc::channel(1);
        drop(closed);
        let mut events = std::mem::replace(&mut h.events, none);
        let readies = tokio::spawn(async move { ready(&mut events).await });
        let mut ins = std::mem::take(&mut h.ins);
        let feed = tokio::spawn(async move {
            let w = &mut ins[0];
            for e in 1..=30u64 {
                let rows = page(e as usize * ROWS..(e as usize + 1) * ROWS);
                // At epoch 4, the stream whose barrier comes first starts a
                // merge of its four runs, and the other's is refused.
                let order: &[u32] = match (e <= 4, a_first) {
                    (false, _) => &[1],
                    (true, true) => &[0, 1],
                    (true, false) => &[1, 0],
                };
                for &s in order {
                    put(w, FrameKind::Rows, s, rows.clone()).await;
                    put(w, FrameKind::Barrier, s, epoch_barrier(1, e)).await;
                }
                if e == 4 {
                    put(w, FrameKind::EndOfStream, 0, vec![]).await;
                }
            }
            put(w, FrameKind::EndOfStream, 1, vec![]).await;
            w.shutdown().await.unwrap();
        });
        h.finish().await;
        feed.await.unwrap();
        let last = readies.await.unwrap().pop().unwrap();
        let named: usize = last
            .snapshots
            .iter()
            .filter(|s| s.operator == "versions.b")
            .map(|s| s.runs.len())
            .sum();
        let job = plan.state.job.as_ref().unwrap();
        let merges = job.stores("versions.b").counts().merges;
        println!("a first at epoch 4: {a_first}; b merged {merges} times, names {named} runs");
        let what = format!("b merged {merges} times, names {named} runs, a first: {a_first}");
        // Merges run in the background, so how many finish varies; before
        // the room was released there were none, and all 30 runs stayed.
        assert!(merges >= 1 && named < 30, "{what}");
    }
}

/// Of the runs a lookup finds gone, those that can be forgotten are, and
/// the attempt fails naming only one that can't: a run a lookup took a
/// block of before it went.
#[tokio::test]
async fn a_gone_run_that_cant_be_forgotten_is_named_alone() {
    use super::super::versions::Versions;
    use openetl_normalize::versions::{HASH_COLUMN, SCHEMA_KEY, schema_version};

    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    // Two runs of read 1, of epochs 1 and 2.
    let mut h = start_lanes(users(d, 1, 1000, &Restored::default(), 1));
    let mut ins = std::mem::take(&mut h.ins);
    let feed = tokio::spawn(async move {
        let w = &mut ins[0];
        for e in 1..=2u64 {
            let at = (e as usize - 1) * 1000;
            put(w, FrameKind::Rows, 0, page(at..at + 1000)).await;
            put(w, FrameKind::Barrier, 0, epoch_barrier(1, e)).await;
        }
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        w.shutdown().await.unwrap();
    });
    let (outs, mut ready) = h.finish().await;
    feed.await.unwrap();
    let first = written(&outs);
    let r1 = ready.pop().unwrap();
    let runs: Vec<String> = r1
        .snapshots
        .iter()
        .filter(|s| s.operator == OP)
        .flat_map(|s| s.runs.clone())
        .collect();
    assert_eq!(runs.len(), 2, "{runs:?}");
    let mut req = users(d, 2, 2000, &Restored::of(&r1), 1);
    req.first_epoch = 3;
    let plan = Arc::new(Plan::from_request(req).unwrap());
    let lanes = plan.lanes.clone();
    let mut h = start_plan(plan);
    let flow = loop {
        match lanes.0.get().and_then(std::sync::Weak::upgrade) {
            Some(f) => break f,
            None => tokio::task::yield_now().await,
        }
    };
    let schema = ipc::decode_schema(&r1.schemas["users"]).unwrap();
    let version = schema_version(&schema, &[]).to_string();
    let batch = |ids: &[i64]| {
        use arrow_schema::{Field, Schema};
        let hash = Field::new(HASH_COLUMN, DataType::UInt64, false)
            .with_metadata([(SCHEMA_KEY.to_owned(), version.clone())].into());
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_openetl_id", DataType::FixedSizeBinary(16), false),
            hash,
        ]);
        let records = ids.iter().map(|&id| record(&first, id));
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(arrow_array::Int64Array::from(ids.to_vec())),
                Arc::new(arrow_array::FixedSizeBinaryArray::try_from_iter(records).unwrap()),
                Arc::new(arrow_array::UInt64Array::from(vec![7; ids.len()])),
            ],
        )
        .unwrap()
    };
    let mut v = Versions::open(&flow, 0).await.unwrap();
    // A lookup takes a block of epoch 1's run, and asks epoch 2's filters.
    v.apply(&flow, Out::Batch(0, batch(&[5]))).await.unwrap();
    for run in &runs {
        std::fs::remove_file(d.join("state").join(run)).unwrap();
    }
    let ids: Vec<i64> = (100..150).chain(1100..1150).collect();
    let err = match v.apply(&flow, Out::Batch(0, batch(&ids))).await {
        Err(e) => e,
        Ok(_) => panic!("a run read from was forgotten"),
    };
    assert_eq!(err.code, ErrorCode::CheckpointCorrupt, "{}", err.message);
    let (read, unread) = (&runs[1], &runs[0]);
    assert!(read.contains("/e1/") && unread.contains("/e2/"), "{runs:?}");
    let name = |r: &String| r.split_once('/').map_or(r.clone(), |p| p.1.to_owned());
    assert!(err.message.contains(&name(read)), "{}", err.message);
    assert!(!err.message.contains(&name(unread)), "{}", err.message);
    drop(flow);
    h.end_all(1).await;
    let _ = h.job.await;
}
