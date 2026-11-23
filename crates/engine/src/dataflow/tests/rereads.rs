//! Stateful statements across syncs, through the record and a restore, and
//! over rows a read gives again.

use openetl_proto::engine::v1::StateSnapshot;

use super::*;

/// Each key's latest row of a, written where its v is under 50.
const LATEST_UNDER_50: &str = "INSERT INTO minutes SELECT id, email, v, at FROM \
     (SELECT *, row_number() OVER (PARTITION BY email ORDER BY at DESC) AS rn FROM a) \
     WHERE rn = 1 AND v < 50";

/// Each writer lane's rows of a dedupe, as key, op and id, in order: a
/// delete has no id.
fn ops(outs: &[Vec<frame::Frame>]) -> Vec<(String, String, Option<i64>)> {
    let mut got = Vec::new();
    for f in outs.iter().flatten() {
        if f.kind != FrameKind::Batch || f.stream != 0 {
            continue;
        }
        let b = ipc::decode_batch(f.payload.clone()).unwrap();
        let ops = column_strings(&b, "_openetl_op");
        for ((e, op), id) in column_strings(&b, "email")
            .into_iter()
            .zip(ops)
            .zip(column_ints(&b, "id"))
        {
            got.push((e.unwrap(), op.unwrap_or_default(), id));
        }
    }
    got.sort();
    got
}

/// One sync of a stream that isn't read in the order of its time, on two
/// lanes, each lane's rows a split of their own read to its end, restored
/// from `restored`: what the writer lanes got and the epoch made ready.
async fn held_sync(
    state: &std::path::Path,
    generation: u64,
    restored: Vec<StateSnapshot>,
    lanes: [Vec<String>; 2],
) -> (Vec<Vec<frame::Frame>>, Vec<EpochReady>) {
    let mut req = dedupe_request(state, LATEST_UNDER_50, (2, generation, generation), None);
    req.quarantine_url = format!("file://{}", state.join("q").display());
    req.restored_snapshots = restored;
    event_time(&mut req).held = true;
    let mut h = start_lanes(req);
    for (l, (w, rs)) in h.ins.iter_mut().zip(lanes).enumerate() {
        let id = format!("s{l}");
        put(w, FrameKind::Split, 0, split(&id, None)).await;
        let refs: Vec<&str> = rs.iter().map(String::as_str).collect();
        put(w, FrameKind::Rows, 0, rows(&refs)).await;
        put(w, FrameKind::Split, 0, split(&id, Some(watermark::END))).await;
        barriers(w, generation, generation).await;
    }
    h.end_all(2).await;
    h.finish().await
}

/// A latest-row dedupe over a stream read by another cursor than its event
/// time, whose read is held to its end each sync, keeps its state in the
/// record at that end, as no key of it is ever done: the next sync deletes
/// a key whose newer row the filter drops, and drops a row older than the
/// key's latest that an earlier sync wrote.
#[tokio::test]
async fn a_held_reads_latest_row_dedupe_keeps_its_keys_to_the_next_sync() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let (outs, ready) = held_sync(
        &state,
        1,
        Vec::new(),
        [vec![minute_row(1, "x", 10)], vec![minute_row(2, "y", 20)]],
    )
    .await;
    assert_eq!(
        ops(&outs),
        [
            ("x".into(), "insert".into(), Some(1)),
            ("y".into(), "insert".into(), Some(2))
        ]
    );
    assert_eq!(ready.len(), 1);
    let restored = ready[0].snapshots.clone();

    // x's newer row fails the filter; y's row is older than the one the
    // last sync wrote; z is new.
    let (outs, ready) = held_sync(
        &state,
        2,
        restored,
        [
            vec![minute_row(60, "x", 30)],
            vec![minute_row(3, "y", 5), minute_row(4, "z", 40)],
        ],
    )
    .await;
    assert_eq!(
        ops(&outs),
        [
            ("x".into(), "delete".into(), None),
            ("z".into(), "insert".into(), Some(4))
        ]
    );
    assert_eq!(ready.len(), 1);
}

/// x's rows of each minute and y's, by count.
const SESSIONS: &str = "INSERT INTO minutes SELECT window_start, email, count(*) AS n \
     FROM a GROUP BY session(at, INTERVAL '30 seconds'), email";

/// A row of a, of its own v.
fn version(id: i64, email: &str, v: i64, second: i64) -> String {
    format!(
        r#"{{"id": {id}, "email": "{email}", "v": {v}, "at": "1970-01-01T00:00:{second:02}Z"}}"#
    )
}

/// What a statement over a, read by its event time as cursor, gives over
/// rows a read gives again. The first attempt reads x at 10s and 20s on
/// lane 0 and y at 15s on lane 1, and stops at a barrier, as one does that
/// fails after it. The next, on three lanes, restores its record and reads
/// each split again from its watermark, to the end: x's rows again, a
/// second version of x's row at 20s, of its id and time and another v, x
/// at 30s, and y's row again. `held` reads it as a stream not read in the
/// order of its time is, `read_only` runs the second as a redo, and
/// `keyed` keeps a's primary key, without which it collapses. `lead` comes
/// before the second's rows on lane 0. Its reads may give a record again.
async fn reread(
    sql: &str,
    over: bool,
    flags: (bool, bool, bool),
    lead: Option<&str>,
) -> (Vec<Vec<frame::Frame>>, Vec<EpochReady>) {
    reread_volatile(sql, over, flags, lead, &[]).await
}

/// `reread`, with a's `volatile` columns.
async fn reread_volatile(
    sql: &str,
    over: bool,
    (held, read_only, keyed): (bool, bool, bool),
    lead: Option<&str>,
    volatile: &[&str],
) -> (Vec<Vec<frame::Frame>>, Vec<EpochReady>) {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let request = |lanes, generation| {
        let mut req = match over {
            true => over_request(&state, sql, lanes, generation, generation),
            false => window_request(&state, sql, lanes, generation, generation),
        };
        req.streams[0].cursor_field = "at".into();
        req.quarantine_url = format!("file://{}", dir.path().join("q").display());
        event_time(&mut req).held = held;
        event_time(&mut req).rereads = true;
        req.streams[0].volatile_columns = volatile.iter().map(|&c| c.to_owned()).collect();
        if !keyed {
            req.streams[0].primary_key.clear();
        }
        req
    };
    let mut h = start_lanes(request(2, 1));
    for (l, w) in h.ins.iter_mut().enumerate() {
        put(w, FrameKind::Split, 0, split(&format!("s{l}"), None)).await;
        let rs = match l {
            0 => vec![minute_row(1, "x", 10), minute_row(2, "x", 20)],
            _ => vec![minute_row(10, "y", 15)],
        };
        let refs: Vec<&str> = rs.iter().map(String::as_str).collect();
        put(w, FrameKind::Rows, 0, rows(&refs)).await;
        barriers(w, 1, 1).await;
    }
    h.end_all(2).await;
    let (_, first) = h.finish().await;
    assert_eq!(first.len(), 1);
    let marks: BTreeMap<String, i64> = match first[0].watermarks.contains_key("a") {
        true => split_marks(&first[0], "a").into_iter().collect(),
        false => BTreeMap::new(),
    };

    let mut req = request(3, 2);
    req.restored_snapshots = first[0].snapshots.clone();
    req.state_read_only = read_only;
    if let Some(least) = marks.values().min() {
        event_time(&mut req).floor = Some(*least);
    }
    let mut h = start_lanes(req);
    for (l, w) in h.ins.iter_mut().enumerate() {
        let id = format!("s{l}");
        let rs = match l {
            0 => lead
                .map(str::to_owned)
                .into_iter()
                .chain([
                    minute_row(1, "x", 10),
                    minute_row(2, "x", 20),
                    version(2, "x", 99, 20),
                    minute_row(3, "x", 30),
                ])
                .collect(),
            1 => vec![minute_row(10, "y", 15)],
            _ => Vec::new(),
        };
        if l < 2 {
            put(w, FrameKind::Split, 0, split(&id, marks.get(&id).copied())).await;
            let refs: Vec<&str> = rs.iter().map(String::as_str).collect();
            put(w, FrameKind::Rows, 0, rows(&refs)).await;
            put(w, FrameKind::Split, 0, split(&id, Some(watermark::END))).await;
        }
        barriers(w, 2, 2).await;
    }
    h.end_all(2).await;
    h.finish().await
}

/// A tumbling window takes a row a read gives again once, while its
/// window is kept: the copy of x's row at 10s, behind the watermark, and
/// at 20s, at it. A second version of a row, of its record ID and other
/// values, is a row of its own. So is the redo's.
#[tokio::test]
async fn a_window_takes_a_row_read_again_once() {
    for read_only in [false, true] {
        let (outs, ready) = reread(MINUTES, false, (false, read_only, true), None).await;
        assert_eq!(
            all_rows(&outs, 2),
            [row(0, "x", 4, 2), row(0, "y", 1, 2)],
            "read_only {read_only}"
        );
        // A redo reports no operator.
        let repeats: u64 = ready[0].operators.iter().map(|o| o.repeats).sum();
        assert_eq!(repeats, if read_only { 0 } else { 3 });
        assert_eq!(quarantined(&ready, "minutes"), 0);
    }
}

/// A column the stream says is volatile doesn't tell a row read again from
/// the row the window took: the second version of x's row at 20s, whose v
/// alone differs, is a repeat.
#[tokio::test]
async fn a_window_leaves_volatile_columns_out_of_a_row_read_again() {
    let (outs, ready) = reread_volatile(MINUTES, false, (false, false, true), None, &["v"]).await;
    assert_eq!(all_rows(&outs, 2), [row(0, "x", 3, 2), row(0, "y", 1, 2)]);
    let repeats: u64 = ready[0].operators.iter().map(|o| o.repeats).sum();
    assert_eq!(repeats, 4);
}

/// A session takes a row a read gives again once: a session that holds
/// its time took it.
#[tokio::test]
async fn a_session_takes_a_row_read_again_once() {
    let (outs, ready) = reread(SESSIONS, false, (false, false, true), None).await;
    assert_eq!(all_rows(&outs, 2), [row(10, "x", 4, 2), row(15, "y", 1, 2)]);
    assert_eq!(ready[0].operators[0].repeats, 3);
}

/// OVER takes a row a read gives again once where the row still waits for
/// the watermark: x's at 20s, which the least of the lanes' watermarks,
/// y's 15s, hadn't passed. x's at 10s had left: its copy is late. A second
/// version of a row, of its record ID and other values, is a row of its
/// own, a peer of the first.
#[tokio::test]
async fn over_takes_a_waiting_row_read_again_once() {
    let (outs, ready) = reread(RUNNING, true, (false, false, true), None).await;
    let mut got: Vec<(i64, i64)> = over_rows(&outs, 2)
        .into_iter()
        .flatten()
        .map(|r| (r.id, r.total))
        .collect();
    got.sort();
    assert_eq!(got, [(2, 102), (2, 102), (3, 105), (10, 10)]);
    assert_eq!(ready[0].operators[0].repeats, 2);
    assert_eq!(quarantined(&ready, "minutes"), 1, "x at 10s had left");
}

/// A stream not read in the order of its time holds every row OVER takes
/// to the end of its read, so a restore from a barrier inside it, which
/// reads its rows again, gives each once.
#[tokio::test]
async fn over_takes_a_held_reads_rows_read_again_once() {
    let (outs, ready) = reread(RUNNING, true, (true, false, true), None).await;
    let mut got: Vec<(i64, i64)> = over_rows(&outs, 2)
        .into_iter()
        .flatten()
        .map(|r| (r.id, r.total))
        .collect();
    got.sort();
    assert_eq!(got, [(1, 1), (2, 102), (2, 102), (3, 105), (10, 10)]);
    assert_eq!(ready[0].operators[0].repeats, 3);
}

/// A row read again is a repeat whatever columns the stream gained since,
/// that it has no value of, and wherever it is in its batch: here after a
/// row of a column the first read had none of.
#[tokio::test]
async fn a_window_takes_a_row_read_again_once_beside_a_new_column() {
    let new = r#"{"id": 5, "email": "x", "v": 5, "at": "1970-01-01T00:00:25Z", "note": "n"}"#;
    let (outs, ready) = reread(MINUTES, false, (false, false, true), Some(new)).await;
    assert_eq!(all_rows(&outs, 2), [row(0, "x", 5, 2), row(0, "y", 1, 2)]);
    assert_eq!(ready[0].operators[0].repeats, 3);
}

/// Over a stream without a primary key that collapses, a row's record ID
/// is its values': a copy read again is a repeat, and another version is
/// a row of its own.
#[tokio::test]
async fn a_window_over_a_collapsed_stream_takes_a_row_read_again_once() {
    let (outs, ready) = reread(MINUTES, false, (false, false, false), None).await;
    assert_eq!(all_rows(&outs, 2), [row(0, "x", 4, 2), row(0, "y", 1, 2)]);
    assert_eq!(ready[0].operators[0].repeats, 3);
}

/// The bytes of state the record carries after one sync of `n` rows of x,
/// all in one minute, which stays open, over a stream whose reads may give
/// a record again or not.
async fn open_minute(rereads: bool, n: i64) -> usize {
    let dir = tempfile::tempdir().unwrap();
    let mut req = window_request(&dir.path().join("state"), MINUTES, 1, 1, 1);
    req.streams[0].cursor_field = "at".into();
    event_time(&mut req).rereads = rereads;
    let mut h = start_lanes(req);
    let w = &mut h.ins[0];
    put(w, FrameKind::Split, 0, split("s", None)).await;
    let rs: Vec<String> = (1..=n).map(|i| minute_row(i, "x", i)).collect();
    let refs: Vec<&str> = rs.iter().map(String::as_str).collect();
    put(w, FrameKind::Rows, 0, rows(&refs)).await;
    barriers(w, 1, 1).await;
    h.end_all(2).await;
    let (_, ready) = h.finish().await;
    ready[0].snapshots.iter().map(|s| s.pending.len()).sum()
}

/// A window over a stream replayed exactly keeps no rows' IDs: its state is
/// its windows', whatever their rows. Over one that may read a record
/// again, it grows with the rows.
#[tokio::test]
async fn a_window_over_an_exact_stream_keeps_no_rows_ids() {
    let (two, forty) = (open_minute(false, 2).await, open_minute(false, 40).await);
    assert!(two > 0);
    assert_eq!(two, forty);
    let (two, forty) = (open_minute(true, 2).await, open_minute(true, 40).await);
    assert!(forty >= two + 38 * 24, "{two} and {forty} bytes");
}
