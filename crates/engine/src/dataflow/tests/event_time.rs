//! Event time and group windows through the dataflow, against a model.
//!
//! A case spreads a stream's splits over lanes: rows whose times are out of
//! order within the watermark's delay and, some, beyond it; rows without a
//! time; splits that read nothing and move on as idle ones do; barriers
//! anywhere. The model works out each lane's watermark from its frames,
//! which windows each row goes to or that it's late, and the table a merge
//! of the output leaves. The engine must leave that table, quarantine the
//! rows the model calls late and no others, and report each split's
//! watermark at each barrier as the model has it.
//!
//! With one lane, the window takes rows and watermarks in the lane's order,
//! so the model is exact. With several, it takes the lanes in an order of
//! its own, bounded by barriers: a lane's rows past its barrier k come
//! after every lane's watermarks before theirs, and before any lane's past
//! k + 1. A row whose fate differs within those bounds is left out of the
//! input; a row that moves its split's watermark never is.

use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::watermark::tests::Rng;
use crate::watermark::{END, NONE};

const KEYS: [&str; 3] = ["x", "y", "z"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Window {
    Tumble,
    Hop,
    Session,
}

/// Tumbling windows of 10 seconds; hopping windows of 15 every 5; sessions
/// with gaps of 4.
const TUMBLE: i64 = 10 * SEC;
const SLIDE: i64 = 5 * SEC;
const HOP: i64 = 15 * SEC;
const GAP: i64 = 4 * SEC;

impl Window {
    fn sql(self, having: bool) -> String {
        let w = match self {
            Window::Tumble => "tumble(at, INTERVAL '10 seconds')",
            Window::Hop => "hop(at, INTERVAL '5 seconds', INTERVAL '15 seconds')",
            Window::Session => "session(at, INTERVAL '4 seconds')",
        };
        let having = match having {
            true => " HAVING sum(v) > 0",
            false => "",
        };
        format!(
            "INSERT INTO minutes SELECT window_start, email, count(*) AS n, sum(v) AS total, max(id) AS top \
             FROM a GROUP BY {w}, email{having}"
        )
    }

    /// The starts of the windows a time falls in.
    fn starts(self, t: i64) -> Vec<i64> {
        match self {
            Window::Tumble => vec![t - t.rem_euclid(TUMBLE)],
            Window::Hop => {
                let mut s = t - t.rem_euclid(SLIDE);
                let mut out = Vec::new();
                while s + HOP > t {
                    out.push(s);
                    s -= SLIDE;
                }
                out
            }
            Window::Session => Vec::new(),
        }
    }

    fn size(self) -> i64 {
        match self {
            Window::Tumble => TUMBLE,
            Window::Hop => HOP,
            Window::Session => GAP,
        }
    }
}

#[derive(Clone, Debug)]
struct Row {
    id: i64,
    key: &'static str,
    v: i64,
    /// Microseconds, in whole seconds.
    at: Option<i64>,
}

impl Row {
    fn json(&self) -> String {
        let at = match self.at {
            Some(t) => {
                let s = t / SEC;
                format!(
                    r#""1970-01-01T{:02}:{:02}:{:02}Z""#,
                    s / 3600,
                    s / 60 % 60,
                    s % 60
                )
            }
            None => "null".into(),
        };
        format!(
            r#"{{"id": {}, "email": "{}", "v": {}, "at": {at}}}"#,
            self.id, self.key, self.v
        )
    }
}

/// What a lane reads, in order.
#[derive(Clone, Debug)]
enum Ev {
    /// A batch of a split's rows.
    Rows(usize, Vec<Row>),
    /// A Split frame that raises a split's watermark, as the runner sends
    /// for an idle split: microseconds.
    Raise(usize, i64),
    /// A split's read ends.
    End(usize),
    Barrier,
}

#[derive(Clone, Debug)]
struct Case {
    window: Window,
    having: bool,
    /// Microseconds.
    lateness: i64,
    delay: i64,
    held: bool,
    lanes: Vec<Vec<Ev>>,
}

impl Case {
    /// A case of `window`, with `barriers` per lane, or a few.
    fn generate(seed: u64, window: Window, barriers: Option<usize>) -> Case {
        let mut rng = Rng(seed);
        let lanes = match rng.below(5) {
            0..=2 => 1,
            _ => 2 + rng.below(2),
        };
        let splits = lanes + rng.below(4);
        let delay = rng.below(4) as i64;
        let lateness = [0, 0, 6, 20][rng.below(4)] * SEC;
        let having = rng.below(3) == 0;
        let lane_of: Vec<usize> = (0..splits)
            .map(|s| match s < lanes {
                true => s,
                false => rng.below(lanes),
            })
            .collect();
        let mut id = 0;
        let mut per_split: Vec<Vec<Ev>> = Vec::new();
        for s in 0..splits {
            let quiet = splits > 1 && rng.below(5) == 0;
            let mut evs = Vec::new();
            let mut top = 60 + rng.below(30) as i64;
            let batches = match quiet {
                true => 0,
                false => rng.below(12),
            };
            for _ in 0..batches {
                let mut rows = Vec::new();
                for _ in 0..1 + rng.below(4) {
                    top += rng.below(4) as i64;
                    // Mostly within the delay of the split's latest; some
                    // behind it, which may be late.
                    let t = match rng.below(7) {
                        0 => top - delay - 1 - rng.below(30) as i64,
                        _ => top - rng.below(delay as usize + 1) as i64,
                    };
                    id += 1;
                    rows.push(Row {
                        id,
                        key: KEYS[rng.below(KEYS.len())],
                        v: rng.below(90) as i64 - 30,
                        at: (rng.below(30) != 0).then_some(t.max(0) * SEC),
                    });
                }
                evs.push(Ev::Rows(s, rows));
                if rng.below(8) == 0 {
                    evs.push(Ev::Raise(s, (top - delay + rng.below(8) as i64) * SEC));
                }
            }
            if quiet {
                let mut at = 60 + rng.below(30) as i64;
                for _ in 0..1 + rng.below(5) {
                    at += rng.below(20) as i64;
                    evs.push(Ev::Raise(s, at * SEC));
                }
            }
            evs.push(Ev::End(s));
            per_split.push(evs);
        }
        let barriers = barriers.unwrap_or_else(|| 1 + rng.below(5));
        let mut lane_evs = Vec::new();
        for l in 0..lanes {
            let mine: Vec<usize> = (0..splits).filter(|&s| lane_of[s] == l).collect();
            let mut at = vec![0; splits];
            let mut evs = Vec::new();
            loop {
                let open: Vec<usize> = mine
                    .iter()
                    .copied()
                    .filter(|&s| at[s] < per_split[s].len())
                    .collect();
                if open.is_empty() {
                    break;
                }
                let s = open[rng.below(open.len())];
                evs.push(per_split[s][at[s]].clone());
                at[s] += 1;
            }
            for _ in 1..barriers {
                let p = rng.below(evs.len() + 1);
                evs.insert(p, Ev::Barrier);
            }
            evs.push(Ev::Barrier);
            lane_evs.push(evs);
        }
        Case {
            window,
            having,
            lateness,
            delay: delay * SEC,
            held: false,
            lanes: lane_evs,
        }
    }

    /// The same input with its barriers taken out and `n` put back in each
    /// lane, the last after its splits' ends, at positions from `seed`.
    fn rebarriered(&self, seed: u64, n: usize) -> Case {
        let mut rng = Rng(seed);
        let mut out = self.clone();
        for evs in &mut out.lanes {
            evs.retain(|e| !matches!(e, Ev::Barrier));
            for _ in 1..n {
                let p = rng.below(evs.len() + 1);
                evs.insert(p, Ev::Barrier);
            }
            evs.push(Ev::Barrier);
        }
        out
    }

    fn rows(&self) -> usize {
        self.lanes
            .iter()
            .flatten()
            .map(|e| match e {
                Ev::Rows(_, r) => r.len(),
                _ => 0,
            })
            .sum()
    }
}

/// What a lane sends the window, in order.
#[derive(Debug)]
enum Sent {
    /// A batch, after `k` of the lane's barriers, when the lane's watermark
    /// was `w`.
    Batch {
        rows: Vec<Row>,
        k: usize,
        w: i64,
    },
    Mark(i64),
}

/// A lane as the model has it.
#[derive(Debug, Default)]
struct LaneModel {
    sent: Vec<Sent>,
    /// The lane's watermark at each barrier: NONE before the first.
    at_barrier: Vec<i64>,
    /// Each split's watermark at each barrier, without those it has none of.
    marks: Vec<BTreeMap<String, i64>>,
    /// Times a split that read no rows moved the lane's watermark.
    idle_moves: usize,
}

fn lane_model(case: &Case, l: usize) -> LaneModel {
    let mut m = LaneModel {
        at_barrier: vec![NONE],
        ..Default::default()
    };
    let mut wm: BTreeMap<usize, i64> = BTreeMap::new();
    let mut read: BTreeSet<usize> = BTreeSet::new();
    for e in &case.lanes[l] {
        if let Ev::Rows(s, _) | Ev::Raise(s, _) | Ev::End(s) = e {
            wm.insert(*s, NONE);
        }
    }
    let mut sent = NONE;
    let mut k = 0;
    for e in &case.lanes[l] {
        let mut raised = None;
        match e {
            Ev::Rows(s, rows) => {
                read.insert(*s);
                m.sent.push(Sent::Batch {
                    rows: rows.clone(),
                    k,
                    w: sent,
                });
                if let Some(latest) = rows.iter().filter_map(|r| r.at).max()
                    && !case.held
                {
                    let w = wm.get_mut(s).unwrap();
                    *w = (*w).max(latest - case.delay);
                }
            }
            Ev::Raise(s, x) => {
                let w = wm.get_mut(s).unwrap();
                *w = (*w).max(*x);
                raised = Some(*s);
            }
            Ev::End(s) => {
                wm.insert(*s, END);
            }
            Ev::Barrier => {
                k += 1;
                m.at_barrier.push(sent);
                m.marks.push(
                    wm.iter()
                        .filter(|(_, w)| **w != NONE)
                        .map(|(s, w)| (s.to_string(), *w))
                        .collect(),
                );
                continue;
            }
        }
        let now = match wm.values().any(|w| *w == NONE) {
            true => NONE,
            false => wm.values().copied().min().unwrap_or(END),
        };
        if now != NONE && now > sent {
            sent = now;
            m.sent.push(Sent::Mark(now));
            if raised.is_some_and(|s| !read.contains(&s)) {
                m.idle_moves += 1;
            }
        }
    }
    m
}

/// A window's aggregates: count(*), sum(v) and max(id).
type Agg = (i64, i64, i64);

fn agg(rows: &[Row]) -> Agg {
    (
        rows.len() as i64,
        rows.iter().map(|r| r.v).sum(),
        rows.iter().map(|r| r.id).max().unwrap(),
    )
}

#[derive(Clone, Debug)]
struct Session {
    start: i64,
    /// Its last row's time and the gap.
    end: i64,
    rows: Vec<Row>,
    visible: bool,
}

/// A group window over rows that come with the window's watermark.
struct Model {
    window: Window,
    having: bool,
    lateness: i64,
    w: i64,
    /// Tumbling and hopping windows' rows, by key and start.
    windows: BTreeMap<(&'static str, i64), Vec<Row>>,
    sessions: BTreeMap<&'static str, Vec<Session>>,
    /// The sessions' rows the output has.
    out: BTreeMap<(i64, String), Agg>,
    late: BTreeSet<i64>,
}

impl Model {
    fn new(case: &Case) -> Model {
        Model {
            window: case.window,
            having: case.having,
            lateness: case.lateness,
            w: NONE,
            windows: BTreeMap::new(),
            sessions: BTreeMap::new(),
            out: BTreeMap::new(),
            late: BTreeSet::new(),
        }
    }

    fn passes(&self, a: &Agg) -> bool {
        !self.having || a.1 > 0
    }

    /// The windows a row at `t` goes to when the watermark is at `w`.
    fn placed(&self, t: i64, w: i64) -> Vec<i64> {
        self.window
            .starts(t)
            .into_iter()
            .filter(|s| (s + self.window.size()).saturating_add(self.lateness) > w)
            .collect()
    }

    /// A batch the window takes while its watermark is at `w`.
    fn rows(&mut self, rows: &[Row], w: i64) {
        if self.window == Window::Session {
            return self.session_rows(rows, w);
        }
        for r in rows {
            let Some(t) = r.at else {
                self.late.insert(r.id);
                continue;
            };
            let placed = self.placed(t, w);
            if placed.is_empty() {
                self.late.insert(r.id);
            }
            for s in placed {
                self.windows.entry((r.key, s)).or_default().push(r.clone());
            }
        }
    }

    fn session_rows(&mut self, rows: &[Row], w: i64) {
        let mut by_key: BTreeMap<&'static str, Vec<&Row>> = BTreeMap::new();
        for r in rows {
            match r.at {
                Some(_) => by_key.entry(r.key).or_default().push(r),
                None => {
                    self.late.insert(r.id);
                }
            }
        }
        for (key, rs) in by_key {
            let mut list = self.sessions.remove(key).unwrap_or_default();
            let before: BTreeMap<i64, (i64, usize, bool)> = list
                .iter()
                .map(|s| (s.start, (s.end, s.rows.len(), s.visible)))
                .collect();
            for r in rs {
                let t = r.at.unwrap();
                let end = t + GAP;
                let meets = |s: &Session| s.start <= end && t <= s.end;
                let (start, last) = list
                    .iter()
                    .filter(|s| meets(s))
                    .fold((t, end), |(a, b), s| (a.min(s.start), b.max(s.end)));
                if (last + 1).saturating_add(self.lateness) <= w {
                    self.late.insert(r.id);
                    continue;
                }
                let mut merged = Session {
                    start,
                    end: last,
                    rows: vec![r.clone()],
                    visible: false,
                };
                let mut rest = Vec::new();
                for s in list {
                    match meets(&s) {
                        true => {
                            if s.start == start {
                                merged.visible = s.visible;
                            }
                            merged.rows.extend(s.rows);
                        }
                        false => rest.push(s),
                    }
                }
                rest.push(merged);
                rest.sort_by_key(|s| s.start);
                list = rest;
            }
            for (start, (_, _, visible)) in &before {
                if *visible && !list.iter().any(|s| s.start == *start) {
                    self.out.remove(&(*start, key.to_owned()));
                }
            }
            for s in &mut list {
                let changed = before
                    .get(&s.start)
                    .is_none_or(|(end, n, _)| *end != s.end || *n != s.rows.len());
                if changed && s.end < w {
                    emit(&mut self.out, self.having, key, s);
                }
            }
            self.sessions.insert(key, list);
        }
    }

    fn advance(&mut self, w: i64) {
        if w <= self.w {
            return;
        }
        if self.window == Window::Session {
            for (key, list) in &mut self.sessions {
                for s in list.iter_mut() {
                    if s.end >= self.w && s.end < w {
                        emit(&mut self.out, self.having, key, s);
                    }
                }
                list.retain(|s| (s.end + 1).saturating_add(self.lateness) > w);
            }
        }
        self.w = w;
    }

    /// The table a merge of the output leaves.
    fn table(&self) -> BTreeMap<(i64, String), Agg> {
        match self.window {
            Window::Session => self.out.clone(),
            _ => self
                .windows
                .iter()
                .filter(|((_, s), _)| s + self.window.size() <= self.w)
                .map(|((k, s), rows)| ((*s, k.to_string()), agg(rows)))
                .filter(|(_, a)| self.passes(a))
                .collect(),
        }
    }

    /// The windows with rows that haven't closed.
    fn open(&self) -> usize {
        match self.window {
            Window::Session => self
                .sessions
                .values()
                .flatten()
                .filter(|s| s.end + 1 > self.w)
                .count(),
            _ => self
                .windows
                .keys()
                .filter(|(_, s)| s + self.window.size() > self.w)
                .count(),
        }
    }
}

fn emit(out: &mut BTreeMap<(i64, String), Agg>, having: bool, key: &str, s: &mut Session) {
    let a = agg(&s.rows);
    if !having || a.1 > 0 {
        out.insert((s.start, key.to_owned()), a);
        s.visible = true;
    } else if s.visible {
        out.remove(&(s.start, key.to_owned()));
        s.visible = false;
    }
}

/// What the model expects of a case: the input with the rows it leaves out
/// taken out, the table, the late rows, and each epoch's watermarks.
struct Expected {
    case: Case,
    table: BTreeMap<(i64, String), Agg>,
    late: BTreeSet<i64>,
    marks: Vec<BTreeMap<String, i64>>,
    dropped: usize,
    idle_moves: usize,
    /// Windows with rows that the input leaves open.
    open: usize,
}

fn expect(case: &Case) -> Expected {
    let lanes: Vec<LaneModel> = (0..case.lanes.len()).map(|l| lane_model(case, l)).collect();
    let mut model = Model::new(case);
    let mut keep: BTreeSet<i64> = BTreeSet::new();
    let mut dropped = 0;
    if lanes.len() == 1 {
        for s in &lanes[0].sent {
            match s {
                Sent::Batch { rows, w, .. } => {
                    keep.extend(rows.iter().map(|r| r.id));
                    model.rows(rows, *w);
                }
                Sent::Mark(w) => model.advance(*w),
            }
        }
    } else {
        for (a, lane) in lanes.iter().enumerate() {
            for s in &lane.sent {
                let Sent::Batch { rows, k, w } = s else {
                    continue;
                };
                let others = lanes.iter().enumerate().filter(|(b, _)| *b != a);
                let lo = others
                    .clone()
                    .map(|(_, b)| b.at_barrier[*k])
                    .fold(*w, i64::min);
                let hi = others.map(|(_, b)| b.at_barrier[*k + 1]).fold(*w, i64::min);
                let mut kept = Vec::new();
                for r in rows {
                    let sure = match (r.at, case.window) {
                        (None, _) => true,
                        (Some(t), Window::Session) => (t + 1).saturating_add(case.lateness) > hi,
                        (Some(t), _) => model.placed(t, lo) == model.placed(t, hi),
                    };
                    if sure {
                        keep.insert(r.id);
                        kept.push(r.clone());
                        continue;
                    }
                    // A row that moves its split's watermark is never late,
                    // whatever the bounds.
                    let t = r.at.unwrap();
                    assert!(
                        t - case.delay <= *w,
                        "a row at {t} that moves its split past {w} is unsure"
                    );
                    dropped += 1;
                }
                model.rows(&kept, lo);
            }
        }
        let last = lanes.iter().map(|l| *l.at_barrier.last().unwrap()).min();
        model.advance(last.unwrap());
    }
    let mut kept = case.clone();
    for evs in &mut kept.lanes {
        for e in evs.iter_mut() {
            if let Ev::Rows(_, rows) = e {
                rows.retain(|r| keep.contains(&r.id));
            }
        }
    }
    let epochs = lanes[0].marks.len();
    let marks = (0..epochs)
        .map(|n| {
            lanes
                .iter()
                .flat_map(|l| l.marks[n].clone())
                .collect::<BTreeMap<_, _>>()
        })
        .collect();
    Expected {
        case: kept,
        table: model.table(),
        late: model.late.clone(),
        marks,
        dropped,
        idle_moves: lanes.iter().map(|l| l.idle_moves).sum(),
        open: model.open(),
    }
}

/// What the engine gave for a case.
#[derive(Debug, PartialEq)]
struct Got {
    table: BTreeMap<(i64, String), Agg>,
    late: BTreeSet<i64>,
    marks: Vec<BTreeMap<String, i64>>,
    updates: usize,
    deletes: usize,
}

/// Runs a case through the engine's dataflow, its lanes' frames written in
/// an order from `seed`, every output read as it comes.
async fn run_case(name: &str, case: &Case, seed: u64) -> Got {
    let dir = tempfile::tempdir().unwrap();
    let lanes = case.lanes.len();
    let mut req = window_request(name, &case.window.sql(case.having), lanes as u32, 1, 1);
    req.quarantine_url = format!("file://{}", dir.path().display());
    let et = event_time(&mut req);
    et.delay_micros = case.delay;
    et.lateness_micros = case.lateness;
    et.held = case.held;
    let mut h = start_lanes(req);
    let mut drains = JoinSet::new();
    for (i, mut r) in std::mem::take(&mut h.outs).into_iter().enumerate() {
        drains.spawn(async move { (i, drain(&mut r).await) });
    }
    let mut events = std::mem::replace(&mut h.events, mpsc::channel(1).1);
    let collected = tokio::spawn(async move { ready(&mut events).await });

    // Each lane's frames: its splits join first.
    let mut frames: Vec<Vec<(FrameKind, u32, Vec<u8>)>> = Vec::new();
    for evs in &case.lanes {
        let mut fs = Vec::new();
        let mut joined = BTreeSet::new();
        for e in evs {
            if let Ev::Rows(s, _) | Ev::Raise(s, _) | Ev::End(s) = e
                && joined.insert(*s)
            {
                fs.push((FrameKind::Split, 0, split(&s.to_string(), None)));
            }
        }
        let mut n = 0;
        for e in evs {
            match e {
                Ev::Rows(_, rs) if rs.is_empty() => {}
                Ev::Rows(s, rs) => {
                    fs.push((FrameKind::Split, 0, split(&s.to_string(), None)));
                    let js: Vec<String> = rs.iter().map(Row::json).collect();
                    let refs: Vec<&str> = js.iter().map(String::as_str).collect();
                    fs.push((FrameKind::Rows, 0, rows(&refs)));
                }
                Ev::Raise(s, x) => fs.push((FrameKind::Split, 0, split(&s.to_string(), Some(*x)))),
                Ev::End(s) => fs.push((FrameKind::Split, 0, split(&s.to_string(), Some(END)))),
                Ev::Barrier => {
                    n += 1;
                    fs.push((FrameKind::Barrier, 0, barrier_of(1, n)));
                    fs.push((FrameKind::Barrier, 1, barrier_of(1, n)));
                }
            }
        }
        frames.push(fs);
    }
    let mut rng = Rng(seed);
    let mut at = vec![0; lanes];
    while (0..lanes).any(|l| at[l] < frames[l].len()) {
        let l = rng.below(lanes);
        for _ in 0..1 + rng.below(4) {
            let Some((kind, stream, payload)) = frames[l].get(at[l]) else {
                break;
            };
            put(&mut h.ins[l], *kind, *stream, payload.clone()).await;
            at[l] += 1;
        }
    }
    h.end_all(2).await;
    let mut outs: Vec<Vec<frame::Frame>> = (0..lanes).map(|_| Vec::new()).collect();
    while let Some(d) = drains.join_next().await {
        let (i, fs) = d.unwrap();
        outs[i] = fs;
    }
    h.job.await.unwrap().unwrap();
    let ready = collected.await.unwrap();

    let mut got = Got {
        table: BTreeMap::new(),
        late: BTreeSet::new(),
        marks: ready
            .iter()
            .map(|r| {
                r.watermarks
                    .get("a")
                    .map(|m| m.splits.iter().map(|(k, v)| (k.clone(), *v)).collect())
                    .unwrap_or_default()
            })
            .collect(),
        updates: 0,
        deletes: 0,
    };
    for frames in &outs {
        let mut seqs: BTreeMap<(i64, String), i64> = BTreeMap::new();
        for b in batches(frames, 0) {
            let starts: Vec<i64> = b
                .column_by_name("window_start")
                .unwrap()
                .as_primitive::<arrow_array::types::TimestampMicrosecondType>()
                .iter()
                .map(Option::unwrap)
                .collect();
            let keys = column_strings(&b, "email");
            let (ns, totals, tops) = (
                column_ints(&b, "n"),
                column_ints(&b, "total"),
                column_ints(&b, "top"),
            );
            let ops = column_strings(&b, "_openetl_op");
            let seq = column_ints(&b, "_openetl_seq");
            for i in 0..b.num_rows() {
                let k = (starts[i], keys[i].clone().unwrap());
                // Each key's versions come in the order of their seqs, so
                // a merge that keeps the newest keeps the last.
                let s = seq[i].unwrap();
                let prev = seqs.insert(k.clone(), s);
                assert!(
                    prev.is_none_or(|p| p < s),
                    "{name}: {k:?}'s seq went from {prev:?} to {s}"
                );
                match ops[i].as_deref() {
                    Some("delete") => {
                        got.deletes += 1;
                        got.table.remove(&k);
                    }
                    op => {
                        if op == Some("update") {
                            got.updates += 1;
                        }
                        got.table
                            .insert(k, (ns[i].unwrap(), totals[i].unwrap(), tops[i].unwrap()));
                    }
                }
            }
        }
    }
    for q in ready.iter().flat_map(|r| &r.quarantine) {
        if q.stream != "minutes" {
            continue;
        }
        let path = q.url.strip_prefix("file://").unwrap();
        let text = std::fs::read_to_string(path).unwrap();
        let mut lines = 0;
        for line in text.lines() {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            let record: serde_json::Value =
                serde_json::from_str(v["record"].as_str().unwrap()).unwrap();
            assert!(
                got.late.insert(record["id"].as_i64().unwrap()),
                "{name}: {record} quarantined twice"
            );
            lines += 1;
        }
        assert_eq!(lines, q.rows, "{name}: {}", q.url);
    }
    got
}

/// What the cases ran.
#[derive(Debug, Default)]
struct Tally {
    cases: usize,
    several_lanes: usize,
    rows: usize,
    windows: usize,
    late: usize,
    dropped: usize,
    updates: usize,
    deletes: usize,
    idle_moves: usize,
}

impl Tally {
    fn add(&mut self, e: &Expected, g: &Got) {
        self.cases += 1;
        self.several_lanes += usize::from(e.case.lanes.len() > 1);
        self.rows += e.case.rows();
        self.windows += e.table.len();
        self.late += e.late.len();
        self.dropped += e.dropped;
        self.updates += g.updates;
        self.deletes += g.deletes;
        self.idle_moves += e.idle_moves;
    }
}

fn check(name: &str, e: &Expected, g: &Got) {
    assert_eq!(g.table, e.table, "{name}: the table");
    assert_eq!(g.late, e.late, "{name}: the late rows");
    assert_eq!(g.marks, e.marks, "{name}: the splits' watermarks");
    // A split's watermark never goes back.
    for pair in g.marks.windows(2) {
        for (s, w) in &pair[0] {
            assert!(
                pair[1].get(s).is_some_and(|n| n >= w),
                "{name}: split {s} went back"
            );
        }
    }
}

/// Generated cases of each window, with and without lateness and HAVING,
/// on one lane and several: the engine's table, its quarantine and its
/// splits' watermarks are the model's.
#[tokio::test]
async fn windows_and_watermarks_are_the_models() {
    let mut tally = Tally::default();
    for window in [Window::Tumble, Window::Hop, Window::Session] {
        for seed in 0..100 {
            let name = format!("{window:?}-{seed}");
            let case = Case::generate(seed * 7919 + 1, window, None);
            let e = expect(&case);
            let g = run_case(&name, &e.case, seed).await;
            check(&name, &e, &g);
            tally.add(&e, &g);
        }
    }
    assert_eq!(tally.cases, 300);
    assert!(tally.several_lanes >= 100, "{tally:?}");
    assert!(tally.rows >= 8_000, "{tally:?}");
    assert!(tally.windows >= 3_500, "{tally:?}");
    assert!(tally.late >= 500, "{tally:?}");
    assert!(tally.updates >= 150, "{tally:?}");
    assert!(tally.deletes >= 8, "{tally:?}");
    assert!(tally.idle_moves >= 150, "{tally:?}");
}

/// `case` with only the rows `of` has.
fn keep_rows(case: &Case, of: &Case) -> Case {
    let ids: BTreeSet<i64> = of
        .lanes
        .iter()
        .flatten()
        .flat_map(|e| match e {
            Ev::Rows(_, rs) => rs.iter().map(|r| r.id).collect(),
            _ => Vec::new(),
        })
        .collect();
    let mut out = case.clone();
    for e in out.lanes.iter_mut().flatten() {
        if let Ev::Rows(_, rs) = e {
            rs.retain(|r| ids.contains(&r.id));
        }
    }
    out
}

/// The same input read as one bounded read, in one epoch, and as a
/// continuous one cut into many epochs, leaves the same table and
/// quarantines the same rows, for each window. A stream read in the order
/// of its time whose read completes moves on to the clock less the delay,
/// not to the end of input, so windows past that stay open; a held
/// stream's read ends at the end of input, which closes every window, and
/// none of its rows is late.
#[tokio::test]
async fn a_bounded_read_and_a_continuous_one_leave_the_same_table() {
    // The clock less the delay as the reads complete: half a second past
    // 100 seconds, which no generated time is.
    let clock = 100 * SEC + SEC / 2;
    let (mut compared, mut open) = (0, 0);
    for window in [Window::Tumble, Window::Hop, Window::Session] {
        for seed in 0..10 {
            let mut case = Case::generate(seed * 104_729 + 3, window, Some(1));
            for e in case.lanes.iter_mut().flatten() {
                if let Ev::End(s) = e {
                    *e = Ev::Raise(*s, clock);
                }
            }
            // With several lanes, the rows the model is sure of in both.
            let continuous = case.rebarriered(seed + 1, 12);
            let bounded = expect(&continuous).case.rebarriered(0, 1);
            let eb = expect(&bounded);
            let ec = expect(&keep_rows(&continuous, &eb.case));
            assert_eq!(ec.dropped, 0);
            let name = format!("bounded-{window:?}-{seed}");
            let one = run_case(&name, &eb.case, seed).await;
            check(&name, &eb, &one);
            let name = format!("continuous-{window:?}-{seed}");
            let many = run_case(&name, &ec.case, seed + 1).await;
            check(&name, &ec, &many);
            assert_eq!(many.table, one.table, "{name}");
            assert_eq!(many.late, one.late, "{name}");
            compared += many.table.len();
            open += eb.open;

            let mut held = case.clone();
            held.held = true;
            for evs in &mut held.lanes {
                evs.retain(|e| !matches!(e, Ev::Raise(_, w) if *w != clock));
                for e in evs.iter_mut() {
                    if let Ev::Raise(s, _) = e {
                        *e = Ev::End(*s);
                    }
                }
            }
            let held = held.rebarriered(0, 1);
            let he = expect(&held);
            assert_eq!(he.dropped, 0);
            assert_eq!(he.open, 0);
            let nulls: BTreeSet<i64> = held
                .lanes
                .iter()
                .flatten()
                .flat_map(|e| match e {
                    Ev::Rows(_, rs) => rs.iter().filter(|r| r.at.is_none()).map(|r| r.id).collect(),
                    _ => Vec::new(),
                })
                .collect();
            assert_eq!(he.late, nulls, "a held stream's rows are never late");
            let name = format!("held-bounded-{window:?}-{seed}");
            let one = run_case(&name, &held, seed).await;
            check(&name, &he, &one);
            let name = format!("held-continuous-{window:?}-{seed}");
            let cut = held.rebarriered(seed + 2, 12);
            let many = run_case(&name, &cut, seed + 2).await;
            check(&name, &expect(&cut), &many);
            assert_eq!(many.table, one.table, "{name}");
            compared += many.table.len();
        }
    }
    assert!(compared >= 650, "{compared} windows compared");
    assert!(open >= 100, "{open} windows left open by ordered reads");
}
