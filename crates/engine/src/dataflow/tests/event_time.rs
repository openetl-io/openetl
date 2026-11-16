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
//!
//! A killed case runs as jobs one after another, each restored from the
//! record of the last barrier the one before committed, on another number
//! of lanes. The model takes what each job commits, from where the record
//! has the window, each job's lanes bounded as above.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::AtomicUsize;

use openetl_proto::engine::v1::StateSnapshot;

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
    /// A split a restored job reads on from where the record has it: it
    /// joins at the record's watermark, NONE if it has none. Before
    /// anything else of the lane.
    Resume(usize, i64),
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

    /// The same input on one lane: every lane's splits, one lane after
    /// another, with `n` barriers.
    fn one_lane(&self, n: usize) -> Case {
        let mut out = self.clone();
        let evs = out.lanes.concat();
        out.lanes = vec![evs];
        out.rebarriered(0, n)
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
    lane_model_to(case, l, case.lanes[l].len())
}

/// Lane l as the model has it after its first `read` events. Every split of
/// the lane joins it before anything else, as the job reads them.
fn lane_model_to(case: &Case, l: usize, read: usize) -> LaneModel {
    let mut m = LaneModel {
        at_barrier: vec![NONE],
        ..Default::default()
    };
    let evs = &case.lanes[l];
    let mut wm: BTreeMap<usize, i64> = BTreeMap::new();
    for e in evs {
        match e {
            Ev::Resume(s, w) => {
                wm.insert(*s, *w);
            }
            Ev::Rows(s, _) | Ev::Raise(s, _) | Ev::End(s) => {
                wm.entry(*s).or_insert(NONE);
            }
            Ev::Barrier => {}
        }
    }
    let least = |wm: &BTreeMap<usize, i64>| match wm.values().any(|w| *w == NONE) {
        true => NONE,
        false => wm.values().copied().min().unwrap_or(END),
    };
    let mut sent = NONE;
    // Splits that resume at a record's watermarks give the lane its own
    // before anything else of it.
    if evs.iter().any(|e| matches!(e, Ev::Resume(..))) && least(&wm) != NONE {
        sent = least(&wm);
        m.sent.push(Sent::Mark(sent));
    }
    let mut read_rows: BTreeSet<usize> = BTreeSet::new();
    let mut k = 0;
    for e in &evs[..read] {
        let mut raised = None;
        match e {
            Ev::Resume(..) => continue,
            Ev::Rows(s, rows) => {
                read_rows.insert(*s);
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
                // A lane without splits holds none back from its first
                // barrier.
                if wm.is_empty() && sent != END {
                    sent = END;
                    m.sent.push(Sent::Mark(END));
                }
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
        let now = least(&wm);
        if now != NONE && now > sent {
            sent = now;
            m.sent.push(Sent::Mark(now));
            if raised.is_some_and(|s| !read_rows.contains(&s)) {
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
    /// The latest the watermark less the lateness has been, as jobs with
    /// another lateness restore it: a window that ends at or before it is
    /// gone. NONE for none.
    gone: i64,
    /// Rows late only because a window was gone under an earlier lateness.
    floored: usize,
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
            gone: NONE,
            floored: 0,
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
            .filter(|s| !self.ended(s + self.window.size(), w))
            .collect()
    }

    /// Whether a window that ends at `end` is gone at the watermark `w`.
    fn ended(&self, end: i64, w: i64) -> bool {
        end.saturating_add(self.lateness) <= w || end <= self.gone
    }

    /// A job starts with `lateness`, its window at `floor`.
    fn start(&mut self, lateness: i64, floor: i64) {
        self.lateness = lateness;
        if floor != NONE {
            self.gone = self.gone.max(floor.saturating_sub(lateness));
        }
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
                let lateness = self.lateness;
                let ok = |s: &i64| (s + self.window.size()).saturating_add(lateness) > w;
                self.floored += usize::from(self.window.starts(t).iter().any(ok));
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
            // Sessions given and gone, as after a lowered lateness; those
            // the window's own watermark hasn't closed yet no sure row
            // meets.
            let mut list = self.sessions.remove(key).unwrap_or_default();
            list.retain(|s| !self.ended(s.end + 1, self.w));
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
                // A row that gives a session its start is late where its
                // own gap has gone.
                let starts = list.iter().filter(|s| meets(s)).all(|s| t < s.start);
                let late = |gone: &dyn Fn(i64) -> bool| gone(last + 1) || (starts && gone(end + 1));
                if late(&|x| self.ended(x, w)) {
                    self.late.insert(r.id);
                    let lateness = self.lateness;
                    self.floored += usize::from(!late(&|x| x.saturating_add(lateness) <= w));
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
        self.gone = self.gone.max(w.saturating_sub(self.lateness));
        if self.window == Window::Session {
            for (key, list) in &mut self.sessions {
                for s in list.iter_mut() {
                    if s.end >= self.w && s.end < w {
                        emit(&mut self.out, self.having, key, s);
                    }
                }
                let (lateness, gone) = (self.lateness, self.gone);
                list.retain(|s| (s.end + 1).saturating_add(lateness) > w && s.end + 1 > gone);
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
    let mut unsure = BTreeSet::new();
    let dropped = take(&mut model, case, &lanes, NONE, true, &mut unsure);
    Expected {
        case: without(case, &unsure),
        table: model.table(),
        late: model.late.clone(),
        marks: epoch_marks(&lanes),
        dropped,
        idle_moves: lanes.iter().map(|l| l.idle_moves).sum(),
        open: model.open(),
    }
}

/// The window takes what a job's lanes send, `lanes` as the model has
/// them, from its `floor`, then moves to where the job's last barrier has
/// it. A row whose fate depends on the lanes' order goes in `unsure`, to
/// be left out of the input; how many did. `last`: the input ends in the
/// job.
fn take(
    model: &mut Model,
    case: &Case,
    lanes: &[LaneModel],
    floor: i64,
    last: bool,
    unsure: &mut BTreeSet<i64>,
) -> usize {
    let mut dropped = 0;
    if lanes.len() == 1 {
        for s in &lanes[0].sent {
            match s {
                Sent::Batch { rows, w, .. } => model.rows(rows, (*w).max(floor)),
                Sent::Mark(w) => model.advance((*w).max(floor)),
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
                    .fold(*w, i64::min)
                    .max(floor);
                let hi = others
                    .map(|(_, b)| b.at_barrier[*k + 1])
                    .fold(*w, i64::min)
                    .max(floor);
                let mut kept = Vec::new();
                for r in rows {
                    let sure = match (r.at, case.window) {
                        (None, _) => true,
                        (Some(t), Window::Session) => {
                            (t + 1).saturating_add(case.lateness) > hi && t + 1 > model.gone
                        }
                        (Some(t), _) => model.placed(t, lo) == model.placed(t, hi),
                    };
                    if sure {
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
                    unsure.insert(r.id);
                    dropped += 1;
                }
                model.rows(&kept, lo);
            }
        }
    }
    let end = lanes
        .iter()
        .map(|l| *l.at_barrier.last().unwrap())
        .min()
        .unwrap()
        .max(floor);
    // A session that closed and then grew past the watermark keeps the row
    // it gave until it closes again: which sessions did depends on the
    // lanes' order, so only an input that closes every session can be sure
    // of the table.
    assert!(
        !last || lanes.len() == 1 || case.window != Window::Session || end == END,
        "sessions on several lanes that the input leaves open"
    );
    model.advance(end);
    dropped
}

/// `case` without the rows in `unsure`.
fn without(case: &Case, unsure: &BTreeSet<i64>) -> Case {
    let mut out = case.clone();
    for e in out.lanes.iter_mut().flatten() {
        if let Ev::Rows(_, rows) = e {
            rows.retain(|r| !unsure.contains(&r.id));
        }
    }
    out
}

/// Each epoch's splits' watermarks, of every lane.
fn epoch_marks(lanes: &[LaneModel]) -> Vec<BTreeMap<String, i64>> {
    (0..lanes[0].marks.len())
        .map(|n| {
            lanes
                .iter()
                .flat_map(|l| l.marks[n].clone())
                .collect::<BTreeMap<_, _>>()
        })
        .collect()
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
    let ran = run_job(name, dir.path(), case, &Job::first(), seed).await;
    got(name, &[ran])
}

/// A job of a case: its generation, the epoch it starts at and the record
/// it restores, which names the window's state and has its watermark at
/// `floor`: NONE without one. Unless it's killed, it reads its input to
/// the end.
struct Job {
    generation: u64,
    first_epoch: u64,
    restored: Vec<StateSnapshot>,
    floor: i64,
    workers: usize,
    kill: Option<Kill>,
}

impl Job {
    fn first() -> Job {
        Job {
            generation: 1,
            first_epoch: 1,
            restored: Vec::new(),
            floor: NONE,
            workers: 1,
            kill: None,
        }
    }
}

/// What a job committed: each writer lane's frames up to the barrier of its
/// last committed epoch, and those epochs' EpochReadys, in order.
struct Ran {
    outs: Vec<Vec<frame::Frame>>,
    ready: Vec<EpochReady>,
    /// It was killed after its engine had read frames past the last
    /// barrier it committed.
    read_past: bool,
}

/// An edge's reader, counting the bytes the engine has taken from it.
struct Counted {
    inner: BufReader<DuplexStream>,
    taken: Arc<AtomicUsize>,
}

impl tokio::io::AsyncRead for Counted {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let polled = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        self.taken
            .fetch_add(buf.filled().len() - before, Ordering::Relaxed);
        polled
    }
}

impl AsyncBufRead for Counted {
    fn poll_fill_buf(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<&[u8]>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_fill_buf(cx)
    }

    fn consume(mut self: std::pin::Pin<&mut Self>, amt: usize) {
        std::pin::Pin::new(&mut self.inner).consume(amt);
        self.taken.fetch_add(amt, Ordering::Relaxed);
    }
}

/// As start_lanes, with each lane's count of the bytes the engine has
/// taken from its edge.
fn start_counted(
    req: openetl_proto::engine::v1::StartJobRequest,
) -> (LaneHarness, Vec<Arc<AtomicUsize>>) {
    let plan = Arc::new(Plan::from_request(req).unwrap());
    let (mut ins, mut readers, mut taken, mut outs, mut writers) =
        (vec![], vec![], vec![], vec![], vec![]);
    for _ in 0..plan.inbound.len() {
        let (w, r) = duplex(1 << 16);
        let n = Arc::new(AtomicUsize::new(0));
        ins.push(w);
        readers.push(Counted {
            inner: BufReader::new(r),
            taken: n.clone(),
        });
        taken.push(n);
    }
    for _ in 0..plan.writer_lanes {
        let (w, r) = duplex(1 << 16);
        writers.push(w);
        outs.push(BufReader::new(r));
    }
    let (tx, events) = mpsc::channel(64);
    let job = tokio::spawn(run(plan, readers, writers, tx));
    let h = LaneHarness {
        ins,
        outs,
        events,
        job,
    };
    (h, taken)
}

/// Runs a job of a case through the engine's dataflow, with its state and
/// quarantine below `dir`, its lanes' frames written in an order from
/// `seed`, every output read as it comes. A killed job reads its lanes up
/// to its kill, and is aborted once its engine has taken every frame
/// written, made the EpochReady of each barrier it commits, and given that
/// barrier on each writer lane.
async fn run_job(name: &str, dir: &std::path::Path, case: &Case, job: &Job, seed: u64) -> Ran {
    // A window runs in one engine: see WORKERS.
    assert_eq!(
        job.workers, 1,
        "{name}: a window on {} engines",
        job.workers
    );
    let lanes = case.lanes.len();
    let mut req = window_request(
        &dir.join("state"),
        &case.window.sql(case.having),
        lanes as u32,
        job.generation,
        job.first_epoch,
    );
    req.pipeline = name.into();
    req.quarantine_url = format!("file://{}", dir.display());
    req.restored_snapshots = job.restored.clone();
    let et = event_time(&mut req);
    et.delay_micros = case.delay;
    et.lateness_micros = case.lateness;
    et.held = case.held;
    et.floor = (job.floor != NONE).then_some(job.floor);
    let (mut h, taken) = start_counted(req);
    let strict = job.kill.is_none();
    let mut drains = JoinSet::new();
    let mut given = Vec::new();
    for (i, mut r) in std::mem::take(&mut h.outs).into_iter().enumerate() {
        let (tx, rx) = tokio::sync::watch::channel(0);
        given.push(rx);
        drains.spawn(async move { (i, drain_counting(&mut r, tx, strict).await) });
    }
    let mut events = std::mem::replace(&mut h.events, mpsc::channel(1).1);
    let (tx, mut readies) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(ev) = events.recv().await {
            let r = match ev {
                Ok(EventsResponse {
                    event: Some(events_response::Event::EpochReady(r)),
                }) => Ok(r),
                Ok(_) => continue,
                Err(e) => Err(e.message().to_owned()),
            };
            if tx.send(r).is_err() {
                return;
            }
        }
    });

    // Each lane's frames: its splits join first, a resumed one at the
    // record's watermark.
    let mut frames: Vec<Vec<(FrameKind, u32, Vec<u8>)>> = Vec::new();
    // Frames past the last barrier the job commits.
    let mut past_cut = 0;
    for (l, evs) in case.lanes.iter().enumerate() {
        let mut fs = Vec::new();
        let mut joined = BTreeSet::new();
        for e in evs {
            match e {
                Ev::Resume(s, w) if joined.insert(*s) => fs.push((
                    FrameKind::Split,
                    0,
                    split(&s.to_string(), (*w != NONE).then_some(*w)),
                )),
                Ev::Rows(s, _) | Ev::Raise(s, _) | Ev::End(s) if joined.insert(*s) => {
                    fs.push((FrameKind::Split, 0, split(&s.to_string(), None)));
                }
                _ => {}
            }
        }
        let (cut, read) = match &job.kill {
            Some(k) => (past(evs, k.cut), past(evs, k.cut) + k.tails[l]),
            None => (evs.len(), evs.len()),
        };
        let mut n = job.first_epoch - 1;
        for (i, e) in evs[..read].iter().enumerate() {
            let before = fs.len();
            match e {
                Ev::Resume(..) => {}
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
                    fs.push((FrameKind::Barrier, 0, barrier_of(job.generation, n)));
                    fs.push((FrameKind::Barrier, 1, barrier_of(job.generation, n)));
                }
            }
            if i >= cut {
                past_cut += fs.len() - before;
            }
        }
        frames.push(fs);
    }
    let written: Vec<usize> = frames
        .iter()
        .map(|fs| fs.iter().map(|(_, _, p)| frame::HEADER_LEN + p.len()).sum())
        .collect();
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

    let wait = std::time::Duration::from_secs(60);
    let mut ready = Vec::new();
    let committed = match &job.kill {
        Some(k) => {
            // The engine has taken every frame written, those past the
            // barriers it commits too.
            tokio::time::timeout(wait, async {
                while (0..lanes).any(|l| taken[l].load(Ordering::Relaxed) < written[l]) {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{name}: the engine took its frames for a minute"));
            while ready.len() < k.cut {
                let r = tokio::time::timeout(wait, readies.recv())
                    .await
                    .unwrap_or_else(|_| {
                        panic!("{name}: no EpochReady {} in a minute", ready.len() + 1)
                    })
                    .unwrap_or_else(|| panic!("{name}: the job ended before its kill"));
                ready.push(r.unwrap_or_else(|e| panic!("{name}: {e}")));
            }
            for g in &mut given {
                tokio::time::timeout(wait, g.wait_for(|n| *n >= k.cut))
                    .await
                    .unwrap_or_else(|_| panic!("{name}: a writer lane short of barrier {}", k.cut))
                    .unwrap();
            }
            h.job.abort();
            assert!(
                h.job.await.is_err_and(|e| e.is_cancelled()),
                "{name}: the job ended"
            );
            std::mem::drop(h.ins);
            k.cut
        }
        None => {
            h.end_all(2).await;
            h.job.await.unwrap().unwrap();
            while let Some(r) = readies.recv().await {
                ready.push(r.unwrap_or_else(|e| panic!("{name}: {e}")));
            }
            usize::MAX
        }
    };
    for (n, r) in ready.iter().enumerate() {
        assert_eq!(
            r.epoch,
            Some(Epoch {
                generation: job.generation,
                n: job.first_epoch + n as u64,
            }),
            "{name}"
        );
    }
    let mut outs: Vec<Vec<frame::Frame>> = (0..lanes).map(|_| Vec::new()).collect();
    while let Some(d) = drains.join_next().await {
        let (i, mut fs) = d.unwrap();
        let mut barriers = 0;
        let keep = match committed {
            0 => 0,
            c => fs
                .iter()
                .position(|f| {
                    barriers += usize::from(f.kind == FrameKind::Barrier && f.stream == 0);
                    barriers == c
                })
                .map_or(fs.len(), |end| end + 1),
        };
        fs.truncate(keep);
        outs[i] = fs;
    }
    Ran {
        outs,
        ready,
        read_past: job.kill.is_some() && past_cut > 0,
    }
}

/// A writer lane's frames, until it closes, or for a job that's killed, at
/// whatever it closes on. `barriers` counts its first stream's barriers.
async fn drain_counting(
    r: &mut BufReader<DuplexStream>,
    barriers: tokio::sync::watch::Sender<usize>,
    strict: bool,
) -> Vec<frame::Frame> {
    let mut out = Vec::new();
    loop {
        let f = match frame::read(r).await {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(_) if !strict => break,
            Err(e) => panic!("{e}"),
        };
        if f.kind == FrameKind::Barrier && f.stream == 0 {
            barriers.send_modify(|n| *n += 1);
        }
        out.push(f);
    }
    out
}

/// What the jobs of a case committed, one after another: the table a merge
/// of their outputs leaves, the rows they quarantined, and each epoch's
/// splits' watermarks.
fn got(name: &str, runs: &[Ran]) -> Got {
    let ready: Vec<&EpochReady> = runs.iter().flat_map(|r| &r.ready).collect();
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
    // A key is on one writer lane of a job, and the jobs come one after
    // another.
    let mut seqs: BTreeMap<(i64, String), i64> = BTreeMap::new();
    for frames in runs.iter().flat_map(|r| &r.outs) {
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
    check_against(name, &e.table, &e.late, &e.marks, g);
}

fn check_against(
    name: &str,
    table: &BTreeMap<(i64, String), Agg>,
    late: &BTreeSet<i64>,
    marks: &[BTreeMap<String, i64>],
    g: &Got,
) {
    assert_eq!(&g.table, table, "{name}: the table");
    assert_eq!(&g.late, late, "{name}: the late rows");
    assert_eq!(g.marks, marks, "{name}: the splits' watermarks");
    // A split's watermark never goes back. A split that completed isn't
    // read after a restore, so the epochs after it may not have it.
    let mut last: BTreeMap<&String, i64> = BTreeMap::new();
    for epoch in &g.marks {
        for (s, w) in epoch {
            if let Some(p) = last.insert(s, *w) {
                assert!(*w >= p, "{name}: split {s} went back from {p} to {w}");
            }
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

/// The numbers of engines a killed case's jobs draw from. A window runs in
/// one engine for now.
const WORKERS: &[usize] = &[1];

/// A job is killed once it has committed `cut` barriers and read
/// `tails[l]` more of lane l's events.
#[derive(Clone, Debug)]
struct Kill {
    cut: usize,
    tails: Vec<usize>,
}

/// A job of a case that's killed and restored. Each job after the first
/// restores from the record of the last barrier the one before committed,
/// or from the record that one restored if it committed none.
#[derive(Clone, Debug)]
struct Phase {
    case: Case,
    workers: usize,
    /// None for the job the input ends in.
    kill: Option<Kill>,
    /// Before the job starts, the runs that the record it restores doesn't
    /// name are deleted.
    prune: bool,
}

/// Where lane `evs` is just past its barrier `cut`, or past the splits it
/// resumes for 0.
fn past(evs: &[Ev], cut: usize) -> usize {
    if cut == 0 {
        return evs
            .iter()
            .take_while(|e| matches!(e, Ev::Resume(..)))
            .count();
    }
    let mut barriers = 0;
    1 + evs
        .iter()
        .position(|e| {
            barriers += usize::from(matches!(e, Ev::Barrier));
            barriers == cut
        })
        .unwrap()
}

/// A case of `window` from `seed`, killed once or twice. A job after a kill
/// may run with another lateness, as when a version changes it.
fn killed(seed: u64, window: Window) -> Vec<Phase> {
    let mut rng = Rng(seed ^ 0x6b1d_5eed);
    let mut lateness = Rng(seed ^ 0x1a7e_9e55);
    let mut case = Case::generate(seed, window, Some(2 + rng.below(4)));
    let kills = 1 + rng.below(2);
    let mut phases = Vec::new();
    for p in 0..=kills {
        let workers = WORKERS[rng.below(WORKERS.len())];
        let prune = p > 0 && rng.below(2) == 0;
        if p == kills {
            phases.push(Phase {
                case,
                workers,
                kill: None,
                prune,
            });
            break;
        }
        let barriers = case.lanes[0]
            .iter()
            .filter(|e| matches!(e, Ev::Barrier))
            .count();
        // Mostly after a barrier but the last.
        let cut = match rng.below(6) {
            0 => 0,
            _ => 1 + rng.below(barriers - 1),
        };
        let tails = case
            .lanes
            .iter()
            .map(|evs| rng.below(evs.len() - past(evs, cut) + 1))
            .collect();
        // Another number of lanes.
        let mut lanes = 1 + rng.below(3);
        if lanes >= case.lanes.len() {
            lanes += 1;
        }
        let barriers = match p + 1 == kills {
            true => 1 + rng.below(4),
            false => 2 + rng.below(3),
        };
        let mut next = restored(&case, cut, lanes, barriers, &mut rng);
        if lateness.below(2) == 0 {
            next.lateness = [0, 3, 6, 20, 40][lateness.below(5)] * SEC;
        }
        if next.lateness > case.lateness {
            stale(&mut next, case.lateness, p, &mut lateness);
        }
        phases.push(Phase {
            case,
            workers,
            kill: Some(Kill { cut, tails }),
            prune,
        });
        case = next;
    }
    phases
}

/// Rows a job restored with a raised lateness reads first, of the first
/// split it resumes: up to 12 seconds before where the record's watermark
/// less the lateness `was` had windows gone, and within the new lateness.
/// None moves its split's watermark. `phase` keeps their IDs apart.
fn stale(case: &mut Case, was: i64, phase: usize, rng: &mut Rng) {
    let at = floor(case);
    let first = case
        .lanes
        .iter()
        .enumerate()
        .find_map(|(l, evs)| match evs.first() {
            Some(Ev::Resume(s, _)) => Some((l, *s)),
            _ => None,
        });
    let (Some((l, s)), false) = (first, at == NONE) else {
        return;
    };
    let gone = (at - was) / SEC;
    let rows = (0..3)
        .map(|j| Row {
            id: 1_000_000 + 10 * phase as i64 + j,
            key: KEYS[rng.below(KEYS.len())],
            v: rng.below(90) as i64 - 30,
            at: Some((gone - 1 - rng.below(12) as i64).max(0) * SEC),
        })
        .collect();
    let resumed = case.lanes[l]
        .iter()
        .take_while(|e| matches!(e, Ev::Resume(..)))
        .count();
    case.lanes[l].insert(resumed, Ev::Rows(s, rows));
}

/// What a job restored from `case`'s record of barrier `cut` reads, on
/// `lanes` lanes with `barriers` barriers, the last after every split's
/// end: each split that hasn't completed, from where the record has it, at
/// the split's watermark there. For `cut` 0, the record `case` restored.
fn restored(case: &Case, cut: usize, lanes: usize, barriers: usize, rng: &mut Rng) -> Case {
    let mut rest: BTreeMap<usize, Vec<Ev>> = BTreeMap::new();
    for evs in &case.lanes {
        for e in &evs[past(evs, cut)..] {
            if let Ev::Rows(s, _) | Ev::Raise(s, _) | Ev::End(s) = e {
                rest.entry(*s).or_default().push(e.clone());
            }
        }
    }
    let mut marks: BTreeMap<usize, i64> = BTreeMap::new();
    if cut == 0 {
        for e in case.lanes.iter().flatten() {
            if let Ev::Resume(s, w) = e {
                marks.insert(*s, *w);
            }
        }
    } else {
        for l in 0..case.lanes.len() {
            for (s, w) in &lane_model(case, l).marks[cut - 1] {
                marks.insert(s.parse().unwrap(), *w);
            }
        }
    }
    let lane_of: BTreeMap<usize, usize> = rest.keys().map(|s| (*s, rng.below(lanes))).collect();
    let mut out = case.clone();
    out.lanes = (0..lanes)
        .map(|l| {
            let mine: Vec<usize> = rest.keys().copied().filter(|s| lane_of[s] == l).collect();
            let mut evs: Vec<Ev> = mine
                .iter()
                .map(|s| Ev::Resume(*s, marks.get(s).copied().unwrap_or(NONE)))
                .collect();
            let resumed = evs.len();
            let mut at: BTreeMap<usize, usize> = mine.iter().map(|s| (*s, 0)).collect();
            loop {
                let open: Vec<usize> = mine
                    .iter()
                    .copied()
                    .filter(|s| at[s] < rest[s].len())
                    .collect();
                if open.is_empty() {
                    break;
                }
                let s = open[rng.below(open.len())];
                evs.push(rest[&s][at[&s]].clone());
                *at.get_mut(&s).unwrap() += 1;
            }
            for _ in 1..barriers {
                let p = resumed + rng.below(evs.len() - resumed + 1);
                evs.insert(p, Ev::Barrier);
            }
            evs.push(Ev::Barrier);
            evs
        })
        .collect();
    out
}

/// Where a job's window starts: the least watermark its splits resume at,
/// NONE if one of them has none or none resumes.
fn floor(case: &Case) -> i64 {
    let marks: Vec<i64> = case
        .lanes
        .iter()
        .flatten()
        .filter_map(|e| match e {
            Ev::Resume(_, w) => Some(*w),
            _ => None,
        })
        .collect();
    match marks.contains(&NONE) {
        true => NONE,
        false => marks.into_iter().min().unwrap_or(NONE),
    }
}

/// What the model expects of a killed case: its jobs with the rows it
/// leaves out taken out, the table, the late rows, and the watermarks of
/// each epoch the jobs commit.
struct KilledExpected {
    phases: Vec<Phase>,
    table: BTreeMap<(i64, String), Agg>,
    late: BTreeSet<i64>,
    marks: Vec<BTreeMap<String, i64>>,
    dropped: usize,
    /// Late rows of windows gone under an earlier job's lateness, which
    /// the job's own would have taken.
    floored: usize,
}

/// The window takes what each job commits, from where the record it
/// restores has the window: a killed job's epochs up to its kill's
/// barrier.
fn expect_killed(phases: &[Phase]) -> KilledExpected {
    let mut model = Model::new(&phases[0].case);
    let mut unsure = BTreeSet::new();
    let (mut marks, mut dropped) = (Vec::new(), 0);
    for ph in phases {
        let case = &ph.case;
        model.start(case.lateness, floor(case));
        let lanes: Vec<LaneModel> = (0..case.lanes.len())
            .map(|l| {
                let evs = &case.lanes[l];
                let read = ph.kill.as_ref().map_or(evs.len(), |k| past(evs, k.cut));
                lane_model_to(case, l, read)
            })
            .collect();
        dropped += take(
            &mut model,
            case,
            &lanes,
            floor(case),
            ph.kill.is_none(),
            &mut unsure,
        );
        marks.extend(epoch_marks(&lanes));
    }
    KilledExpected {
        phases: phases
            .iter()
            .map(|ph| Phase {
                case: without(&ph.case, &unsure),
                ..ph.clone()
            })
            .collect(),
        table: model.table(),
        late: model.late.clone(),
        marks,
        dropped,
        floored: model.floored,
    }
}

/// Runs a killed case's jobs one after another, each restored from the
/// record the one before left, below one directory. What they committed,
/// and what they ran.
async fn run_killed(name: &str, phases: &[Phase], seed: u64) -> (Got, Restores) {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let mut runs = Vec::new();
    let (mut restored, mut epoch, mut r) = (Vec::new(), 0, Restores::default());
    for (p, ph) in phases.iter().enumerate() {
        if ph.prune {
            r.pruned += prune(&state, &restored);
        }
        r.from_runs += usize::from(restored.iter().any(|s: &StateSnapshot| !s.runs.is_empty()));
        let job = Job {
            generation: p as u64 + 1,
            first_epoch: epoch + 1,
            restored: restored.clone(),
            floor: floor(&ph.case),
            workers: ph.workers,
            kill: ph.kill.clone(),
        };
        let ran = run_job(name, dir.path(), &ph.case, &job, seed + p as u64).await;
        // The windows kept, as the records count them across kills and
        // restores: never below 0, and none once the input ends.
        for op in ran.ready.iter().flat_map(|e| &e.operators) {
            r.windows += op.windows_opened as i64 - op.windows_dropped as i64;
            r.opened += op.windows_opened as usize;
            assert!(r.windows >= 0, "{name}: {} windows kept", r.windows);
        }
        if let Some(last) = ran.ready.last() {
            restored = last.snapshots.clone();
            epoch += ran.ready.len() as u64;
        }
        r.read_past += usize::from(ran.read_past);
        runs.push(ran);
    }
    assert_eq!(r.windows, 0, "{name}: windows kept at the end of input");
    (got(name, &runs), r)
}

/// What a killed case's jobs ran.
#[derive(Debug, Default)]
struct Restores {
    /// Jobs killed after their engine had read frames past the last
    /// barrier they committed.
    read_past: usize,
    /// Jobs restored from a record that names runs.
    from_runs: usize,
    /// Runs that pruning deleted.
    pruned: usize,
    /// The windows the committed epochs kept, opened less dropped, and
    /// those they opened.
    windows: i64,
    opened: usize,
}

/// Deletes each file below `state` that `kept` doesn't name, as retention
/// may once no record it keeps names it. How many it deleted.
fn prune(state: &std::path::Path, kept: &[StateSnapshot]) -> usize {
    let named: BTreeSet<std::path::PathBuf> = kept
        .iter()
        .flat_map(|s| &s.runs)
        .map(|r| state.join(r))
        .collect();
    let mut deleted = 0;
    let mut dirs = vec![state.to_path_buf()];
    while let Some(d) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let path = e.path();
            if path.is_dir() {
                dirs.push(path);
            } else if !named.contains(&path) && std::fs::remove_file(&path).is_ok() {
                deleted += 1;
            }
        }
    }
    deleted
}

/// What the killed cases ran.
#[derive(Debug, Default)]
struct KillTally {
    cases: usize,
    kills: usize,
    restores: usize,
    /// Kills of a job that had committed nothing.
    uncommitted: usize,
    /// Kills of a job whose engine had read frames past the last barrier
    /// it committed.
    read_past: usize,
    /// Jobs on several lanes.
    several_lanes: usize,
    /// Jobs restored from a record that names runs.
    from_runs: usize,
    pruned: usize,
    opened: usize,
    windows: usize,
    late: usize,
    dropped: usize,
    updates: usize,
    /// Jobs restored with a higher lateness than the one before, and with
    /// a lower one.
    raised: usize,
    lowered: usize,
    floored: usize,
}

/// Generated cases of each window, killed once or twice: at a random
/// barrier, or before any, after reading on past it, so the epoch the job
/// is in is lost and the runs it wrote are orphans. The next job restores
/// the record of the last barrier committed onto another number of lanes,
/// some after the runs that record doesn't name are deleted, and reads on
/// from where the record has each split. What the jobs committed, merged,
/// is the model's table, and they quarantine the model's late rows and
/// report its watermarks. Some jobs run with a raised or lowered lateness:
/// a window gone under an earlier one stays gone.
#[tokio::test]
async fn killed_and_restored_windows_are_the_models() {
    let mut tally = KillTally::default();
    for window in [Window::Tumble, Window::Hop, Window::Session] {
        for seed in 0..50 {
            let name = format!("killed-{window:?}-{seed}");
            let e = expect_killed(&killed(seed * 104_723 + 5, window));
            let (g, r) = run_killed(&name, &e.phases, seed).await;
            check_against(&name, &e.table, &e.late, &e.marks, &g);
            tally.cases += 1;
            tally.restores += e.phases.len() - 1;
            for pair in e.phases.windows(2) {
                let (was, now) = (pair[0].case.lateness, pair[1].case.lateness);
                tally.raised += usize::from(now > was);
                tally.lowered += usize::from(now < was);
            }
            tally.floored += e.floored;
            for ph in &e.phases {
                tally.several_lanes += usize::from(ph.case.lanes.len() > 1);
                let Some(k) = &ph.kill else {
                    continue;
                };
                tally.kills += 1;
                tally.uncommitted += usize::from(k.cut == 0);
            }
            tally.read_past += r.read_past;
            tally.from_runs += r.from_runs;
            tally.pruned += r.pruned;
            tally.opened += r.opened;
            tally.windows += e.table.len();
            tally.late += e.late.len();
            tally.dropped += e.dropped;
            tally.updates += g.updates;
        }
    }
    assert_eq!(tally.cases, 150);
    assert!(
        tally.kills >= 200 && tally.restores == tally.kills,
        "{tally:?}"
    );
    assert!(tally.uncommitted >= 15, "{tally:?}");
    assert!(tally.read_past >= 150, "{tally:?}");
    assert!(tally.several_lanes >= 180, "{tally:?}");
    assert!(tally.from_runs >= 150, "{tally:?}");
    // Runs of epochs that a kill lost, which the engine may or may not have
    // written by then.
    assert!(tally.pruned >= 10, "{tally:?}");
    assert!(tally.windows >= 2_000, "{tally:?}");
    // Every window of the table was opened at least once by a committed
    // epoch, so the count above checked something.
    assert!(tally.opened >= tally.windows, "{tally:?}");
    assert!(tally.late >= 150, "{tally:?}");
    assert!(tally.updates >= 50, "{tally:?}");
    assert!(tally.raised >= 40 && tally.lowered >= 20, "{tally:?}");
    assert!(tally.floored >= 20, "{tally:?}");
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
            // Sessions left open may keep rows they gave before they grew,
            // as the lanes' order has it: see expect.
            if window == Window::Session {
                case = case.one_lane(1);
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

/// A session the output has a row of when the record is taken, restored
/// from its runs by another attempt, then grown by a late row past HAVING,
/// is deleted with the end and the values of the row the output has.
#[tokio::test]
async fn a_restored_session_deletes_the_row_the_output_has() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let sql = "INSERT INTO minutes SELECT window_start, window_end, email, sum(v) AS total \
               FROM a GROUP BY session(at, INTERVAL '30 seconds'), email HAVING sum(v) > 0";
    let row = |id: i64, email: &str, v: i64, second: i64| {
        format!(
            r#"{{"id": {id}, "email": "{email}", "v": {v}, "at": "1970-01-01T00:{:02}:{:02}Z"}}"#,
            second / 60,
            second % 60
        )
    };
    // What a writer lane gave of the output: op, end in seconds, total.
    let given = |outs: &[Vec<frame::Frame>]| -> Vec<(String, i64, Option<i64>)> {
        let mut got = Vec::new();
        for frames in outs {
            for b in batches(frames, 0) {
                let ends: Vec<i64> = b
                    .column_by_name("window_end")
                    .unwrap()
                    .as_primitive::<arrow_array::types::TimestampMicrosecondType>()
                    .iter()
                    .map(|e| e.unwrap() / SEC)
                    .collect();
                let ops = column_strings(&b, "_openetl_op");
                let totals = column_ints(&b, "total");
                for i in 0..b.num_rows() {
                    got.push((ops[i].clone().unwrap(), ends[i], totals[i]));
                }
            }
        }
        got
    };

    let mut req = window_request(&state, sql, 1, 1, 1);
    req.quarantine_url = format!("file://{}", dir.path().display());
    event_time(&mut req).lateness_micros = 120 * SEC;
    let mut h = start_lanes(req);
    let w = &mut h.ins[0];
    put(w, FrameKind::Split, 0, split("s", None)).await;
    put(
        w,
        FrameKind::Rows,
        0,
        rows(&[&row(1, "x", 5, 0), &row(2, "y", 1, 100)]),
    )
    .await;
    barriers(w, 1, 1).await;
    h.end_all(2).await;
    let (outs, first) = h.finish().await;
    assert_eq!(given(&outs), [("insert".to_owned(), 30, Some(5))]);
    let snapshots = first[0].snapshots.clone();
    assert!(
        snapshots.iter().any(|s| !s.runs.is_empty()),
        "{snapshots:?}"
    );

    let mut req = window_request(&state, sql, 2, 2, 2);
    req.quarantine_url = format!("file://{}", dir.path().display());
    req.restored_snapshots = snapshots;
    let et = event_time(&mut req);
    et.lateness_micros = 120 * SEC;
    et.floor = Some(100 * SEC);
    let mut h = start_lanes(req);
    for (l, w) in h.ins.iter_mut().enumerate() {
        if l == 0 {
            put(w, FrameKind::Split, 0, split("s", Some(100 * SEC))).await;
            // Late, within the lateness: x's session grows to 50 seconds,
            // with a sum below 0.
            put(w, FrameKind::Rows, 0, rows(&[&row(3, "x", -9, 20)])).await;
        }
        barriers(w, 2, 2).await;
    }
    h.end_all(2).await;
    let (outs, _) = h.finish().await;
    assert_eq!(given(&outs), [("delete".to_owned(), 30, Some(5))]);
}
