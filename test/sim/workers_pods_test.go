package sim

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// ---- A worker: a runner in a pod of its own, with one engine and its lanes.

type wwPhase uint8

const (
	wwStart wwPhase = iota
	wwJoin
	wwJoined
	wwRestore
	wwConnect
	wwRun
)

type wWorker struct {
	m      *wModel
	a      *wAttempt
	idx    int
	addr   string
	runner string
	// From the pod's spec, as its attempt and index are.
	gen                   uint64
	phase                 wwPhase
	dead, stopped         bool
	pausedAt, pausedUntil int
	started, lastAck      int
	lay                   wLayout
	peers                 []string
	restored              *wRecord
	lanes                 []*wLane
	connected             map[int]bool
	inbox                 []wMsg
	barrier, shared       uint64
	slowed                bool
}

func (w *wWorker) name() string   { return fmt.Sprintf("a%d.w%d", w.a.n, w.idx) }
func (w *wWorker) live() bool     { return !w.dead && !w.stopped }
func (w *wWorker) paused() bool   { return w.m.tick < w.pausedUntil }
func (w *wWorker) assigned() bool { return w.phase >= wwRestore }
func (w *wWorker) pause(until int) {
	w.pausedAt, w.pausedUntil = w.m.tick, until
	w.m.stats["workers paused"]++
}

func (w *wWorker) send(msg wMsg) {
	if w.live() {
		w.inbox = append(w.inbox, msg)
	}
}

func (w *wWorker) kill() {
	w.dead = true
	w.m.tracef("%s killed", w.name())
	w.m.killed(w.gen, "workers")
}

func (w *wWorker) stop(reason string) {
	if w.live() {
		w.stopped = true
		w.m.tracef("%s stopped: %s", w.name(), reason)
	}
}

// fail stops the worker and tells its coordinator, which fails the attempt.
func (w *wWorker) fail(reason string) {
	if w.live() {
		w.m.route(w, wMsg{kind: "failed", attempt: w.a.n, gen: w.gen, worker: w.idx, reason: reason})
		w.stop(reason)
	}
}

// resume ends a pause. A pod that stalled notices its coordinator is gone
// only a timeout later, and goes on meanwhile.
func (w *wWorker) resume() {
	if w.live() && w.pausedUntil == w.m.tick {
		w.lastAck, w.started = w.m.tick, w.m.tick
	}
}

// beat sends a heartbeat on the attempt protocol, which the runner routes
// by the attempt's ID; an answer acknowledges it.
func (w *wWorker) beat() {
	if !w.live() || w.paused() || !w.assigned() {
		return
	}
	co := w.m.attempts[w.a.n-1].co
	if co.live() && !co.paused() && co.gen == w.gen {
		co.beats[w.idx], w.lastAck = w.m.tick, w.m.tick
	}
}

// watch stops a worker that never got its assignment, or that lost its
// coordinator.
func (w *wWorker) watch() {
	switch {
	case !w.live() || w.paused():
	case !w.assigned() && w.m.tick-w.started > wJoinTimeout+wTimeout:
		w.stop("never assigned")
	case w.assigned() && w.m.tick-w.lastAck > wTimeout:
		w.stop("lost its coordinator")
	}
}

func (w *wWorker) actions(acts *[]wAction) {
	if !w.live() || w.paused() {
		return
	}
	add := func(op string, k actKind, do func(decision)) {
		*acts = append(*acts, wAction{who: w.name(), op: op, kind: k, pause: w.pause, do: do})
	}
	switch w.phase {
	case wwStart:
		add("start", aLocal, w.start)
	case wwJoin:
		add("join", aCall, w.join)
	case wwRestore:
		for _, l := range w.lanes {
			if !l.restored {
				add(fmt.Sprintf("restore l%d", l.idx), aCall, l.restore)
			}
		}
	case wwConnect:
		for i, p := range w.peers {
			if i != w.idx && !w.connected[i] {
				add("connect "+p, aCall, func(d decision) { w.connect(d, i) })
			}
		}
	case wwRun:
		for _, l := range w.lanes {
			l.actions(acts)
		}
		if w.shareReady() {
			add("share", aCall, w.share)
		}
	}
	if len(w.inbox) > 0 {
		add("recv", aLocal, w.recv)
	}
}

func (w *wWorker) start(d decision) {
	if d != proceed {
		w.kill()
		return
	}
	w.phase = wwJoin
}

// join opens the attempt protocol with the attempt's ID and generation.
func (w *wWorker) join(d decision) {
	ok, err := w.m.call(w.kill, d, func() error {
		w.m.route(w, wMsg{kind: "join", attempt: w.a.n, gen: w.gen, worker: w.idx})
		return nil
	})
	if ok && err == nil {
		w.phase = wwJoined
	}
}

func (w *wWorker) recv(d decision) {
	if d != proceed {
		w.kill()
		return
	}
	msg := w.inbox[0]
	w.inbox = w.inbox[1:]
	switch msg.kind {
	case "assign":
		if w.phase <= wwJoined && msg.gen == w.gen {
			w.assign(msg)
		}
	case "barrier":
		if msg.gen == w.gen {
			w.barrier = max(w.barrier, msg.epoch)
		}
	case "ack":
		if msg.gen == w.gen {
			w.ack(msg)
		}
	case "refused":
		w.m.stats["stale workers refused"]++
		w.stop("refused: " + msg.reason)
	default:
		w.stop(msg.kind)
	}
}

// ack is the source's acknowledgement of the splits the worker reads, at
// the committed record's positions. Positions below are gone.
func (w *wWorker) ack(msg wMsg) {
	m := w.m
	for _, l := range w.lanes {
		for _, c := range l.splits {
			pos := -1
			for _, s := range msg.splits {
				if s.ID == c.split {
					pos = s.Pos
				}
			}
			if m.cfg.broken == "ack-read-positions" {
				pos = c.pos
			}
			if pos > m.trimmed[c.split] {
				m.trimmed[c.split] = pos
				m.tracef("%s acknowledged split %d to %d", w.name(), c.split, pos)
			}
		}
	}
}

// assign takes the coordinator's assignment: the layout, which says this
// worker's lanes, their key groups and splits, the peers' addresses, and the
// record to restore from.
func (w *wWorker) assign(msg wMsg) {
	w.lay, w.peers, w.restored = msg.lay, msg.peers, msg.restored
	w.lastAck, w.connected = w.m.tick, map[int]bool{}
	e := w.restored.epoch()
	w.barrier, w.shared = e, e
	for i, spec := range w.lay.lanes {
		if spec.worker != w.idx {
			continue
		}
		l := &wLane{w: w, idx: i, kg: spec.kg, outs: map[int]*wConn{}, at: map[uint64][]wSplit{},
			snaps: map[uint64][]checkpoint.Snapshot{}, buf: map[uint64]map[string][]row{}, staged: map[uint64][]byte{}}
		for s, li := range w.lay.splits {
			if li == i {
				l.splits = append(l.splits, &wCursor{split: s})
			}
		}
		w.lanes = append(w.lanes, l)
	}
	w.phase = wwRestore
}

// afterRestore connects the worker's own lanes, in one engine, and goes on
// to its peers.
func (w *wWorker) afterRestore() {
	for _, a := range w.lanes {
		for _, b := range w.lanes {
			c := &wConn{from: a, to: b}
			a.outs[b.idx], b.ins = c, append(b.ins, c)
		}
	}
	w.phase = wwConnect
	w.maybeRun()
}

func (w *wWorker) maybeRun() {
	for i := range w.peers {
		if i != w.idx && !w.connected[i] {
			return
		}
	}
	w.phase = wwRun
	for _, l := range w.lanes {
		l.sendMark()
	}
}

// connect opens the exchange to a peer's engine. The engine there refuses
// a connection of another attempt or generation.
func (w *wWorker) connect(d decision, peer int) {
	m := w.m
	if !w.slowed && m.slowConnects > 0 && m.rng.IntN(3) == 0 {
		m.slowConnects--
		w.slowed = true
		m.tracef("%s stalls before connecting", w.name())
		w.pause(m.tick + wTimeout + 2 + m.rng.IntN(4))
		return
	}
	addr := w.peers[peer]
	var refused error
	ok, err := m.call(w.kill, d, func() error {
		p := m.pods[addr]
		if p == nil || !p.live() || !p.assigned() {
			return errNoPod
		}
		if (p.a != w.a || p.gen != w.gen) && m.cfg.broken != "any-generation" {
			refused = fmt.Errorf("%w by %s at %s: it's generation %d, not %d", errRefuse, p.name(), addr, p.gen, w.gen)
			return refused
		}
		for _, a := range w.lanes {
			for j, spec := range w.lay.lanes {
				if spec.worker == peer {
					b := p.lane(j)
					c := &wConn{from: a, to: b, cross: true}
					a.outs[j], b.ins = c, append(b.ins, c)
				}
			}
		}
		if p.a != w.a {
			m.stats["stale connections taken"]++
			m.tracef("%s connected to %s at %s, of generation %d", w.name(), p.name(), addr, p.gen)
		}
		return nil
	})
	switch {
	case !ok:
	case refused != nil:
		m.stats["stale connections refused"]++
		m.tracef("%s: %v", w.name(), refused)
		w.fail(refused.Error())
	case err == nil:
		w.connected[peer] = true
		w.maybeRun()
	}
}

// lane is the worker's lane j, or, to an engine that doesn't check
// generations, its first lane.
func (w *wWorker) lane(j int) *wLane {
	for _, l := range w.lanes {
		if l.idx == j {
			return l
		}
	}
	return w.lanes[0]
}

func (w *wWorker) shareReady() bool {
	e := w.shared + 1
	if e > w.barrier {
		return false
	}
	for _, l := range w.lanes {
		if l.precommitted < e {
			return false
		}
	}
	return true
}

func (w *wWorker) share(d decision) {
	e := w.shared + 1
	s := &wShare{Gen: w.gen, Worker: w.idx, Epoch: e, Committables: map[int][]byte{}}
	for _, l := range w.lanes {
		s.Splits = append(s.Splits, l.at[e]...)
		s.Committables[l.idx] = l.staged[e]
		s.Snapshots = append(s.Snapshots, l.snaps[e]...)
	}
	ok, err := w.m.call(w.kill, d, func() error {
		w.m.route(w, wMsg{kind: "share", attempt: w.a.n, gen: w.gen, worker: w.idx, epoch: e, share: s})
		return nil
	})
	if !ok || err != nil {
		return
	}
	w.shared = e
	for _, l := range w.lanes {
		delete(l.at, e)
		delete(l.staged, e)
		delete(l.snaps, e)
	}
}

// ---- A lane: reads its splits and runs the row-wise statement, sends each
// row to its key group's lane, and runs the keyed operator and a writer for
// its own key groups.

type wCursor struct {
	split    int
	pos      int
	complete bool
	wm       int64
	// The record's watermark, which a restore starts from.
	floor   int64
	maxSeen int64
	// The last barrier it placed.
	placed uint64
	// The tick it ran out of rows to read, or -1; and, with idle splits
	// left out of the lane's watermark, whether it's idle.
	quiet int
	idle  bool
}

type wConn struct {
	from, to *wLane
	q        []wFrame
	// Between two engines, over the network.
	cross     bool
	slowUntil int
}

type wFrame struct {
	kind  byte // rows, watermark or barrier
	rows  []wRow
	mark  int64
	epoch uint64
}

type wRow struct {
	wEvent
	Gen uint64
}

type wLane struct {
	w        *wWorker
	idx      int
	kg       checkpoint.KeyGroupRange
	restored bool
	// Reading: the epoch it's in, the watermark it last sent, its frames out
	// by receiving lane, and its splits at each barrier.
	splits     []*wCursor
	rr         int
	cur        uint64
	sentWM     int64
	idleSent   bool
	outs       map[int]*wConn
	at         map[uint64][]wSplit
	readerPast uint64
	// The keyed operator: its senders' watermarks and its own, the least its
	// own may be once every sender's is in after a restore, its state and
	// the groups changed since the last snapshot, the file holding each
	// group's state, and the senders whose barrier for kcur arrived.
	ins        []*wConn
	senderWM   map[int]int64
	senderIdle map[int]bool
	opWM       int64
	opFloor    int64
	state      map[int]map[wKey]*wAcc
	dirty      map[int]bool
	files      map[int]string
	kcur       uint64
	aligned    map[int]bool
	snapDue    bool
	snaps      map[uint64][]checkpoint.Snapshot
	keyedPast  uint64
	// The writer: rows by epoch and stream, and committables.
	buf          map[uint64]map[string][]row
	staged       map[uint64][]byte
	precommitted uint64
}

func (l *wLane) name() string { return fmt.Sprintf("%s.l%d", l.w.name(), l.idx) }

// restore reads the snapshot files the record names for the lane's key
// groups, and its splits' positions and watermarks.
func (l *wLane) restore(d decision) {
	w, m := l.w, l.w.m
	rec := w.restored
	state, files := map[int]map[wKey]*wAcc{}, map[int]string{}
	var gone string
	ok, err := m.call(w.kill, d, func() error {
		for _, s := range rec.snapshots() {
			r := *s.KeyGroups
			if r.Last < l.kg.First || r.First > l.kg.Last {
				continue
			}
			f, err := m.readState(s.URL)
			if errors.Is(err, checkpoint.ErrNotFound) {
				gone = s.URL
			}
			if err != nil {
				return err
			}
			for g, entries := range f.Groups {
				if !inRange(r, g) || !inRange(l.kg, g) {
					continue
				}
				files[g], state[g] = s.URL, map[wKey]*wAcc{}
				for _, e := range entries {
					a := &wAcc{Sum: e.Sum, Count: e.Count}
					for _, c := range e.Rows {
						c.Restored = true
						a.Rows = append(a.Rows, c)
					}
					state[g][wKey{e.Key, e.W}] = a
				}
			}
		}
		return nil
	})
	switch {
	case !ok:
		return
	case gone != "":
		if h, _, _ := m.readHead(); h.Generation == w.gen {
			m.problem("restore", "%s: %v names %s, which is gone", l.name(), rec, gone)
		}
		w.fail("restore: " + gone + " is gone")
		return
	case err != nil:
		return
	}
	l.state, l.files, l.dirty = state, files, map[int]bool{}
	e := rec.epoch()
	l.cur, l.kcur = e+1, e+1
	l.readerPast, l.keyedPast, l.precommitted = e, e, e
	l.sentWM, l.opWM, l.senderWM, l.senderIdle, l.aligned = wNoMark, wNoMark, map[int]int64{}, map[int]bool{}, map[int]bool{}
	l.opFloor = wNoMark
	if rec != nil {
		for _, o := range m.opMarks[rec.ref()] {
			if o.kg.Last >= l.kg.First && o.kg.First <= l.kg.Last {
				l.opFloor = max(l.opFloor, o.wm)
			}
		}
	}
	for _, c := range l.splits {
		sp := rec.split(c.split)
		c.pos, c.complete, c.wm, c.floor, c.maxSeen, c.placed = sp.Pos, sp.Complete, sp.Watermark, sp.Watermark, wNoMark, e
		c.quiet, c.idle = -1, false
		if m.cfg.broken == "watermark-not-restored" && !c.complete {
			c.wm = wNoMark
		}
		if c.pos < m.trimmed[c.split] {
			if h, _, _ := m.readHead(); h.Generation == w.gen {
				m.problem("exactly-once", "%s resumes split %d at %d, but positions below %d were acknowledged and are gone", l.name(), c.split, c.pos, m.trimmed[c.split])
				m.lost = "data lost"
			}
			w.fail("read below what was acknowledged")
			return
		}
	}
	l.restored = true
	m.stats["lanes restored"]++
	m.tracef("%s restored %v: %d key groups with state", l.name(), rec, len(l.state))
	for _, p := range m.cut(rec, l.kg, l.state) {
		m.problem("restore", "%s restored %v: %s", l.name(), rec, p)
	}
	for _, x := range w.lanes {
		if !x.restored {
			return
		}
	}
	w.afterRestore()
}

func (l *wLane) actions(acts *[]wAction) {
	add := func(op string, k actKind, do func(decision)) {
		*acts = append(*acts, wAction{who: l.name(), op: op, kind: k, pause: l.w.pause, do: do})
	}
	if l.readable() != nil {
		add("read", aLocal, l.read)
	}
	if l.w.barrier >= l.cur && l.canPlace() {
		add("place", aLocal, l.place)
	}
	for _, c := range l.ins {
		if l.deliverable(c) {
			add("take from "+c.from.name(), aLocal, func(d decision) { l.take(d, c) })
		}
	}
	if l.snapDue {
		add("snapshot", aCall, l.snapshot)
	}
	if e := l.precommitted + 1; l.readerPast >= e && l.keyedPast >= e {
		add("stage", aCall, l.stage)
	}
}

// readable is the next split to read a page of, in turn: one with a row
// that has arrived, that waits at no barrier it placed for the lane's
// others.
func (l *wLane) readable() *wCursor {
	for i := range l.splits {
		c := l.splits[(l.rr+i)%len(l.splits)]
		if !c.complete && c.placed < l.cur && l.arrived(c, c.pos) {
			return c
		}
	}
	return nil
}

func (l *wLane) arrived(c *wCursor, i int) bool {
	m := l.w.m
	return m.cfg.rate == 0 || m.cfg.avail[c.split][i] <= m.tick
}

// idle advances the watermark of each split that has had nothing to read
// for the idle timeout, as an event of the split, so never past a barrier
// it placed. It advances to what no row still to come is behind: the tick's
// event time, less the delay.
func (w *wWorker) idle() {
	m := w.m
	if !w.live() || w.paused() || w.phase != wwRun || m.cfg.rate == 0 {
		return
	}
	for _, l := range w.lanes {
		for _, c := range l.splits {
			held := c.placed >= l.cur && m.cfg.broken != "idle-advances-held"
			if c.complete || held || l.arrived(c, c.pos) {
				c.quiet = -1
				continue
			}
			if c.quiet < 0 {
				c.quiet = m.tick
			}
			if m.tick-c.quiet < m.cfg.idleTicks {
				continue
			}
			if m.cfg.broken == "idle-excluded" {
				if !c.idle {
					c.idle = true
					m.tracef("%s: split %d is idle", l.name(), c.split)
					l.sendMark()
				}
				continue
			}
			if wm := int64(m.tick)*m.cfg.rate - m.cfg.delay; wm > c.wm {
				c.wm = wm
				m.stats["idle splits advanced"]++
				m.tracef("%s: split %d is idle, at %d", l.name(), c.split, wm)
				m.observeMark(l, c)
				l.sendMark()
			}
		}
	}
}

func (l *wLane) atResumePoint(c *wCursor) bool {
	return c.complete || l.w.m.cfg.resumeAt[c.split][c.pos]
}

func (l *wLane) canPlace() bool {
	all := true
	for _, c := range l.splits {
		if c.placed < l.cur {
			if l.atResumePoint(c) {
				return true
			}
			all = false
		}
	}
	return all
}

func (l *wLane) emit(e uint64, stream string, r row) {
	if l.buf[e] == nil {
		l.buf[e] = map[string][]row{}
	}
	l.buf[e][stream] = append(l.buf[e][stream], r)
}

func (l *wLane) push(f wFrame) {
	for _, j := range slices.Sorted(maps.Keys(l.outs)) {
		l.outs[j].q = append(l.outs[j].q, f)
	}
}

// read reads a page of a split: the row-wise statement's output goes to the
// lane's writer, and each kept row to its key group's lane.
func (l *wLane) read(d decision) {
	if d != proceed {
		l.w.kill()
		return
	}
	m := l.w.m
	c := l.readable()
	l.rr++
	evs := m.cfg.splits[c.split]
	end := c.pos + 1
	for end < min(c.pos+m.cfg.page, len(evs)) && l.arrived(c, end) {
		end++
	}
	c.quiet, c.idle = -1, false
	routed := map[int][]wRow{}
	for _, e := range evs[c.pos:end] {
		c.maxSeen = max(c.maxSeen, e.T)
		if !e.kept() {
			continue
		}
		l.emit(l.cur, "filtered", row{ID: e.ID, Seq: e.T, Key: e.Key, Value: string(must(json.Marshal(wContrib{ID: e.ID, Gen: l.w.gen})))})
		to := l.w.lay.owner(keyGroupOf(e.Key))
		routed[to] = append(routed[to], wRow{wEvent: e, Gen: l.w.gen})
	}
	for _, to := range slices.Sorted(maps.Keys(routed)) {
		l.outs[to].q = append(l.outs[to].q, wFrame{kind: 'r', rows: routed[to]})
	}
	c.pos = end
	if c.pos == len(evs) {
		c.complete, c.wm = true, wEndMark
	} else {
		c.wm = max(c.wm, c.maxSeen-m.cfg.delay)
	}
	m.observeMark(l, c)
	l.sendMark()
}

// sendMark sends the lane's watermark, the least of its splits', once it
// has gone up.
// With idle splits left out, a lane whose splits are all idle says so
// instead, and its receivers leave it out until it says it's active again.
func (l *wLane) sendMark() {
	wm, active := int64(wEndMark), len(l.splits) == 0
	for _, c := range l.splits {
		if !c.idle {
			wm, active = min(wm, c.wm), true
		}
	}
	switch {
	case !active && !l.idleSent:
		l.idleSent = true
		l.push(wFrame{kind: 'i'})
		return
	case !active:
		return
	case l.idleSent:
		l.idleSent = false
		l.push(wFrame{kind: 'a'})
	}
	if wm > l.sentWM {
		l.sentWM = wm
		l.push(wFrame{kind: 'w', mark: wm})
	}
}

// place places the requested barrier on each split at a resume point, and
// once every split of the lane has, sends it on.
func (l *wLane) place(d decision) {
	if d != proceed {
		l.w.kill()
		return
	}
	for _, c := range l.splits {
		if c.placed < l.cur && l.atResumePoint(c) {
			c.placed = l.cur
			l.at[l.cur] = append(l.at[l.cur], wSplit{ID: c.split, Pos: c.pos, Complete: c.complete, Watermark: c.wm})
		}
	}
	for _, c := range l.splits {
		if c.placed < l.cur {
			return
		}
	}
	l.sendMark()
	l.push(wFrame{kind: 'b', epoch: l.cur})
	l.readerPast = l.cur
	l.cur++
}

// deliverable says whether the lane takes c's next frame: not past the
// sender's barrier until every sender's has arrived.
func (l *wLane) deliverable(c *wConn) bool {
	if len(c.q) == 0 || l.snapDue || l.w.m.tick < c.slowUntil {
		return false
	}
	if !l.aligned[c.from.idx] {
		return true
	}
	return l.w.m.cfg.broken == "unaligned" && c.q[0].kind != 'b'
}

func (l *wLane) take(d decision, c *wConn) {
	m := l.w.m
	if d != proceed {
		l.w.kill()
		return
	}
	dup := false
	if c.cross && m.netFaults > 0 && m.rng.IntN(100) < wNetPct {
		m.netFaults--
		fault := m.rng.IntN(3)
		silent := m.cfg.broken == "silent-exchange" && c.q[0].kind == 'r'
		switch {
		case fault == 0:
			c.slowUntil = m.tick + 1 + m.rng.IntN(4)
			m.stats["exchange frames delayed"]++
			m.tracef("%s: frames from %s delayed", l.name(), c.from.name())
			return
		case silent && fault == 1:
			c.q = c.q[1:]
			m.tracef("%s: a frame from %s lost", l.name(), c.from.name())
			return
		case silent:
			dup = true
			m.tracef("%s: a frame from %s duplicated", l.name(), c.from.name())
		default:
			// TLS sees a record lost, repeated or out of order, and the
			// connection breaks.
			m.stats["exchanges broken"]++
			l.w.fail("the exchange from " + c.from.name() + " broke")
			return
		}
	}
	f := c.q[0]
	if !dup {
		c.q = c.q[1:]
	}
	s := c.from.idx
	switch f.kind {
	case 'r':
		for _, r := range f.rows {
			l.fold(r)
		}
	case 'w':
		l.mark(c, f.mark)
	case 'i', 'a':
		l.senderIdle[s] = f.kind == 'i'
		l.advance()
	case 'b':
		if f.epoch != l.kcur {
			m.problem("record", "%s: barrier %d from %s arrived in epoch %d", l.name(), f.epoch, c.from.name(), l.kcur)
		}
		l.aligned[s] = true
		if l.allAligned() {
			l.snapDue = true
		}
	}
}

// senders is the lanes the keyed operator takes rows from: every lane of
// the attempt, connected yet or not.
func (l *wLane) senders() []int {
	if l.w.m.cfg.broken == "senders-by-connection" {
		var out []int
		for _, c := range l.ins {
			if !slices.Contains(out, c.from.idx) {
				out = append(out, c.from.idx)
			}
		}
		return out
	}
	out := make([]int, len(l.w.lay.lanes))
	for i := range out {
		out[i] = i
	}
	return out
}

// allAligned says every sender has sent the barrier.
func (l *wLane) allAligned() bool {
	for _, s := range l.senders() {
		if !l.aligned[s] {
			return false
		}
	}
	return true
}

func (l *wLane) fold(r wRow) {
	m := l.w.m
	size := m.cfg.size
	k := wKey{r.Key, windowStart(r.T, size)}
	if k.W+size <= l.opWM {
		m.stats["late rows"]++
		l.emit(l.kcur, "late", row{ID: r.ID, Seq: r.T, Key: r.Key, Value: string(must(json.Marshal(wContrib{ID: r.ID, Gen: r.Gen})))})
		return
	}
	g := keyGroupOf(r.Key)
	if l.state[g] == nil {
		l.state[g] = map[wKey]*wAcc{}
	}
	a := l.state[g][k]
	if a == nil {
		a = &wAcc{}
		l.state[g][k] = a
	}
	a.Sum, a.Count = a.Sum+r.value(), a.Count+1
	a.Rows = append(a.Rows, wContrib{ID: r.ID, Gen: r.Gen})
	l.dirty[g] = true
}

// mark takes a sender's watermark. The operator's is the least of every
// sender's of the attempt, and closes the windows it passes.
func (l *wLane) mark(c *wConn, wm int64) {
	m := l.w.m
	s := c.from.idx
	if prev, ok := l.senderWM[s]; ok && wm < prev {
		m.problem("watermark", "%s: %s's watermark went back on the exchange, from %d to %d", l.name(), c.from.name(), prev, wm)
		return
	}
	l.senderWM[s] = wm
	l.advance()
}

// advance moves the operator's watermark to the least of its senders',
// leaving out those that said they're idle, and closes the windows it
// passes. It never goes back.
func (l *wLane) advance() {
	m := l.w.m
	op, all, active := int64(wEndMark), true, false
	for _, s := range l.senders() {
		x, ok := l.senderWM[s]
		if !ok {
			x, all = wNoMark, false
		}
		if !l.senderIdle[s] {
			op, active = min(op, x), true
		}
	}
	if !active {
		return
	}
	if all && l.opFloor != wNoMark {
		if op < l.opFloor {
			m.problem("watermark", "%s: the operator's watermark is %d once every sender's is in, below %d at the barrier it restored", l.name(), op, l.opFloor)
		}
		l.opFloor = wNoMark
	}
	if m.cfg.broken == "watermark-max" {
		op = wNoMark
		for _, x := range l.senderWM {
			op = max(op, x)
		}
	}
	if op <= l.opWM {
		return
	}
	l.opWM = op
	size := m.cfg.size
	for _, g := range slices.Sorted(maps.Keys(l.state)) {
		keys := slices.SortedFunc(maps.Keys(l.state[g]), func(a, b wKey) int { return cmp.Or(cmp.Compare(a.W, b.W), cmp.Compare(a.Key, b.Key)) })
		for _, k := range keys {
			if k.W+size > op {
				continue
			}
			a := l.state[g][k]
			l.emit(l.kcur, "agg", row{ID: fmt.Sprintf("%d/%s", k.W, k.Key), Seq: k.W, Key: k.Key, Value: string(must(json.Marshal(wEntry{Key: k.Key, W: k.W, Sum: a.Sum, Count: a.Count, Rows: a.Rows})))})
			delete(l.state[g], k)
			l.dirty[g] = true
		}
	}
}

// snapshot writes the key groups that changed since the last barrier to a
// file, and names, for the others, the files that hold them.
func (l *wLane) snapshot(d decision) {
	w, m := l.w, l.w.m
	e := l.kcur
	f := wStateFile{Generation: w.gen, Epoch: e, Groups: map[int][]wEntry{}}
	for _, g := range slices.Sorted(maps.Keys(l.dirty)) {
		for _, k := range slices.SortedFunc(maps.Keys(l.state[g]), func(a, b wKey) int { return cmp.Or(cmp.Compare(a.W, b.W), cmp.Compare(a.Key, b.Key)) }) {
			a := l.state[g][k]
			f.Groups[g] = append(f.Groups[g], wEntry{Key: k.Key, W: k.W, Sum: a.Sum, Count: a.Count, Rows: slices.Clone(a.Rows)})
		}
	}
	url := ""
	if len(f.Groups) > 0 {
		url = m.stateURL(w.gen, e, l.kg)
	}
	if url != "" && d == proceed && m.slowWrites > 0 && m.rng.IntN(4) == 0 {
		m.slowWrites--
		at := m.tick + 1 + m.rng.IntN(2*wTimeout)
		m.later = append(m.later, wLater{at, func() {
			m.tracef("%s's write of %s lands", l.name(), url)
			_ = m.putState(url, f)
		}})
		m.stats["snapshot writes landing late"]++
		m.tracef("%s: the write of %s times out, and lands at t%d", l.name(), url, at)
		return
	}
	ok, err := m.call(w.kill, d, func() error {
		if url == "" {
			return nil
		}
		return m.putState(url, f)
	})
	if !ok || err != nil {
		return
	}
	for g := range l.dirty {
		if len(f.Groups[g]) > 0 {
			l.files[g] = url
		} else {
			delete(l.files, g)
			delete(l.state, g)
		}
	}
	if url != "" {
		m.stats["snapshot files"]++
	}
	var snaps []checkpoint.Snapshot
	for g := l.kg.First; g <= l.kg.Last; g++ {
		u := l.files[g]
		switch {
		case u == "":
		case len(snaps) > 0 && snaps[len(snaps)-1].URL == u && snaps[len(snaps)-1].KeyGroups.Last == g-1:
			snaps[len(snaps)-1].KeyGroups.Last = g
		default:
			snaps = append(snaps, checkpoint.Snapshot{Operator: wOperator, URL: u, KeyGroups: &checkpoint.KeyGroupRange{First: g, Last: g}})
		}
	}
	l.snaps[e] = snaps
	m.snapped[w.gen] = max(m.snapped[w.gen], e)
	at := checkpoint.RecordRef{Epoch: e, Generation: w.gen}
	m.opMarks[at] = append(m.opMarks[at], wOpMark{l.kg, l.opWM})
	l.dirty, l.aligned, l.snapDue = map[int]bool{}, map[int]bool{}, false
	l.keyedPast, l.kcur = e, e+1
}

// stage is the writer's pre-commit: it stages the epoch's rows, by
// generation, epoch, stream and lane, and makes the lane's committable.
func (l *wLane) stage(d decision) {
	w, m := l.w, l.w.m
	e := l.precommitted + 1
	st := warehouseStaged{Gen: w.gen, N: e, Lane: l.idx}
	for _, s := range wStreams {
		if len(l.buf[e][s]) > 0 {
			st.Streams = append(st.Streams, stagedStream{Name: s, Mode: int(destination.Append), Dedupe: int(destination.DedupeNone)})
		}
	}
	ok, err := m.call(w.kill, d, func() error {
		m.wh.mu.Lock()
		defer m.wh.mu.Unlock()
		for _, s := range st.Streams {
			m.wh.staging[stagingKey{w.gen, e, s.Name, l.idx}] = slices.Clone(l.buf[e][s.Name])
		}
		return nil
	})
	if !ok || err != nil {
		return
	}
	l.staged[e], l.precommitted = must(json.Marshal(st)), e
	delete(l.buf, e)
}
