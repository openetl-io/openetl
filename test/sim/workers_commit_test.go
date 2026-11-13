package sim

import (
	"cmp"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// Commits run on workers, never on the coordinator. After HEAD moves to
// record N, the coordinator asks the workers that commit epoch N to, on the
// attempt protocol, asks again a worker that hasn't answered in time, and
// acknowledges the epoch only once every one has answered. The record is
// one per epoch for the whole attempt; what is per writer lane is its
// commit, its marker and its recovery.
//
// There are two destinations. The staged one is stage and apply: every
// writer lane stages its epoch, and one worker applies every lane's staging
// in a destination transaction, behind the destination's fence.
//
// The txn one is Kafka-like: each writer lane's epoch is a transaction open
// in a producer of the worker that runs the lane, and only that worker can
// commit it. A transactional ID is stable per pipeline, writer lane and
// pool slot; a producer's init fences the one before it with that ID and
// aborts its open transaction. Each transaction carries the marker
// (generation, epoch, lane), and the broker aborts one left open for the
// transaction timeout. A lane writes its epoch into the transaction at its
// pre-commit, where destination-kafka writes rows as they come; nothing is
// visible before the commit either way. Whether a writer also stages a
// durable copy of its epoch before its share is its own choice, which the
// seed makes per lane.
//
// The next attempt's workers settle the destination from the record HEAD
// names before any worker is assigned work. For the staged destination one
// worker raises the fence and applies what isn't. For the txn one each
// committable of the record goes to a worker, whatever lanes the attempt
// has, which fences its transactional ID first, and then:
//  1. finds its marker: done;
//  2. or, if the writer staged, commits the staging in a transaction of its
//     own;
//  3. or redoes the lane's part of the epoch from the source: it reads, from
//     the record before's positions to the record's, the splits and key
//     groups the committable names, folds them into the key groups' state
//     of the record before, closes the windows the record's watermarks
//     close, and commits that.
//
// A single runner's recovery fences and finds the marker, or else rolls
// back to the record before. On several writers that rule finds some lanes
// committed and others not, and can't recover the epoch.

var (
	wDestFlag     = flag.String("wdest", "", "workers kind: only the staged or the txn destination")
	wStagingFlag  = flag.String("wstaging", "", "workers kind, txn destination: writers that stage: all, none or mixed")
	wExchangeFlag = flag.String("wexchange", "", "workers kind: on or off, with or without the keyed operator and its exchange")
)

const (
	// Producers per writer lane, as destination-kafka's pool has.
	wSlots = 3
	// Ticks the coordinator waits for a worker's answer before asking again.
	wReqTimeout = 3
	// Per cent of the commit's messages lost or delayed, and of its steps
	// that fail, while their faults last.
	wMsgPct         = 10
	wCommitFaultPct = 30
	// The random stream the destination's settings come from.
	wDestStream = 35
)

var (
	// The transaction a commit names isn't open: the broker aborted it, or
	// a producer's init did.
	errTxnGone = errors.New("sim: no such open transaction")
	// The producer still has another epoch's transaction open.
	errTxnBusy = errors.New("sim: another transaction is open")
	// What a redo from the source needs is gone.
	errRedo = errors.New("sim: can't redo from the source")
)

// wShape is what a seed's destination is, or what a broken variant needs
// of it; "" is any.
type wShape struct {
	dest, staging, exchange string
}

var variantShapes = map[string]wShape{
	"ack-before-commits":         {dest: "txn"},
	"answer-on-receipt":          {dest: "txn"},
	"commit-before-record":       {dest: "txn"},
	"recovery-skips-uncommitted": {dest: "txn"},
	"single-runner-recovery":     {dest: "txn"},
	"stale-worker-commits":       {dest: "txn"},
	"ids-by-generation":          {dest: "txn"},
	"resume-prepared":            {dest: "txn", staging: "none"},
	"apply-unfenced":             {dest: "staged"},
	"replay-staged-lane":         {dest: "txn", staging: "all"},
	"unstaged-lost-as-applied":   {dest: "txn", staging: "none"},
	"staging-unfenced":           {dest: "txn", staging: "all"},
	"redo-by-new-layout":         {dest: "txn", staging: "none"},
	"replay-own-watermarks":      {dest: "txn", staging: "none", exchange: "on"},
	"prune-to-head":              {dest: "txn", staging: "none"},
	"replay-not-exact":           {dest: "txn", staging: "none"},
}

// randomShape draws the destination's shape, which the flags and the
// variant can fix.
func randomShape(c *wCfg, rng interface{ IntN(int) int }) {
	c.dest, c.staging, c.exchange = "staged", "all", rng.IntN(3) != 0
	if rng.IntN(2) == 0 {
		c.dest, c.staging = "txn", []string{"all", "none", "mixed"}[rng.IntN(3)]
	}
	c.stageMask = rng.IntN(1 << 6)
	c.txnTimeout, c.msgFaults, c.commitFaults = 15+rng.IntN(60), rng.IntN(4), rng.IntN(4)
	need := variantShapes[c.broken]
	if d := cmp.Or(need.dest, *wDestFlag); d != "" {
		c.dest = d
	}
	if s := cmp.Or(need.staging, *wStagingFlag); s != "" && c.dest == "txn" {
		c.staging = s
	}
	if c.dest == "staged" {
		c.staging = "all"
	}
	switch cmp.Or(need.exchange, *wExchangeFlag) {
	case "on":
		c.exchange = true
	case "off":
		c.exchange = false
	}
}

// stages says whether writer lane l stages its epoch.
func (m *wModel) stages(l int) bool {
	switch m.cfg.staging {
	case "all":
		return true
	case "mixed":
		return m.cfg.stageMask>>(l%6)&1 == 1
	}
	return false
}

func (m *wModel) txn() bool { return m.cfg.dest == "txn" }

// shape is the seed's shape, for the stats.
func (m *wModel) shape() string {
	ex := "exchange"
	if !m.cfg.exchange {
		ex = "no exchange"
	}
	if m.txn() {
		return fmt.Sprintf("txn, staging %s, %s", m.cfg.staging, ex)
	}
	return "staged, " + ex
}

// commitStep says a is a step of a commit or of recovery, where the seed's
// commit faults go.
func (m *wModel) commitStep(a wAction) bool {
	for _, p := range []string{"request commit", "commit ", "settle ", "open producers", "record.swap", "ack", "rewind"} {
		if strings.HasPrefix(a.op, p) {
			return true
		}
	}
	return false
}

// ---- The txn destination's broker.

// wTxnID is a transactional ID.
type wTxnID struct {
	Lane, Slot int
	// Zero, but with ids-by-generation and staging-unfenced: the generation
	// of the attempt whose producer it is.
	Gen uint64
}

func (id wTxnID) String() string {
	if id.Gen != 0 {
		return fmt.Sprintf("txn-g%d-l%d-s%d", id.Gen, id.Lane, id.Slot)
	}
	return fmt.Sprintf("txn-l%d-s%d", id.Lane, id.Slot)
}

func cmpTxnID(a, b wTxnID) int {
	return cmp.Or(cmp.Compare(a.Gen, b.Gen), cmp.Compare(a.Lane, b.Lane), cmp.Compare(a.Slot, b.Slot))
}

// wMarker names a lane's transaction of an epoch, as its marker record does.
type wMarker struct {
	Gen, N uint64
	Lane   int
}

func (k wMarker) String() string { return fmt.Sprintf("l%d of %d-g%d", k.Lane, k.N, k.Gen) }

type wTxn struct {
	mk     wMarker
	rows   map[string][]row
	opened int
}

// wBroker is the txn destination's durable state; what transactions made
// visible is in the model's warehouse tables, so that every check reads it.
type wBroker struct {
	// The producer epoch of each transactional ID, which an init raises.
	pepoch  map[wTxnID]uint64
	open    map[wTxnID]*wTxn
	markers map[wMarker]bool
}

func newTxnBroker() *wBroker {
	return &wBroker{pepoch: map[wTxnID]uint64{}, open: map[wTxnID]*wTxn{}, markers: map[wMarker]bool{}}
}

// wTxnStaged is a txn writer lane's committable: its transaction and the
// producer that holds it; what of the epoch it holds, the splits whose rows
// it wrote and the key groups whose windows it did, in the layout that
// wrote it; and whether the writer staged a copy, of which streams.
type wTxnStaged struct {
	Lane     int                      `json:"lane"`
	Slot     int                      `json:"slot"`
	IDGen    uint64                   `json:"id_gen,omitempty"`
	Producer uint64                   `json:"producer_epoch"`
	Gen      uint64                   `json:"gen"`
	N        uint64                   `json:"n"`
	Splits   []int                    `json:"splits,omitempty"`
	KG       checkpoint.KeyGroupRange `json:"key_groups"`
	Staged   bool                     `json:"staged,omitempty"`
	Streams  []string                 `json:"streams,omitempty"`
}

func (s wTxnStaged) id() wTxnID      { return wTxnID{Lane: s.Lane, Slot: s.Slot, Gen: s.IDGen} }
func (s wTxnStaged) marker() wMarker { return wMarker{Gen: s.Gen, N: s.N, Lane: s.Lane} }

func (r *wRecord) txnCommittables() []wTxnStaged {
	if r == nil {
		return nil
	}
	out := make([]wTxnStaged, len(r.Committables))
	for i, b := range r.Committables {
		_ = json.Unmarshal(b, &out[i])
	}
	return out
}

// ownID is the transactional ID of a worker's lane and slot.
func (w *wWorker) ownID(lane, slot int) wTxnID {
	id := wTxnID{Lane: lane, Slot: slot}
	if w.m.cfg.broken == "ids-by-generation" {
		id.Gen = w.gen
	}
	return id
}

// initProducer is initTransactions: it fences the producer the ID had, and
// aborts its open transaction.
func (m *wModel) initProducer(id wTxnID) uint64 {
	b := m.broker
	b.pepoch[id]++
	if t := b.open[id]; t != nil {
		delete(b.open, id)
		m.stats["transactions aborted by an init"]++
		m.tracef("an init of %v aborts the transaction of %v", id, t.mk)
	}
	return b.pepoch[id]
}

// produce writes a lane's epoch into the producer's transaction, opening it.
// A retry writes the same rows again.
func (m *wModel) produce(id wTxnID, pe uint64, mk wMarker, rows map[string][]row) error {
	b := m.broker
	if b.pepoch[id] != pe {
		return fmt.Errorf("%w: producer %v", destination.ErrFenced, id)
	}
	t := b.open[id]
	switch {
	case t == nil:
		t = &wTxn{mk: mk, opened: m.tick}
		b.open[id] = t
	case t.mk != mk:
		return fmt.Errorf("%w: %v has %v open, writing %v", errTxnBusy, id, t.mk, mk)
	}
	t.rows = rows
	return nil
}

// commitTxn commits the producer's open transaction of mk. A producer
// another has fenced can't; a try after one whose outcome was unknown finds
// the marker.
func (m *wModel) commitTxn(committer uint64, id wTxnID, pe uint64, mk wMarker) error {
	b := m.broker
	if b.pepoch[id] != pe {
		return fmt.Errorf("%w: producer %v", destination.ErrFenced, id)
	}
	t := b.open[id]
	if t == nil || t.mk != mk {
		if b.markers[mk] {
			return nil
		}
		return fmt.Errorf("%w: %v has none of %v", errTxnGone, id, mk)
	}
	delete(b.open, id)
	m.makeVisible(committer, mk, t.rows)
	return nil
}

// makeVisible is a transaction's commit, by a worker of generation
// committer: its rows land, with its marker. It drops the lane's staging,
// which nothing needs once the marker is visible.
func (m *wModel) makeVisible(committer uint64, mk wMarker, rows map[string][]row) {
	if !m.headed[checkpoint.RecordRef{Epoch: mk.N, Generation: mk.Gen}] {
		m.problem("visibility", "the transaction of %v became visible, though HEAD never moved to its record", mk)
	}
	if g := m.decided[mk]; g > committer {
		m.problem("generation", "a worker of generation %d made the transaction of %v visible after generation %d settled it", committer, mk, g)
	}
	if m.broker.markers[mk] {
		m.stats["markers made visible twice"]++
	}
	m.wh.mu.Lock()
	defer m.wh.mu.Unlock()
	for _, s := range wStreams {
		for _, r := range rows[s] {
			for _, c := range contribsOf(s, r) {
				if c.Gen != mk.Gen && !c.Restored {
					m.problem("generation", "the transaction of %v holds %s's row %s, which generation %d read", mk, s, c.ID, c.Gen)
				}
			}
			m.wh.tables[s] = append(m.wh.tables[s], r)
			m.wh.applies[r.ID]++
			m.wh.landed[r.ID] = append(m.wh.landed[r.ID], epochRef{mk.Gen, mk.N})
		}
		delete(m.wh.staging, stagingKey{mk.Gen, mk.N, s, mk.Lane})
	}
	m.broker.markers[mk] = true
	m.stats["transactions committed"]++
	m.tracef("the transaction of %v is visible", mk)
}

// expireTxns aborts transactions open longer than the transaction timeout.
func (m *wModel) expireTxns() {
	b := m.broker
	if m.cfg.broken == "resume-prepared" {
		// Transactions of two-phase commit don't time out.
		return
	}
	for _, id := range slices.SortedFunc(maps.Keys(b.open), cmpTxnID) {
		if t := b.open[id]; m.tick-t.opened > m.cfg.txnTimeout {
			delete(b.open, id)
			m.stats["transactions timed out"]++
			m.tracef("the broker aborts the transaction of %v on %v: open since t%d", t.mk, id, t.opened)
		}
	}
}

// ---- Settling the destination.

// settle is a worker's part of recovery for one committable of rec, the
// record HEAD names, in the order of the comment at the top.
func (m *wModel) settle(w *wWorker, c wTxnStaged, rec *wRecord, lay wLayout) (string, error) {
	mk, id := c.marker(), c.id()
	if m.cfg.broken == "ids-by-generation" {
		id = w.ownID(c.Lane, c.Slot)
	}
	if m.cfg.broken == "resume-prepared" {
		return m.resumePrepared(w, id, mk)
	}
	if m.cfg.broken == "staging-unfenced" && c.Staged {
		// It commits the staging under a producer of its own.
		id.Gen = w.gen
	}
	pe := m.initProducer(id)
	m.decided[mk] = max(m.decided[mk], w.gen)
	if m.cfg.broken == "replay-staged-lane" && c.Staged {
		return m.redo(w, c, rec, lay, id, pe)
	}
	switch {
	case m.broker.markers[mk]:
		return "applied", nil
	case m.cfg.broken == "single-runner-recovery":
		return "lost", nil
	case c.Staged:
		rows := map[string][]row{}
		m.wh.mu.Lock()
		for _, s := range c.Streams {
			r, ok := m.wh.staging[stagingKey{mk.Gen, mk.N, s, mk.Lane}]
			if !ok {
				m.wh.mu.Unlock()
				return "", fmt.Errorf("%w for %s of %v", errStagingMissing, s, mk)
			}
			rows[s] = r
		}
		m.wh.mu.Unlock()
		if err := m.produce(id, pe, mk, rows); err != nil {
			return "", err
		}
		return "from staging", m.commitTxn(w.gen, id, pe, mk)
	case m.cfg.broken == "unstaged-lost-as-applied":
		return "applied", nil
	}
	return m.redo(w, c, rec, lay, id, pe)
}

// redo redoes a committable's part of rec's epoch from the source, and
// commits it as that epoch's on the lane's transactional ID.
func (m *wModel) redo(w *wWorker, c wTxnStaged, rec *wRecord, lay wLayout, id wTxnID, pe uint64) (string, error) {
	rows, err := m.replay(w, c, rec, lay)
	if err != nil {
		return "", err
	}
	if err := m.produce(id, pe, c.marker(), rows); err != nil {
		return "", err
	}
	return "redone from the source", m.commitTxn(w.gen, id, pe, c.marker())
}

// replay is a committable's part of rec's epoch, read again from the
// source: the row-wise statement's output of its splits, and the windows of
// its key groups that the epoch closed, from the state of the record
// before and every split's rows since. Its rows are the epoch's, and carry
// its generation.
func (m *wModel) replay(w *wWorker, c wTxnStaged, rec *wRecord, lay wLayout) (map[string][]row, error) {
	prev, err := m.readRecord(rec.Previous)
	if err != nil {
		if errors.Is(err, checkpoint.ErrNotFound) {
			m.problem("restore", "%s can't redo %v: %v", w.name(), c.marker(), err)
			return nil, fmt.Errorf("%w: %w", errRedo, err)
		}
		return nil, err
	}
	splits, kg := c.Splits, c.KG
	if m.cfg.broken == "redo-by-new-layout" {
		splits, kg = nil, checkpoint.KeyGroupRange{First: 1, Last: 0}
		if c.Lane < len(lay.lanes) {
			kg = lay.lanes[c.Lane].kg
		}
		for s, l := range lay.splits {
			if l == c.Lane {
				splits = append(splits, s)
			}
		}
	}
	rng := func(s int) []wEvent {
		from, to := prev.split(s).Pos, rec.split(s).Pos
		if m.trimmed[s] > from {
			m.problem("exactly-once", "%s redoes %v from split %d at %d, but positions below %d were acknowledged", w.name(), c.marker(), s, from, m.trimmed[s])
		}
		evs := m.cfg.splits[s][from:to]
		if m.cfg.broken == "replay-not-exact" && len(evs) > 0 {
			evs = append([]wEvent{evs[0]}, evs...)
		}
		return evs
	}
	rows := map[string][]row{}
	for _, s := range splits {
		for _, e := range rng(s) {
			if e.kept() {
				rows["filtered"] = append(rows["filtered"], row{ID: e.ID, Seq: e.T, Key: e.Key, Value: string(must(json.Marshal(wContrib{ID: e.ID, Gen: c.Gen})))})
			}
		}
	}
	m.stats["lanes redone from the source"]++
	if !m.cfg.exchange {
		return rows, nil
	}
	state, err := m.stateAt(prev)
	if err != nil {
		m.problem("restore", "%s can't redo %v: %v", w.name(), c.marker(), err)
		return nil, fmt.Errorf("%w: %w", errRedo, err)
	}
	acc := map[wKey]*wAcc{}
	for g, ws := range state {
		if !inRange(kg, g) {
			continue
		}
		for k, a := range ws {
			x := &wAcc{Sum: a.Sum, Count: a.Count}
			for _, r := range a.Rows {
				r.Restored = true
				x.Rows = append(x.Rows, r)
			}
			acc[k] = x
		}
	}
	size, wm := m.cfg.size, int64(wEndMark)
	for s := range m.cfg.splits {
		seen := int64(wNoMark)
		for _, e := range rng(s) {
			seen = max(seen, e.T)
			if !e.kept() || !inRange(kg, keyGroupOf(e.Key)) {
				continue
			}
			k := wKey{e.Key, windowStart(e.T, size)}
			if acc[k] == nil {
				acc[k] = &wAcc{}
			}
			acc[k].Sum, acc[k].Count = acc[k].Sum+e.value(), acc[k].Count+1
			acc[k].Rows = append(acc[k].Rows, wContrib{ID: e.ID, Gen: c.Gen})
		}
		at := rec.split(s)
		if m.cfg.broken == "replay-own-watermarks" && !at.Complete {
			// The replay's own: what it read, less the delay.
			at.Watermark = prev.split(s).Watermark
			if seen != wNoMark {
				at.Watermark = max(at.Watermark, seen-m.cfg.delay)
			}
		}
		wm = min(wm, at.Watermark)
	}
	for _, k := range slices.SortedFunc(maps.Keys(acc), cmpWKey) {
		if k.W+size > wm {
			continue
		}
		a := acc[k]
		rows["agg"] = append(rows["agg"], row{ID: fmt.Sprintf("%d/%s", k.W, k.Key), Seq: k.W, Key: k.Key, Value: string(must(json.Marshal(wEntry{Key: k.Key, W: k.W, Sum: a.Sum, Count: a.Count, Rows: a.Rows})))})
		delete(acc, k)
	}
	// What stays open must be what the record's snapshots hold.
	at, err := m.stateAt(rec)
	if err != nil {
		return nil, err
	}
	want := map[wKey]*wAcc{}
	for g, ws := range at {
		if inRange(kg, g) {
			maps.Copy(want, ws)
		}
	}
	for _, k := range slices.SortedFunc(maps.Keys(acc), cmpWKey) {
		if want[k] == nil || want[k].Sum != acc[k].Sum || want[k].Count != acc[k].Count {
			m.problem("restore", "%s redid %v and holds window %d of %s open, which %v doesn't", w.name(), c.marker(), k.W, k.Key, rec)
		}
	}
	for _, k := range slices.SortedFunc(maps.Keys(want), cmpWKey) {
		if acc[k] == nil {
			m.problem("restore", "%s redid %v and closed window %d of %s, which %v holds open", w.name(), c.marker(), k.W, k.Key, rec)
		}
	}
	return rows, nil
}

func cmpWKey(a, b wKey) int { return cmp.Or(cmp.Compare(a.W, b.W), cmp.Compare(a.Key, b.Key)) }

// resumePrepared settles a committable as Kafka's two-phase commit
// (KIP-939) would, with no staging: the init keeps the transaction it finds
// prepared, and recovery commits it if it's the one the record names. Any
// other init, such as a stale worker's starting its producers, aborts it as
// before; then the epoch is lost, and HEAD goes back.
func (m *wModel) resumePrepared(w *wWorker, id wTxnID, mk wMarker) (string, error) {
	b := m.broker
	t := b.open[id]
	b.pepoch[id]++
	m.decided[mk] = max(m.decided[mk], w.gen)
	switch {
	case b.markers[mk]:
		if t != nil {
			delete(b.open, id)
		}
		return "applied", nil
	case t != nil && t.mk == mk:
		delete(b.open, id)
		m.makeVisible(w.gen, mk, t.rows)
		return "resumed", nil
	}
	if t != nil {
		delete(b.open, id)
	}
	return "lost", nil
}

// lossy sends one of the commit's messages, which may be lost, or arrive
// ticks later.
func (m *wModel) lossy(what string, deliver func()) {
	if m.msgFaults == 0 || m.rng.IntN(100) >= wMsgPct {
		deliver()
		return
	}
	m.msgFaults--
	if m.rng.IntN(2) == 0 {
		m.stats[what+" lost"]++
		m.tracef("a message of the %s is lost", what)
		return
	}
	at := m.tick + 1 + m.rng.IntN(4)
	m.later = append(m.later, wLater{at, deliver})
	m.stats[what+" delayed"]++
	m.tracef("a message of the %s arrives at t%d", what, at)
}

// ---- The coordinator's part.

// committers are the workers that commit rec's epoch: every worker's lanes'
// transactions, or one worker that applies every lane's staging.
func (c *wCoord) committers() []int {
	if !c.m.txn() {
		return []int{int(c.rec.Epoch % uint64(c.lay.workers))}
	}
	out := make([]int, c.lay.workers)
	for i := range out {
		out[i] = i
	}
	return out
}

// settleMsg is what worker i settles of the restored record: committables
// of the txn destination, or the staged one's epoch.
func (c *wCoord) settleMsg(i int) wMsg {
	msg := wMsg{kind: "settle", attempt: c.a.n, gen: c.gen, restored: c.restored, lay: c.lay}
	if !c.m.txn() {
		msg.recover = i == int(c.restored.epoch()%uint64(c.lay.workers))
		return msg
	}
	for j, s := range c.restored.txnCommittables() {
		if c.m.cfg.broken == "recovery-skips-uncommitted" {
			// Each worker settles the lanes of the record it runs now.
			if s.Lane < len(c.lay.lanes) && c.lay.lanes[s.Lane].worker == i {
				msg.tasks = append(msg.tasks, s)
			}
			continue
		}
		if j%c.lay.workers == i {
			msg.tasks = append(msg.tasks, s)
		}
	}
	return msg
}

// takeSettled takes a worker's answer to its settling, and once every
// worker's is in, assigns the attempt's work.
func (c *wCoord) takeSettled(worker int, results []string) {
	if c.phase != coSettle || c.settled[worker] != nil {
		return
	}
	c.settled[worker] = results
	if len(c.settled) < c.lay.workers {
		return
	}
	n := map[string]int{}
	for _, rs := range c.settled {
		for _, r := range rs {
			n[r]++
		}
	}
	m := c.m
	for r, k := range n {
		m.stats["settled: "+r] += k
	}
	if c.m.txn() && n["applied"] > 0 && n["applied"] < len(c.restored.Committables) {
		m.stats["epochs found partly committed"]++
	}
	m.tracef("%s settled %v: %v", c.name(), c.restored, n)
	if n["lost"] > 0 {
		c.phase = coRewind
		return
	}
	c.assignAll()
}

// rewind moves HEAD back to the record before the restored one, as a
// single runner does when its destination lost the epoch.
func (c *wCoord) rewind(d decision) {
	var prev *wRecord
	var fenced error
	ok, err := c.m.call(c.kill, d, func() error {
		p, err := c.m.readRecord(c.restored.Previous)
		if err != nil {
			return err
		}
		prev = p
		next := checkpoint.Head{Generation: c.gen, Owner: c.owner()}
		if p != nil {
			ref := p.ref()
			next.Record, next.Previous, next.First = &ref, p.Previous, p.Previous == nil
		}
		fenced, err = c.swapHead(next)
		return err
	})
	switch {
	case !ok:
	case fenced != nil:
		c.fail(fenced)
	case err == nil:
		c.m.stats["epochs rolled back"]++
		c.m.tracef("%s rolled HEAD back to %v", c.name(), prev)
		c.restored, c.next = prev, prev.epoch()+1
		c.assignAll()
	}
}

// requestCommit asks worker i to commit the record's epoch.
func (c *wCoord) requestCommit(d decision, i int) {
	w := c.a.workers[i]
	msg := wMsg{kind: "commit", attempt: c.a.n, gen: c.gen, epoch: c.rec.Epoch}
	if !c.m.txn() {
		msg.committables = c.rec.Committables
	}
	ok, err := c.m.call(c.kill, d, func() error {
		c.m.lossy("commit's requests", func() { w.send(msg) })
		return nil
	})
	if !ok || err != nil {
		return
	}
	if _, again := c.asked[i]; again {
		c.m.stats["commit requests sent again"]++
	}
	c.asked[i] = c.m.tick
	if c.m.cfg.broken == "ack-before-commits" && len(c.asked) == len(c.committers()) {
		c.committedEpoch()
	}
}

func (c *wCoord) takeCommitted(worker int, epoch uint64) {
	if c.phase != coCommit || epoch != c.rec.Epoch {
		c.m.stats["answers of other epochs ignored"]++
		return
	}
	c.answered[worker] = true
	if len(c.answered) == len(c.committers()) {
		c.committedEpoch()
	}
}

func (c *wCoord) committedEpoch() {
	c.m.committed[c.gen] = c.rec.Epoch
	c.m.tracef("%s committed epoch %d", c.name(), c.rec.Epoch)
	c.phase = coAck
}

// ---- The worker's part.

func (w *wWorker) startSettle(msg wMsg) {
	w.tasks, w.settleRec, w.settleLay, w.recovering = msg.tasks, msg.restored, msg.lay, msg.recover
	n := len(w.tasks)
	if w.recovering {
		n++
	}
	w.results, w.lastAck, w.phase = make([]string, n), w.m.tick, wwSettle
	w.maybeSettled()
}

func (w *wWorker) maybeSettled() {
	if slices.Contains(w.results, "") {
		return
	}
	w.phase = wwSettled
	w.m.route(w, wMsg{kind: "settled", attempt: w.a.n, gen: w.gen, worker: w.idx, results: w.results})
}

// settleActions are the worker's steps of settling: one a committable, or
// one for the staged destination.
func (w *wWorker) settleActions(add func(string, actKind, func(decision))) {
	for i, t := range w.tasks {
		if w.results[i] == "" {
			add("settle "+t.marker().String(), aCall, func(d decision) { w.settle(d, i) })
		}
	}
	if i := len(w.tasks); w.recovering && w.results[i] == "" {
		add("settle the destination", aCall, func(d decision) { w.settle(d, i) })
	}
}

func (w *wWorker) settle(d decision, i int) {
	m := w.m
	var res string
	ok, err := m.call(w.kill, d, func() (err error) {
		if i == len(w.tasks) {
			res = "applied"
			if missing, _, _ := m.wh.missing(mustMerge(w.settleRec)); len(missing) > 0 {
				res = "redone"
			}
			return m.recoverDest(w.gen, w.settleRec)
		}
		res, err = m.settle(w, w.tasks[i], w.settleRec, w.settleLay)
		return err
	})
	switch {
	case !ok:
		return
	case errors.Is(err, errStagingMissing):
		m.problem("exactly-once", "%s can't settle %v: %v", w.name(), w.settleRec, err)
		w.fail(err.Error())
		return
	case errors.Is(err, destination.ErrFenced), errors.Is(err, errTxnGone), errors.Is(err, errTxnBusy), errors.Is(err, errRedo):
		w.fail(err.Error())
		return
	case err != nil:
		return
	}
	w.results[i] = res
	w.maybeSettled()
}

// mustMerge is the staged destination's epoch of rec, or none.
func mustMerge(rec *wRecord) warehouseStaged {
	if rec == nil {
		return warehouseStaged{}
	}
	st, _ := mergeStaged(stagedOf(rec.Committables))
	return st
}

// open starts a producer for each slot of each of the worker's lanes,
// which fences the producers those IDs had.
func (w *wWorker) open(d decision) {
	pes := map[wTxnID]uint64{}
	ok, err := w.m.call(w.kill, d, func() error {
		for _, l := range w.lanes {
			for s := range wSlots {
				id := w.ownID(l.idx, s)
				pes[id] = w.m.initProducer(id)
			}
		}
		return nil
	})
	if !ok || err != nil {
		return
	}
	w.producers = pes
	w.toConnect()
}

// stageTxn is a lane's pre-commit on the txn destination: it writes the
// epoch into its slot's transaction, and if the lane's writer stages,
// stages a copy first.
func (l *wLane) stageTxn(d decision) {
	w, m := l.w, l.w.m
	e := l.precommitted + 1
	slot := int(e % wSlots)
	id := w.ownID(l.idx, slot)
	st := wTxnStaged{Lane: l.idx, Slot: slot, IDGen: id.Gen, Producer: w.producers[id], Gen: w.gen, N: e, KG: l.kg, Staged: m.stages(l.idx)}
	for _, c := range l.splits {
		st.Splits = append(st.Splits, c.split)
	}
	rows := map[string][]row{}
	for _, s := range wStreams {
		if len(l.buf[e][s]) > 0 {
			st.Streams = append(st.Streams, s)
			rows[s] = slices.Clone(l.buf[e][s])
		}
	}
	ok, err := m.call(w.kill, d, func() error {
		if st.Staged {
			m.wh.mu.Lock()
			for _, s := range st.Streams {
				m.wh.staging[stagingKey{w.gen, e, s, l.idx}] = rows[s]
			}
			m.wh.mu.Unlock()
		}
		return m.produce(id, st.Producer, st.marker(), rows)
	})
	switch {
	case !ok:
		return
	case errors.Is(err, destination.ErrFenced), errors.Is(err, errTxnBusy):
		w.fail(err.Error())
		return
	case err != nil:
		return
	}
	w.rows[st.marker()] = rows
	l.staged[e], l.precommitted = must(json.Marshal(st)), e
	delete(l.buf, e)
}

// commitRequested takes the coordinator's request to commit an epoch. A
// request for one it has committed is answered again.
func (w *wWorker) commitRequested(msg wMsg) {
	e := msg.epoch
	if h, _, err := w.m.readHead(); err == nil && h.Generation > w.gen {
		w.m.stats["commit requests a fenced generation's worker took"]++
	}
	if !w.m.txn() && !w.done[e] {
		w.toApply[e] = msg.committables
	}
	switch {
	case w.done[e]:
		w.answer(e)
		return
	case w.m.txn() && w.pending[e] == nil:
		return
	case w.m.cfg.broken == "answer-on-receipt":
		w.answer(e)
	}
	if !slices.Contains(w.due, e) {
		w.due = append(w.due, e)
	}
}

// commit commits epoch e: its lanes' transactions, or every lane's staging;
// and answers once it has.
func (w *wWorker) commit(d decision, e uint64) {
	m := w.m
	if !m.txn() {
		w.apply(d, e)
		return
	}
	cs := w.pending[e]
	commit := func() error {
		for _, c := range cs {
			if err := m.commitTxn(w.gen, c.id(), c.Producer, c.marker()); err != nil {
				return err
			}
		}
		return nil
	}
	if m.cfg.broken == "stale-worker-commits" {
		inner := commit
		commit = func() error {
			err := inner()
			if !errors.Is(err, destination.ErrFenced) {
				return err
			}
			// Fenced, it starts a new producer for each ID, and commits the
			// epoch again from what it holds.
			for _, c := range cs {
				pe := m.initProducer(c.id())
				if err := m.produce(c.id(), pe, c.marker(), w.rows[c.marker()]); err != nil {
					return err
				}
				if err := m.commitTxn(w.gen, c.id(), pe, c.marker()); err != nil {
					return err
				}
			}
			m.stats["fenced commits made again"]++
			return nil
		}
	}
	ok, err := m.call(w.kill, d, commit)
	if !w.committed(ok, d, err, e) {
		return
	}
	for _, c := range cs {
		delete(w.rows, c.marker())
	}
	delete(w.pending, e)
}

// apply applies every lane's staging of epoch e in the staged
// destination's transaction: all of it, or with scope stream one stream a
// step.
func (w *wWorker) apply(d decision, e uint64) {
	m := w.m
	st, err := mergeStaged(stagedOf(w.toApply[e]))
	if err != nil {
		w.fail(err.Error())
		return
	}
	streams := st.Streams
	stream := m.cfg.scope == destination.ScopeStream && len(streams) > 0
	if stream {
		streams = streams[w.appliedStreams[e] : w.appliedStreams[e]+1]
	}
	ok, err := m.call(w.kill, d, func() error { return m.applyEpoch(w.gen, st, streams) })
	if ok && err == nil && stream {
		if w.appliedStreams[e]++; w.appliedStreams[e] < len(st.Streams) {
			m.stats["applies between streams"]++
			return
		}
	}
	if w.committed(ok, d, err, e) {
		delete(w.toApply, e)
	}
}

// committed takes the outcome of a commit's step, and answers once the
// epoch is committed.
func (w *wWorker) committed(ok bool, d decision, err error, e uint64) bool {
	m := w.m
	switch {
	case !ok && d == crashAfter:
		m.stats["workers killed between their commit and its answer"]++
		return false
	case !ok:
		return false
	case errors.Is(err, destination.ErrFenced), errors.Is(err, errTxnGone), errors.Is(err, errTxnBusy), errors.Is(err, errStagingMissing):
		m.stats["commits refused"]++
		w.fail(err.Error())
		return false
	case err != nil:
		return false
	}
	w.due = slices.DeleteFunc(w.due, func(x uint64) bool { return x == e })
	w.done[e] = true
	m.tracef("%s committed epoch %d", w.name(), e)
	if m.cfg.broken != "answer-on-receipt" {
		w.answer(e)
	}
	return true
}

func (w *wWorker) answer(e uint64) {
	msg := wMsg{kind: "committed", attempt: w.a.n, gen: w.gen, worker: w.idx, epoch: e}
	w.m.lossy("commit's answers", func() { w.m.route(w, msg) })
}
