package sim

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// The txn destination is Kafka-like: each writer lane's epoch is a
// transaction open in a producer of the worker that runs the lane, and no
// other process can commit it. The coordinator never writes to it. After
// HEAD moves to record N, it asks each worker to commit its lanes'
// transactions of epoch N, on the attempt protocol, asks again a worker
// that hasn't answered in time, and acknowledges the epoch only once every
// worker has answered.
//
// A transactional ID is stable per pipeline, writer lane and pool slot, as
// destination-kafka's is per pipeline and slot; a producer's init fences the
// one before it with that ID and aborts its open transaction. Each
// transaction carries the marker (generation, epoch, lane), and the broker
// aborts one left open for the transaction timeout. A lane writes its epoch
// into the transaction at its pre-commit, where destination-kafka writes
// rows as they come; nothing is visible before the commit either way.
//
// A single runner's recovery fences the recorded transaction's producer,
// and finds the epoch's marker or else rolls back to the record before. On
// several workers that rule finds some lanes committed and some not, and
// can't redo the rest: the transactions died with their producers. So a
// worker also stages a copy of each lane's epoch, by generation, epoch and
// lane, before it sends its share, and recovery settles each committable
// the record names: a worker of the new attempt fences its producer, finds
// its marker, or else produces the copy in a transaction of its own and
// commits it. Every committable is settled before the attempt's workers are
// assigned, restore, or start producers of their own.

const (
	// Producers per writer lane, as destination-kafka's pool has.
	wSlots = 3
	// Ticks the coordinator waits for a worker's answer before asking again.
	wReqTimeout = 3
	// Per cent of the commit's messages lost or delayed, and of its steps
	// that fail, while their faults last.
	wMsgPct         = 10
	wCommitFaultPct = 30
	// The random stream the txn destination's settings come from, so that
	// seeds of the staged destination run as they did before it.
	wTxnStream = 35
)

var (
	// The transaction a commit names isn't open: the broker aborted it, or
	// a producer's init did.
	errTxnGone = errors.New("sim: no such open transaction")
	// The producer still has another epoch's transaction open.
	errTxnBusy = errors.New("sim: another transaction is open")
)

// txnVariants are the broken variants of the txn destination's protocol,
// whose seeds all use it.
var txnVariants = []string{"ack-before-commits", "answer-on-receipt", "commit-before-record", "recovery-skips-uncommitted",
	"single-runner-recovery", "stale-worker-commits", "ids-by-generation", "resume-prepared"}

// wTxnID is a transactional ID.
type wTxnID struct {
	Lane, Slot int
	// Zero, but with ids-by-generation: the generation of the attempt whose
	// producer it is.
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

// wBroker is the destination's durable state; what transactions made
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

// wTxnStaged is a writer lane's committable: its transaction and the
// producer that holds it, and the streams staged in its copy.
type wTxnStaged struct {
	Lane     int      `json:"lane"`
	Slot     int      `json:"slot"`
	IDGen    uint64   `json:"id_gen,omitempty"`
	Producer uint64   `json:"producer_epoch"`
	Gen      uint64   `json:"gen"`
	N        uint64   `json:"n"`
	Streams  []string `json:"streams,omitempty"`
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

func (m *wModel) txn() bool { return m.cfg.dest == "txn" }

// commitStep says a is a step of the txn destination's commit or recovery,
// where its own faults go.
func (m *wModel) commitStep(a wAction) bool {
	if !m.txn() {
		return false
	}
	for _, p := range []string{"request commit", "commit ", "settle ", "open producers", "record.swap", "ack", "rewind"} {
		if strings.HasPrefix(a.op, p) {
			return true
		}
	}
	return false
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
// committer: its rows land, with its marker. It drops the lane's staged
// copy, which nothing needs once the marker is visible.
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

// settle is a worker's part of recovery for one committable of the record
// HEAD names: it fences the committable's producer, which aborts its
// transaction if it's still open, and then finds the marker, or produces
// the lane's staged copy in a transaction of its own and commits it.
func (m *wModel) settle(w *wWorker, c wTxnStaged) (string, error) {
	mk, id := c.marker(), c.id()
	if m.cfg.broken == "ids-by-generation" {
		id = w.ownID(c.Lane, c.Slot)
	}
	if m.cfg.broken == "resume-prepared" {
		return m.resumePrepared(w, id, mk)
	}
	pe := m.initProducer(id)
	m.decided[mk] = max(m.decided[mk], w.gen)
	if m.broker.markers[mk] {
		return "applied", nil
	}
	if m.cfg.broken == "single-runner-recovery" {
		return "lost", nil
	}
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
	return "redone", m.commitTxn(w.gen, id, pe, mk)
}

// resumePrepared settles a committable as Kafka's two-phase commit
// (KIP-939) would, with no staged copy: the init keeps the transaction it
// finds prepared, and recovery commits it if it's the one the record names.
// Any other init, such as a stale worker's starting its producers, aborts
// it as before; then the epoch is lost, and HEAD goes back.
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
		return "redone", nil
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

// tasksFor is the committables of the restored record worker i settles.
func (c *wCoord) tasksFor(i int) []wTxnStaged {
	var out []wTxnStaged
	for j, s := range c.restored.txnCommittables() {
		if c.m.cfg.broken == "recovery-skips-uncommitted" {
			// Each worker settles the lanes of the record it runs now.
			if s.Lane < len(c.lay.lanes) && c.lay.lanes[s.Lane].worker == i {
				out = append(out, s)
			}
			continue
		}
		if j%c.lay.workers == i {
			out = append(out, s)
		}
	}
	return out
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
		m.stats["committables "+r+" at recovery"] += k
	}
	if n["applied"] > 0 && n["applied"] < len(c.restored.Committables) {
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

// requestCommit asks worker i to commit its lanes' transactions of the
// record's epoch.
func (c *wCoord) requestCommit(d decision, i int) {
	w := c.a.workers[i]
	msg := wMsg{kind: "commit", attempt: c.a.n, gen: c.gen, epoch: c.rec.Epoch}
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
	if c.m.cfg.broken == "ack-before-commits" && len(c.asked) == c.lay.workers {
		c.committedEpoch()
	}
}

func (c *wCoord) takeCommitted(worker int, epoch uint64) {
	if c.phase != coCommit || epoch != c.rec.Epoch {
		c.m.stats["answers of other epochs ignored"]++
		return
	}
	c.answered[worker] = true
	if len(c.answered) == c.lay.workers {
		c.committedEpoch()
	}
}

func (c *wCoord) committedEpoch() {
	c.m.committed[c.gen] = c.rec.Epoch
	c.m.tracef("%s committed epoch %d", c.name(), c.rec.Epoch)
	c.phase = coAck
}

// ---- The worker's part.

func (w *wWorker) startSettle(tasks []wTxnStaged) {
	w.tasks, w.results, w.lastAck, w.phase = tasks, make([]string, len(tasks)), w.m.tick, wwSettle
	w.maybeSettled()
}

func (w *wWorker) maybeSettled() {
	if slices.Contains(w.results, "") {
		return
	}
	w.phase = wwSettled
	w.m.route(w, wMsg{kind: "settled", attempt: w.a.n, gen: w.gen, worker: w.idx, results: w.results})
}

func (w *wWorker) settle(d decision, i int) {
	var res string
	ok, err := w.m.call(w.kill, d, func() (err error) { res, err = w.m.settle(w, w.tasks[i]); return err })
	switch {
	case !ok:
		return
	case errors.Is(err, errStagingMissing):
		w.m.problem("exactly-once", "%s can't settle %v: %v", w.name(), w.tasks[i].marker(), err)
		w.fail(err.Error())
		return
	case errors.Is(err, destination.ErrFenced), errors.Is(err, errTxnGone), errors.Is(err, errTxnBusy):
		w.fail(err.Error())
		return
	case err != nil:
		return
	}
	w.results[i] = res
	w.maybeSettled()
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

// stageTxn is a lane's pre-commit on the txn destination: it stages a copy
// of the epoch, and writes the epoch into its slot's transaction.
func (l *wLane) stageTxn(d decision) {
	w, m := l.w, l.w.m
	e := l.precommitted + 1
	slot := int(e % wSlots)
	id := w.ownID(l.idx, slot)
	st := wTxnStaged{Lane: l.idx, Slot: slot, IDGen: id.Gen, Producer: w.producers[id], Gen: w.gen, N: e}
	rows := map[string][]row{}
	for _, s := range wStreams {
		if len(l.buf[e][s]) > 0 {
			st.Streams = append(st.Streams, s)
			rows[s] = slices.Clone(l.buf[e][s])
		}
	}
	ok, err := m.call(w.kill, d, func() error {
		m.wh.mu.Lock()
		for _, s := range st.Streams {
			m.wh.staging[stagingKey{w.gen, e, s, l.idx}] = rows[s]
		}
		m.wh.mu.Unlock()
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
func (w *wWorker) commitRequested(e uint64) {
	if h, _, err := w.m.readHead(); err == nil && h.Generation > w.gen {
		w.m.stats["commit requests a fenced generation's worker took"]++
	}
	switch {
	case w.done[e]:
		w.answer(e)
		return
	case w.pending[e] == nil:
		return
	case w.m.cfg.broken == "answer-on-receipt":
		w.answer(e)
	}
	if !slices.Contains(w.due, e) {
		w.due = append(w.due, e)
	}
}

// commit commits its lanes' transactions of epoch e, and answers.
func (w *wWorker) commit(d decision, e uint64) {
	m := w.m
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
	switch {
	case !ok && d == crashAfter:
		m.stats["workers killed between their commit and its answer"]++
		return
	case !ok:
		return
	case errors.Is(err, destination.ErrFenced), errors.Is(err, errTxnGone), errors.Is(err, errTxnBusy):
		m.stats["commits refused"]++
		w.fail(err.Error())
		return
	case err != nil:
		return
	}
	w.due = slices.DeleteFunc(w.due, func(x uint64) bool { return x == e })
	w.done[e] = true
	for _, c := range cs {
		delete(w.rows, c.marker())
	}
	delete(w.pending, e)
	m.tracef("%s committed its transactions of epoch %d", w.name(), e)
	if m.cfg.broken != "answer-on-receipt" {
		w.answer(e)
	}
}

func (w *wWorker) answer(e uint64) {
	msg := wMsg{kind: "committed", attempt: w.a.n, gen: w.gen, worker: w.idx, epoch: e}
	w.m.lossy("commit's answers", func() { w.m.route(w, msg) })
}
