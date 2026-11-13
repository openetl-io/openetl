package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// The workers kind models what the engine and the runner don't have yet: a
// SQL transform between the source and the destination, run by several
// workers under a coordinator, with an exchange between them by key group.
// It runs no runner code: one goroutine plays every pod, a step at a time,
// each chosen by the seed, so the designed protocol is checked before it's
// built, and later work can be checked against it. The transform is
//
//	INSERT INTO filtered SELECT id, key, t, v * 3 + 1 AS v FROM events WHERE v % 4 <> 0;
//	INSERT INTO agg SELECT window_start, key, sum(v * 3 + 1), count(*)
//	FROM events WHERE v % 4 <> 0 GROUP BY tumble(t, size), key;
//
// events is read in splits. Each lane runs the row-wise statement on the
// splits it reads, writes what it keeps, and sends each kept row through the
// exchange to the lane that owns its key's group. There the keyed operator
// folds it into its window, closes a window once the operator's watermark,
// the least of its senders', passes the window's end, and the lane's writer
// stages what it emits. A row whose window has closed is late, and goes to
// late as the design quarantines it.
//
// A split's watermark is the latest event time it has read less a delay.
// In half the seeds rows arrive over time, and a split with nothing to read
// for a while is idle: its watermark advances to the tick's event time less
// the delay, as an event of the split, so never past a barrier it placed. A
// lane sends the least of its splits' watermarks through the exchange.
//
// A barrier flows in band from the splits through the exchange. Each lane
// aligns it across every sender of the attempt, snapshots the key groups that
// changed, and stages its epoch; each worker sends its share of the epoch;
// the coordinator writes the record, with a watermark per split, and commits,
// and the workers acknowledge the record before upstream. Any lost pod or
// broken exchange fails the attempt, and the next one restores every worker
// from the record under a new generation, on as many workers and lanes as
// its seed says.
//
// The model's contract, checked on every seed:
//   - exactly-once: each row the filter keeps lands once in filtered, and its
//     effect once: in one row of agg, or in late;
//   - aggregate: agg is what a run without failures gives;
//   - restore: what a lane restores, and every record kept, is with what
//     committed epochs emitted the fold of the rows before the record's
//     positions;
//   - watermark: a split's watermark never goes back, within an attempt,
//     after a restore, along the records, or on an exchange; nor does a
//     keyed operator's across a restore;
//   - generation: nothing an older generation's pods sent, staged or wrote
//     is committed.
//
// No row is behind its own split's watermark, so none is late, and the
// output doesn't depend on how the splits interleave.

var brokenFlag = flag.String("broken", "", "run the workers kind with a broken variant of its protocol (see brokenVariants)")

const (
	// Ticks without a heartbeat before a pod is taken as lost.
	wTimeout     = 3
	wJoinTimeout = 6
	wMaxTicks    = 4000
	wMaxSteps    = 400_000
	// Per mille of steps that let time pass, or replace the newest attempt.
	wTickPer    = 25
	wReplacePer = 15
	wPausePct   = 2
	wNetPct     = 8
	wNoMark     = math.MinInt64
	wEndMark    = math.MaxInt64
	wOperator   = "agg"
	wPipeline   = "org/pipe/"
)

var (
	wHeadKey  = wPipeline + "_checkpoints/HEAD"
	wStreams  = []string{"agg", "filtered", "late"}
	errRefuse = errors.New("sim: refused")
	errNoPod  = errors.New("sim: nobody at the address")
)

type wEvent struct {
	ID  string `json:"id"`
	Key string `json:"key"`
	T   int64  `json:"t"`
	V   int64  `json:"v"`
}

// kept is the row-wise statement's WHERE, and value its projection.
func (e wEvent) kept() bool   { return e.V%4 != 0 }
func (e wEvent) value() int64 { return e.V*3 + 1 }

func keyGroupOf(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % checkpoint.KeyGroups)
}

func windowStart(t, size int64) int64 {
	w := t / size * size
	if w > t {
		w -= size
	}
	return w
}

func inRange(r checkpoint.KeyGroupRange, g int) bool { return g >= r.First && g <= r.Last }

type wCfg struct {
	splits [][]wEvent
	// By split: whether a resume point may follow the row before index i.
	resumeAt    [][]bool
	size, delay int64
	page        int
	faults      int
	pauses      int
	netFaults   int
	zombies     int
	// Rows arriving over time: event time per tick, the tick each row of
	// each split arrives at, and the ticks a split with nothing to read waits
	// before it's idle. Rate 0: every row is there from the start.
	rate      int64
	avail     [][]int
	idleTicks int
	// Workers that stall before connecting to their peers, long enough for
	// their attempt to fail; and snapshot writes that land ticks after their
	// caller timed out and wrote again, whatever became of it.
	slowConnects, slowWrites int
	keep                     int
	scope                    destination.Scope
	// Runners the coordinators run on, and whether a new pod may take the
	// address of one that stopped.
	runners int
	reuse   bool
	broken  string
}

func (c wCfg) String() string {
	var b strings.Builder
	b.WriteString("rows=[")
	for i, s := range c.splits {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprint(&b, len(s))
	}
	b.WriteString("]")
	if c.rate > 0 {
		fmt.Fprintf(&b, " arriving=%d/tick idle=%d", c.rate, c.idleTicks)
	}
	fmt.Fprintf(&b, " size=%d delay=%d page=%d faults=%d pauses=%d net=%d zombies=%d slowConnects=%d slowWrites=%d keep=%d scope=%d runners=%d reuse=%v",
		c.size, c.delay, c.page, c.faults, c.pauses, c.netFaults, c.zombies, c.slowConnects, c.slowWrites, c.keep, c.scope, c.runners, c.reuse)
	if c.broken != "" {
		fmt.Fprintf(&b, " broken=%s", c.broken)
	}
	return b.String()
}

func randomWorkersCfg(rng *rand.Rand, broken string) wCfg {
	c := wCfg{
		size: 3 + int64(rng.IntN(10)), delay: int64(rng.IntN(5)), page: 1 + rng.IntN(4),
		faults: rng.IntN(9), pauses: rng.IntN(3), netFaults: rng.IntN(4), zombies: rng.IntN(3), slowConnects: rng.IntN(3), slowWrites: rng.IntN(3),
		keep: 2 + rng.IntN(3), scope: destination.ScopePipeline, runners: 1 + rng.IntN(2), reuse: rng.IntN(4) != 0, broken: broken,
	}
	if rng.IntN(2) == 0 {
		c.scope = destination.ScopeStream
	}
	keys := 2 + rng.IntN(12)
	if rng.IntN(2) == 0 {
		c.rate, c.idleTicks = 1+int64(rng.IntN(4)), 1+rng.IntN(3)
	}
	for s := range 1 + rng.IntN(4) {
		n := 3 + rng.IntN(25)
		// Event times run ahead by up to 3 a row, each up to delay behind:
		// no row is behind its split's watermark, maxSeen - delay. Arriving
		// over time, a row is readable from the tick its clock reaches, and
		// some splits go quiet for a while.
		clock := int64(rng.IntN(10))
		evs, avail := make([]wEvent, n), make([]int, n+1)
		for i := range evs {
			clock += int64(rng.IntN(4))
			if c.rate > 0 && rng.IntN(6) == 0 {
				clock += c.rate * int64(c.idleTicks+1+rng.IntN(4))
			}
			evs[i] = wEvent{ID: fmt.Sprintf("s%d.%d", s, i), Key: fmt.Sprintf("k%d", rng.IntN(keys)), T: clock - rng.Int64N(c.delay+1), V: int64(rng.IntN(100))}
			if c.rate > 0 {
				avail[i] = int(clock / c.rate)
			}
		}
		at := make([]bool, n+1)
		for i := range at {
			at[i] = i == n || rng.IntN(100) < 60
		}
		c.splits, c.resumeAt, c.avail = append(c.splits, evs), append(c.resumeAt, at), append(c.avail, avail)
	}
	return c
}

// wLayout is an attempt's placement, which nothing durable names: its
// workers, each lane's worker and key groups, and each split's lane.
type wLayout struct {
	workers int
	lanes   []wLaneSpec
	splits  []int
}

type wLaneSpec struct {
	worker int
	kg     checkpoint.KeyGroupRange
}

// randomLayout gives 1 to 3 workers 1 or 2 lanes each, cuts the key groups
// into one contiguous range a lane, equal or not, and gives each split a
// lane.
func randomLayout(rng *rand.Rand, splits int) wLayout {
	l := wLayout{workers: 1 + rng.IntN(3)}
	per := make([]int, l.workers)
	n := 0
	for i := range per {
		per[i] = 1 + rng.IntN(2)
		n += per[i]
	}
	cuts := map[int]bool{}
	if rng.IntN(2) == 0 {
		for i := 1; i < n; i++ {
			cuts[i*checkpoint.KeyGroups/n] = true
		}
	}
	for len(cuts) < n-1 {
		cuts[1+rng.IntN(checkpoint.KeyGroups-1)] = true
	}
	ends := append(slices.Sorted(maps.Keys(cuts)), checkpoint.KeyGroups)
	first := 0
	for w, k := range per {
		for range k {
			end := ends[len(l.lanes)]
			l.lanes = append(l.lanes, wLaneSpec{worker: w, kg: checkpoint.KeyGroupRange{First: first, Last: end - 1}})
			first = end
		}
	}
	for range splits {
		l.splits = append(l.splits, rng.IntN(n))
	}
	return l
}

func (l wLayout) owner(g int) int {
	for i, s := range l.lanes {
		if inRange(s.kg, g) {
			return i
		}
	}
	return -1
}

func (l wLayout) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d workers:", l.workers)
	for i, s := range l.lanes {
		fmt.Fprintf(&b, " l%d@w%d=%d-%d", i, s.worker, s.kg.First, s.kg.Last)
	}
	fmt.Fprintf(&b, " splits→lanes %v", l.splits)
	return b.String()
}

// wSplit is a split as the record has it: where its read resumes, and its
// watermark, which the record of today doesn't have.
type wSplit struct {
	ID       int  `json:"id"`
	Pos      int  `json:"pos"`
	Complete bool `json:"complete,omitempty"`
	// wNoMark before the split's first row, wEndMark once it completed.
	Watermark int64 `json:"watermark"`
}

// wRecord is a checkpoint record as the design has it: the record of today,
// whose snapshots name files by operator and key-group range, with a
// watermark per split.
type wRecord struct {
	Epoch        uint64                `json:"epoch"`
	Generation   uint64                `json:"generation"`
	Previous     *checkpoint.RecordRef `json:"previous,omitempty"`
	KeyGroups    int                   `json:"key_groups"`
	Splits       []wSplit              `json:"splits"`
	Committables [][]byte              `json:"committables"`
	Snapshots    []checkpoint.Snapshot `json:"snapshots,omitempty"`
	// The generations of the shares merged into it, for the checks.
	sharedBy []uint64
}

func (r *wRecord) ref() checkpoint.RecordRef {
	return checkpoint.RecordRef{Epoch: r.Epoch, Generation: r.Generation}
}

func (r *wRecord) String() string {
	if r == nil {
		return "no record"
	}
	return fmt.Sprintf("record %d-g%d", r.Epoch, r.Generation)
}

func (r *wRecord) epoch() uint64 {
	if r == nil {
		return 0
	}
	return r.Epoch
}

// split is split id as r has it: from the start before any record.
func (r *wRecord) split(id int) wSplit {
	if r != nil {
		for _, s := range r.Splits {
			if s.ID == id {
				return s
			}
		}
	}
	return wSplit{ID: id, Watermark: wNoMark}
}

func (r *wRecord) snapshots() []checkpoint.Snapshot {
	if r == nil {
		return nil
	}
	return r.Snapshots
}

// final says every split completed: the attempt that restores it has
// nothing to do.
func (r *wRecord) final() bool {
	if r == nil || len(r.Splits) == 0 {
		return false
	}
	for _, s := range r.Splits {
		if !s.Complete {
			return false
		}
	}
	return true
}

func wRecordKey(ref checkpoint.RecordRef) string {
	return fmt.Sprintf("%s_checkpoints/%020d-g%020d.json", wPipeline, ref.Epoch, ref.Generation)
}

func parseWRecordKey(key string) (checkpoint.RecordRef, bool) {
	name, ok := strings.CutPrefix(key, wPipeline+"_checkpoints/")
	if !ok || !strings.HasSuffix(name, ".json") {
		return checkpoint.RecordRef{}, false
	}
	e, g, ok := strings.Cut(strings.TrimSuffix(name, ".json"), "-g")
	epoch, err1 := strconv.ParseUint(e, 10, 64)
	gen, err2 := strconv.ParseUint(g, 10, 64)
	return checkpoint.RecordRef{Epoch: epoch, Generation: gen}, ok && err1 == nil && err2 == nil
}

// wStateFile is a snapshot file: some of an operator's key groups at a
// barrier, and who wrote it.
type wStateFile struct {
	Generation uint64           `json:"generation"`
	Epoch      uint64           `json:"epoch"`
	Groups     map[int][]wEntry `json:"groups"`
}

type wEntry struct {
	Key   string     `json:"key"`
	W     int64      `json:"w"`
	Sum   int64      `json:"sum"`
	Count int64      `json:"count"`
	Rows  []wContrib `json:"rows"`
}

// wContrib is a row folded into a window: which, the generation that read
// it, and whether it was restored from a snapshot since.
type wContrib struct {
	ID       string `json:"id"`
	Gen      uint64 `json:"g"`
	Restored bool   `json:"r,omitempty"`
}

type wKey struct {
	Key string
	W   int64
}

type wAcc struct {
	Sum, Count int64
	Rows       []wContrib
}

// stateURL names a snapshot file by generation, epoch, operator and the
// lane's key groups, as quarantined and held files are named by generation
// and epoch. Two broken variants drop parts of the name.
func (m *wModel) stateURL(gen, epoch uint64, kg checkpoint.KeyGroupRange) string {
	groups := fmt.Sprintf("kg-%03d-%03d", kg.First, kg.Last)
	switch m.cfg.broken {
	case "names-without-generation":
		return fmt.Sprintf("%s_state/%d/%s/%s", wPipeline, epoch, wOperator, groups)
	case "names-by-group":
		return fmt.Sprintf("%s_state/%s/%s", wPipeline, wOperator, groups)
	}
	return fmt.Sprintf("%s_state/g%d/%d/%s/%s", wPipeline, gen, epoch, wOperator, groups)
}

// stateGenEpoch is the generation and epoch a snapshot file's name says
// wrote it.
func stateGenEpoch(key string) (gen, epoch uint64, ok bool) {
	rest, ok := strings.CutPrefix(key, wPipeline+"_state/g")
	if !ok {
		return 0, 0, false
	}
	parts := strings.Split(rest, "/")
	gen, err1 := strconv.ParseUint(parts[0], 10, 64)
	epoch, err2 := strconv.ParseUint(parts[1], 10, 64)
	return gen, epoch, err1 == nil && err2 == nil
}

// ---- The model: its durable world, its pods, and the scheduler.

type actKind uint8

const (
	// Within a pod: the step may kill it.
	aLocal actKind = iota
	// To object storage, the destination or another pod: the step may fail,
	// time out with its outcome unknown, or kill its pod before or after.
	aCall
)

type wAction struct {
	who, op string
	kind    actKind
	pause   func(until int)
	do      func(decision)
}

type wModel struct {
	cfg   wCfg
	rng   *rand.Rand
	tick  int
	store *checkpoint.MemStore
	wh    *warehouse
	// By split: positions below this were acknowledged, and are gone.
	trimmed  []int
	attempts []*wAttempt
	// Pod addresses, and who is at each now.
	pods    map[string]*wWorker
	nextPod int

	faults, pauses, netFaults, zombies, slowConnects, slowWrites int
	// Writes in flight, which land at their tick.
	later []wLater

	trace, steps []string
	problems     []string
	// Data a current attempt needed was acknowledged away.
	lost   string
	giveUp error
	stats  map[string]int
	// The last watermark each split had in each attempt.
	marks map[[2]int]int64
	// By generation: the last epoch its lanes snapshotted, and the last it
	// committed; and how many lanes its layout had.
	snapped, committed map[uint64]uint64
	lanesOf            map[uint64]int
	// Each keyed lane's watermark at each snapshot, by its key groups.
	opMarks map[checkpoint.RecordRef][]wOpMark
}

type wOpMark struct {
	kg checkpoint.KeyGroupRange
	wm int64
}

type wLater struct {
	at   int
	land func()
}

type wAttempt struct {
	n       int
	co      *wCoord
	workers []*wWorker
}

func newWorkersModel(seed uint64, broken string) *wModel {
	rng := rand.New(rand.NewPCG(seed, uint64(len("workers"))))
	cfg := randomWorkersCfg(rng, broken)
	return &wModel{
		cfg: cfg, rng: rng, store: checkpoint.NewMemStore(), wh: newWarehouse(true),
		trimmed: make([]int, len(cfg.splits)), pods: map[string]*wWorker{},
		faults: cfg.faults, pauses: cfg.pauses, netFaults: cfg.netFaults, zombies: cfg.zombies, slowConnects: cfg.slowConnects, slowWrites: cfg.slowWrites,
		stats: map[string]int{}, marks: map[[2]int]int64{},
		snapped: map[uint64]uint64{}, committed: map[uint64]uint64{}, lanesOf: map[uint64]int{},
		opMarks: map[checkpoint.RecordRef][]wOpMark{},
	}
}

func (m *wModel) tracef(format string, args ...any) {
	m.trace = append(m.trace, fmt.Sprintf("t%d "+format, append([]any{m.tick}, args...)...))
}

// problem records a broken invariant, under its name.
func (m *wModel) problem(invariant, format string, args ...any) {
	p := fmt.Sprintf("["+invariant+"] "+format, args...)
	if slices.Contains(m.problems, p) {
		return
	}
	m.problems = append(m.problems, p)
	m.tracef("PROBLEM %s", p)
}

func (m *wModel) newest() *wAttempt { return m.attempts[len(m.attempts)-1] }

func (m *wModel) launch() {
	a := &wAttempt{n: len(m.attempts) + 1}
	a.co = &wCoord{m: m, a: a, runner: fmt.Sprintf("runner-%d", m.rng.IntN(m.cfg.runners))}
	m.attempts = append(m.attempts, a)
	m.tracef("a%d started on %s", a.n, a.co.runner)
}

// orchestrate plays Temporal: it retries the newest attempt once it failed.
func (m *wModel) orchestrate() {
	a := m.newest()
	if a.co.live() || a.co.succeeded() || m.giveUp != nil {
		return
	}
	if a.n >= maxAttempts {
		m.giveUp = fmt.Errorf("gave up after %d attempts; last error: %w", a.n, a.co.err)
		return
	}
	m.launch()
}

func (m *wModel) anyLive() bool {
	for _, a := range m.attempts {
		if a.co.live() {
			return true
		}
		for _, w := range a.workers {
			if w.live() {
				return true
			}
		}
	}
	return false
}

func (m *wModel) enabled() []wAction {
	var acts []wAction
	for _, a := range m.attempts {
		a.co.actions(&acts)
		for _, w := range a.workers {
			w.actions(&acts)
		}
	}
	return acts
}

func (m *wModel) decide(a wAction) decision {
	if m.faults == 0 || m.rng.IntN(100) >= faultPct {
		return proceed
	}
	m.faults--
	if a.kind == aCall {
		return []decision{failBefore, unknownOutcome, crashBefore, crashAfter}[m.rng.IntN(4)]
	}
	return crashBefore
}

// call applies a call's decision, as outcome does: it reports whether the
// caller lives, and the error it sees.
func (m *wModel) call(kill func(), d decision, apply func() error) (bool, error) {
	switch d {
	case failBefore:
		return true, errTransient
	case unknownOutcome:
		_ = apply()
		return true, errTimeout
	case crashBefore:
		kill()
		return false, errCrashed
	case crashAfter:
		_ = apply()
		kill()
		return false, errCrashed
	}
	return true, apply()
}

// run plays the seed: until the newest attempt has succeeded and every pod
// has stopped.
func (m *wModel) run() error {
	m.launch()
	for step := 0; ; step++ {
		m.orchestrate()
		switch {
		case m.giveUp != nil:
			return m.giveUp
		case m.lost != "":
			return errors.New(m.lost)
		case m.newest().co.succeeded() && !m.anyLive():
			return nil
		case step > wMaxSteps || m.tick > wMaxTicks:
			return fmt.Errorf("stuck: %d steps, %d ticks, %d attempts", step, m.tick, len(m.attempts))
		}
		acts := m.enabled()
		r := m.rng.IntN(1000)
		switch {
		case len(acts) == 0 || r < wTickPer:
			m.steps = append(m.steps, fmt.Sprintf("%d: tick", step))
			m.advance()
			continue
		case r < wTickPer+wReplacePer && m.zombies > 0 && m.newest().co.live():
			m.zombies--
			m.stats["replaced while running"]++
			m.steps = append(m.steps, fmt.Sprintf("%d: replace a%d", step, m.newest().n))
			m.tracef("replacing a%d while it runs", m.newest().n)
			m.launch()
			continue
		}
		a := acts[m.rng.IntN(len(acts))]
		if m.pauses > 0 && m.rng.IntN(100) < wPausePct {
			m.pauses--
			until := m.tick + wTimeout + 1 + m.rng.IntN(4)
			m.steps = append(m.steps, fmt.Sprintf("%d: pause %s", step, a.who))
			a.pause(until)
			continue
		}
		d := m.decide(a)
		m.steps = append(m.steps, fmt.Sprintf("%d: %s %s %s", step, a.who, a.op, d))
		if d != proceed {
			m.tracef("%s %s: %s", a.who, a.op, d)
		}
		a.do(d)
	}
}

// advance lets a tick pass: pods heartbeat, take peers that missed theirs
// as lost, and Temporal times out a coordinator that stalled.
func (m *wModel) advance() {
	m.tick++
	m.later = slices.DeleteFunc(m.later, func(l wLater) bool {
		if l.at <= m.tick {
			l.land()
		}
		return l.at <= m.tick
	})
	for _, a := range m.attempts {
		a.co.resume()
		for _, w := range a.workers {
			w.resume()
		}
	}
	for _, a := range m.attempts {
		for _, w := range a.workers {
			w.beat()
			w.idle()
		}
	}
	for _, a := range m.attempts {
		a.co.watch()
		for _, w := range a.workers {
			w.watch()
		}
	}
	if co := m.newest().co; co.live() && co.paused() && m.tick-co.pausedAt > wTimeout {
		m.tracef("%s timed out", co.name())
		m.stats["coordinators timed out"]++
		m.launch()
	}
}

// route takes a worker's message to its attempt's coordinator, which the
// runner finds by the attempt's ID. Without one there, the runner refuses it.
func (m *wModel) route(w *wWorker, msg wMsg) {
	co := m.attempts[w.a.n-1].co
	if m.cfg.broken == "runners-any-attempt" {
		// The runner hands it to whichever coordinator runs there now.
		for _, a := range slices.Backward(m.attempts) {
			if a.co.runner == w.runner && a.co.live() {
				co = a.co
				break
			}
		}
	}
	if co.live() && co.runner == w.runner {
		co.inbox.push(w, msg)
		return
	}
	w.send(wMsg{kind: "refused", reason: "no such attempt on " + w.runner})
}

// place gives a new pod an address: one a stopped pod had, or a new one.
func (m *wModel) place(w *wWorker) string {
	var free []string
	for _, addr := range slices.Sorted(maps.Keys(m.pods)) {
		if !m.pods[addr].live() {
			free = append(free, addr)
		}
	}
	addr := ""
	if m.cfg.reuse && len(free) > 0 && m.rng.IntN(2) == 0 {
		addr = free[m.rng.IntN(len(free))]
		m.stats["addresses reused"]++
	} else {
		addr = fmt.Sprintf("pod-%d", m.nextPod)
		m.nextPod++
	}
	m.pods[addr] = w
	return addr
}

// killed counts a pod lost while its generation had snapshotted an epoch it
// hadn't committed.
func (m *wModel) killed(gen uint64, what string) {
	m.stats["killed "+what]++
	if gen > 0 && m.snapped[gen] > m.committed[gen] {
		m.stats["kills between a snapshot and its commit"]++
	}
}

func (m *wModel) readRecord(ref *checkpoint.RecordRef) (*wRecord, error) {
	if ref == nil {
		return nil, nil
	}
	data, _, err := m.store.Get(context.Background(), wRecordKey(*ref))
	if err != nil {
		return nil, fmt.Errorf("record %d-g%d: %w", ref.Epoch, ref.Generation, err)
	}
	var r wRecord
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	if r.ref() != *ref {
		return nil, fmt.Errorf("record %d-g%d holds %d-g%d", ref.Epoch, ref.Generation, r.Epoch, r.Generation)
	}
	return &r, nil
}

func (m *wModel) readHead() (checkpoint.Head, checkpoint.Version, error) {
	data, v, err := m.store.Get(context.Background(), wHeadKey)
	if err != nil {
		return checkpoint.Head{}, "", err
	}
	var h checkpoint.Head
	return h, v, json.Unmarshal(data, &h)
}

func (m *wModel) readState(url string) (wStateFile, error) {
	var f wStateFile
	data, _, err := m.store.Get(context.Background(), url)
	if err != nil {
		return f, err
	}
	return f, json.Unmarshal(data, &f)
}

// putState writes a snapshot file, over one of the same name, as object
// storage does.
func (m *wModel) putState(url string, f wStateFile) error {
	ctx := context.Background()
	data := must(json.Marshal(f))
	if h, _, _ := m.readHead(); h.Generation > f.Generation {
		m.stats["snapshot files a fenced generation wrote"]++
	}
	if old, v, err := m.store.Get(ctx, url); err == nil {
		var was wStateFile
		if json.Unmarshal(old, &was) == nil && (was.Generation != f.Generation || was.Epoch != f.Epoch) {
			m.stats["snapshot files written over"]++
			m.tracef("generation %d writes %s over generation %d's epoch %d", f.Generation, url, was.Generation, was.Epoch)
		}
		_, err = m.store.Swap(ctx, url, data, v)
		return err
	}
	_, err := m.store.Create(ctx, url, data)
	return err
}

func sameHead(a, b checkpoint.Head) bool {
	same := func(x, y *checkpoint.RecordRef) bool { return x == nil && y == nil || x != nil && y != nil && *x == *y }
	return a.Generation == b.Generation && a.Owner == b.Owner && a.First == b.First && same(a.Record, b.Record) && same(a.Previous, b.Previous)
}

func stagedOf(cs [][]byte) []destination.Staged {
	out := make([]destination.Staged, len(cs))
	for i, c := range cs {
		out[i] = c
	}
	return out
}

// recoverDest is a committer's Recover: it raises the destination's fence to
// gen, and applies rec's epoch if it isn't.
func (m *wModel) recoverDest(gen uint64, rec *wRecord) error {
	wh := m.wh
	wh.mu.Lock()
	if wh.fence > gen {
		wh.mu.Unlock()
		return fmt.Errorf("%w: fence is %d, recovering as %d", destination.ErrFenced, wh.fence, gen)
	}
	wh.fence = gen
	wh.mu.Unlock()
	if rec == nil {
		return nil
	}
	st, err := mergeStaged(stagedOf(rec.Committables))
	if err != nil {
		return err
	}
	missing, needs, have := wh.missing(st)
	switch {
	case len(missing) == 0:
		return nil
	case have == needs:
		m.stats["epochs redone"]++
		return m.applyEpoch(gen, st, missing)
	}
	return fmt.Errorf("%v isn't applied, and %d of its %d staged parts are gone", rec, needs-have, needs)
}

// applyEpoch applies streams of an epoch's staging as committer gen, once it
// has checked nothing in it came from another generation's pods.
func (m *wModel) applyEpoch(gen uint64, st warehouseStaged, streams []stagedStream) error {
	m.wh.mu.Lock()
	for _, s := range streams {
		for _, l := range s.Lanes {
			for _, r := range m.wh.staging[stagingKey{st.Gen, st.N, s.Name, l}] {
				for _, c := range contribsOf(s.Name, r) {
					if c.Gen != st.Gen && !c.Restored {
						m.problem("generation", "epoch %d-g%d applies %s's row %s, which generation %d read", st.N, st.Gen, s.Name, c.ID, c.Gen)
					}
				}
			}
		}
	}
	m.wh.mu.Unlock()
	err := m.wh.applyTx(gen, st, streams, map[string]uint64{})
	if errors.Is(err, errStagingMissing) {
		m.problem("record", "epoch %d-g%d: %v", st.N, st.Gen, err)
	}
	return err
}

// prune deletes what no later attempt of generation gen, restored from from,
// can need, as checkpoint.Session.Prune does: records more than keep back
// along the chain, and older generations' records of epochs after from;
// and snapshot files of older generations no record it keeps names.
func (m *wModel) prune(gen uint64, from *wRecord) error {
	ctx := context.Background()
	kept, named := map[string]bool{}, map[string]bool{}
	floor := uint64(0)
	for r, i := from, 0; r != nil && i < m.cfg.keep; i++ {
		kept[wRecordKey(r.ref())] = true
		for _, s := range r.Snapshots {
			named[s.URL] = true
		}
		floor = r.Epoch
		next, err := m.readRecord(r.Previous)
		if errors.Is(err, checkpoint.ErrNotFound) {
			break
		}
		if err != nil {
			return err
		}
		r = next
	}
	records, err := m.store.List(ctx, wPipeline+"_checkpoints/")
	if err != nil {
		return err
	}
	var doomed []string
	for _, k := range records {
		ref, ok := parseWRecordKey(k)
		switch {
		case !ok || kept[k]:
		case ref.Epoch > from.epoch():
			if ref.Generation < gen {
				doomed = append(doomed, k)
			}
		case ref.Epoch < floor:
			doomed = append(doomed, k)
		}
	}
	files, err := m.store.List(ctx, wPipeline+"_state/")
	if err != nil {
		return err
	}
	for _, k := range files {
		g, n, ok := stateGenEpoch(k)
		switch {
		case !ok:
		case m.cfg.broken == "prune-by-epoch" && g <= gen && n < floor:
			doomed = append(doomed, k)
		case g < gen && !named[k]:
			doomed = append(doomed, k)
		}
	}
	for _, k := range doomed {
		if err := m.store.Delete(ctx, k); err != nil {
			return err
		}
	}
	m.stats["pruned objects"] += len(doomed)
	return nil
}

// ---- The coordinator: the attempt's runner.

type coPhase uint8

const (
	coClaim coPhase = iota
	coLoad
	coRecover
	coPrune
	coLaunch
	coRun
	coCreate
	coSwap
	coApply
	coAck
	coPruneRun
)

type wCoord struct {
	m      *wModel
	a      *wAttempt
	runner string
	phase  coPhase
	dead   bool
	// It stopped, with err if it failed.
	over                  bool
	err                   error
	pausedAt, pausedUntil int
	// A claim's outcome is unknown.
	claiming bool
	gen      uint64
	head     checkpoint.Head
	ver      checkpoint.Version
	restored *wRecord
	lay      wLayout
	launched int
	joined   map[int]bool
	beats    map[int]int
	inbox    wInbox
	// The epoch whose barrier was requested, or 0; the next to request.
	epoch, next uint64
	shares      map[int]*wShare
	rec         *wRecord
	// With scope stream: how many streams of rec are applied.
	applied    int
	ackPrev    *wRecord
	sincePrune int
}

func (c *wCoord) name() string    { return fmt.Sprintf("a%d.co", c.a.n) }
func (c *wCoord) owner() string   { return fmt.Sprintf("a%d", c.a.n) }
func (c *wCoord) live() bool      { return !c.dead && !c.over }
func (c *wCoord) succeeded() bool { return c.over && c.err == nil && !c.dead }
func (c *wCoord) paused() bool    { return c.m.tick < c.pausedUntil }
func (c *wCoord) pause(until int) {
	c.pausedAt, c.pausedUntil = c.m.tick, until
	c.m.stats["coordinators paused"]++
}
func (c *wCoord) allJoined() bool             { return c.joined != nil && len(c.joined) == c.lay.workers }
func (c *wCoord) failf(f string, args ...any) { c.fail(fmt.Errorf(f, args...)) }

func (c *wCoord) kill() {
	c.dead = true
	c.m.tracef("%s killed", c.name())
	c.m.killed(c.gen, "coordinators")
}

// fail ends the attempt: it tells its workers to stop, which may not reach
// them.
func (c *wCoord) fail(err error) {
	if !c.live() {
		return
	}
	c.over, c.err = true, err
	c.m.tracef("%s failed: %v", c.name(), err)
	for _, w := range c.a.workers {
		w.send(wMsg{kind: "abort", attempt: c.a.n, gen: c.gen})
	}
}

func (c *wCoord) succeed() {
	c.over = true
	c.m.tracef("%s succeeded", c.name())
	for _, w := range c.a.workers {
		w.send(wMsg{kind: "end", attempt: c.a.n, gen: c.gen})
	}
}

// resume ends a pause: the coordinator takes its workers' beats from now.
func (c *wCoord) resume() {
	if c.live() && c.pausedUntil == c.m.tick {
		for i := range c.beats {
			c.beats[i] = c.m.tick
		}
		c.launched = max(c.launched, c.m.tick)
	}
}

// watch fails the attempt once a worker hasn't joined or beaten in time.
func (c *wCoord) watch() {
	if !c.live() || c.paused() || c.joined == nil {
		return
	}
	for i := range c.lay.workers {
		switch {
		case !c.joined[i] && c.m.tick-c.launched > wJoinTimeout:
			c.failf("worker %d didn't join", i)
			return
		case c.joined[i] && c.m.tick-c.beats[i] > wTimeout:
			c.failf("lost worker %d", i)
			return
		}
	}
}

func (c *wCoord) actions(acts *[]wAction) {
	if !c.live() || c.paused() {
		return
	}
	add := func(op string, k actKind, do func(decision)) {
		*acts = append(*acts, wAction{who: c.name(), op: op, kind: k, pause: c.pause, do: do})
	}
	switch c.phase {
	case coClaim:
		add("claim", aCall, c.claim)
	case coLoad:
		add("load", aCall, c.load)
	case coRecover:
		add("recover", aCall, c.recover)
	case coPrune, coPruneRun:
		add("prune", aCall, c.prune)
	case coLaunch:
		add("launch", aLocal, c.launch)
	case coRun:
		if c.epoch == 0 && c.allJoined() {
			add("barrier", aCall, c.requestBarrier)
		}
	case coCreate:
		add("record.create", aCall, c.create)
	case coSwap:
		add("record.swap", aCall, c.swap)
	case coApply:
		add("apply", aCall, c.apply)
	case coAck:
		add("ack", aCall, c.ack)
	}
	for _, from := range c.inbox.senders() {
		add("recv "+from.name(), aLocal, func(d decision) { c.recv(d, from) })
	}
}

func (c *wCoord) claim(d decision) {
	ok, err := c.m.call(c.kill, d, c.doClaim)
	if !ok {
		return
	}
	if err != nil {
		c.claiming = c.claiming || errors.Is(err, errTimeout)
		return
	}
	c.m.tracef("%s claimed generation %d", c.name(), c.gen)
	c.phase = coLoad
}

// doClaim takes the pipeline under HEAD's generation plus one, as
// checkpoint.Log.Claim does.
func (c *wCoord) doClaim() error {
	ctx := context.Background()
	for range 16 {
		cur, v, err := c.m.readHead()
		fresh := errors.Is(err, checkpoint.ErrNotFound)
		if err != nil && !fresh {
			return err
		}
		if c.claiming && cur.Owner == c.owner() {
			c.head, c.ver, c.gen = cur, v, cur.Generation
			return nil
		}
		next := checkpoint.Head{Generation: cur.Generation + 1, Owner: c.owner(), Record: cur.Record, Previous: cur.Previous, First: cur.First}
		data := must(json.Marshal(next))
		var nv checkpoint.Version
		if fresh {
			nv, err = c.m.store.Create(ctx, wHeadKey, data)
		} else {
			nv, err = c.m.store.Swap(ctx, wHeadKey, data, v)
		}
		if errors.Is(err, checkpoint.ErrPrecondition) {
			continue
		}
		if err != nil {
			return err
		}
		c.head, c.ver, c.gen = next, nv, next.Generation
		return nil
	}
	return errors.New("sim: claim gave up")
}

func (c *wCoord) load(d decision) {
	var rec *wRecord
	ok, err := c.m.call(c.kill, d, func() (err error) { rec, err = c.m.readRecord(c.head.Record); return err })
	if !ok || err != nil {
		return
	}
	c.restored = rec
	c.m.tracef("%s restores from %v", c.name(), rec)
	c.phase = coRecover
}

func (c *wCoord) recover(d decision) {
	ok, err := c.m.call(c.kill, d, func() error { return c.m.recoverDest(c.gen, c.restored) })
	switch {
	case !ok:
	case errors.Is(err, destination.ErrFenced):
		c.fail(err)
	case err == nil:
		c.phase = coPrune
	case !errors.Is(err, errTransient) && !errors.Is(err, errTimeout):
		c.m.tracef("%s: recover: %v", c.name(), err)
	}
}

// prune is best effort: a failure leaves the rest to a later one.
func (c *wCoord) prune(d decision) {
	from := c.restored
	if c.phase == coPruneRun {
		from = c.rec
	}
	if ok, _ := c.m.call(c.kill, d, func() error { return c.m.prune(c.gen, from) }); !ok {
		return
	}
	if c.phase == coPrune {
		c.phase = coLaunch
		return
	}
	c.afterCommit()
}

// launch places the attempt's workers: how many, their lanes and key-group
// ranges, and which lanes read which splits, are the attempt's own.
func (c *wCoord) launch(d decision) {
	if d == crashBefore {
		c.kill()
		return
	}
	if c.restored.final() {
		c.succeed()
		return
	}
	m := c.m
	c.lay = randomLayout(m.rng, len(m.cfg.splits))
	m.lanesOf[c.gen] = len(c.lay.lanes)
	if c.restored != nil && m.lanesOf[c.restored.Generation] != len(c.lay.lanes) {
		m.stats["restores onto other lanes"]++
	}
	m.stats[fmt.Sprintf("workers:%d", c.lay.workers)]++
	c.launched, c.joined, c.beats = m.tick, map[int]bool{}, map[int]int{}
	c.next = c.restored.epoch() + 1
	for i := range c.lay.workers {
		w := &wWorker{m: m, a: c.a, idx: i, runner: c.runner, gen: c.gen, started: m.tick}
		w.addr = m.place(w)
		c.a.workers = append(c.a.workers, w)
	}
	m.tracef("%s launched %v", c.name(), c.lay)
	c.phase = coRun
}

func (c *wCoord) requestBarrier(d decision) {
	e := c.next
	ok, err := c.m.call(c.kill, d, func() error {
		for _, w := range c.a.workers {
			w.send(wMsg{kind: "barrier", attempt: c.a.n, gen: c.gen, epoch: e})
		}
		return nil
	})
	if !ok || err != nil {
		return
	}
	c.m.tracef("%s requested barrier %d", c.name(), e)
	c.epoch, c.shares = e, map[int]*wShare{}
}

// recv takes a message: the runner routed it here by its attempt, and the
// coordinator refuses one of another attempt or generation.
func (c *wCoord) recv(d decision, from *wWorker) {
	if d != proceed {
		c.kill()
		return
	}
	msg := c.inbox.pop(from)
	if (msg.attempt != c.a.n || msg.gen != c.gen) && c.m.cfg.broken != "runners-any-attempt" {
		c.m.stats["stale messages refused"]++
		from.send(wMsg{kind: "refused", reason: fmt.Sprintf("%s is generation %d", c.name(), c.gen)})
		return
	}
	switch msg.kind {
	case "join":
		if c.joined == nil || msg.worker >= c.lay.workers {
			return
		}
		c.joined[msg.worker], c.beats[msg.worker] = true, c.m.tick
		peers := make([]string, len(c.a.workers))
		for i, w := range c.a.workers {
			peers[i] = w.addr
		}
		from.send(wMsg{kind: "assign", attempt: c.a.n, gen: c.gen, lay: c.lay, peers: peers, restored: c.restored})
	case "share":
		c.takeShare(msg.share)
	case "failed":
		c.failf("worker %d failed: %s", msg.worker, msg.reason)
	}
}

func (c *wCoord) takeShare(s *wShare) {
	switch {
	case c.epoch == 0 || s.Epoch != c.epoch:
		c.m.stats["shares of other epochs ignored"]++
		return
	case c.shares[s.Worker] != nil:
		c.m.stats["duplicate shares ignored"]++
		return
	}
	c.shares[s.Worker] = s
	if len(c.shares) < c.lay.workers {
		return
	}
	rec := &wRecord{Epoch: c.epoch, Generation: c.gen, Previous: c.head.Record, KeyGroups: checkpoint.KeyGroups}
	type lc struct {
		lane int
		c    []byte
	}
	var cs []lc
	for _, w := range slices.Sorted(maps.Keys(c.shares)) {
		s := c.shares[w]
		rec.sharedBy = append(rec.sharedBy, s.Gen)
		rec.Splits = append(rec.Splits, s.Splits...)
		rec.Snapshots = append(rec.Snapshots, s.Snapshots...)
		for _, l := range slices.Sorted(maps.Keys(s.Committables)) {
			cs = append(cs, lc{l, s.Committables[l]})
		}
	}
	slices.SortFunc(rec.Splits, func(a, b wSplit) int { return a.ID - b.ID })
	slices.SortFunc(rec.Snapshots, func(a, b checkpoint.Snapshot) int { return a.KeyGroups.First - b.KeyGroups.First })
	slices.SortFunc(cs, func(a, b lc) int { return a.lane - b.lane })
	for _, x := range cs {
		rec.Committables = append(rec.Committables, x.c)
	}
	c.rec, c.phase = rec, coCreate
}

func (c *wCoord) create(d decision) {
	ctx := context.Background()
	key, data := wRecordKey(c.rec.ref()), must(json.Marshal(c.rec))
	var clash error
	ok, err := c.m.call(c.kill, d, func() error {
		_, err := c.m.store.Create(ctx, key, data)
		if err == nil {
			return nil
		}
		// It may exist from an earlier call that timed out.
		got, _, gerr := c.m.store.Get(ctx, key)
		switch {
		case gerr != nil:
			return err
		case !bytes.Equal(got, data):
			clash = fmt.Errorf("%v exists with other content", c.rec)
			return clash
		}
		return nil
	})
	switch {
	case !ok:
	case clash != nil:
		c.m.problem("record", "%s: %v", c.name(), clash)
		c.fail(clash)
	case err == nil:
		c.phase = coSwap
	}
}

// swap moves HEAD to the record, as checkpoint.Session.Commit does. HEAD
// fences an older generation.
func (c *wCoord) swap(d decision) {
	ctx := context.Background()
	ref := c.rec.ref()
	next := checkpoint.Head{Generation: c.gen, Owner: c.owner(), Record: &ref, Previous: c.rec.Previous, First: c.rec.Previous == nil}
	var fenced error
	ok, err := c.m.call(c.kill, d, func() error {
		nv, err := c.m.store.Swap(ctx, wHeadKey, must(json.Marshal(next)), c.ver)
		if err == nil {
			c.head, c.ver = next, nv
			return nil
		}
		cur, v, rerr := c.m.readHead()
		switch {
		case rerr != nil:
			return rerr
		case sameHead(cur, next):
			c.head, c.ver = next, v
			return nil
		case cur.Generation != c.gen || cur.Owner != c.owner():
			fenced = fmt.Errorf("%w: HEAD is generation %d, this is %d", checkpoint.ErrFenced, cur.Generation, c.gen)
			return fenced
		}
		return err
	})
	switch {
	case !ok:
	case fenced != nil:
		c.m.stats["coordinators fenced by HEAD"]++
		c.fail(fenced)
	case err == nil:
		c.m.tracef("%s moved HEAD to %v: %v", c.name(), c.rec, c.rec.Splits)
		c.m.onRecord(c.rec)
		c.applied, c.phase = 0, coApply
	}
}

func (c *wCoord) apply(d decision) {
	st, err := mergeStaged(stagedOf(c.rec.Committables))
	if err != nil {
		c.fail(err)
		return
	}
	streams := st.Streams
	if c.m.cfg.scope == destination.ScopeStream && len(streams) > 0 {
		streams = streams[c.applied : c.applied+1]
	}
	ok, err := c.m.call(c.kill, d, func() error { return c.m.applyEpoch(c.gen, st, streams) })
	switch {
	case !ok:
		return
	case errors.Is(err, destination.ErrFenced):
		c.m.stats["coordinators fenced by the destination"]++
		c.fail(err)
		return
	case errors.Is(err, errStagingMissing):
		c.fail(err)
		return
	case err != nil:
		return
	}
	if c.m.cfg.scope == destination.ScopeStream && len(st.Streams) > 0 {
		if c.applied++; c.applied < len(st.Streams) {
			return
		}
	}
	c.m.committed[c.gen] = c.rec.Epoch
	c.m.tracef("%s committed epoch %d", c.name(), c.rec.Epoch)
	c.phase = coAck
}

// ack has the workers acknowledge upstream what the record before this one
// read, trailing by one as the runner does: their lanes run the source's
// connectors. It's best effort.
func (c *wCoord) ack(d decision) {
	prev := c.ackPrev
	ok, _ := c.m.call(c.kill, d, func() error {
		if prev != nil {
			for _, w := range c.a.workers {
				w.send(wMsg{kind: "ack", attempt: c.a.n, gen: c.gen, epoch: prev.Epoch, splits: prev.Splits})
			}
		}
		return nil
	})
	if !ok {
		return
	}
	c.ackPrev = c.rec
	if c.sincePrune++; c.sincePrune >= c.m.cfg.keep {
		c.sincePrune, c.phase = 0, coPruneRun
		return
	}
	c.afterCommit()
}

func (c *wCoord) afterCommit() {
	c.m.stats["epochs committed"]++
	if c.rec.final() {
		c.succeed()
		return
	}
	c.next, c.epoch, c.shares, c.phase = c.rec.Epoch+1, 0, nil, coRun
}

// ---- The attempt protocol's messages.

type wMsg struct {
	kind    string
	attempt int
	gen     uint64
	worker  int
	epoch   uint64
	share   *wShare
	reason  string
	// An acknowledgement's: the committed record's splits.
	splits []wSplit
	// An assignment's.
	lay      wLayout
	peers    []string
	restored *wRecord
}

// wShare is a worker's share of an epoch: its splits' positions and
// watermarks, its lanes' committables and snapshots.
type wShare struct {
	Gen          uint64
	Worker       int
	Epoch        uint64
	Splits       []wSplit
	Committables map[int][]byte
	Snapshots    []checkpoint.Snapshot
}

// wInbox keeps each sender's messages in order, and the senders apart.
type wInbox struct {
	from []*wWorker
	msgs map[*wWorker][]wMsg
}

func (b *wInbox) push(w *wWorker, msg wMsg) {
	if b.msgs == nil {
		b.msgs = map[*wWorker][]wMsg{}
	}
	if len(b.msgs[w]) == 0 {
		b.from = append(b.from, w)
	}
	b.msgs[w] = append(b.msgs[w], msg)
}

func (b *wInbox) senders() []*wWorker { return b.from }

func (b *wInbox) pop(w *wWorker) wMsg {
	msg := b.msgs[w][0]
	if b.msgs[w] = b.msgs[w][1:]; len(b.msgs[w]) == 0 {
		b.from = slices.DeleteFunc(b.from, func(x *wWorker) bool { return x == w })
	}
	return msg
}

// ---- A seed.

func runWorkers(seed uint64, broken string) seedResult {
	m := newWorkersModel(seed, broken)
	res := seedResult{config: "workers " + m.cfg.String(), stats: m.stats}
	err := m.run()
	res.trace = strings.Join(lastN(m.trace, 400), "\n")
	res.steps = m.steps
	m.stats["attempts"] += len(m.attempts)
	switch {
	case err != nil:
		res.failure = err.Error()
	default:
		m.check()
	}
	if len(m.problems) > 0 {
		ps := m.problems
		if len(ps) > 8 {
			ps = append(slices.Clone(ps[:8]), fmt.Sprintf("… and %d more", len(ps)-8))
		}
		res.failure = strings.Join(append([]string{res.failure}, ps...), "; ")
		res.failure = strings.TrimPrefix(res.failure, "; ")
	}
	res.problems = m.problems
	return res
}
