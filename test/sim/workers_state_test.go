package sim

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/openetl-io/openetl/internal/checkpoint"
)

// The keyed operator's state, as runs on object storage that the checkpoint
// record names.
//
// The keyed lane drops a row whose dedupe ID it has seen, folds the rest
// into their windows, and closes a window when its timer fires. Its state
// is of three kinds: a value per window, its accumulator; a set of dedupe
// IDs, each expiring at its row's event time plus the horizon; and timers,
// each window's end. The source repeats some rows, as a queue redelivers
// one: a repeat is a row of its own in filtered, and has no effect on agg.
//
// A lane keeps its changes since its last run as its pending set. At a
// barrier it writes them as a run: one object, a section per kind. The
// record names sections, by object, kind and key-group range, with the
// newest epoch each holds, and never objects alone. A run counts only once
// a record names it: one written in an epoch whose record never lands is an
// orphan. A restore applies the values sections the record names for its
// lane's key groups, oldest first, a deletion removing a window, and takes
// the set's IDs and the timers the record's horizon hasn't passed: the
// least of its splits' watermarks, which is the keyed operator's at the
// barrier. The lane names those sections on, for its own key groups, so a
// rescale splits and joins ranges of sections and copies nothing. A
// restored keyed operator goes on from the horizon: what's at or before it
// is gone, so a timer set there would never fire.
//
// What the engine builds, on unless a seed's flag or variant turns it off:
//   - drop: a set's or timers' section the horizon has passed whole is no
//     longer named, nor are the pending set's entries it has passed;
//   - vdrop: nor is a values section each entry of which a newer named one
//     overrides, or deletes what no older named one holds. A section whose
//     content the lane hasn't read counts as holding everything; a lazy
//     lane reads each such section it names at its first barrier that
//     writes a run. A run all of whose sections are dead isn't written.
//
// Each seed switches on, or not, what the later steps add:
//   - merge: a lane that names more than wMergeBound values sections merges
//     the newest of them, or all, into one, and one that names two or more
//     set's sections merges the newest of those, leaving out the IDs the
//     horizon passed; the merge counts only when the lane's next record
//     names its output in their place, and drops deletions only when it
//     took the oldest;
//   - pending: while a lane's pending set is within its share of the job's
//     threshold, the record carries it and no run is written; a restore
//     takes the record's pending set, of its own key groups, as its lane's;
//     a pending set leaves out a window's deletion that no section the lane
//     names may hold, and keeps one of a window an older section holds;
//   - lazy: a restore reads timers only, and a lane reads a key group's
//     values and set when a row or a due timer first needs them.
//
// Retention deletes an object once no record it keeps names any of its
// sections, and only of an older generation, or of its own and of an epoch
// no later than its newest record's: one of its own of a later epoch may be
// the run or the merge a lane is about to name. It lists every object of
// state, as a run stays named across any number of generations.
//
// Its invariants, with the workers kind's: restore, that the state each
// record names is the fold of the rows before its positions, its set and
// timers too, that every object it names is there, and that no lane reads
// one that's gone; and retention, that nothing the records HEAD keeps name
// is ever deleted.

var wStateFlag = flag.String("wstate", "", "workers kind: state's features to force on, or off with a leading -, as drop,-merge; step1 turns off drop, vdrop, merge and pending")

const (
	// The random stream state's features and the repeated rows come from.
	wStateStream = 36
	// Values sections a lane names before it merges.
	wMergeBound = 3
	kValues     = "values"
	kSet        = "set"
	kTimers     = "timers"
)

var (
	wKinds      = []string{kValues, kSet, kTimers}
	wFeatures   = []string{"dedupe", "drop", "vdrop", "merge", "pending", "lazy"}
	wLaterSteps = []string{"drop", "vdrop", "merge", "pending"}
)

// randomState draws state's features, which the flag and the variant can
// fix, and repeats some rows.
func randomState(c *wCfg, rng *rand.Rand) {
	on := map[string]bool{}
	for _, f := range wFeatures {
		on[f] = rng.IntN(2) == 0
	}
	on["drop"], on["vdrop"] = true, true
	if rng.IntN(3) != 0 {
		on["dedupe"] = true
	}
	c.horizon, c.threshold = 1+rng.Int64N(2*c.size), 4+rng.IntN(40)
	for _, spec := range []string{*wStateFlag, variantShapes[c.broken].state} {
		for f := range strings.SplitSeq(spec, ",") {
			switch {
			case f == "":
			case f == "step1":
				for _, x := range wLaterSteps {
					on[x] = false
				}
			case strings.HasPrefix(f, "-"):
				on[f[1:]] = false
			default:
				on[f] = true
			}
		}
	}
	c.dedupe = on["dedupe"] && c.exchange
	c.drop, c.vdrop, c.merge, c.pending, c.lazy = on["drop"], on["vdrop"], on["merge"], on["pending"], on["lazy"]
	if c.dedupe {
		addRepeats(c, rng)
	}
}

// addRepeats puts after some rows a repeat of an earlier one of its split
// whose dedupe ID the split's watermark, read or idle, can't have expired.
// The watermark may have passed the repeat's window: it's dropped before
// it could go to late.
func addRepeats(c *wCfg, rng *rand.Rand) {
	for s, evs := range c.splits {
		var out []wEvent
		var avail []int
		var at []bool
		maxT := int64(wNoMark)
		for i, e := range evs {
			out, avail, at = append(out, e), append(avail, c.avail[s][i]), append(at, c.resumeAt[s][i])
			maxT = max(maxT, e.T)
			if rng.IntN(4) != 0 {
				continue
			}
			var cands []int
			for j, x := range out {
				if x.Of == "" && x.T+c.horizon > max(maxT, int64(c.avail[s][i])*c.rate)-c.delay {
					cands = append(cands, j)
				}
			}
			r := out[cands[rng.IntN(len(cands))]]
			r.ID, r.Of = fmt.Sprintf("s%d.r%d", s, len(out)), r.ID
			out, avail, at = append(out, r), append(avail, c.avail[s][i]), append(at, rng.IntN(100) < 60)
		}
		c.splits[s], c.avail[s], c.resumeAt[s] = out, append(avail, c.avail[s][len(evs)]), append(at, true)
	}
}

func (c wCfg) stateString() string {
	var on []string
	for _, f := range []struct {
		name string
		on   bool
	}{{"dedupe", c.dedupe}, {"drop", c.drop}, {"vdrop", c.vdrop}, {"merge", c.merge}, {"pending", c.pending}, {"lazy", c.lazy}} {
		if f.on {
			on = append(on, f.name)
		}
	}
	s := "state=" + cmp.Or(strings.Join(on, ","), "none")
	if c.dedupe {
		s += fmt.Sprintf(" horizon=%d", c.horizon)
	}
	if c.pending {
		s += fmt.Sprintf(" threshold=%d", c.threshold)
	}
	return s
}

// ---- Runs, sections and what records name.

// wStEntry is an entry of a section, or of a pending set: a window's
// accumulator, or its deletion; a dedupe ID, and when it expires; or a
// window's timer, and when it fires.
type wStEntry struct {
	Kind  string     `json:"kind"`
	G     int        `json:"g"`
	Key   string     `json:"key,omitempty"`
	W     int64      `json:"w,omitempty"`
	ID    string     `json:"id,omitempty"`
	At    int64      `json:"at,omitempty"`
	Del   bool       `json:"del,omitempty"`
	Sum   int64      `json:"sum,omitempty"`
	Count int64      `json:"count,omitempty"`
	Rows  []wContrib `json:"rows,omitempty"`
}

// slot is what the entry is of: a later entry of the same slot overrides it.
func (e wStEntry) slot() string {
	if e.Kind == kSet {
		return fmt.Sprintf("%s/%03d/%s", e.Kind, e.G, e.ID)
	}
	return fmt.Sprintf("%s/%03d/%s/%d", e.Kind, e.G, e.Key, e.W)
}

func sortedEntries(m map[string]wStEntry) []wStEntry {
	out := make([]wStEntry, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		out = append(out, m[k])
	}
	return out
}

// wRun is a run: a lane's changes at a barrier, or a merge's output, a
// section per kind.
type wRun struct {
	Generation uint64                   `json:"generation"`
	Epoch      uint64                   `json:"epoch"`
	Lane       int                      `json:"lane"`
	KG         checkpoint.KeyGroupRange `json:"kg"`
	Merge      bool                     `json:"merge,omitempty"`
	Sections   map[string][]wStEntry    `json:"sections"`
}

// wSection is a section as a record names it: the object, its kind, the key
// groups of it that count, the newest epoch whose changes it holds, and
// for a set's or timers' section, its earliest and latest times.
type wSection struct {
	URL   string                   `json:"url"`
	Kind  string                   `json:"kind"`
	KG    checkpoint.KeyGroupRange `json:"kg"`
	Upto  uint64                   `json:"upto"`
	First int64                    `json:"first,omitempty"`
	Last  int64                    `json:"last,omitempty"`
}

func (s wSection) same(o wSection) bool { return s.URL == o.URL && s.Kind == o.Kind && s.KG == o.KG }

// cmpSection orders sections oldest first. Two sections of one epoch never
// hold the same key group.
func cmpSection(a, b wSection) int {
	return cmp.Or(cmp.Compare(a.Upto, b.Upto), strings.Compare(a.URL, b.URL), strings.Compare(a.Kind, b.Kind), cmp.Compare(a.KG.First, b.KG.First))
}

func sectionsOf(url string, r wRun, kg checkpoint.KeyGroupRange, upto uint64) []wSection {
	var out []wSection
	for _, k := range wKinds {
		es := r.Sections[k]
		if len(es) == 0 {
			continue
		}
		s := wSection{URL: url, Kind: k, KG: kg, Upto: upto}
		if k != kValues {
			s.First, s.Last = es[0].At, es[0].At
			for _, e := range es {
				s.First, s.Last = min(s.First, e.At), max(s.Last, e.At)
			}
		}
		out = append(out, s)
	}
	return out
}

func intersect(a, b checkpoint.KeyGroupRange) (checkpoint.KeyGroupRange, bool) {
	r := checkpoint.KeyGroupRange{First: max(a.First, b.First), Last: min(a.Last, b.Last)}
	return r, r.First <= r.Last
}

// wNamed is what a lane's share of an epoch names.
type wNamed struct {
	Sections []wSection
	Pending  []wStEntry
	// The keyed operator's watermark at the barrier.
	Mark int64
}

// wMerged is a merge's output, nil if every entry it read was a deletion,
// and the sections it replaces once a record names it.
type wMerged struct {
	in  []wSection
	out *wSection
}

// wManifest is state-before-record's: what a lane's key groups hold, as a
// store of its own would commit it.
type wManifest struct {
	Generation uint64                   `json:"generation"`
	Epoch      uint64                   `json:"epoch"`
	KG         checkpoint.KeyGroupRange `json:"kg"`
	Sections   []wSection               `json:"sections"`
	Pending    []wStEntry               `json:"pending"`
}

func (r *wRecord) sections() []wSection {
	if r == nil {
		return nil
	}
	return r.Sections
}

func (r *wRecord) pending() []wStEntry {
	if r == nil {
		return nil
	}
	return r.Pending
}

// horizon is the least watermark of the record's splits: the keyed
// operator's at its barrier, which every timer and dedupe ID at or below it
// has passed.
func (r *wRecord) horizon() int64 {
	if r == nil || len(r.Splits) == 0 {
		return wNoMark
	}
	h := int64(wEndMark)
	for _, s := range r.Splits {
		h = min(h, s.Watermark)
	}
	return h
}

// stateURL names a run by generation, epoch, operator and the lane's key
// groups, as quarantined and held files are named by generation and epoch.
// Two broken variants drop parts of the name.
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

// stateGenEpoch is the generation and epoch a run's name says wrote it.
func stateGenEpoch(key string) (gen, epoch uint64, ok bool) {
	rest, ok := strings.CutPrefix(key, wPipeline+"_state/g")
	if !ok {
		return 0, 0, false
	}
	parts := strings.Split(rest, "/")
	if len(parts) < 2 {
		return 0, 0, false
	}
	var err1, err2 error
	_, err1 = fmt.Sscan(parts[0], &gen)
	_, err2 = fmt.Sscan(parts[1], &epoch)
	return gen, epoch, err1 == nil && err2 == nil
}

func (m *wModel) readRun(url string) (wRun, error) {
	var r wRun
	data, _, err := m.store.Get(context.Background(), url)
	if err != nil {
		return r, fmt.Errorf("%s: %w", url, err)
	}
	return r, json.Unmarshal(data, &r)
}

// putRun writes a run, over one of the same name, as object storage does.
func (m *wModel) putRun(url string, r wRun) error {
	ctx := context.Background()
	data := must(json.Marshal(r))
	if h, _, _ := m.readHead(); h.Generation > r.Generation {
		m.stats["runs a fenced generation wrote"]++
	}
	if old, v, err := m.store.Get(ctx, url); err == nil {
		var was wRun
		if json.Unmarshal(old, &was) == nil && (was.Generation != r.Generation || was.Epoch != r.Epoch) {
			m.stats["runs written over"]++
			m.tracef("generation %d writes %s over generation %d's epoch %d", r.Generation, url, was.Generation, was.Epoch)
		}
		_, err = m.store.Swap(ctx, url, data, v)
		return err
	}
	_, err := m.store.Create(ctx, url, data)
	return err
}

func (m *wModel) manifestURL(kg checkpoint.KeyGroupRange) string {
	return fmt.Sprintf("%s_state/manifest/kg-%03d-%03d", wPipeline, kg.First, kg.Last)
}

func (m *wModel) putManifest(f wManifest) error {
	ctx := context.Background()
	url, data := m.manifestURL(f.KG), must(json.Marshal(f))
	if _, v, err := m.store.Get(ctx, url); err == nil {
		_, err = m.store.Swap(ctx, url, data, v)
		return err
	}
	_, err := m.store.Create(ctx, url, data)
	return err
}

// keptNames is the objects the records HEAD keeps name: its own and those
// before it, as many as retention keeps.
func (m *wModel) keptNames() map[string]bool {
	named := map[string]bool{}
	h, _, err := m.readHead()
	if err != nil {
		return named
	}
	ref := h.Record
	for i := 0; ref != nil && i < m.cfg.keep; i++ {
		rec, err := m.readRecord(ref)
		if err != nil {
			break
		}
		for _, s := range rec.Sections {
			named[s.URL] = true
		}
		ref = rec.Previous
	}
	return named
}

// deleteState deletes state's objects, as by generation gen.
func (m *wModel) deleteState(gen uint64, keys []string) error {
	kept := m.keptNames()
	for _, k := range keys {
		if kept[k] {
			m.problem("retention", "generation %d deletes %s, which a record HEAD keeps names", gen, k)
		}
		if err := m.store.Delete(context.Background(), k); err != nil {
			return err
		}
	}
	return nil
}

// ---- State in memory.

// wState is a lane's state, or what a record's sections hold: windows'
// accumulators, dedupe IDs and when they expire, and timers, by key group.
type wState struct {
	vals   map[int]map[wKey]*wAcc
	set    map[int]map[string]int64
	timers map[int]map[wKey]int64
}

func newWState() wState {
	return wState{vals: map[int]map[wKey]*wAcc{}, set: map[int]map[string]int64{}, timers: map[int]map[wKey]int64{}}
}

// apply applies an entry read from storage, leaving out what horizon h has
// passed.
func (s wState) apply(e wStEntry, h int64) {
	k := wKey{e.Key, e.W}
	switch e.Kind {
	case kValues:
		if e.Del {
			delete(s.vals[e.G], k)
			return
		}
		a := &wAcc{Sum: e.Sum, Count: e.Count}
		for _, c := range e.Rows {
			c.Restored = true
			a.Rows = append(a.Rows, c)
		}
		if s.vals[e.G] == nil {
			s.vals[e.G] = map[wKey]*wAcc{}
		}
		s.vals[e.G][k] = a
	case kSet:
		if e.At > h {
			if s.set[e.G] == nil {
				s.set[e.G] = map[string]int64{}
			}
			s.set[e.G][e.ID] = e.At
		}
	case kTimers:
		if e.At > h {
			if s.timers[e.G] == nil {
				s.timers[e.G] = map[wKey]int64{}
			}
			s.timers[e.G][k] = e.At
		}
	}
}

// viewState is the state of the key groups want takes, of kinds, that
// sections and then a pending set hold, read through read.
func viewState(sections []wSection, pending []wStEntry, h int64, want func(int) bool, kinds []string, read func(string) (wRun, error)) (wState, error) {
	st := newWState()
	ss := slices.Clone(sections)
	slices.SortFunc(ss, cmpSection)
	runs := map[string]wRun{}
	for _, s := range ss {
		if !slices.Contains(kinds, s.Kind) || !anyWanted(s.KG, want) {
			continue
		}
		r, ok := runs[s.URL]
		if !ok {
			var err error
			if r, err = read(s.URL); err != nil {
				return st, err
			}
			runs[s.URL] = r
		}
		for _, e := range r.Sections[s.Kind] {
			if inRange(s.KG, e.G) && want(e.G) {
				st.apply(e, h)
			}
		}
	}
	for _, e := range pending {
		if slices.Contains(kinds, e.Kind) && want(e.G) {
			st.apply(e, h)
		}
	}
	return st, nil
}

func anyWanted(kg checkpoint.KeyGroupRange, want func(int) bool) bool {
	for g := kg.First; g <= kg.Last; g++ {
		if want(g) {
			return true
		}
	}
	return false
}

// stateAt reads the state a record names.
func (m *wModel) stateAt(rec *wRecord) (wState, error) {
	st, err := viewState(rec.sections(), rec.pending(), rec.horizon(), func(int) bool { return true }, wKinds, m.readRun)
	if err != nil {
		return st, fmt.Errorf("%v names %w", rec, err)
	}
	return st, nil
}

// ---- A lane's state.

func (l *wLane) loadedAll() bool     { return l.loaded == nil }
func (l *wLane) isLoaded(g int) bool { return l.loaded == nil || l.loaded[g] }
func (l *wLane) change(e wStEntry)   { l.pend[e.slot()], l.chg[e.slot()] = e, e }

// share is the lane's share of the job's threshold for pending sets.
func (l *wLane) share() int { return l.w.m.cfg.threshold / len(l.w.lay.lanes) }

// wants says which key groups the lane restores and reads: its own.
func (l *wLane) wants() func(int) bool {
	if l.w.m.cfg.broken == "restore-whole-sections" {
		return func(int) bool { return true }
	}
	kg := l.kg
	return func(g int) bool { return inRange(kg, g) }
}

// restoreNames is what the lane restores from: the sections rec names for
// its key groups, and its pending set.
func (l *wLane) restoreNames(rec *wRecord, read func(string) (wRun, error)) ([]wSection, []wStEntry, error) {
	m := l.w.m
	switch m.cfg.broken {
	case "state-before-record":
		return l.fromManifests()
	case "restore-by-listing":
		return l.fromListing(rec, read)
	}
	var out []wSection
	for _, s := range rec.sections() {
		if m.cfg.broken == "restore-whole-sections" {
			if _, ok := intersect(s.KG, l.kg); ok {
				out = append(out, s)
			}
			continue
		}
		if kg, ok := intersect(s.KG, l.kg); ok {
			s.KG = kg
			out = append(out, s)
		}
	}
	var pend []wStEntry
	for _, e := range rec.pending() {
		if inRange(l.kg, e.G) || m.cfg.broken == "pending-whole" {
			pend = append(pend, e)
		}
	}
	return out, pend, nil
}

// fromListing is restore-by-listing's: every run in the store, whatever
// names it, and the record's pending set.
func (l *wLane) fromListing(rec *wRecord, read func(string) (wRun, error)) ([]wSection, []wStEntry, error) {
	m := l.w.m
	keys, err := m.store.List(context.Background(), wPipeline+"_state/")
	if err != nil {
		return nil, nil, err
	}
	var out []wSection
	for _, k := range keys {
		if _, _, ok := stateGenEpoch(k); !ok {
			continue
		}
		r, err := read(k)
		if err != nil {
			return nil, nil, err
		}
		if kg, ok := intersect(r.KG, l.kg); ok {
			out = append(out, sectionsOf(k, r, kg, r.Epoch)...)
		}
	}
	var pend []wStEntry
	for _, e := range rec.pending() {
		if inRange(l.kg, e.G) {
			pend = append(pend, e)
		}
	}
	return out, pend, nil
}

// fromManifests is state-before-record's: for each key group, the newest
// manifest that holds it.
func (l *wLane) fromManifests() ([]wSection, []wStEntry, error) {
	m := l.w.m
	ctx := context.Background()
	keys, err := m.store.List(ctx, wPipeline+"_state/manifest/")
	if err != nil {
		return nil, nil, err
	}
	var fs []wManifest
	for _, k := range keys {
		data, _, err := m.store.Get(ctx, k)
		if err != nil {
			return nil, nil, err
		}
		var f wManifest
		if err := json.Unmarshal(data, &f); err != nil {
			return nil, nil, err
		}
		fs = append(fs, f)
	}
	var out []wSection
	var pend []wStEntry
	for g := l.kg.First; g <= l.kg.Last; g++ {
		best := -1
		for i, f := range fs {
			if inRange(f.KG, g) && (best < 0 || cmp.Or(cmp.Compare(f.Epoch, fs[best].Epoch), cmp.Compare(f.Generation, fs[best].Generation)) > 0) {
				best = i
			}
		}
		if best < 0 {
			continue
		}
		one := checkpoint.KeyGroupRange{First: g, Last: g}
		for _, s := range fs[best].Sections {
			if inRange(s.KG, g) {
				s.KG = one
				out = append(out, s)
			}
		}
		for _, e := range fs[best].Pending {
			if e.G == g {
				pend = append(pend, e)
			}
		}
	}
	return out, pend, nil
}

// restoreState reads what the lane restores from rec: every kind, or with
// lazy, timers only.
func (l *wLane) restoreState(d decision) bool {
	w, m := l.w, l.w.m
	rec := w.restored
	var (
		st       wState
		sections []wSection
		pend     []wStEntry
		gone     string
	)
	known := map[string]wRun{}
	read := func(u string) (wRun, error) {
		r, err := m.readRun(u)
		if errors.Is(err, checkpoint.ErrNotFound) {
			gone = u
		}
		if err == nil {
			known[u] = r
		}
		return r, err
	}
	kinds := wKinds
	if m.cfg.lazy {
		kinds = []string{kTimers}
	}
	ok, err := m.call(w.kill, d, func() (err error) {
		if sections, pend, err = l.restoreNames(rec, read); err != nil {
			return err
		}
		h := rec.horizon()
		if m.cfg.broken == "restore-keeps-passed" {
			h = wNoMark
		}
		st, err = viewState(sections, pend, h, l.wants(), kinds, read)
		return err
	})
	switch {
	case !ok:
		return false
	case gone != "":
		if h, _, _ := m.readHead(); h.Generation == w.gen {
			m.problem("restore", "%s: %v names %s, which is gone", l.name(), rec, gone)
		}
		w.fail("restore: " + gone + " is gone")
		return false
	case err != nil:
		return false
	}
	l.st, l.handles, l.resHandles, l.restoreH = st, sections, slices.Clone(sections), rec.horizon()
	l.pend, l.chg, l.known, l.due, l.named, l.merged = map[string]wStEntry{}, map[string]wStEntry{}, map[string]wRun{}, map[int]bool{}, map[uint64]wNamed{}, nil
	l.loaded = nil
	if m.cfg.lazy {
		l.loaded = map[int]bool{}
	} else {
		l.known = known
	}
	if m.cfg.broken != "pending-to-memory-only" {
		for _, e := range pend {
			l.pend[e.slot()] = e
		}
	}
	view := st
	if m.cfg.lazy {
		// What the lane will read, key group by key group.
		if view, err = viewState(sections, pend, rec.horizon(), l.wants(), wKinds, m.readRun); err != nil {
			return true
		}
	}
	for _, p := range m.cut(rec, l.kg, view) {
		m.problem("restore", "%s restored %v: %s", l.name(), rec, p)
	}
	return true
}

// needs is the key groups the lane must read before it goes on: those of
// timers due, and of the rows at the head of what it would take.
func (l *wLane) needs() []int {
	if l.loadedAll() {
		return nil
	}
	gs := map[int]bool{}
	for g := range l.due {
		gs[g] = true
	}
	for _, c := range l.ins {
		if !l.ready(c) || c.q[0].kind != 'r' {
			continue
		}
		for _, r := range c.q[0].rows {
			if g := keyGroupOf(r.Key); !l.loaded[g] {
				gs[g] = true
			}
		}
	}
	return slices.Sorted(maps.Keys(gs))
}

// load reads, with lazy, the values and the set of the key groups the lane
// needs, from the sections it names now and its pending set.
func (l *wLane) load(d decision) {
	w, m := l.w, l.w.m
	groups := l.needs()
	sections := l.handles
	if m.cfg.broken == "lazy-stale-handles" {
		sections = l.resHandles
	}
	want := func(g int) bool { _, ok := slices.BinarySearch(groups, g); return ok }
	var st wState
	var gone string
	read := func(u string) (wRun, error) {
		r, err := m.readRun(u)
		if errors.Is(err, checkpoint.ErrNotFound) {
			gone = u
		}
		return r, err
	}
	ok, err := m.call(w.kill, d, func() (err error) {
		st, err = viewState(sections, sortedEntries(l.pend), max(l.restoreH, l.opWM), want, []string{kValues, kSet}, read)
		return err
	})
	switch {
	case !ok:
		return
	case gone != "":
		if h, _, _ := m.readHead(); h.Generation == w.gen {
			m.problem("restore", "%s reads key groups %v from %s, which is gone", l.name(), groups, gone)
		}
		w.fail("load: " + gone + " is gone")
		return
	case err != nil:
		return
	}
	for _, g := range groups {
		if v := st.vals[g]; v != nil {
			l.st.vals[g] = v
		}
		if s := st.set[g]; s != nil {
			l.st.set[g] = s
		}
		l.loaded[g] = true
	}
	m.stats["key groups read lazily"] += len(groups)
	l.fire()
}

// fold takes a row the exchange brought: a repeat of a row it has seen is
// dropped, and the rest go to their windows, or to late once theirs closed.
func (l *wLane) fold(r wRow) {
	m := l.w.m
	size, uid, g := m.cfg.size, r.uid(), keyGroupOf(r.Key)
	k := wKey{r.Key, windowStart(r.T, size)}
	late := k.W+size <= l.opWM
	if m.cfg.dedupe && (!late || m.cfg.broken != "late-before-dedupe") {
		if _, seen := l.st.set[g][uid]; seen {
			m.stats["repeats dropped"]++
			if late {
				m.stats["repeats dropped after their window closed"]++
			}
			return
		}
		if l.st.set[g] == nil {
			l.st.set[g] = map[string]int64{}
		}
		at := r.T + m.cfg.horizon
		l.st.set[g][uid] = at
		l.change(wStEntry{Kind: kSet, G: g, ID: uid, At: at})
	}
	if late {
		m.stats["late rows"]++
		l.emit(l.kcur, "late", row{ID: uid, Seq: r.T, Key: r.Key, Value: string(must(json.Marshal(wContrib{ID: uid, Gen: r.Gen})))})
		return
	}
	if l.st.vals[g] == nil {
		l.st.vals[g] = map[wKey]*wAcc{}
	}
	a := l.st.vals[g][k]
	if a == nil {
		a = &wAcc{}
		l.st.vals[g][k] = a
		if l.st.timers[g] == nil {
			l.st.timers[g] = map[wKey]int64{}
		}
		// A timer at or before the horizon is gone at once: it never fires.
		if k.W+size > l.restoreH {
			l.st.timers[g][k] = k.W + size
			l.change(wStEntry{Kind: kTimers, G: g, Key: k.Key, W: k.W, At: k.W + size})
		} else {
			m.stats["timers gone at once"]++
		}
	}
	a.Sum, a.Count = a.Sum+r.value(), a.Count+1
	a.Rows = append(a.Rows, wContrib{ID: uid, Gen: r.Gen})
	l.change(wStEntry{Kind: kValues, G: g, Key: k.Key, W: k.W, Sum: a.Sum, Count: a.Count, Rows: slices.Clone(a.Rows)})
}

// fire fires the timers the operator's watermark has passed, closing their
// windows, and expires the dedupe IDs it has passed. A timer of a key group
// not read yet waits for it.
func (l *wLane) fire() {
	m := l.w.m
	op := l.opWM
	l.due = map[int]bool{}
	for _, g := range slices.Sorted(maps.Keys(l.st.timers)) {
		for _, k := range slices.SortedFunc(maps.Keys(l.st.timers[g]), cmpWKey) {
			if l.st.timers[g][k] > op {
				continue
			}
			if !l.isLoaded(g) {
				l.due[g] = true
				continue
			}
			delete(l.st.timers[g], k)
			a := l.st.vals[g][k]
			if a == nil {
				m.problem("restore", "%s: the timer of window %d of %s fires, and the window isn't in state", l.name(), k.W, k.Key)
				continue
			}
			l.emit(l.kcur, "agg", row{ID: fmt.Sprintf("%d/%s", k.W, k.Key), Seq: k.W, Key: k.Key, Value: string(must(json.Marshal(wEntry{Key: k.Key, W: k.W, Sum: a.Sum, Count: a.Count, Rows: a.Rows})))})
			delete(l.st.vals[g], k)
			l.change(wStEntry{Kind: kValues, G: g, Key: k.Key, W: k.W, Del: true})
		}
		if len(l.st.timers[g]) == 0 {
			delete(l.st.timers, g)
		}
	}
	for _, ids := range l.st.set {
		maps.DeleteFunc(ids, func(_ string, at int64) bool { return at <= op })
	}
}

// nextNames is what the lane's record of epoch e names, and what it writes
// for it: a run of its pending set, unless the record carries that; a
// merge's output in place of its inputs; and less what's dead.
func (l *wLane) nextNames(e uint64) (run wRun, url string, sections []wSection, pend map[string]wStEntry) {
	w, m := l.w, l.w.m
	h := l.opWM
	pend = maps.Clone(l.pend)
	if m.cfg.drop {
		maps.DeleteFunc(pend, func(_ string, x wStEntry) bool { return x.Kind != kValues && x.At <= h })
	}
	sections = slices.Clone(l.handles)
	if m.cfg.pending {
		maps.DeleteFunc(pend, func(_ string, x wStEntry) bool {
			if x.Kind != kValues || !x.Del {
				return false
			}
			if m.cfg.broken == "pending-drops-deletes" {
				return true
			}
			gone := !l.mayHold(sections, x)
			if gone {
				m.stats["pending deletions of what no section holds left out"]++
			}
			return gone
		})
	}
	if len(pend) > 0 && (!m.cfg.pending || len(pend) > l.share()) {
		run = wRun{Generation: w.gen, Epoch: e, Lane: l.idx, KG: l.kg, Sections: map[string][]wStEntry{}}
		for _, x := range sortedEntries(pend) {
			run.Sections[x.Kind] = append(run.Sections[x.Kind], x)
		}
		url = m.stateURL(w.gen, e, l.kg)
		sections = append(sections, sectionsOf(url, run, l.kg, e)...)
		pend = map[string]wStEntry{}
	}
	if mg := l.merged; mg != nil {
		sections = slices.DeleteFunc(sections, func(s wSection) bool {
			return slices.ContainsFunc(mg.in, s.same)
		})
		if mg.out != nil {
			sections = append(sections, *mg.out)
		}
	}
	if m.cfg.drop {
		sections = slices.DeleteFunc(sections, func(s wSection) bool {
			at := s.Last
			if m.cfg.broken == "drop-by-first" {
				at = s.First
			}
			return s.Kind != kValues && at <= h
		})
	}
	if m.cfg.vdrop {
		known := maps.Clone(l.known)
		if url != "" {
			known[url] = run
			l.learn(sections, known)
		}
		sections = l.liveValues(sections, pend, known)
	}
	if url != "" && !slices.ContainsFunc(sections, func(s wSection) bool { return s.URL == url }) {
		m.stats["runs left unwritten, all dead"]++
		run, url = wRun{}, ""
	}
	slices.SortFunc(sections, cmpSection)
	return run, url, sections, pend
}

// mayHold says whether a values section of sections may hold x's window:
// one the lane hasn't read may hold any.
func (l *wLane) mayHold(sections []wSection, x wStEntry) bool {
	return slices.ContainsFunc(sections, func(s wSection) bool {
		if s.Kind != kValues || !inRange(s.KG, x.G) {
			return false
		}
		r, ok := l.known[s.URL]
		return !ok || slices.ContainsFunc(r.Sections[kValues], func(y wStEntry) bool { return y.slot() == x.slot() })
	})
}

// learn reads each values section of sections the lane hasn't read, as a
// lane does at its first barrier that writes a run, into known and the
// lane's own.
func (l *wLane) learn(sections []wSection, known map[string]wRun) {
	w, m := l.w, l.w.m
	for _, s := range sections {
		if _, ok := known[s.URL]; ok || s.Kind != kValues {
			continue
		}
		r, err := m.readRun(s.URL)
		if errors.Is(err, checkpoint.ErrNotFound) {
			if h, _, _ := m.readHead(); h.Generation == w.gen {
				m.problem("restore", "%s reads %s, which it names, and it's gone", l.name(), s.URL)
			}
			continue
		}
		if err != nil {
			continue
		}
		known[s.URL], l.known[s.URL] = r, r
		m.stats["values sections read to tell if dead"]++
	}
}

// liveValues leaves out each values section that's dead: each entry of it,
// within the groups it's named for, is overridden by a newer section or the
// pending set, or deletes what no older section holds. A section whose
// content the lane hasn't read counts as holding everything.
func (l *wLane) liveValues(sections []wSection, pend map[string]wStEntry, known map[string]wRun) []wSection {
	m := l.w.m
	var vals []wSection
	for _, s := range sections {
		if s.Kind == kValues {
			vals = append(vals, s)
		}
	}
	slices.SortFunc(vals, cmpSection)
	holds := func(s wSection, x wStEntry) bool {
		if !inRange(s.KG, x.G) {
			return false
		}
		r, ok := known[s.URL]
		if !ok {
			return true
		}
		return slices.ContainsFunc(r.Sections[kValues], func(y wStEntry) bool { return y.slot() == x.slot() })
	}
	overridden := func(i int, x wStEntry) bool {
		if _, ok := pend[x.slot()]; ok {
			return true
		}
		for _, s := range vals[i+1:] {
			if r, ok := known[s.URL]; ok && inRange(s.KG, x.G) && slices.ContainsFunc(r.Sections[kValues], func(y wStEntry) bool { return y.slot() == x.slot() }) {
				return true
			}
		}
		return false
	}
	dead := map[int]bool{}
	for i, s := range vals {
		r, ok := known[s.URL]
		if !ok {
			continue
		}
		live := false
		for _, x := range r.Sections[kValues] {
			if !inRange(s.KG, x.G) || overridden(i, x) {
				continue
			}
			if x.Del {
				older := m.cfg.broken != "drop-tombstones" && slices.ContainsFunc(vals[:i], func(o wSection) bool {
					return !dead[slices.IndexFunc(vals, o.same)] && holds(o, x)
				})
				if !older {
					continue
				}
			}
			live = true
			break
		}
		if !live {
			dead[i] = true
			m.stats["values sections dropped"]++
		}
	}
	return slices.DeleteFunc(sections, func(s wSection) bool {
		i := slices.IndexFunc(vals, s.same)
		return i >= 0 && dead[i]
	})
}

// mergeable says the lane would merge: it names more values sections than
// the bound, or two set's sections or more, and no merge of its waits for
// its record.
func (l *wLane) mergeable() bool {
	return len(l.mergeKinds()) > 0
}

// mergeKinds are the kinds of state the lane would merge.
func (l *wLane) mergeKinds() []string {
	if !l.w.m.cfg.merge || l.merged != nil || !l.restored {
		return nil
	}
	n := map[string]int{}
	for _, s := range l.handles {
		n[s.Kind]++
	}
	var out []string
	if n[kValues] > wMergeBound {
		out = append(out, kValues)
	}
	if n[kSet] >= 2 && !strings.HasPrefix(l.w.m.cfg.broken, "merge-") {
		out = append(out, kSet)
	}
	return out
}

// merge merges the newest of the lane's sections of a kind, or all of
// them, into one, which counts once the lane's next record names it in
// their place. Only a merge of all of them drops deletions, and a set's
// merge leaves out the IDs the lane's horizon passed.
func (l *wLane) merge(d decision) {
	w, m := l.w, l.w.m
	kinds := l.mergeKinds()
	if len(kinds) == 0 {
		return
	}
	kind := kinds[m.rng.IntN(len(kinds))]
	var secs []wSection
	for _, s := range l.handles {
		if s.Kind == kind {
			secs = append(secs, s)
		}
	}
	slices.SortFunc(secs, cmpSection)
	n, k := len(secs), len(secs)
	if m.rng.IntN(2) == 0 {
		k = 2 + m.rng.IntN(n-1)
	}
	if m.cfg.broken == "merge-drops-tombstones" {
		k = 2
	}
	in, bottom := secs[n-k:], k == n
	h := l.opWM
	url := m.stateURL(w.gen, l.kcur, l.kg) + ".merge"
	out := wRun{Generation: w.gen, Epoch: l.kcur, Lane: l.idx, KG: l.kg, Merge: true, Sections: map[string][]wStEntry{}}
	var gone string
	ok, err := m.call(w.kill, d, func() error {
		latest := map[string]wStEntry{}
		for _, s := range in {
			r, err := m.readRun(s.URL)
			if errors.Is(err, checkpoint.ErrNotFound) {
				gone = s.URL
			}
			if err != nil {
				return err
			}
			for _, x := range r.Sections[kind] {
				if inRange(s.KG, x.G) && inRange(l.kg, x.G) {
					latest[x.slot()] = x
				}
			}
		}
		for _, x := range sortedEntries(latest) {
			switch {
			case kind == kSet && m.cfg.drop && x.At <= h:
			case !x.Del || !bottom && m.cfg.broken != "merge-drops-tombstones":
				out.Sections[kind] = append(out.Sections[kind], x)
			}
		}
		if len(out.Sections[kind]) > 0 {
			if err := m.putRun(url, out); err != nil {
				return err
			}
		}
		if m.cfg.broken == "merge-before-named" {
			var urls []string
			for _, s := range in {
				urls = append(urls, s.URL)
			}
			slices.Sort(urls)
			return m.deleteState(w.gen, slices.Compact(urls))
		}
		return nil
	})
	switch {
	case !ok:
		return
	case gone != "":
		if h, _, _ := m.readHead(); h.Generation == w.gen {
			m.problem("restore", "%s merges %s, which is gone", l.name(), gone)
		}
		w.fail("merge: " + gone + " is gone")
		return
	case err != nil:
		return
	}
	mg := &wMerged{in: in}
	if len(out.Sections[kind]) > 0 {
		l.known[url] = out
		sec := sectionsOf(url, out, l.kg, in[len(in)-1].Upto)[0]
		mg.out = &sec
	}
	l.merged = mg
	m.stats["merges"]++
	m.stats["merges of "+kind]++
	if bottom {
		m.stats["merges of every "+kind+" section"]++
	}
}

// snapshot is the lane's barrier: it writes its run, if it has one, and
// says what its share of the record names.
func (l *wLane) snapshot(d decision) {
	w, m := l.w, l.w.m
	e := l.kcur
	run, url, sections, pend := l.nextNames(e)
	named := wNamed{Sections: sections, Pending: sortedEntries(pend), Mark: l.opWM}
	if m.cfg.broken == "pending-epoch-only" {
		named.Pending = nil
		for _, x := range sortedEntries(pend) {
			if _, ok := l.chg[x.slot()]; ok {
				named.Pending = append(named.Pending, x)
			}
		}
	}
	write := func() error {
		if url != "" {
			if err := m.putRun(url, run); err != nil {
				return err
			}
		}
		if m.cfg.broken == "state-before-record" {
			return m.putManifest(wManifest{Generation: w.gen, Epoch: e, KG: l.kg, Sections: sections, Pending: named.Pending})
		}
		return nil
	}
	if url != "" && d == proceed && m.slowWrites > 0 && m.rng.IntN(4) == 0 {
		m.slowWrites--
		at := m.tick + 1 + m.rng.IntN(2*wTimeout)
		m.later = append(m.later, wLater{at, func() {
			m.tracef("%s's write of %s lands", l.name(), url)
			_ = write()
		}})
		m.stats["run writes landing late"]++
		m.tracef("%s: the write of %s times out, and lands at t%d", l.name(), url, at)
		return
	}
	ok, err := m.call(w.kill, d, func() error {
		if url != "" && m.cfg.broken == "snapshot-unwaited" {
			// The pod writes it in the background, a tick on, if it's still up.
			m.later = append(m.later, wLater{m.tick + 1, func() {
				if w.live() {
					_ = write()
				}
			}})
			return nil
		}
		return write()
	})
	if !ok || err != nil {
		return
	}
	if url != "" {
		l.known[url] = run
		m.stats["runs"]++
	} else if len(pend) > 0 {
		m.stats["pending sets in the record"]++
	}
	l.handles, l.pend, l.chg, l.merged, l.named[e] = sections, pend, map[string]wStEntry{}, nil, named
	m.snapped[w.gen] = max(m.snapped[w.gen], e)
	at := checkpoint.RecordRef{Epoch: e, Generation: w.gen}
	m.opMarks[at] = append(m.opMarks[at], wOpMark{l.kg, l.opWM})
	l.aligned, l.snapDue = map[int]bool{}, false
	l.keyedPast, l.kcur = e, e+1
}
