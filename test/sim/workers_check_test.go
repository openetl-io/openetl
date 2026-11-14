package sim

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/openetl-io/openetl/internal/checkpoint"
)

// The workers kind's invariants. Each problem is filed under its
// invariant's name.

// contribsOf is the input rows a destination row carries: an agg row the
// rows folded into it, and filtered or late rows themselves.
func contribsOf(stream string, r row) []wContrib {
	if stream == "agg" {
		var e wEntry
		_ = json.Unmarshal([]byte(r.Value), &e)
		return e.Rows
	}
	var c wContrib
	_ = json.Unmarshal([]byte(r.Value), &c)
	return []wContrib{c}
}

// observeMark checks a split's watermark never goes back within an attempt,
// nor below the record the attempt restored it from.
func (m *wModel) observeMark(l *wLane, c *wCursor) {
	k := [2]int{l.w.a.n, c.split}
	if last, ok := m.marks[k]; ok && c.wm < last {
		m.problem("watermark", "%s: split %d's watermark went back from %d to %d", l.name(), c.split, last, c.wm)
	}
	if c.wm < c.floor {
		m.problem("watermark", "%s: split %d's watermark is %d, below %d in the record it restored", l.name(), c.split, c.wm, c.floor)
	}
	m.marks[k] = c.wm
}

// onRecord checks a record HEAD has just moved to: it covers every split,
// each split's watermark is at least the one before's, and nothing in it
// is another generation's.
func (m *wModel) onRecord(rec *wRecord) {
	for _, g := range rec.sharedBy {
		if g != rec.Generation {
			m.problem("generation", "%v merges a share of generation %d", rec, g)
		}
	}
	if len(rec.Splits) != len(m.cfg.splits) {
		m.problem("record", "%v has %d splits of %d", rec, len(rec.Splits), len(m.cfg.splits))
	}
	prev, err := m.readRecord(rec.Previous)
	if err != nil {
		prev = nil
	}
	m.checkLink(rec, prev)
}

// checkLink checks rec against the record before it, prev, if it's kept:
// watermarks, and the snapshot files rec names, each written by rec's
// generation or named by prev, whose state it carries.
func (m *wModel) checkLink(rec, prev *wRecord) {
	if prev != nil {
		for _, s := range rec.Splits {
			if p := prev.split(s.ID); s.Watermark < p.Watermark {
				m.problem("watermark", "split %d's watermark went back from %d in %v to %d in %v", s.ID, p.Watermark, prev, s.Watermark, rec)
			}
		}
	}
	carried := map[string]bool{}
	for _, s := range prev.snapshots() {
		carried[s.URL] = true
	}
	for _, s := range rec.Snapshots {
		f, err := m.readState(s.URL)
		switch {
		case err != nil:
		case f.Generation == rec.Generation && f.Epoch <= rec.Epoch:
		case prev != nil && carried[s.URL]:
		case prev == nil && rec.Previous != nil:
			// The record before was pruned.
		default:
			m.problem("generation", "%v names %s, which generation %d wrote for epoch %d", rec, s.URL, f.Generation, f.Epoch)
		}
	}
}

// landed is the rows of a stream that epochs up to upTo applied, in the
// order they were.
func (m *wModel) landed(stream string, upTo uint64) []row {
	seen := map[string]int{}
	var out []row
	for _, r := range m.wh.tables[stream] {
		at := m.wh.landed[r.ID][seen[r.ID]]
		seen[r.ID]++
		if at[1] <= upTo {
			out = append(out, r)
		}
	}
	return out
}

// cut checks a lane's state at a record, for key groups kg: the windows it
// holds, and those epochs up to the record's emitted, are the fold of the
// rows before the record's positions that weren't late, each window in one
// place.
func (m *wModel) cut(rec *wRecord, kg checkpoint.KeyGroupRange, state map[int]map[wKey]*wAcc) []string {
	if !m.cfg.exchange {
		return nil
	}
	late := map[string]bool{}
	for _, r := range m.landed("late", rec.epoch()) {
		late[r.ID] = true
	}
	want := map[wKey]*wAcc{}
	for s, evs := range m.cfg.splits {
		for _, e := range evs[:rec.split(s).Pos] {
			if !e.kept() || late[e.ID] || !inRange(kg, keyGroupOf(e.Key)) {
				continue
			}
			k := wKey{e.Key, windowStart(e.T, m.cfg.size)}
			if want[k] == nil {
				want[k] = &wAcc{}
			}
			want[k].Sum, want[k].Count = want[k].Sum+e.value(), want[k].Count+1
			want[k].Rows = append(want[k].Rows, wContrib{ID: e.ID})
		}
	}
	var problems []string
	got := map[wKey]*wAcc{}
	where := map[wKey]string{}
	add := func(k wKey, a *wAcc, at string) {
		if was, ok := where[k]; ok {
			problems = append(problems, fmt.Sprintf("window %d of %s is %s and %s", k.W, k.Key, was, at))
			return
		}
		got[k], where[k] = a, at
	}
	for _, g := range slices.Sorted(maps.Keys(state)) {
		if inRange(kg, g) {
			for k, a := range state[g] {
				add(k, a, "in state")
			}
		}
	}
	for _, r := range m.landed("agg", rec.epoch()) {
		var e wEntry
		_ = json.Unmarshal([]byte(r.Value), &e)
		if inRange(kg, keyGroupOf(e.Key)) {
			add(wKey{e.Key, e.W}, &wAcc{Sum: e.Sum, Count: e.Count, Rows: e.Rows}, "emitted")
		}
	}
	ids := func(a *wAcc) []string {
		var out []string
		for _, c := range a.Rows {
			out = append(out, c.ID)
		}
		slices.Sort(out)
		return out
	}
	keys := slices.SortedFunc(maps.Keys(want), func(a, b wKey) int { return cmp.Or(cmp.Compare(a.W, b.W), cmp.Compare(a.Key, b.Key)) })
	for _, k := range keys {
		w, g := want[k], got[k]
		switch {
		case g == nil:
			problems = append(problems, fmt.Sprintf("window %d of %s, of rows %v, is neither in state nor emitted", k.W, k.Key, ids(w)))
		case g.Sum != w.Sum || g.Count != w.Count || !slices.Equal(ids(g), ids(w)):
			problems = append(problems, fmt.Sprintf("window %d of %s %s holds rows %v (sum %d), want %v (sum %d)", k.W, k.Key, where[k], ids(g), g.Sum, ids(w), w.Sum))
		}
	}
	for k := range got {
		if want[k] == nil {
			problems = append(problems, fmt.Sprintf("window %d of %s %s holds rows %v, past the record's positions", k.W, k.Key, where[k], ids(got[k])))
		}
	}
	slices.Sort(problems)
	return problems
}

// stateAt reads the state a record's snapshots hold.
func (m *wModel) stateAt(rec *wRecord) (map[int]map[wKey]*wAcc, error) {
	state := map[int]map[wKey]*wAcc{}
	for _, s := range rec.snapshots() {
		f, err := m.readState(s.URL)
		if err != nil {
			return nil, fmt.Errorf("%v names %s: %w", rec, s.URL, err)
		}
		for g, entries := range f.Groups {
			if !inRange(*s.KeyGroups, g) {
				continue
			}
			state[g] = map[wKey]*wAcc{}
			for _, e := range entries {
				state[g][wKey{e.Key, e.W}] = &wAcc{Sum: e.Sum, Count: e.Count, Rows: e.Rows}
			}
		}
	}
	return state, nil
}

// check checks a seed that ended: the destination's tables, and every
// record kept along the chain from HEAD.
func (m *wModel) check() {
	kept := map[string]bool{}
	want := map[wKey]*wAcc{}
	for _, evs := range m.cfg.splits {
		for _, e := range evs {
			if !e.kept() {
				continue
			}
			kept[e.ID] = true
			k := wKey{e.Key, windowStart(e.T, m.cfg.size)}
			if want[k] == nil {
				want[k] = &wAcc{}
			}
			want[k].Sum, want[k].Count = want[k].Sum+e.value(), want[k].Count+1
		}
	}
	once := func(what string, n map[string]int) {
		for _, id := range slices.Sorted(maps.Keys(kept)) {
			switch c := n[id]; {
			case c == 0:
				m.problem("exactly-once", "%s: LOST row %s", what, id)
			case c > 1:
				m.problem("exactly-once", "%s: row %s ×%d", what, id, c)
			}
		}
		for _, id := range slices.Sorted(maps.Keys(n)) {
			if !kept[id] {
				m.problem("exactly-once", "%s: row %s, which the filter drops", what, id)
			}
		}
	}
	filtered, effects := map[string]int{}, map[string]int{}
	for _, r := range m.wh.tables["filtered"] {
		filtered[r.ID]++
	}
	for _, s := range []string{"agg", "late"} {
		for _, r := range m.wh.tables[s] {
			for _, c := range contribsOf(s, r) {
				effects[c.ID]++
			}
		}
	}
	once("filtered", filtered)
	if !m.cfg.exchange {
		// Without the keyed operator, only filtered is written.
		want = nil
		for _, s := range []string{"agg", "late"} {
			if n := len(m.wh.tables[s]); n > 0 {
				m.problem("aggregate", "%s has %d rows, with no keyed operator", s, n)
			}
		}
	} else {
		once("agg and late", effects)
	}

	got := map[wKey][]wEntry{}
	for _, r := range m.wh.tables["agg"] {
		var e wEntry
		_ = json.Unmarshal([]byte(r.Value), &e)
		got[wKey{e.Key, e.W}] = append(got[wKey{e.Key, e.W}], e)
	}
	keys := slices.SortedFunc(maps.Keys(want), func(a, b wKey) int { return cmp.Or(cmp.Compare(a.W, b.W), cmp.Compare(a.Key, b.Key)) })
	for _, k := range keys {
		w, g := want[k], got[k]
		switch {
		case len(g) != 1:
			m.problem("aggregate", "window %d of %s has %d rows, want 1", k.W, k.Key, len(g))
		case g[0].Sum != w.Sum || g[0].Count != w.Count:
			m.problem("aggregate", "window %d of %s is sum %d count %d, want sum %d count %d", k.W, k.Key, g[0].Sum, g[0].Count, w.Sum, w.Count)
		}
	}
	for _, k := range slices.SortedFunc(maps.Keys(got), func(a, b wKey) int { return cmp.Or(cmp.Compare(a.W, b.W), cmp.Compare(a.Key, b.Key)) }) {
		if want[k] == nil {
			m.problem("aggregate", "window %d of %s, which no run without failures has", k.W, k.Key)
		}
	}

	h, _, err := m.readHead()
	if err != nil {
		m.problem("record", "read HEAD: %v", err)
		return
	}
	all := checkpoint.KeyGroupRange{First: 0, Last: checkpoint.KeyGroups - 1}
	var chain []*wRecord
	for ref := h.Record; ref != nil; {
		rec, err := m.readRecord(ref)
		if errors.Is(err, checkpoint.ErrNotFound) && len(chain) >= 2 {
			break // pruned
		}
		if err != nil {
			m.problem("record", "a restore would fail: %v", err)
			return
		}
		chain = append(chain, rec)
		ref = rec.Previous
	}
	if len(chain) == 0 || !chain[0].final() {
		m.problem("record", "HEAD's record isn't the read's end")
	}
	for i, rec := range chain {
		var prev *wRecord
		if i+1 < len(chain) {
			prev = chain[i+1]
		}
		m.checkLink(rec, prev)
		state, err := m.stateAt(rec)
		if err != nil {
			m.problem("restore", "%v", err)
			continue
		}
		for _, p := range m.cut(rec, all, state) {
			m.problem("restore", "%v: %s", rec, p)
		}
	}
	if len(chain) > 0 && len(chain[0].Snapshots) > 0 {
		m.problem("restore", "%v, after the read's end, still holds state", chain[0])
	}
}
