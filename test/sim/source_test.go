package sim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type record struct {
	Key   string `json:"k"`
	Seq   int64  `json:"s"`
	Value string `json:"v"`
	// Its value doesn't fit the column's type: 1 until a type override, 2
	// even after one. Its cell is held, with quarantine: cell.
	Bad int `json:"b,omitempty"`
	// Its value has the type the column changed to.
	Wide bool `json:"w,omitempty"`
}

// recordID is the fake engine's _openetl_id.
func recordID(stream, key string, seq int64) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%s\x00%d", stream, key, seq))
	return hex.EncodeToString(sum[:8])
}

type upstream struct {
	name     string
	replay   source.Replay
	records  []record
	pageSize int
	// Exact: whether a resume point may follow the record before index i.
	resumeAt []bool
	// Superset: how far below the cursor floor a read starts.
	lookback int64
	// Exact: the next sync goes on from where this one ended, as CDC does,
	// instead of reading it all again.
	cdc bool
	// The stream's parts, each read as a split of its own, p0, p1 and so
	// on; with one, the whole stream. Records go to parts by key, or in
	// turn. resplit: how likely a resume point of a split, of a stream not
	// split by key, splits its remainder in two.
	parts   int
	byKey   bool
	resplit int

	mu sync.Mutex
	// Exact: by split, positions below this were discarded after an ack.
	trimmed map[string]int
}

// newUpstream makes a stream's records. With twice, some are held twice, as
// a table without a primary key can.
func newUpstream(rng *rand.Rand, name string, replay source.Replay, twice bool) *upstream {
	u := &upstream{name: name, replay: replay, pageSize: 1 + rng.IntN(5), parts: 1, trimmed: map[string]int{}}
	n := 5 + rng.IntN(36)
	keys := 2 + rng.IntN(8)
	last := map[string]int64{}
	seq := int64(0)
	for i := range n {
		if replay == source.Exact || i == 0 {
			seq++ // offsets, or LSNs: strictly increasing
		} else {
			seq += int64(rng.IntN(2)) // cursor values, with ties
		}
		// A key's versions have strictly increasing seq; ties are between keys.
		key := ""
		for range 4 {
			k := fmt.Sprintf("k%d", rng.IntN(keys))
			if last[k] < seq {
				key = k
				break
			}
		}
		if key == "" {
			key = fmt.Sprintf("x%d", i)
		}
		last[key] = seq
		r := record{Key: key, Seq: seq, Value: fmt.Sprintf("%s@%d", key, seq)}
		u.records = append(u.records, r)
		if twice && rng.IntN(5) == 0 {
			u.records = append(u.records, r)
		}
	}
	n = len(u.records)
	u.resumeAt = make([]bool, n+1)
	for i := range u.resumeAt {
		u.resumeAt[i] = rng.IntN(100) < 60
	}
	u.lookback = int64(rng.IntN(3))
	return u
}

// splitState is a split's state: its part, its range of the part's records
// (hi 0: to the part's end), and where it is: the next record for exact
// streams, the last cursor read for superset ones.
type splitState struct {
	Part   int    `json:"part"`
	Lo     int    `json:"lo,omitempty"`
	Hi     int    `json:"hi,omitempty"`
	Pos    int    `json:"pos,omitempty"`
	Cursor *int64 `json:"cursor,omitempty"`
}

func (u *upstream) partOf(i int) int {
	switch {
	case u.parts <= 1:
		return 0
	case u.byKey:
		h := fnv.New32a()
		_, _ = h.Write([]byte(u.records[i].Key))
		return int(h.Sum32() % uint32(u.parts))
	}
	return i % u.parts
}

// part is the indexes of part k's records, in order.
func (u *upstream) part(k int) []int {
	var out []int
	for i := range u.records {
		if u.partOf(i) == k {
			out = append(out, i)
		}
	}
	return out
}

func (u *upstream) splitID(k int) string {
	if u.parts <= 1 {
		return source.WholeStream
	}
	return fmt.Sprintf("p%d", k)
}

func decodeSplit(state []byte) (splitState, error) {
	var st splitState
	if state == nil {
		return st, nil
	}
	return st, json.Unmarshal(state, &st)
}

// splits are the stream's splits for a read that starts where ended did:
// one per part, from the beginning for exact streams, unless they're CDC,
// which go on from where each part ended; from the cursor for superset ones.
func (u *upstream) splits(ended []source.Split) ([]source.Split, error) {
	out := make([]source.Split, u.parts)
	for k := range u.parts {
		st := splitState{Part: k}
		for _, e := range ended {
			prev, err := decodeSplit(e.State)
			if err != nil {
				return nil, err
			}
			switch {
			case e.State == nil || prev.Part != k:
			case u.replay == source.Exact && u.cdc:
				st.Lo, st.Pos = max(st.Lo, prev.Pos), max(st.Pos, prev.Pos)
			case u.replay == source.Superset && prev.Cursor != nil:
				if st.Cursor == nil || *prev.Cursor > *st.Cursor {
					st.Cursor = prev.Cursor
				}
			}
		}
		out[k] = source.Split{ID: u.splitID(k), State: must(json.Marshal(st))}
	}
	return out, nil
}

// start is where a split's read starts, and where its range ends.
func (u *upstream) start(id string, st splitState) (list []int, pos, hi int, err error) {
	list = u.part(st.Part)
	hi = len(list)
	if st.Hi > 0 {
		hi = st.Hi
	}
	if u.replay == source.Exact {
		u.mu.Lock()
		defer u.mu.Unlock()
		if st.Pos < u.trimmed[id] {
			return nil, 0, 0, fmt.Errorf("%w: %s's %q resumes at %d, but positions below %d were acknowledged and discarded", errDataLost, u.name, id, st.Pos, u.trimmed[id])
		}
		return list, max(st.Pos, st.Lo), hi, nil
	}
	pos = st.Lo
	if st.Cursor != nil {
		floor := *st.Cursor - u.lookback
		for pos < hi && u.records[list[pos]].Seq < floor {
			pos++
		}
	}
	return list, pos, hi, nil
}

func (u *upstream) stateAfter(st splitState, list []int, pos int) []byte {
	if u.replay == source.Exact {
		st.Pos = pos
	} else if pos > st.Lo {
		c := u.records[list[pos-1]].Seq
		st.Cursor = &c
	}
	return must(json.Marshal(st))
}

// next is where a sync after this one starts the split: from the beginning
// for exact streams, as a full refresh, so an attempt that reads a stream
// its sync completed reads below what was acknowledged, unless they're CDC;
// from the cursor for superset ones.
func (u *upstream) next(st splitState, list []int, hi int) []byte {
	if u.replay == source.Exact && !u.cdc || hi == 0 {
		return nil
	}
	return u.stateAfter(st, list, hi)
}

// resplits says whether a split resuming at pos splits its remainder.
func (u *upstream) resplits(id string, pos, hi int) bool {
	if u.byKey || u.resplit == 0 || hi-pos < 2 {
		return false
	}
	h := fnv.New32a()
	_, _ = fmt.Fprintf(h, "%s/%s/%d", u.name, id, pos)
	return int(h.Sum32()%100) < u.resplit
}

var errDataLost = fmt.Errorf("sim: data lost")

type simSource struct {
	w       *world
	attempt int
	// A continuous pipeline's: no stream completes, and onDone is called
	// once Read has emitted every record.
	continuous bool
	onDone     func()
}

func (s *simSource) Splits(_ context.Context, cs source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	return s.w.upstreams[cs.Name].splits(ended)
}

func (s *simSource) Read(ctx context.Context, req source.ReadRequest, emitter source.Emitter) error {
	type cursor struct {
		u       *upstream
		id      string
		st      splitState
		list    []int
		pos, hi int
		out     gatedEmitter
	}
	var open []*cursor
	for _, cs := range req.Streams {
		u := s.w.upstreams[cs.Name]
		splits := cs.Splits
		if splits == nil {
			splits = []source.Split{{ID: source.WholeStream, State: cs.State}}
		}
		for _, sp := range splits {
			st, err := decodeSplit(sp.State)
			if err != nil {
				return err
			}
			list, pos, hi, err := u.start(sp.ID, st)
			if err != nil {
				s.w.markLost(err)
				return err
			}
			c := &cursor{u: u, id: sp.ID, st: st, list: list, pos: pos, hi: hi, out: gatedEmitter{ctx, s, emitter.Split(cs.Name, sp.ID)}}
			if pos >= hi {
				if s.continuous {
					continue
				}
				if err := c.out.Complete(u.next(st, list, hi)); err != nil {
					return err
				}
				continue
			}
			open = append(open, c)
		}
	}
	for i := 0; len(open) > 0; i++ {
		c := open[i%len(open)]
		if err := outcome(s.w.sched.point(ctx, s.attempt, "src.fetch"), nop); err != nil {
			return err
		}
		end := min(c.pos+c.u.pageSize, c.hi)
		rows := make([]json.RawMessage, 0, end-c.pos)
		for _, r := range c.list[c.pos:end] {
			rows = append(rows, must(json.Marshal(c.u.records[r])))
		}
		if err := c.out.Rows(rows); err != nil {
			return err
		}
		c.pos = end
		last := c.pos == c.hi
		if last || c.u.replay == source.Superset || c.u.resumeAt[c.list[c.pos-1]+1] {
			var added []source.Split
			if !last && c.u.resplits(c.id, c.pos, c.hi) {
				// The remainder from mid goes to a new split, with the range's
				// end, and this one ends at mid.
				mid := c.pos + (c.hi-c.pos)/2
				rest := splitState{Part: c.st.Part, Lo: mid, Hi: c.st.Hi, Pos: mid}
				id := fmt.Sprintf("%s.%d", c.id, mid)
				added = []source.Split{{ID: id, State: must(json.Marshal(rest))}}
				open = append(open, &cursor{u: c.u, id: id, st: rest, list: c.list, pos: mid, hi: c.hi, out: gatedEmitter{ctx, s, emitter.Split(c.u.name, id)}})
				c.st.Hi, c.hi = mid, mid
			}
			if err := c.out.ResumePoint(c.u.stateAfter(c.st, c.list, c.pos), added...); err != nil {
				return err
			}
			last = c.pos == c.hi
		}
		if last {
			if !s.continuous {
				if err := c.out.Complete(c.u.next(c.st, c.list, c.hi)); err != nil {
					return err
				}
			}
			open = slices.DeleteFunc(open, func(o *cursor) bool { return o == c })
		}
	}
	if s.onDone != nil {
		s.onDone()
	}
	return nil
}

// Committed discards what exact streams acknowledged. Reading below it later
// is data loss.
func (s *simSource) Committed(ctx context.Context, states map[string][]byte, splits map[string][]source.Split) error {
	return outcome(s.w.sched.point(ctx, s.attempt, "src.ack"), func() error {
		acked := map[string][]source.Split{}
		for name, state := range states {
			acked[name] = []source.Split{{ID: source.WholeStream, State: state}}
		}
		maps.Copy(acked, splits)
		for name, ss := range acked {
			u := s.w.upstreams[name]
			if u.replay != source.Exact {
				continue
			}
			for _, sp := range ss {
				if sp.State == nil {
					continue
				}
				st, err := decodeSplit(sp.State)
				if err != nil {
					return err
				}
				u.mu.Lock()
				u.trimmed[sp.ID] = max(u.trimmed[sp.ID], st.Pos)
				u.mu.Unlock()
			}
		}
		return nil
	})
}

// gatedEmitter makes every call on a split a yield point.
type gatedEmitter struct {
	ctx context.Context
	s   *simSource
	out source.SplitEmitter
}

func (g gatedEmitter) gate() error {
	return outcome(g.s.w.sched.point(g.ctx, g.s.attempt, "src.emit"), nop)
}

func (g gatedEmitter) Rows(rows []json.RawMessage) error {
	if err := g.gate(); err != nil {
		return err
	}
	return g.out.Rows(rows)
}

func (g gatedEmitter) ResumePoint(state []byte, added ...source.Split) error {
	if err := g.gate(); err != nil {
		return err
	}
	return g.out.ResumePoint(state, added...)
}

func (g gatedEmitter) Complete(next []byte) error {
	if err := g.gate(); err != nil {
		return err
	}
	return g.out.Complete(next)
}

func nop() error { return nil }

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type simStore struct {
	w       *world
	attempt int
}

func (s *simStore) call(ctx context.Context, op string, f func() error) error {
	return outcome(s.w.sched.point(ctx, s.attempt, op), f)
}

func (s *simStore) Get(ctx context.Context, key string) (data []byte, v checkpoint.Version, err error) {
	if err := s.call(ctx, "store.get", func() error { data, v, err = s.w.store.Get(ctx, key); return err }); err != nil {
		return nil, "", err
	}
	return data, v, nil
}

func (s *simStore) Create(ctx context.Context, key string, data []byte) (v checkpoint.Version, err error) {
	if err := s.call(ctx, "store.create", func() error {
		if v, err = s.w.store.Create(ctx, key, data); err == nil && strings.HasSuffix(key, ".json") {
			var rec checkpoint.Record
			if json.Unmarshal(data, &rec) == nil {
				for _, st := range rec.Forced {
					s.w.forced[st] = true
				}
				if isRecordKey(key) {
					s.w.written = append(s.w.written, &rec)
				}
			}
		}
		return err
	}); err != nil {
		return "", err
	}
	return v, nil
}

func (s *simStore) Swap(ctx context.Context, key string, data []byte, prev checkpoint.Version) (v checkpoint.Version, err error) {
	if err := s.call(ctx, "store.swap", func() error { v, err = s.w.store.Swap(ctx, key, data, prev); return err }); err != nil {
		return "", err
	}
	return v, nil
}

func (s *simStore) Delete(ctx context.Context, key string) error {
	return s.call(ctx, "store.delete", func() error { return s.w.store.Delete(ctx, key) })
}

func (s *simStore) List(ctx context.Context, prefix string) (keys []string, err error) {
	if err := s.call(ctx, "store.list", func() error { keys, err = s.w.store.List(ctx, prefix); return err }); err != nil {
		return nil, err
	}
	return keys, nil
}

var rowSchema = arrow.NewSchema([]arrow.Field{
	{Name: "_openetl_id", Type: arrow.BinaryTypes.String},
	{Name: "_openetl_seq", Type: arrow.PrimitiveTypes.Int64},
	{Name: "key", Type: arrow.BinaryTypes.String},
	{Name: "value", Type: arrow.BinaryTypes.String},
}, nil)

func toArrow(stream string, rows []json.RawMessage) (arrow.RecordBatch, error) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, rowSchema)
	defer b.Release()
	for _, raw := range rows {
		var r record
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, err
		}
		b.Field(0).(*array.StringBuilder).Append(recordID(stream, r.Key, r.Seq))
		b.Field(1).(*array.Int64Builder).Append(r.Seq)
		b.Field(2).(*array.StringBuilder).Append(r.Key)
		b.Field(3).(*array.StringBuilder).Append(r.Value)
	}
	return b.NewRecordBatch(), nil
}

type row struct {
	ID    string
	Seq   int64
	Key   string
	Value string
	// With holds: the value is null; it has the type the column changed to;
	// the batch lists it as held; its field keeps the target's value.
	Null, Wide, Held, Last bool
	// In a target, the version whose value the cell holds back; in staging,
	// a patch.
	HeldBy string
	Patch  bool
}

func rowsOf(batch arrow.RecordBatch) []row {
	schema := batch.Schema()
	col := func(name string) arrow.Array {
		if i := schema.FieldIndices(name); len(i) > 0 {
			return batch.Column(i[0])
		}
		return nil
	}
	ids := col("_openetl_id").(*array.String)
	seqs := col("_openetl_seq").(*array.Int64)
	keys := col("key").(*array.String)
	vals := col("value").(interface {
		arrow.Array
		Value(int) string
	})
	field := schema.Field(schema.FieldIndices("value")[0])
	wide := field.Type.ID() == arrow.LARGE_STRING
	last := false
	if i := field.Metadata.FindKey(destination.HeldKey); i >= 0 {
		last = field.Metadata.Values()[i] == "last"
	}
	held, _ := col(destination.HeldColumn).(*array.List)
	out := make([]row, batch.NumRows())
	for i := range out {
		out[i] = row{ID: ids.Value(i), Seq: seqs.Value(i), Key: keys.Value(i), Value: vals.Value(i), Null: vals.IsNull(i), Wide: wide, Last: last}
		if held != nil && held.IsValid(i) {
			start, end := held.ValueOffsets(i)
			out[i].Held = end > start
		}
	}
	return out
}
