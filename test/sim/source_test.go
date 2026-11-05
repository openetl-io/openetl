package sim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
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
	// Exact: whether a resume point follows the page ending at offset i.
	resumeAt []bool
	// Superset: how far below the cursor floor a read starts.
	lookback int64
	// Exact: the next sync goes on from where this one ended, as CDC does,
	// instead of reading it all again.
	cdc bool

	mu      sync.Mutex
	trimmed int // exact: offsets below this were discarded after an ack
}

// newUpstream makes a stream's records. With twice, some are held twice, as
// a table without a primary key can.
func newUpstream(rng *rand.Rand, name string, replay source.Replay, twice bool) *upstream {
	u := &upstream{name: name, replay: replay, pageSize: 1 + rng.IntN(5)}
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

type exactState struct {
	Offset int `json:"offset"`
}

type cursorState struct {
	Cursor int64 `json:"cursor"`
}

func (u *upstream) start(state []byte) (int, error) {
	if u.replay == source.Exact {
		var st exactState
		if state != nil {
			if err := json.Unmarshal(state, &st); err != nil {
				return 0, err
			}
		}
		u.mu.Lock()
		defer u.mu.Unlock()
		if st.Offset < u.trimmed {
			return 0, fmt.Errorf("%w: %s resumes at offset %d, but offsets below %d were acknowledged and discarded", errDataLost, u.name, st.Offset, u.trimmed)
		}
		return st.Offset, nil
	}
	if state == nil {
		return 0, nil
	}
	var st cursorState
	if err := json.Unmarshal(state, &st); err != nil {
		return 0, err
	}
	floor := st.Cursor - u.lookback
	for i, r := range u.records {
		if r.Seq >= floor {
			return i, nil
		}
	}
	return len(u.records), nil
}

func (u *upstream) stateAfter(pos int) []byte {
	if u.replay == source.Exact {
		return must(json.Marshal(exactState{Offset: pos}))
	}
	return must(json.Marshal(cursorState{Cursor: u.records[pos-1].Seq}))
}

// next is where a sync after this one starts: from the beginning for exact
// streams, as a full refresh, so an attempt that reads a stream its sync
// completed reads below what was acknowledged, unless they're CDC; from the
// cursor for superset ones.
func (u *upstream) next() []byte {
	if u.replay == source.Exact && !u.cdc || len(u.records) == 0 {
		return nil
	}
	return u.stateAfter(len(u.records))
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

func (s *simSource) Read(ctx context.Context, req source.ReadRequest, emitter source.Emitter) error {
	out := gatedEmitter{ctx, s, emitter}
	type cursor struct {
		u   *upstream
		pos int
	}
	var open []*cursor
	for _, cs := range req.Streams {
		u := s.w.upstreams[cs.Name]
		pos, err := u.start(cs.State)
		if err != nil {
			s.w.markLost(err)
			return err
		}
		if pos >= len(u.records) {
			if s.continuous {
				continue
			}
			if err := out.Complete(u.name, u.next()); err != nil {
				return err
			}
			continue
		}
		open = append(open, &cursor{u, pos})
	}
	for i := 0; len(open) > 0; i++ {
		c := open[i%len(open)]
		if err := outcome(s.w.sched.point(ctx, s.attempt, "src.fetch"), nop); err != nil {
			return err
		}
		end := min(c.pos+c.u.pageSize, len(c.u.records))
		rows := make([]json.RawMessage, 0, end-c.pos)
		for _, r := range c.u.records[c.pos:end] {
			rows = append(rows, must(json.Marshal(r)))
		}
		if err := out.Rows(c.u.name, rows); err != nil {
			return err
		}
		c.pos = end
		last := c.pos == len(c.u.records)
		if last || c.u.replay == source.Superset || c.u.resumeAt[c.pos] {
			if err := out.ResumePoint(c.u.name, c.u.stateAfter(c.pos)); err != nil {
				return err
			}
		}
		if last {
			if !s.continuous {
				if err := out.Complete(c.u.name, c.u.next()); err != nil {
					return err
				}
			}
			open = append(open[:i%len(open)], open[i%len(open)+1:]...)
		}
	}
	if s.onDone != nil {
		s.onDone()
	}
	return nil
}

// Committed discards what exact streams acknowledged. Reading below it later
// is data loss.
func (s *simSource) Committed(ctx context.Context, states map[string][]byte) error {
	return outcome(s.w.sched.point(ctx, s.attempt, "src.ack"), func() error {
		for name, state := range states {
			u := s.w.upstreams[name]
			if u.replay != source.Exact || state == nil {
				continue
			}
			var st exactState
			if err := json.Unmarshal(state, &st); err != nil {
				return err
			}
			u.mu.Lock()
			u.trimmed = max(u.trimmed, st.Offset)
			u.mu.Unlock()
		}
		return nil
	})
}

// gatedEmitter makes every Emitter call a yield point.
type gatedEmitter struct {
	ctx context.Context
	s   *simSource
	out source.Emitter
}

func (g gatedEmitter) gate() error {
	return outcome(g.s.w.sched.point(g.ctx, g.s.attempt, "src.emit"), nop)
}

func (g gatedEmitter) Rows(stream string, rows []json.RawMessage) error {
	if err := g.gate(); err != nil {
		return err
	}
	return g.out.Rows(stream, rows)
}

func (g gatedEmitter) ResumePoint(stream string, state []byte) error {
	if err := g.gate(); err != nil {
		return err
	}
	return g.out.ResumePoint(stream, state)
}

func (g gatedEmitter) Complete(stream string, next []byte) error {
	if err := g.gate(); err != nil {
		return err
	}
	return g.out.Complete(stream, next)
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
