package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/url"
	"slices"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// Holds (ADR-0025). The holds kind is the warehouse taking holds, as
// Postgres does, with keyed streams that hold the cells whose values don't
// fit, or their whole value column after a type change that the
// destination doesn't make. holdFlow plays the engine's part. A retry may
// take a release of what HEAD holds, as a run restarted for one does, with
// the type overridden or the column altered, or sometimes not, and so does the
// next sync, which reads the records added since.

const heldValue = "HELD"

type holdCfg struct {
	cells bool
	held  runner.Held
	// The value column's type changes at this record; 0: never.
	wideAt int
	// The destination doesn't change the value column's type, as Postgres
	// doesn't; the user does.
	fixedType bool
}

func (h *holdCfg) streamHolds() *runner.StreamHolds {
	if h == nil {
		return nil
	}
	return &runner.StreamHolds{Cells: h.cells, Held: h.held}
}

func randomHolds(rng *rand.Rand, mode destination.WriteMode, records int) *holdCfg {
	h := &holdCfg{}
	switch rng.IntN(3) {
	case 0:
		h.held = runner.Held{Mode: runner.HeldValue, Value: `"` + heldValue + `"`}
	case 1:
		if mode == destination.Merge {
			h.held.Mode = runner.HeldLast
			if rng.IntN(2) == 0 {
				h.held.Value = `"` + heldValue + `"`
			}
		}
	}
	// A stream holds cells, or its column after a type change; a value
	// that doesn't fit the column held whole is another story.
	if rng.IntN(2) == 0 {
		h.cells = true
	} else {
		h.wideAt, h.fixedType = 1+rng.IntN(max(1, records-1)), rng.IntN(4) != 0
	}
	return h
}

// markHolds makes some records' values from the one at from on not fit, or
// changes the value column's type from wideAt on.
func markHolds(rng *rand.Rand, u *upstream, h *holdCfg, from int) {
	for i := from; i < len(u.records); i++ {
		switch {
		case h.cells && rng.IntN(100) < 15:
			u.records[i].Bad = 1 + rng.IntN(5)/4
		case h.wideAt > 0 && i >= h.wideAt:
			u.records[i].Wide = true
		}
	}
}

// extendUpstream adds a few records for the next sync: later versions of
// the stream's keys, and new keys.
func extendUpstream(rng *rand.Rand, u *upstream, h *holdCfg) {
	from := len(u.records)
	last := map[string]int64{}
	var seq int64
	for _, r := range u.records {
		last[r.Key], seq = r.Seq, max(seq, r.Seq)
	}
	keys := slices.Sorted(maps.Keys(last))
	for i := range 2 + rng.IntN(9) {
		if u.replay == source.Exact || i == 0 {
			seq++
		} else {
			seq += int64(rng.IntN(2))
		}
		key := fmt.Sprintf("y%d", i)
		for range 4 {
			if k := keys[rng.IntN(len(keys))]; last[k] < seq {
				key = k
				break
			}
		}
		last[key] = seq
		u.records = append(u.records, record{Key: key, Seq: seq, Value: fmt.Sprintf("%s@%d", key, seq)})
		u.resumeAt = append(u.resumeAt, rng.IntN(100) < 60)
	}
	if h != nil {
		markHolds(rng, u, h, from)
	}
}

// ---- The destination: heldTables are what the warehouse keeps for holds.

type heldTables struct {
	created map[string]bool
	// The value column has the type it changed to.
	wide map[string]bool
	// The value column's type doesn't change unless the user alters it.
	fixedType map[string]bool
	problems  []string
	patched   int
	refused   int
}

func newHeldTables(streams []streamCfg) *heldTables {
	t := &heldTables{created: map[string]bool{}, wide: map[string]bool{}, fixedType: map[string]bool{}}
	for _, s := range streams {
		if s.holds != nil && s.holds.fixedType {
			t.fixedType[s.name] = true
		}
	}
	return t
}

func splitPatches(rows []row) (data, patches []row) {
	for _, r := range rows {
		if r.Patch {
			patches = append(patches, r)
		} else {
			data = append(data, r)
		}
	}
	return data, patches
}

// checkType records a value applied into a column that doesn't have its
// type: the runner must apply an epoch's schemas before its rows.
func (wh *warehouse) checkType(stream string, r row) {
	if !r.Null && r.Wide && !wh.holds.wide[stream] {
		wh.holds.problems = append(wh.holds.problems, fmt.Sprintf("%s: %s@%d's value has the type the column changed to, and the table has the old one", stream, r.Key, r.Seq))
	}
}

// held is the target row for r: a held cell names r's version, and keeps
// the target's value where the field says last. cur is the target's row
// for r's key, if ok.
func (wh *warehouse) held(stream string, r, cur row, ok bool) row {
	if wh.holds == nil {
		return r
	}
	wh.checkType(stream, r)
	if r.Held {
		r.HeldBy = r.ID
		if r.Last && ok {
			r.Value, r.Null, r.Wide = cur.Value, cur.Null, cur.Wide
		}
	}
	r.Held, r.Last = false, false
	return r
}

// patch fills p's cell where the target still holds p's version, by key for
// merge and by _openetl_id for append.
func (wh *warehouse) patch(s stagedStream, p row) {
	wh.checkType(s.Name, p)
	fill := func(t *row) {
		t.Value, t.Null, t.Wide, t.HeldBy = p.Value, p.Null, p.Wide, ""
		wh.holds.patched++
	}
	if destination.WriteMode(s.Mode) == destination.Merge {
		if t, ok := wh.merged[s.Name][p.Key]; ok && t.HeldBy == p.ID {
			fill(&t)
			wh.merged[s.Name][p.Key] = t
		}
		return
	}
	for i := range wh.tables[s.Name] {
		if t := &wh.tables[s.Name][i]; t.ID == p.ID && t.HeldBy == p.ID {
			fill(t)
		}
	}
}

// holdingWriter is a warehouse writer that takes holds.
type holdingWriter struct {
	*warehouseWriter
}

var (
	_ destination.SchemaApplier = (*holdingWriter)(nil)
	_ destination.Patcher       = (*holdingWriter)(nil)
)

// ApplySchema creates a table with the value column's type, and changes
// the type, unless the table's types are fixed: then it refuses, and
// reports the column as fixed until the user has altered it.
func (w *holdingWriter) ApplySchema(ctx context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	var a destination.SchemaApplied
	err := outcome(w.w.sched.point(ctx, w.attempt, "sink.schema"), func() error {
		w.wh.mu.Lock()
		defer w.wh.mu.Unlock()
		t := w.wh.holds
		a = destination.SchemaApplied{Limits: map[string]destination.TableLimits{}}
		for _, s := range streams {
			wide := valueWide(s.Schema)
			switch {
			case !t.created[s.Name]:
				t.created[s.Name], t.wide[s.Name] = true, wide
			case wide && !t.wide[s.Name] && t.fixedType[s.Name]:
				a.Refused = append(a.Refused, destination.RefusedColumn{Stream: s.Name, Column: "value", Reason: "its type doesn't change"})
				t.refused++
			case wide:
				t.wide[s.Name] = true
			}
			if t.fixedType[s.Name] && !t.wide[s.Name] {
				a.Limits[s.Name] = destination.TableLimits{Fixed: []string{"value"}}
			}
		}
		return nil
	})
	return a, err
}

func (w *holdingWriter) Patch(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	rows := rowsOf(batch)
	for i := range rows {
		rows[i].Patch = true
	}
	return outcome(w.w.sched.point(ctx, w.attempt, "sink.patch"), func() error {
		if w.buf[e.N] == nil {
			w.buf[e.N] = map[string][]row{}
		}
		w.buf[e.N][stream] = append(w.buf[e.N][stream], rows...)
		return nil
	})
}

// ---- The engine's part.

func valueField(wide, last bool) arrow.Field {
	f := arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true}
	if wide {
		f.Type = arrow.BinaryTypes.LargeString
	}
	if last {
		f.Metadata = arrow.NewMetadata([]string{destination.HeldKey}, []string{"last"})
	}
	return f
}

func heldSchema(wide, last, listed bool) *arrow.Schema {
	fs := []arrow.Field{rowSchema.Field(0), rowSchema.Field(1), rowSchema.Field(2), valueField(wide, last)}
	if listed {
		fs = append(fs, arrow.Field{Name: destination.HeldColumn, Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true})
	}
	return arrow.NewSchema(fs, nil)
}

func valueWide(s *arrow.Schema) bool {
	fs, ok := s.FieldsByName("value")
	return ok && fs[0].Type.ID() == arrow.LARGE_STRING
}

func decodeSchema(b []byte) *arrow.Schema {
	if b == nil {
		return nil
	}
	return must(arrowipc.DecodeSchema(b))
}

// heldRow is a held value, as a held file keeps it.
type heldRow struct {
	Key   string `json:"k"`
	ID    string `json:"id"`
	Seq   int64  `json:"s"`
	Value string `json:"v"`
	Wide  bool   `json:"w,omitempty"`
	Bad   int    `json:"b,omitempty"`
	// A cell's, which may still not fit; otherwise a column's, which takes
	// the column's type.
	Cell bool `json:"cell,omitempty"`
}

// holdFlow is the engine's part of holds, for a sink that isn't staged.
// It holds cells whose values don't fit and the value column while it's
// held whole, writes the held values at each stream's barrier, reports
// each epoch's holds and schemas, and replays a release's held files as
// patches after each barrier, a chunk at a time.
type holdFlow struct {
	w       *world
	attempt int
	// Values with Bad 1 fit.
	override bool
	chunk    int
}

// fits reports whether a value marked bad fits its column.
func (f *holdFlow) fits(bad int) bool { return bad == 0 || f.override && bad == 1 }

type flowStream struct {
	name string
	// Nil: the stream holds nothing.
	plan *runner.HoldPlan
	wide bool
	// The value column held whole, with the type the destination has.
	whole         *checkpoint.ColumnHold
	deliveredWide bool
	// The schema has _openetl_held.
	listed bool
	held   []heldRow
	cells  uint64
	replay *flowReplay
	at     map[uint64]streamAt
}

func (s *flowStream) last() bool { return s.plan != nil && s.plan.Held.Mode == runner.HeldLast }

type streamAt struct {
	schema, delivered []byte
	holds             *runner.ReadyHolds
}

type flowReplay struct {
	files     []string
	file      int
	row       uint64
	stillHeld uint64
	rows      []heldRow
	open      bool
}

func (r *flowReplay) done() bool { return r.file >= len(r.files) }

func (r *flowReplay) progress() *runner.ReplayProgress {
	return &runner.ReplayProgress{File: r.file, Row: r.row, StillHeld: r.stillHeld, Done: r.done()}
}

func (f *holdFlow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	defer close(out)
	streams := map[string]*flowStream{}
	for _, s := range plan.Streams {
		fs := &flowStream{name: s.Name, at: map[uint64]streamAt{}}
		if sch := decodeSchema(plan.Schemas[s.Name]); sch != nil {
			fs.wide, fs.listed = valueWide(sch), sch.HasField(destination.HeldColumn)
		}
		if hp := plan.Holds[s.Name]; hp != nil {
			fs.plan = hp
			for _, c := range hp.Holding {
				if c.Column == "value" && !c.Discarded {
					fs.whole = &c
					if d := decodeSchema(c.Delivered); d != nil {
						fs.deliveredWide = valueWide(d)
					}
				}
			}
			if r := hp.Replay; r != nil && slices.Contains(r.Columns, "value") {
				fs.replay = &flowReplay{files: r.Files, file: r.File, row: r.Row, stillHeld: r.StillHeld}
			}
		}
		streams[s.Name] = fs
	}
	send := func(fr runner.Frame) error {
		select {
		case out <- fr:
			return nil
		case <-ctx.Done():
			if fr.Batch != nil {
				fr.Batch.Release()
			}
			return ctx.Err()
		}
	}
	crossed := map[uint64]int{}
	for {
		var fr runner.Frame
		select {
		case x, ok := <-in:
			if !ok {
				return nil
			}
			fr = x
		case <-ctx.Done():
			return ctx.Err()
		}
		fs := streams[fr.Stream]
		switch fr.Kind {
		case runner.FrameRows:
			batches, err := f.shape(fs, fr.Rows)
			if err != nil {
				return err
			}
			for _, b := range batches {
				if err := send(runner.Frame{Kind: runner.FrameBatch, Stream: fs.name, Batch: b}); err != nil {
					return err
				}
			}
		case runner.FrameBarrier:
			at, err := f.barrier(ctx, plan, fs, fr.Epoch)
			if err != nil {
				return err
			}
			if err := send(fr); err != nil {
				return err
			}
			fs.at[fr.Epoch] = at
			if fs.replay != nil && !fs.replay.done() {
				patch, err := f.replayChunk(ctx, fs)
				if err != nil {
					return err
				}
				if patch != nil {
					if err := send(runner.Frame{Kind: runner.FramePatch, Stream: fs.name, Batch: patch}); err != nil {
						return err
					}
				}
			}
			if crossed[fr.Epoch]++; crossed[fr.Epoch] < len(plan.Streams) {
				continue
			}
			delete(crossed, fr.Epoch)
			ready := &runner.Ready{Schemas: map[string][]byte{}, Delivered: map[string][]byte{}, Holds: map[string]*runner.ReadyHolds{}}
			for name, s := range streams {
				at := s.at[fr.Epoch]
				delete(s.at, fr.Epoch)
				ready.Schemas[name] = at.schema
				if at.delivered != nil {
					ready.Delivered[name] = at.delivered
				}
				if at.holds != nil {
					ready.Holds[name] = at.holds
				}
			}
			if err := send(runner.Frame{Kind: runner.FrameReady, Epoch: fr.Epoch, Ready: ready}); err != nil {
				return err
			}
		default:
			return fmt.Errorf("sim: frame kind %d into the dataflow", fr.Kind)
		}
	}
}

// shape converts rows to batches, a new one wherever the schema changes:
// the value column's type, or _openetl_held once something is held.
func (f *holdFlow) shape(s *flowStream, raw []json.RawMessage) ([]arrow.RecordBatch, error) {
	var out []arrow.RecordBatch
	var b *array.RecordBuilder
	var shape [2]bool
	flush := func() {
		if b != nil {
			out = append(out, b.NewRecordBatch())
			b.Release()
			b = nil
		}
	}
	for _, m := range raw {
		var r record
		if err := json.Unmarshal(m, &r); err != nil {
			return nil, err
		}
		if r.Wide && !s.wide {
			s.wide = true
			// The engine holds a change to a column the destination can't
			// change where it sees it.
			if s.plan != nil && s.whole == nil && slices.Contains(s.plan.Limits.Fixed, "value") {
				s.whole = &checkpoint.ColumnHold{Column: "value", Delivered: must(arrowipc.EncodeSchema(arrow.NewSchema([]arrow.Field{valueField(false, false)}, nil))), Reason: "fixed"}
				s.deliveredWide = false
			}
		}
		id := recordID(s.name, r.Key, r.Seq)
		held, cell := false, false
		switch {
		case s.whole != nil:
			held = true
		case s.plan != nil && s.plan.Cells && !f.fits(r.Bad):
			held, cell = true, true
		}
		if held {
			s.listed = true
			s.held = append(s.held, heldRow{Key: r.Key, ID: id, Seq: r.Seq, Value: r.Value, Wide: r.Wide, Bad: r.Bad, Cell: cell})
			if cell {
				s.cells++
			}
		}
		wide := s.wide
		if s.whole != nil {
			wide = s.deliveredWide
		}
		if b == nil || shape != [2]bool{wide, s.listed} {
			flush()
			shape = [2]bool{wide, s.listed}
			b = array.NewRecordBuilder(memory.DefaultAllocator, heldSchema(wide, s.last(), s.listed))
		}
		b.Field(0).(*array.StringBuilder).Append(id)
		b.Field(1).(*array.Int64Builder).Append(r.Seq)
		b.Field(2).(*array.StringBuilder).Append(r.Key)
		v := b.Field(3).(array.StringLikeBuilder)
		switch {
		case !held:
			v.Append(r.Value)
		case s.plan.Held.Value != "":
			v.Append(heldValue)
		default:
			v.AppendNull()
		}
		if s.listed {
			l := b.Field(4).(*array.ListBuilder)
			if held {
				l.Append(true)
				l.ValueBuilder().(*array.StringBuilder).Append("value")
			} else {
				l.AppendNull()
			}
		}
	}
	flush()
	return out, nil
}

// barrier writes the epoch's held values, and returns the stream's state at
// the barrier.
func (f *holdFlow) barrier(ctx context.Context, plan runner.Plan, s *flowStream, epoch uint64) (streamAt, error) {
	at := streamAt{schema: must(arrowipc.EncodeSchema(heldSchema(s.wide, s.last(), s.listed)))}
	if s.whole != nil {
		at.delivered = must(arrowipc.EncodeSchema(heldSchema(s.deliveredWide, s.last(), s.listed)))
	}
	if s.plan == nil {
		return at, nil
	}
	h := &runner.ReadyHolds{}
	if len(s.held) > 0 {
		rel := fmt.Sprintf("%d/%d/%s/part-00000.parquet", plan.Generation, epoch, url.PathEscape(s.name))
		if err := f.put(ctx, "org/pipe/"+checkpoint.HeldDir+"/"+rel, must(json.Marshal(s.held))); err != nil {
			return at, fmt.Errorf("sim: write held values of %s in epoch %d: %w", s.name, epoch, err)
		}
		h.Files = []destination.StagedFile{{Path: rel, Rows: uint64(len(s.held))}}
	}
	if s.whole != nil {
		h.Holding = []checkpoint.ColumnHold{*s.whole}
	}
	if s.cells > 0 {
		h.Cells = map[string]checkpoint.CellHold{"value": {Cells: s.cells, Reason: "type"}}
	}
	if s.replay != nil {
		h.Replayed = s.replay.progress()
	}
	at.holds = h
	s.held, s.cells = nil, 0
	return at, nil
}

// replayChunk reads about a chunk of held values from where the replay has
// got, and returns the patches for those that fit now.
func (f *holdFlow) replayChunk(ctx context.Context, s *flowStream) (arrow.RecordBatch, error) {
	r := s.replay
	var fill []heldRow
	for n := 0; n < f.chunk && !r.done(); {
		if !r.open {
			data, err := f.get(ctx, "org/pipe/"+checkpoint.HeldDir+"/"+r.files[r.file])
			if err != nil {
				return nil, fmt.Errorf("sim: held file %s: %w", r.files[r.file], err)
			}
			r.rows, r.open = nil, true
			if err := json.Unmarshal(data, &r.rows); err != nil {
				return nil, err
			}
		}
		if r.row >= uint64(len(r.rows)) {
			r.file, r.row, r.open = r.file+1, 0, false
			continue
		}
		take := min(uint64(f.chunk-n), uint64(len(r.rows))-r.row)
		for _, h := range r.rows[r.row : r.row+take] {
			// A column held whole again gets no patches: the destination
			// doesn't have its type.
			switch {
			case h.Cell && (s.whole != nil || !f.fits(h.Bad)):
				r.stillHeld++
			case s.whole == nil:
				fill = append(fill, h)
			}
		}
		r.row += take
		n += int(take)
	}
	if len(fill) == 0 {
		return nil, nil
	}
	// The patch takes the column's type in the stream.
	b := array.NewRecordBuilder(memory.DefaultAllocator, heldSchema(s.wide, false, true))
	defer b.Release()
	for _, h := range fill {
		b.Field(0).(*array.StringBuilder).Append(h.ID)
		b.Field(1).(*array.Int64Builder).Append(h.Seq)
		b.Field(2).(*array.StringBuilder).Append(h.Key)
		b.Field(3).(array.StringLikeBuilder).Append(h.Value)
		l := b.Field(4).(*array.ListBuilder)
		l.Append(true)
		l.ValueBuilder().(*array.StringBuilder).Append("value")
	}
	return b.NewRecordBatch(), nil
}

// put and get retry transient failures, as object_store does.
func (f *holdFlow) put(ctx context.Context, key string, data []byte) error {
	return f.retry(ctx, "held.write", func() error {
		_, err := f.w.store.Create(ctx, key, data)
		if errors.Is(err, checkpoint.ErrPrecondition) {
			return nil // an earlier try's
		}
		return err
	})
}

func (f *holdFlow) get(ctx context.Context, key string) (data []byte, err error) {
	err = f.retry(ctx, "held.read", func() error {
		data, _, err = f.w.store.Get(ctx, key)
		return err
	})
	return data, err
}

func (f *holdFlow) retry(ctx context.Context, op string, call func() error) error {
	for try := 1; ; try++ {
		err := outcome(f.w.sched.point(ctx, f.attempt, op), call)
		if !errors.Is(err, errTransient) && !errors.Is(err, errTimeout) || try == 4 {
			return err
		}
	}
}

// ---- Releases.

type releaseKey struct {
	stream, column string
	since          checkpoint.RecordRef
}

// release decides, as attempt starts, which of HEAD's holds the user
// releases: each open hold once, with the type overridden and the column
// altered, unless the seed releases without fixing the cause. A cell hold
// is sometimes discarded instead. Every release is sent to every attempt
// after, as the control plane would if it hadn't seen a refusal yet. A
// hold whose latest release didn't clear it is sometimes released again,
// with the cause fixed.
func (w *world) release(attempt int) {
	rec := w.head()
	if rec == nil {
		return
	}
	for _, s := range w.cfg.streams {
		h := rec.Streams[s.name].Holds
		if s.holds == nil || h == nil || h.Replay != nil || w.relRng.IntN(100) >= w.cfg.releasePct {
			continue
		}
		var rs []runner.Release
		refused := map[releaseKey]*checkpoint.Refused{}
		for _, c := range h.Columns {
			if !c.Discarded {
				rs = append(rs, runner.Release{Stream: s.name, Column: c.Column, Since: c.Since})
				refused[releaseKey{s.name, c.Column, c.Since}] = c.Refused
			}
		}
		for _, col := range slices.Sorted(maps.Keys(h.Cells)) {
			c := h.Cells[col]
			rs = append(rs, runner.Release{Stream: s.name, Column: col, Since: c.Since, Discard: w.relRng.IntN(10) == 0})
			refused[releaseKey{s.name, col, c.Since}] = c.Refused
		}
		fix := !w.cfg.unfixed
		for _, r := range rs {
			k := releaseKey{r.Stream, r.Column, r.Since}
			last, again := w.released[k], false
			if last != "" {
				f := refused[k]
				if again = f != nil && f.Release == last && w.relRng.IntN(2) == 0; !again {
					continue
				}
				fix = true
				w.again++
			}
			r.ID = fmt.Sprintf("r%d", len(w.releases)+1)
			w.released[k] = r.ID
			w.releases = append(w.releases, r)
			w.sched.tracef("a%d takes release %s of %s.%s since %d-g%d (discard %v, fixed %v, again %v)",
				attempt, r.ID, r.Stream, r.Column, r.Since.Epoch, r.Since.Generation, r.Discard, fix, again)
			if r.Discard {
				w.discarded[r.Stream] = true
			}
		}
		if fix {
			w.override = true
			w.wh.mu.Lock()
			if w.wh.holds.fixedType[s.name] && w.wh.holds.created[s.name] {
				w.wh.holds.wide[s.name] = true
			}
			w.wh.mu.Unlock()
		}
	}
}

// head is HEAD's record, read without faults.
func (w *world) head() *checkpoint.Record {
	ctx := context.Background()
	data, _, err := w.store.Get(ctx, "org/pipe/_checkpoints/HEAD")
	if err != nil {
		return nil
	}
	var head checkpoint.Head
	if json.Unmarshal(data, &head) != nil || head.Record == nil {
		return nil
	}
	rec, err := checkpoint.NewLog(w.store, "org", "pipe").Record(ctx, *head.Record)
	if err != nil {
		return nil
	}
	return rec
}

func (w *world) headStreams(rec *checkpoint.Record) map[string]checkpoint.StreamState {
	if rec == nil {
		return nil
	}
	return rec.Streams
}

// ---- The oracle.

// checkHolds returns "" if every cell shows its version's value, or is held
// for its own version with its value in a held file a release would read;
// one whose hold was discarded may stay held without it.
func (w *world) checkHolds(stats map[string]int) []string {
	problems := slices.Clone(w.wh.holds.problems)
	stats["patched cells"] += w.wh.holds.patched
	stats["refused columns"] += w.wh.holds.refused
	stats["releases"] += len(w.releases)
	stats["released again"] += w.again
	ctx := context.Background()
	rec := w.head()
	refused := func(r *checkpoint.Refused) {
		if r != nil {
			stats["refused releases at the end"]++
		}
	}
	for _, st := range w.headStreams(rec) {
		if h := st.Holds; h != nil {
			for _, c := range h.Columns {
				refused(c.Refused)
			}
			for _, c := range h.Cells {
				refused(c.Refused)
			}
		}
	}
	log := checkpoint.NewLog(w.store, "org", "pipe")
	for _, s := range w.cfg.streams {
		if s.holds == nil {
			continue
		}
		var h *checkpoint.Holds
		if rec != nil {
			h = rec.Streams[s.name].Holds
		}
		if h != nil && h.Replay != nil {
			problems = append(problems, fmt.Sprintf("%s: the sync ended while a release was being patched in", s.name))
		}
		kept := map[string]bool{}
		if h != nil {
			files, err := log.HeldFiles(ctx, s.name, h.Epochs)
			if err != nil {
				return append(problems, err.Error())
			}
			for _, f := range files {
				data, _, err := w.store.Get(ctx, "org/pipe/"+checkpoint.HeldDir+"/"+f)
				if err != nil {
					problems = append(problems, fmt.Sprintf("%s: held file %s: %v", s.name, f, err))
					continue
				}
				var rows []heldRow
				must(0, json.Unmarshal(data, &rows))
				for _, r := range rows {
					kept[r.ID] = true
				}
			}
		}
		versions := map[string]record{}
		for _, r := range w.upstreams[s.name].records {
			versions[recordID(s.name, r.Key, r.Seq)] = r
		}
		var target []row
		if w.cfg.mode == destination.Merge {
			m := w.wh.merged[s.name]
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				target = append(target, m[k])
			}
		} else {
			target = w.wh.tables[s.name]
		}
		for _, t := range target {
			r, ok := versions[t.ID]
			if !ok {
				continue
			}
			switch {
			case t.HeldBy == "":
				if t.Null || t.Value != r.Value {
					problems = append(problems, fmt.Sprintf("%s: %s@%d shows %q (null %v), not its value", s.name, r.Key, r.Seq, t.Value, t.Null))
				}
			case t.HeldBy != t.ID:
				problems = append(problems, fmt.Sprintf("%s: %s@%d holds the cell of another version, %s", s.name, r.Key, r.Seq, t.HeldBy))
			default:
				stats["cells held at the end"]++
				if w.discarded[s.name] {
					continue
				}
				if !kept[t.ID] {
					problems = append(problems, fmt.Sprintf("%s: LOST %s@%d's held value: no held file a release would read has it", s.name, r.Key, r.Seq))
				}
				if s.holds.held.Mode == runner.HeldNull && !t.Null || s.holds.held.Mode == runner.HeldValue && t.Value != heldValue {
					problems = append(problems, fmt.Sprintf("%s: %s@%d's held cell shows %q (null %v)", s.name, r.Key, r.Seq, t.Value, t.Null))
				}
			}
		}
	}
	return problems
}
