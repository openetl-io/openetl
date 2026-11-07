package conformance

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// held is a kit record whose v is held: it arrives null, and v is what a
// patch fills in. A deleted one deletes its key.
type held struct {
	record
	held, deleted bool
}

var (
	heldField    = arrow.Field{Name: destination.HeldColumn, Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}
	deletedField = arrow.Field{Name: "_openetl_deleted", Type: arrow.FixedWidthTypes.Boolean}
)

// heldSchema is the kit's schema with _openetl_held and extra, and v's
// field metadata md.
func heldSchema(stream string, md map[string]string, extra ...arrow.Field) *arrow.Schema {
	data := append([]arrow.Field{}, dataFields...)
	if md != nil {
		keys, vals := make([]string, 0, len(md)), make([]string, 0, len(md))
		for k, v := range md {
			keys, vals = append(keys, k), append(vals, v)
		}
		data[1].Metadata = arrow.NewMetadata(keys, vals)
	}
	s := streamSchema(stream, data)
	m := s.Metadata()
	return arrow.NewSchema(append(append(s.Fields(), heldField), extra...), &m)
}

// heldBatch's records are versions: seq orders them, and each record's id
// is its version's.
func heldBatch(schema *arrow.Schema, stream string, recs []held, seq time.Time) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	deleted := schema.FieldIndices(deletedField.Name)
	for _, r := range recs {
		b.Field(0).(*array.Int64Builder).Append(r.id)
		if r.held || r.deleted {
			b.Field(1).AppendNull()
		} else {
			b.Field(1).(*array.StringBuilder).Append(r.v)
		}
		b.Field(2).(*array.FixedSizeBinaryBuilder).Append(rowID(stream, r.record))
		op := "insert"
		if r.deleted {
			op = "delete"
		}
		b.Field(3).(*array.StringBuilder).Append(op)
		b.Field(4).(*array.TimestampBuilder).Append(arrow.Timestamp(seq.UnixMicro()))
		b.Field(5).(*array.TimestampBuilder).Append(arrow.Timestamp(time.Now().UnixMicro()))
		lb := b.Field(6).(*array.ListBuilder)
		if r.held {
			lb.Append(true)
			lb.ValueBuilder().(*array.StringBuilder).Append("v")
		} else {
			lb.AppendNull()
		}
		if len(deleted) > 0 {
			b.Field(deleted[0]).(*array.BooleanBuilder).Append(r.deleted)
		}
	}
	return b.NewRecordBatch()
}

// patch fills v: each patch's id names the version it comes from.
type patch struct {
	version record
	v       string
}

func patchBatch(stream string, ps []patch) arrow.RecordBatch {
	ts := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	md := arrow.NewMetadata([]string{"openetl.table"}, []string{stream})
	schema := arrow.NewSchema([]arrow.Field{
		dataFields[0],
		metadataFields()[0],
		{Name: "_openetl_seq", Type: ts},
		{Name: "_openetl_synced_at", Type: ts},
		heldField,
		dataFields[1],
	}, &md)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	now := arrow.Timestamp(time.Now().UnixMicro())
	for _, p := range ps {
		b.Field(0).(*array.Int64Builder).Append(p.version.id)
		b.Field(1).(*array.FixedSizeBinaryBuilder).Append(rowID(stream, p.version))
		b.Field(2).(*array.TimestampBuilder).Append(now)
		b.Field(3).(*array.TimestampBuilder).Append(now)
		lb := b.Field(4).(*array.ListBuilder)
		lb.Append(true)
		lb.ValueBuilder().(*array.StringBuilder).Append("v")
		b.Field(5).(*array.StringBuilder).Append(p.v)
	}
	return b.NewRecordBatch()
}

func (w *writer) patch(e *protocolv1.Epoch, stream string, rec arrow.RecordBatch) error {
	b, err := arrowipc.EncodeBatch(rec)
	if err != nil {
		return err
	}
	return w.send(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_Batch{Batch: &protocolv1.WriteBatch{Epoch: e, Stream: stream, Arrow: b, Patch: true}}})
}

func (r *destinationRun) holds(t T) *protocolv1.HoldCapabilities {
	t.Helper()
	r.process(t)
	r.needRead(t)
	h := r.spec.GetDestination().GetHolds()
	if !h.GetGuarded() || !h.GetByKey() && !h.GetById() {
		t.Skipf("the destination takes no holds")
	}
	return h
}

// heldEpoch writes and commits one epoch of alpha's held records, and
// patches, in w.
func (s *session) heldEpoch(w *writer, e *protocolv1.Epoch, schema *arrow.Schema, recs []held, seq time.Time, ps []patch) {
	s.t.Helper()
	b, err := arrowipc.EncodeSchema(schema)
	if err != nil {
		s.t.Fatalf("%v", err)
	}
	if w.opened.GetAppliesSchema() {
		if err := w.applySchema([]*protocolv1.StreamSchema{{Name: "alpha", ArrowSchema: b}}); err != nil {
			s.t.Fatalf("%s applying _openetl_held: %s: %v", s.where(), codeOf(err), err)
		}
	}
	if len(recs) > 0 {
		rec := heldBatch(schema, "alpha", recs, seq)
		err := w.batch(e, "alpha", rec)
		rec.Release()
		if err != nil {
			s.t.Fatalf("%s held records of %s: %s: %v", s.where(), epochString(e), codeOf(err), err)
		}
	}
	if len(ps) > 0 {
		rec := patchBatch("alpha", ps)
		err := w.patch(e, "alpha", rec)
		rec.Release()
		if err != nil {
			s.t.Fatalf("%s patches of %s: %s: %v", s.where(), epochString(e), codeOf(err), err)
		}
	}
	staged, err := w.preCommit(e)
	if err != nil {
		s.t.Fatalf("%s pre-commit %s: %s: %v", s.where(), epochString(e), codeOf(err), err)
	}
	if err := w.commit(e, staged); err != nil {
		s.t.Fatalf("%s commit %s: %s: %v", s.where(), epochString(e), codeOf(err), err)
	}
}

func (s *session) vs() map[string]string {
	s.t.Helper()
	got := map[string]string{}
	for _, row := range s.read("alpha") {
		v := "<null>"
		if row["v"] != nil {
			v = fmt.Sprint(row["v"])
		}
		got[fmt.Sprint(row["id"])] = v
	}
	return got
}

func (s *session) expectVs(when string, want map[string]string) {
	s.t.Helper()
	got := s.vs()
	for id, v := range want {
		if got[id] != v {
			s.t.Errorf("%s %s: alpha's id %s has v %q, not %q", s.where(), when, id, got[id], v)
		}
	}
	if len(got) != len(want) {
		s.t.Errorf("%s %s: alpha has %d records, not %d: %v", s.where(), when, len(got), len(want), got)
	}
}

// Holds: a held cell arrives as null; a patch fills it in only where the
// target still holds the version the patch comes from, so a patch from an
// older version, a repeated one or one for a key the target doesn't have
// changes nothing.
func (r *destinationRun) checkHoldsPatch(t T) {
	h := r.holds(t)
	mode := protocolv1.WriteMode_WRITE_MODE_APPEND
	if h.GetByKey() && r.supportsMode(protocolv1.WriteMode_WRITE_MODE_MERGE) {
		mode = protocolv1.WriteMode_WRITE_MODE_MERGE
	} else if !h.GetById() {
		t.Skipf("the destination patches only merge, which it doesn't write")
	}
	s := r.session(t, r.spec.GetDestination().GetScopes()[0], mode)
	w, _ := s.begin(1, nil, nil)
	schema := heldSchema("alpha", nil)
	t0 := time.Now()
	old1, old2 := record{id: 1, v: "old1"}, record{id: 2, v: "old2"}
	s.heldEpoch(w, epoch(1, 1), schema, []held{{record: record{id: 0, v: "a0"}}, {record: old1, held: true}, {record: old2, held: true}}, t0, nil)
	s.expectVs("with held cells", map[string]string{"0": "a0", "1": "<null>", "2": "<null>"})
	want := map[string]string{"0": "a0", "1": "p1", "2": "p2"}
	if mode == protocolv1.WriteMode_WRITE_MODE_MERGE {
		s.heldEpoch(w, epoch(1, 2), schema, []held{{record: record{id: 2, v: "new2"}}}, t0.Add(time.Second), nil)
		want["2"] = "new2"
	}
	ps := []patch{{old1, "p1"}, {old2, "p2"}, {record{id: 9, v: "x"}, "p9"}}
	s.heldEpoch(w, epoch(1, 3), schema, nil, t0, ps)
	s.expectVs("after patches", want)
	s.heldEpoch(w, epoch(1, 4), schema, nil, t0, []patch{{old1, "again"}})
	s.expectVs("after a repeated patch", want)
	s.close(w)
}

// Holds: a merge stream's held cell of a column marked "last" keeps the
// target's value, and a key it doesn't have gets what the batch carries.
func (r *destinationRun) checkHoldsLast(t T) {
	h := r.holds(t)
	if !h.GetLast() || !h.GetByKey() || !r.supportsMode(protocolv1.WriteMode_WRITE_MODE_MERGE) {
		t.Skipf("the destination doesn't keep held cells' last values")
	}
	s := r.session(t, r.spec.GetDestination().GetScopes()[0], protocolv1.WriteMode_WRITE_MODE_MERGE)
	w, _ := s.begin(1, nil, nil)
	schema := heldSchema("alpha", map[string]string{destination.HeldKey: "last"})
	t0 := time.Now()
	s.heldEpoch(w, epoch(1, 1), schema, []held{{record: record{id: 0, v: "a0"}}}, t0, nil)
	s.heldEpoch(w, epoch(1, 2), schema, []held{{record: record{id: 0, v: "b0"}, held: true}, {record: record{id: 7, v: "b7"}, held: true}}, t0.Add(time.Second), nil)
	s.expectVs("with last values held", map[string]string{"0": "a0", "7": "<null>"})
	s.heldEpoch(w, epoch(1, 3), schema, nil, t0, []patch{{record{id: 0, v: "b0"}, "b0"}})
	s.expectVs("after the patch", map[string]string{"0": "b0", "7": "<null>"})
	s.close(w)
}

func (s *session) expectDeleted(when string, want map[string]bool) {
	s.t.Helper()
	for _, row := range s.read("alpha") {
		id := fmt.Sprint(row["id"])
		if got := fmt.Sprint(row[deletedField.Name]) == "true"; got != want[id] {
			s.t.Errorf("%s %s: alpha's id %s has %s %v, not %v", s.where(), when, id, deletedField.Name, got, want[id])
		}
	}
}

// Holds: a soft delete keeps the row's columns as they were, held cells
// included, so the held version's patch still fills them in and the row
// stays deleted, while a patch from the delete's version changes nothing.
func (r *destinationRun) checkHoldsSoftDelete(t T) {
	h := r.holds(t)
	if !h.GetByKey() || !r.supportsMode(protocolv1.WriteMode_WRITE_MODE_MERGE) {
		t.Skipf("the destination doesn't patch merge")
	}
	s := r.session(t, r.spec.GetDestination().GetScopes()[0], protocolv1.WriteMode_WRITE_MODE_MERGE)
	w, _ := s.begin(1, nil, nil)
	schema := heldSchema("alpha", nil, deletedField)
	t0 := time.Now()
	old1, del1 := record{id: 1, v: "old1"}, record{id: 1, v: "del1"}
	s.heldEpoch(w, epoch(1, 1), schema, []held{{record: record{id: 0, v: "a0"}}, {record: old1, held: true}}, t0, nil)
	s.heldEpoch(w, epoch(1, 2), schema, []held{{record: del1, deleted: true}}, t0.Add(time.Second), nil)
	if _, ok := s.vs()["1"]; !ok {
		t.Skipf("the destination's readers don't see soft-deleted rows")
	}
	want, deleted := map[string]string{"0": "a0", "1": "<null>"}, map[string]bool{"1": true}
	s.expectVs("after the soft delete", want)
	s.expectDeleted("after the soft delete", deleted)
	s.heldEpoch(w, epoch(1, 3), schema, nil, t0, []patch{{del1, "d1"}})
	s.expectVs("after a patch from the delete's version", want)
	s.heldEpoch(w, epoch(1, 4), schema, nil, t0, []patch{{old1, "p1"}})
	want["1"] = "p1"
	s.expectVs("after the held version's patch", want)
	s.expectDeleted("after the held version's patch", deleted)
	s.close(w)
}

// Holds: ApplySchema's answer lists the columns a table can't take a null
// in, so the platform holds no cell there, and never a metadata column.
func (r *destinationRun) checkHoldsNotNull(t T) {
	r.holds(t)
	if r.s.NotNull == nil {
		t.Skipf("the suite has no NotNull")
	}
	mode, extra := protocolv1.WriteMode_WRITE_MODE_APPEND, []arrow.Field(nil)
	if r.supportsMode(protocolv1.WriteMode_WRITE_MODE_MERGE) {
		// A merge stream's _openetl_deleted takes no null either.
		mode, extra = protocolv1.WriteMode_WRITE_MODE_MERGE, []arrow.Field{deletedField}
	}
	s := r.session(t, r.spec.GetDestination().GetScopes()[0], mode)
	w, _ := s.begin(1, nil, nil)
	if !w.opened.GetAppliesSchema() {
		t.Skipf("the destination's writer applies no schemas")
	}
	b, err := arrowipc.EncodeSchema(heldSchema("alpha", nil, extra...))
	if err != nil {
		t.Fatalf("%v", err)
	}
	notNull := func(when string) []string {
		t.Helper()
		a, err := w.schemaApplied([]*protocolv1.StreamSchema{{Name: "alpha", ArrowSchema: b}})
		if err != nil {
			t.Fatalf("%s applying alpha's schema %s: %s: %v", s.where(), when, codeOf(err), err)
		}
		cols := a.GetLimits()["alpha"].GetNotNull()
		for _, c := range cols {
			if strings.HasPrefix(c, "_openetl_") {
				t.Errorf("%s %s: alpha's limits list the metadata column %s as taking no null", s.where(), when, c)
			}
		}
		return cols
	}
	if slices.Contains(notNull("first"), "v") {
		t.Errorf("%s alpha's limits list v as taking no null before NotNull", s.where())
	}
	if err := r.s.NotNull(t.Context(), s.ns, "alpha", "v"); err != nil {
		t.Fatalf("NotNull(%s, alpha, v): %v", s.ns, err)
	}
	if !slices.Contains(notNull("after NotNull"), "v") {
		t.Errorf("%s alpha's limits don't list v as taking no null after NotNull", s.where())
	}
	s.close(w)
}
