package conformance

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/unsuitable001/openetl/sdk/go/arrowipc"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

// held is a kit record whose v is held: it arrives null, and v is what a
// patch fills in.
type held struct {
	record
	held bool
}

var heldField = arrow.Field{Name: destination.HeldColumn, Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}

// heldSchema is the kit's schema with _openetl_held, and v's field metadata
// md.
func heldSchema(stream string, md map[string]string) *arrow.Schema {
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
	return arrow.NewSchema(append(s.Fields(), heldField), &m)
}

// heldBatch's records are versions: seq orders them, and each record's id
// is its version's.
func heldBatch(schema *arrow.Schema, stream string, recs []held, seq time.Time) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for _, r := range recs {
		b.Field(0).(*array.Int64Builder).Append(r.id)
		if r.held {
			b.Field(1).AppendNull()
		} else {
			b.Field(1).(*array.StringBuilder).Append(r.v)
		}
		b.Field(2).(*array.FixedSizeBinaryBuilder).Append(rowID(stream, r.record))
		b.Field(3).(*array.StringBuilder).Append("insert")
		b.Field(4).(*array.TimestampBuilder).Append(arrow.Timestamp(seq.UnixMicro()))
		b.Field(5).(*array.TimestampBuilder).Append(arrow.Timestamp(time.Now().UnixMicro()))
		lb := b.Field(6).(*array.ListBuilder)
		if r.held {
			lb.Append(true)
			lb.ValueBuilder().(*array.StringBuilder).Append("v")
		} else {
			lb.AppendNull()
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
	if r.s.Read == nil {
		t.Fatalf("the suite has no Read")
	}
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
	s.heldEpoch(w, epoch(1, 1), schema, []held{{record{id: 0, v: "a0"}, false}, {old1, true}, {old2, true}}, t0, nil)
	s.expectVs("with held cells", map[string]string{"0": "a0", "1": "<null>", "2": "<null>"})
	want := map[string]string{"0": "a0", "1": "p1", "2": "p2"}
	if mode == protocolv1.WriteMode_WRITE_MODE_MERGE {
		s.heldEpoch(w, epoch(1, 2), schema, []held{{record{id: 2, v: "new2"}, false}}, t0.Add(time.Second), nil)
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
	s.heldEpoch(w, epoch(1, 1), schema, []held{{record{id: 0, v: "a0"}, false}}, t0, nil)
	s.heldEpoch(w, epoch(1, 2), schema, []held{{record{id: 0, v: "b0"}, true}, {record{id: 7, v: "b7"}, true}}, t0.Add(time.Second), nil)
	s.expectVs("with last values held", map[string]string{"0": "a0", "7": "<null>"})
	s.heldEpoch(w, epoch(1, 3), schema, nil, t0, []patch{{record{id: 0, v: "b0"}, "b0"}})
	s.expectVs("after the patch", map[string]string{"0": "b0", "7": "<null>"})
	s.close(w)
}
