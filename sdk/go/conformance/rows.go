package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"

	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/internal/rpcerr"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// row is one record: each field's value as JSON, and the record as a JSON
// object with sorted keys, which identifies it. Control fields aren't in
// the object: a changelog's snapshot rows read again take a new sequence.
type row struct {
	text   string
	fields map[string]string
}

func newRow(fields map[string]string, control ...string) row {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		if !slices.Contains(control, k) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		b.Write(kb)
		b.WriteByte(':')
		b.WriteString(fields[k])
	}
	b.WriteByte('}')
	return row{text: b.String(), fields: fields}
}

// event is one message of a read, in the order it came.
type event struct {
	stream string
	// A batch: its records, and its Arrow schema unless it was JSON rows.
	batch  bool
	rows   []row
	schema *arrow.Schema
	// A resume point.
	resume bool
	state  []byte
	// The stream's end.
	complete bool
	next     []byte
	hasNext  bool
	progress bool
}

// readResult is a whole read: what it sent, and the error that ended it.
type readResult struct {
	events []event
	err    error
}

func (r *readResult) of(stream string) []event {
	var out []event
	for _, e := range r.events {
		if e.stream == stream {
			out = append(out, e)
		}
	}
	return out
}

// rowsAfter is the stream's records after its i-th event.
func rowsAfter(events []event, i int) []row {
	var out []row
	for _, e := range events[i+1:] {
		out = append(out, e.rows...)
	}
	return out
}

func allRows(events []event) []row { return rowsAfter(events, -1) }

func read(ctx context.Context, conn grpc.ClientConnInterface, req *protocolv1.ReadRequest) (*readResult, error) {
	stream, err := protocolv1.NewSourceServiceClient(conn).Read(ctx, req)
	if err != nil {
		return &readResult{err: rpcerr.FromStatus(err)}, nil
	}
	res := &readResult{}
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return res, nil
		}
		if err != nil {
			res.err = rpcerr.FromStatus(err)
			return res, nil
		}
		switch m := msg.GetMsg().(type) {
		case *protocolv1.ReadResponse_Batch:
			e, err := decodeBatch(m.Batch)
			if err != nil {
				return res, fmt.Errorf("a batch of %s: %w", m.Batch.GetStream(), err)
			}
			res.events = append(res.events, e)
		case *protocolv1.ReadResponse_ResumePoint:
			res.events = append(res.events, event{stream: m.ResumePoint.GetStream(), resume: true, state: m.ResumePoint.GetState()})
		case *protocolv1.ReadResponse_Complete:
			res.events = append(res.events, event{stream: m.Complete.GetStream(), complete: true, next: m.Complete.GetNext(), hasNext: m.Complete.Next != nil})
		case *protocolv1.ReadResponse_Progress:
			res.events = append(res.events, event{stream: m.Progress.GetStream(), progress: true})
		}
	}
}

func decodeBatch(b *protocolv1.RecordBatch) (event, error) {
	e := event{stream: b.GetStream(), batch: true}
	if rows := b.GetRows(); rows != nil {
		for _, r := range rows.GetRows() {
			var m map[string]json.RawMessage
			if err := json.Unmarshal(r, &m); err != nil {
				return e, fmt.Errorf("a row isn't a JSON object: %w", err)
			}
			fields := make(map[string]string, len(m))
			for k, v := range m {
				fields[k] = canonicalJSON(v)
			}
			e.rows = append(e.rows, newRow(fields))
		}
		return e, nil
	}
	rec, err := arrowipc.DecodeBatch(b.GetArrow(), memory.DefaultAllocator)
	if err != nil {
		return e, err
	}
	defer rec.Release()
	e.schema = rec.Schema()
	e.rows = recordRows(rec)
	return e, nil
}

func recordRows(rec arrow.RecordBatch) []row {
	var control []string
	for _, f := range rec.Schema().Fields() {
		if _, ok := f.Metadata.GetValue("openetl.control"); ok {
			control = append(control, f.Name)
		}
	}
	out := make([]row, rec.NumRows())
	for i := range out {
		fields := make(map[string]string, rec.NumCols())
		for c, f := range rec.Schema().Fields() {
			fields[f.Name] = cell(rec.Column(c), i)
		}
		out[i] = newRow(fields, control...)
	}
	return out
}

// cell is a value as JSON. Floats that JSON can't hold are strings.
func cell(a arrow.Array, i int) string {
	if a.IsNull(i) {
		return "null"
	}
	switch a := a.(type) {
	case *array.Float32:
		return floatJSON(float64(a.Value(i)), 32)
	case *array.Float64:
		return floatJSON(a.Value(i), 64)
	}
	b, err := json.Marshal(a.GetOneForMarshal(i))
	if err != nil {
		b, _ = json.Marshal(a.ValueStr(i))
	}
	return canonicalJSON(b)
}

func floatJSON(f float64, bits int) string {
	switch {
	case math.IsNaN(f):
		return `"NaN"`
	case math.IsInf(f, 1):
		return `"+Inf"`
	case math.IsInf(f, -1):
		return `"-Inf"`
	}
	return strconv.FormatFloat(f, 'g', -1, bits)
}

// canonicalJSON compacts b, with objects' keys sorted.
func canonicalJSON(b []byte) string {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return string(b)
	}
	out, err := json.Marshal(v)
	if err != nil {
		return string(b)
	}
	return string(out)
}

// multiset counts rows by their text.
type multiset map[string]int

func countRows(rows []row) multiset {
	m := multiset{}
	for _, r := range rows {
		m[r.text]++
	}
	return m
}

// minus is what a has more of than b.
func (a multiset) minus(b multiset) []string {
	var out []string
	for k, n := range a {
		for range n - b[k] {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

func sample(s []string) string {
	if len(s) > 3 {
		return strings.Join(s[:3], ", ") + ", …"
	}
	return strings.Join(s, ", ")
}
