package source_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// A changelog stream's declared control columns survive the wire, and come
// apart from its fields.
func TestSplitDeclared(t *testing.T) {
	data := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	seq := &arrow.Decimal128Type{Precision: 38, Scale: 0}
	b, err := arrowipc.EncodeSchema(arrow.NewSchema(append(data.Fields(), source.OpField(), source.SeqField(seq)), nil))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := arrowipc.DecodeSchema(b)
	if err != nil {
		t.Fatal(err)
	}
	if got, gotSeq := source.SplitDeclared(decoded); !got.Equal(data) || !arrow.TypeEqual(gotSeq, seq) {
		t.Errorf("split into %v and seq %v", got, gotSeq)
	}
	if got, gotSeq := source.SplitDeclared(data); got != data || gotSeq != nil {
		t.Errorf("a schema without control columns split into %v and seq %v", got, gotSeq)
	}
	if got, gotSeq := source.SplitDeclared(arrow.NewSchema([]arrow.Field{source.OpField(), source.SeqField(seq)}, nil)); got != nil || !arrow.TypeEqual(gotSeq, seq) {
		t.Errorf("control columns alone split into %v and seq %v", got, gotSeq)
	}
}

// splitLog records a ReadSplits run's emits.
type splitLog struct {
	source.Emitter
	mu    sync.Mutex
	state map[string][]byte
}

func (l *splitLog) Split(stream, id string) source.SplitEmitter {
	return splitEmits{l, stream + "/" + id}
}

type splitEmits struct {
	l  *splitLog
	id string
}

func (e splitEmits) Rows([]json.RawMessage) error    { return nil }
func (e splitEmits) Records(arrow.RecordBatch) error { return nil }
func (e splitEmits) ResumePoint(state []byte, _ ...source.Split) error {
	e.l.mu.Lock()
	defer e.l.mu.Unlock()
	e.l.state[e.id] = state
	return nil
}
func (e splitEmits) Complete(next []byte, _ ...source.Split) error {
	return e.ResumePoint(next)
}

func TestReadSplitsReadsThemConcurrentlyWithWhatTheyAdd(t *testing.T) {
	log := &splitLog{state: map[string][]byte{}}
	req := source.ReadRequest{Lanes: 2, Streams: []source.ConfiguredStream{
		{Name: "whole", State: []byte("w")},
		{Name: "split", Splits: []source.Split{{ID: "a"}, {ID: "b", State: []byte("b0")}, {ID: "c"}}},
	}}
	var mu sync.Mutex
	reading, most := 0, 0
	seen := map[string]bool{}
	err := source.ReadSplits(t.Context(), req, log, func(_ context.Context, cs source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		mu.Lock()
		reading++
		most = max(most, reading)
		seen[cs.Name+"/"+sp.ID] = true
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		var added []source.Split
		if sp.ID == "a" {
			added = []source.Split{{ID: "a2", State: []byte("x")}}
		}
		if cs.Name == "whole" && string(sp.State) != "w" {
			return fmt.Errorf("whole read from %q", sp.State)
		}
		if err := e.ResumePoint([]byte(sp.ID), added...); err != nil {
			return err
		}
		mu.Lock()
		reading--
		mu.Unlock()
		return e.Complete([]byte("done"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if most != 2 {
		t.Errorf("%d read at a time, want 2", most)
	}
	for _, id := range []string{"whole/", "split/a", "split/b", "split/c", "split/a2"} {
		if !seen[id] {
			t.Errorf("%s wasn't read", id)
		}
	}
}

func TestReadSplitsStopsAtTheFirstError(t *testing.T) {
	boom := errors.New("boom")
	req := source.ReadRequest{Lanes: 2, Streams: []source.ConfiguredStream{{Name: "s", Splits: []source.Split{{ID: "a"}, {ID: "b"}, {ID: "c"}}}}}
	err := source.ReadSplits(t.Context(), req, &splitLog{state: map[string][]byte{}}, func(ctx context.Context, _ source.ConfiguredStream, sp source.Split, _ source.SplitEmitter) error {
		if sp.ID == "a" {
			return boom
		}
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err %v, want boom", err)
	}
}
