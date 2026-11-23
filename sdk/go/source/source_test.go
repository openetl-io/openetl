package source_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"

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

// ChangedTypes finds a column whose type changed, an extension's included,
// whether the extension is a registered type or field metadata, and not
// one the declared types don't have.
func TestChangedTypes(t *testing.T) {
	uuidMeta := arrow.NewMetadata([]string{"ARROW:extension:name", "ARROW:extension:metadata"}, []string{"arrow.uuid", ""})
	pointMeta := arrow.NewMetadata([]string{"ARROW:extension:name", "ARROW:extension:metadata"}, []string{"example.point", "{}"})
	jsonType, err := extensions.NewJSONType(arrow.BinaryTypes.String)
	if err != nil {
		t.Fatal(err)
	}
	declared := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: extensions.NewUUIDType()},
		{Name: "doc", Type: jsonType},
		{Name: "point", Type: arrow.BinaryTypes.Binary, Metadata: pointMeta},
		{Name: "bytes", Type: arrow.BinaryTypes.Binary},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b, err := arrowipc.EncodeSchema(declared)
	if err != nil {
		t.Fatal(err)
	}
	// As a read's request carries them.
	if declared, err = arrowipc.DecodeSchema(b); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		current []arrow.Field
		want    []string
	}{
		"unchanged, the UUID and the JSON as field metadata": {current: []arrow.Field{
			{Name: "id", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: uuidMeta},
			{Name: "doc", Type: arrow.BinaryTypes.String, Metadata: arrow.NewMetadata([]string{"ARROW:extension:name", "ARROW:extension:metadata"}, []string{"arrow.json", "{}"})},
			{Name: "point", Type: arrow.BinaryTypes.Binary, Metadata: pointMeta},
			{Name: "bytes", Type: arrow.BinaryTypes.Binary},
			{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		}},
		"a new column": {current: []arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}, {Name: "new", Type: arrow.BinaryTypes.String}}},
		"bytes to text, and uuid and json to their storage": {current: []arrow.Field{
			{Name: "id", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}},
			{Name: "doc", Type: arrow.BinaryTypes.String},
			{Name: "bytes", Type: arrow.BinaryTypes.String},
		}, want: []string{"id", "doc", "bytes"}},
		"another extension": {current: []arrow.Field{
			{Name: "point", Type: arrow.BinaryTypes.Binary, Metadata: arrow.NewMetadata([]string{"ARROW:extension:name"}, []string{"example.line"})},
			{Name: "n", Type: arrow.PrimitiveTypes.Int32},
		}, want: []string{"point", "n"}},
	} {
		got, err := source.ChangedTypes(declared, arrow.NewSchema(c.current, nil))
		if err != nil || !slices.Equal(got, c.want) {
			t.Errorf("%s: changed %v, %v; want %v", name, got, err, c.want)
		}
	}
	if got, err := source.ChangedTypes(nil, declared); got != nil || err != nil {
		t.Errorf("without declared types: %v, %v", got, err)
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

// A stream read whole is one split with the stream's state, and its stop:
// its until, or its complete_at.
func TestReadSplitsGivesAWholeStreamItsStop(t *testing.T) {
	req := source.ReadRequest{Streams: []source.ConfiguredStream{
		{Name: "until", State: []byte("1"), Until: []byte("5")},
		{Name: "complete", State: []byte("2"), CompleteAt: []byte("7")},
	}}
	var mu sync.Mutex
	got := map[string]source.Split{}
	err := source.ReadSplits(t.Context(), req, &splitLog{state: map[string][]byte{}}, func(_ context.Context, cs source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		mu.Lock()
		got[cs.Name] = sp
		mu.Unlock()
		return e.Complete(nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	if sp := got["until"]; sp.ID != source.WholeStream || string(sp.State) != "1" || string(sp.Until) != "5" || sp.CompleteAt != nil {
		t.Errorf("the stream with an until was read as %+v", sp)
	}
	if sp := got["complete"]; sp.ID != source.WholeStream || string(sp.State) != "2" || sp.Until != nil || string(sp.CompleteAt) != "7" {
		t.Errorf("the stream with a complete_at was read as %+v", sp)
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

// A continuous read reads every split of a stream that follows its source,
// and those they add, however few lanes it has: none of them ends to free
// one.
func TestReadSplitsReadsEveryFollowedSplitOfAContinuousRead(t *testing.T) {
	for name, mode := range map[string]source.SyncMode{"stream": source.Stream, "cdc": source.CDC, "unset": 0} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			req := source.ReadRequest{Continuous: true, Lanes: 2, Streams: []source.ConfiguredStream{
				{Name: "s", SyncMode: mode, Splits: []source.Split{{ID: "0"}, {ID: "1"}, {ID: "2"}, {ID: "3"}, {ID: "4"}}},
			}}
			want := 6
			var mu sync.Mutex
			started := map[string]bool{}
			all := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- source.ReadSplits(ctx, req, &splitLog{state: map[string][]byte{}}, func(ctx context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
					var added []source.Split
					if sp.ID == "4" {
						added = []source.Split{{ID: "4a"}}
					}
					if err := e.ResumePoint([]byte(sp.ID), added...); err != nil {
						return err
					}
					mu.Lock()
					started[sp.ID] = true
					if len(started) == want {
						close(all)
					}
					mu.Unlock()
					<-ctx.Done()
					return ctx.Err()
				})
			}()
			select {
			case <-all:
			case err := <-done:
				t.Fatalf("ReadSplits returned %v before reading every split", err)
			case <-time.After(10 * time.Second):
				mu.Lock()
				defer mu.Unlock()
				t.Fatalf("within 10s, %d of %d splits were read: %v", len(started), want, started)
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("err %v, want context.Canceled", err)
			}
		})
	}
}

// Splits that end take turns on the lanes, with those they add: a bounded
// read's, whatever their streams' modes, and a continuous read's of streams
// read with FullRefresh or Incremental, beside followed splits, which take
// no lane.
func TestReadSplitsReadsSplitsThatEndOnTheLanes(t *testing.T) {
	splits := []source.Split{{ID: "0"}, {ID: "1"}, {ID: "2"}, {ID: "3"}, {ID: "4"}}
	for name, tc := range map[string]struct {
		continuous bool
		mode       source.SyncMode
	}{
		"bounded stream":         {mode: source.Stream},
		"bounded cdc":            {mode: source.CDC},
		"continuous incremental": {continuous: true, mode: source.Incremental},
		"continuous full":        {continuous: true, mode: source.FullRefresh},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			req := source.ReadRequest{Continuous: tc.continuous, Lanes: 2, Streams: []source.ConfiguredStream{{Name: "b", SyncMode: tc.mode, Splits: splits}}}
			if tc.continuous {
				req.Streams = append(req.Streams, source.ConfiguredStream{Name: "f", SyncMode: source.Stream, Splits: splits})
			}
			var mu sync.Mutex
			reading, most, read, following := 0, 0, 0, 0
			var once sync.Once
			ended := make(chan struct{})
			check := func() {
				if read == 2*len(splits) && (!tc.continuous || following == len(splits)) {
					once.Do(func() { close(ended) })
				}
			}
			res := make(chan error, 1)
			go func() {
				res <- source.ReadSplits(ctx, req, &splitLog{state: map[string][]byte{}}, func(ctx context.Context, cs source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
					mu.Lock()
					if cs.Name == "f" {
						following++
						check()
						mu.Unlock()
						<-ctx.Done()
						return ctx.Err()
					}
					reading++
					most = max(most, reading)
					mu.Unlock()
					time.Sleep(5 * time.Millisecond)
					var added []source.Split
					if len(sp.ID) == 1 {
						added = []source.Split{{ID: sp.ID + "+"}}
					}
					mu.Lock()
					reading--
					read++
					check()
					mu.Unlock()
					return e.Complete(nil, added...)
				})
			}()
			select {
			case <-ended:
			case <-time.After(10 * time.Second):
				mu.Lock()
				defer mu.Unlock()
				t.Fatalf("within 10s, %d splits that end were read, and %d followed splits started", read, following)
			}
			if tc.continuous {
				cancel()
			}
			if err := <-res; tc.continuous && !errors.Is(err, context.Canceled) || !tc.continuous && err != nil {
				t.Fatalf("err %v", err)
			}
			if most > 2 {
				t.Errorf("%d splits that end were read at a time, want at most 2", most)
			}
		})
	}
}
