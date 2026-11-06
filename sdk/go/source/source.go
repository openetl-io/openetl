// Package source defines what a source connector implements. The platform
// commits only at the resume points a source emits.
package source

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/sdk/go/arrowipc"
)

// Replay is whether a stream can carry the same record version more than
// once. The zero value counts as Superset.
type Replay int

const (
	// Exact: resuming yields exactly the records after the resume point (CDC,
	// Kafka, chunked snapshots).
	Exact Replay = iota + 1
	// Superset: records can repeat (cursor floors, lookback windows, webhook
	// retries).
	Superset
)

// SyncMode is how a source reads a stream.
type SyncMode int

const (
	// FullRefresh reads the whole stream every sync. Its tables are
	// overwritten unless the pipeline sets another write mode.
	FullRefresh SyncMode = iota + 1
	// Incremental reads from a cursor floor onwards.
	Incremental
	// CDC reads a changelog.
	CDC
	// Stream reads events that never end (Kafka, Kinesis).
	Stream
)

func (r Replay) String() string {
	switch r {
	case Exact:
		return "exact"
	case Superset:
		return "superset"
	default:
		return "unknown"
	}
}

type ConfiguredStream struct {
	Name string
	// How the pipeline reads the stream; 0 if the platform doesn't say.
	SyncMode SyncMode
	// The field an incremental read orders by: the pipeline's choice, or
	// the catalog's default.
	Cursor string
	// The whole stream's state, where Splits is nil. Nil: start from the
	// beginning.
	State []byte
	// The splits to read; nil: the whole stream, from State.
	Splits []Split
}

// WholeStream is the ID of the split that is the whole stream.
const WholeStream = ""

// Split is one of a stream's splits (ADR-0027): a resumable slice of its
// read, such as a key range, a partition or a shard, which a source may read
// concurrently with the others. IDs are the source's, unique within the
// stream for as long as its read lasts. A stream is read as one split, the
// whole stream, unless the platform asks a Splitter for others.
type Split struct {
	ID string
	// Nil: from its beginning.
	State []byte
}

type ReadRequest struct {
	Streams []ConfiguredStream
	// The pipeline reading, which names what a source keeps for it upstream,
	// such as a replication slot.
	Pipeline string
	// The read never ends: streams read with CDC or Stream follow their
	// source until ctx is cancelled. Otherwise they complete once they've
	// caught up with where their source was when the read started.
	Continuous bool
}

// A changelog stream's typed batches (read with CDC) carry each record's
// op and sequence in control columns, which aren't records' fields. A
// field's metadata marks it one, under ControlKey; its name doesn't matter.
const ControlKey = "openetl.control"

// The ops of a changelog's records.
const (
	OpInsert = "insert"
	OpUpdate = "update"
	OpDelete = "delete"
)

// OpField is a utf8 control column holding each record's op (OpInsert,
// OpUpdate or OpDelete; null is an insert). Every batch of a changelog
// stream has one.
func OpField() arrow.Field { return controlField("openetl.op", arrow.BinaryTypes.String, "op") }

// SeqField is a control column holding each record version's position,
// which orders a key's versions: its _openetl_seq. Every batch of a
// changelog stream has one, of the same type.
func SeqField(t arrow.DataType) arrow.Field { return controlField("openetl.seq", t, "seq") }

// UnchangedField is a control column holding, for each record, the fields
// an update didn't send, such as Postgres's unchanged TOAST values; their
// values are null. Merge keeps what the destination has for them.
func UnchangedField() arrow.Field {
	return controlField("openetl.unchanged", arrow.ListOf(arrow.BinaryTypes.String), "unchanged")
}

func controlField(name string, t arrow.DataType, control string) arrow.Field {
	return arrow.Field{Name: name, Type: t, Nullable: true, Metadata: arrow.NewMetadata([]string{ControlKey}, []string{control})}
}

// SplitDeclared splits a stream's declared schema, as Discover sends it, into
// its fields and the type of its seq control column, if it declares one.
func SplitDeclared(s *arrow.Schema) (*arrow.Schema, arrow.DataType) {
	if s == nil {
		return nil, nil
	}
	var fields []arrow.Field
	var seq arrow.DataType
	for _, f := range s.Fields() {
		switch c, ok := f.Metadata.GetValue(ControlKey); {
		case !ok:
			fields = append(fields, f)
		case c == "seq":
			seq = f.Type
		}
	}
	if len(fields) == s.NumFields() {
		return s, nil
	}
	if len(fields) == 0 {
		return nil, seq
	}
	md := s.Metadata()
	return arrow.NewSchema(fields, &md), seq
}

// Emitter receives what a source reads. Its methods block under
// backpressure, and fail once the read is cancelled; Read returns that error.
// Those that take a stream emit on the whole stream, as Split(stream,
// WholeStream)'s do.
type Emitter interface {
	// Rows sends JSON objects on the raw lane.
	Rows(stream string, rows []json.RawMessage) error
	Records(stream string, batch arrow.RecordBatch) error
	// ResumePoint says stream can restart from state after everything emitted
	// for it so far.
	ResumePoint(stream string, state []byte) error
	// Complete ends a bounded stream. Like a resume point, it covers
	// everything emitted for the stream, and a retry within the same sync
	// doesn't read the stream again. next is where its next sync starts: nil
	// for the beginning, as a full refresh does, or a cursor floor.
	Complete(stream string, next []byte) error
	// Split returns the emitter of one of stream's splits. Different
	// splits' emitters may be called concurrently, each from one goroutine
	// at a time.
	Split(stream, id string) SplitEmitter
}

// SplitEmitter receives what a source reads of one split, as Emitter does
// of a stream.
type SplitEmitter interface {
	Rows(rows []json.RawMessage) error
	Records(batch arrow.RecordBatch) error
	// ResumePoint says the split can restart from state after everything
	// emitted for it so far. The stream gains the added splits from here on:
	// a re-split's remainder, which state no longer covers, or a shard whose
	// parents are read.
	ResumePoint(state []byte, added ...Split) error
	// Complete ends the split, as Emitter.Complete ends a stream read whole,
	// and the stream once every split has. added as ResumePoint's.
	Complete(next []byte, added ...Split) error
}

// Forwarder is an Emitter that also takes batches as the protocol carries
// them, and passes them on without decoding them. Its splits' emitters are
// SplitForwarders.
type Forwarder interface {
	Emitter
	Forward(stream string, batch arrowipc.Encoded) error
}

// SplitForwarder is a SplitEmitter that also takes batches as the protocol
// carries them.
type SplitForwarder interface {
	SplitEmitter
	Forward(batch arrowipc.Encoded) error
}

// WholeSplit is an Emitter's whole stream as a SplitEmitter, for Emitters
// that know only whole streams. It fails other splits' emits, and any
// that add splits.
func WholeSplit(out Emitter, stream, id string) SplitEmitter {
	w := whole{out, stream, id}
	if f, ok := out.(Forwarder); ok {
		return wholeForwarder{w, f}
	}
	return w
}

type wholeForwarder struct {
	whole
	f Forwarder
}

func (w wholeForwarder) Forward(batch arrowipc.Encoded) error {
	if err := w.check(nil); err != nil {
		return err
	}
	return w.f.Forward(w.stream, batch)
}

type whole struct {
	out        Emitter
	stream, id string
}

func (w whole) check(added []Split) error {
	if w.id != WholeStream || len(added) > 0 {
		return fmt.Errorf("source: stream %q is read whole, not in splits", w.stream)
	}
	return nil
}

func (w whole) Rows(rows []json.RawMessage) error {
	if err := w.check(nil); err != nil {
		return err
	}
	return w.out.Rows(w.stream, rows)
}

func (w whole) Records(batch arrow.RecordBatch) error {
	if err := w.check(nil); err != nil {
		return err
	}
	return w.out.Records(w.stream, batch)
}

func (w whole) ResumePoint(state []byte, added ...Split) error {
	if err := w.check(added); err != nil {
		return err
	}
	return w.out.ResumePoint(w.stream, state)
}

func (w whole) Complete(next []byte, added ...Split) error {
	if err := w.check(added); err != nil {
		return err
	}
	return w.out.Complete(w.stream, next)
}

// Progress says how far a read is behind its source, and how much longer
// the source keeps what the read hasn't acknowledged. Nil fields are
// unknown.
type Progress struct {
	// Empty: the whole read, such as a replication slot's.
	Stream string
	// What the source holds that the read hasn't acknowledged: a
	// replication slot's bytes, a Kafka consumer group's offsets.
	LagBytes, LagRecords *uint64
	// How far behind in time: the age of the change being read. Zero once
	// the read has caught up.
	Lag *time.Duration
	// How much more the source can take, or how long it waits, before it
	// discards what the read hasn't acknowledged.
	RetentionLeftBytes *uint64
	RetentionLeft      *time.Duration
}

// A Progresser is an Emitter that also takes a read's progress, for
// metrics. Report it every few seconds at most; the platform keeps the
// latest per stream.
type Progresser interface {
	Emitter
	Progress(Progress) error
}

// Reader reads records. Read returns when every stream is complete, when ctx
// is cancelled, or on error.
type Reader interface {
	Read(ctx context.Context, req ReadRequest, out Emitter) error
}

type CatalogStream struct {
	Name       string
	PrimaryKey []string
	// Where the source splits the stream, every record of a key is in one
	// split.
	SplitsByKey bool
	// The field whose value orders a record's versions; empty for none.
	CursorField string
	Replay      Replay
	// Declared logical types, by field name; nil: inferred from the records.
	// Records may carry a declared field as text, which the engine parses,
	// quarantining what doesn't fit.
	Schema *arrow.Schema
	// How the source can read the stream, the way it reads it first; nil if
	// it doesn't say.
	SyncModes []SyncMode
	// The type of its seq control column, when it's read with CDC. The
	// platform needs it before the stream's first batch.
	Seq arrow.DataType
}

type Catalog struct {
	Streams []CatalogStream
}

// Source is a source connector bound to its config.
type Source interface {
	Reader
	Discover(ctx context.Context) (Catalog, error)
}

// Acknowledger is implemented by sources that acknowledge upstream (Kafka
// offsets, a replication slot's LSN). Committed gets one checkpoint's
// states, once everything before them can be discarded: by stream for the
// streams read whole, and their splits' for those read in splits. It runs
// concurrently with Read.
type Acknowledger interface {
	Committed(ctx context.Context, states map[string][]byte, splits map[string][]Split) error
}

// Splitter is implemented by sources that divide streams into splits.
// Splits returns a stream's splits for a read that starts, given where its
// last read's splits ended, their next states; ended is nil for a stream
// never read. The whole stream alone means it isn't split.
type Splitter interface {
	Splits(ctx context.Context, stream ConfiguredStream, ended []Split) ([]Split, error)
}

// Whole is a stream's one split, the whole stream, for a read that starts
// where its last read ended: a Splitter's answer for a stream it doesn't
// split. It fails if that read had other splits.
func Whole(ended []Split) ([]Split, error) {
	switch {
	case len(ended) == 0:
		return []Split{{ID: WholeStream}}, nil
	case len(ended) == 1 && ended[0].ID == WholeStream:
		return []Split{{ID: WholeStream, State: ended[0].State}}, nil
	}
	return nil, fmt.Errorf("source: the stream's last read had %d splits, and it's read whole", len(ended))
}

// Resplitter is implemented by sources that split a running split's
// remainder, so a lane that has finished its splits can take half of a
// straggler's. Resplit says whether the source will: it then adds the new
// split at the split's next resume point, whose state no longer covers it.
// It runs concurrently with Read.
type Resplitter interface {
	Resplit(ctx context.Context, stream, split string) (bool, error)
}

// Teardowner is implemented by sources that keep something upstream for a
// pipeline, such as a replication slot. Teardown removes it once the
// pipeline is deleted. Removing what's already gone isn't an error.
type Teardowner interface {
	Teardown(ctx context.Context, pipeline string) error
}

// Checker is implemented by sources that can verify their config reaches the
// source without reading it.
type Checker interface {
	Check(ctx context.Context) error
}
