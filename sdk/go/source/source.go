// Package source defines what a source connector implements. The platform
// commits only at the resume points a source emits.
package source

import (
	"context"
	"encoding/json"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/unsuitable001/openetl/sdk/go/arrowipc"
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
	// Nil: start from the beginning.
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

// Emitter receives what a source reads. Its methods block under
// backpressure, and fail once the read is cancelled; Read returns that error.
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
}

// Forwarder is an Emitter that also takes batches as the protocol carries
// them, and passes them on without decoding them.
type Forwarder interface {
	Emitter
	Forward(stream string, batch arrowipc.Encoded) error
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
// per-stream states, once everything before them can be discarded. It runs
// concurrently with Read.
type Acknowledger interface {
	Committed(ctx context.Context, states map[string][]byte) error
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
