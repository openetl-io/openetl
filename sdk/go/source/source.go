// Package source defines what a source connector implements. The platform
// commits only at the resume points a source emits.
package source

import (
	"context"
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow"
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
	// Nil: start from the beginning.
	State []byte
}

type ReadRequest struct {
	Streams []ConfiguredStream
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
	// Complete ends a bounded stream. Emit its final resume point first.
	Complete(stream string) error
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

// Checker is implemented by sources that can verify their config reaches the
// source without reading it.
type Checker interface {
	Check(ctx context.Context) error
}
