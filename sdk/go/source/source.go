// Package source defines what a source connector implements.
//
// A source decides what to fetch and where it is safe to resume. The platform
// decides when to commit: it places checkpoint barriers only at the resume
// points a source emits.
package source

import (
	"context"
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow"
)

// Replay is whether a stream can carry the same record version more than once.
type Replay int

const (
	// Exact streams never repeat a record version: resuming from a resume point
	// yields exactly the records after it (CDC, Kafka, chunked snapshots).
	Exact Replay = iota + 1
	// Superset streams can repeat record versions: cursor floors, lookback
	// windows, webhook retries. Every SDK and declarative stream is Superset.
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

// ConfiguredStream is a stream to read, with the state it resumes from. A nil
// State means the stream starts from its beginning.
type ConfiguredStream struct {
	Name  string
	State []byte
}

// ReadRequest asks a source to read its streams.
type ReadRequest struct {
	Streams []ConfiguredStream
}

// Emitter receives what a source reads. Every method blocks while the platform
// applies backpressure, and returns an error once the read is cancelled; a
// source returns that error from Read.
type Emitter interface {
	// Rows sends raw records on the raw lane: one JSON object per row. The
	// engine infers types and adds metadata columns.
	Rows(stream string, rows []json.RawMessage) error
	// Records sends typed records as an Arrow batch.
	Records(stream string, batch arrow.RecordBatch) error
	// ResumePoint says it is safe to restart stream from state after everything
	// emitted for it so far. The platform commits only at resume points.
	ResumePoint(stream string, state []byte) error
	// Complete says a bounded stream has no more records. Emit a final resume
	// point first, so the next read starts after them.
	Complete(stream string) error
}

// Reader reads records. Read returns when every stream is complete (bounded
// reads), when ctx is cancelled, or on error.
type Reader interface {
	Read(ctx context.Context, req ReadRequest, out Emitter) error
}

// Acknowledger is implemented by sources that acknowledge upstream: Kafka
// offsets, a Postgres slot's confirmed LSN, queue deletes. The platform calls
// Committed with the per-stream states of a checkpoint once it is safe to
// discard everything before them. It runs concurrently with Read.
type Acknowledger interface {
	Committed(ctx context.Context, states map[string][]byte) error
}
