// Package destination defines what a destination connector implements.
package destination

import (
	"context"
	"errors"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// Epoch is checkpoint N within the generation that wrote it. Names a sink
// derives for an epoch, such as markers and staging, must include both.
type Epoch struct {
	Generation uint64
	N          uint64
}

// IsZero reports whether e means "no checkpoint yet".
func (e Epoch) IsZero() bool { return e.N == 0 }

// Staged describes what PreCommit staged. The platform stores it in the
// epoch's checkpoint record and passes it back to Commit and Recover. Keep it
// to a few KB.
type Staged []byte

// Recovery is what Recover found for the epoch the checkpoint log points to.
type Recovery int

const (
	// Applied: every stream of the epoch was already visible.
	Applied Recovery = iota + 1
	// Redone: Recover applied the streams that weren't visible.
	Redone
	// Lost: the epoch wasn't applied and can't be.
	Lost
)

func (r Recovery) String() string {
	switch r {
	case Applied:
		return "applied"
	case Redone:
		return "redone"
	case Lost:
		return "lost"
	default:
		return "unknown"
	}
}

// Class is how a sink makes an epoch visible.
type Class int

const (
	// StageAndApply sinks stage in PreCommit and apply in Commit, with a marker
	// per stream behind a fence: warehouses, Postgres, Parquet.
	StageAndApply Class = iota + 1
	// TransactionalStream sinks flush into a transaction in PreCommit and
	// commit it in Commit: Kafka.
	TransactionalStream
	// DeliverAndAck sinks deliver during the epoch, and PreCommit returns once
	// everything is acknowledged: webhooks, HTTP APIs.
	DeliverAndAck
)

// Scope is whether all streams' epochs become visible together (Pipeline) or
// each stream's on its own (Stream).
type Scope int

const (
	ScopePipeline Scope = iota + 1
	ScopeStream
)

type WriteMode int

const (
	Append WriteMode = iota + 1
	Overwrite
	Merge
)

type Capabilities struct {
	Class  Class
	Scopes []Scope
	// DedupeAtApply: the sink can skip append and overwrite rows whose
	// _openetl_id is already in the target.
	DedupeAtApply         bool
	DefaultCommitInterval time.Duration
	MinCommitInterval     time.Duration
	// The longest table or column name, in bytes; 0 for no limit.
	MaxIdentifierLength int
	// Timestamps keep nanoseconds; otherwise only microseconds.
	NanosecondTimestamps bool
}

func (c Capabilities) SupportsScope(s Scope) bool {
	for _, x := range c.Scopes {
		if x == s {
			return true
		}
	}
	return false
}

type Stream struct {
	Name       string
	Mode       WriteMode
	PrimaryKey []string
	// Dedupe append and overwrite writes on _openetl_id at apply. Only set
	// for sinks with DedupeAtApply.
	Dedupe bool
}

type OpenConfig struct {
	Pipeline string
	// Applies check it against the destination fence.
	Generation uint64
	Scope      Scope
	// A schema or dataset. Empty: the destination's default.
	Namespace string
	Streams   []Stream
}

type Destination interface {
	Capabilities() Capabilities
	Open(ctx context.Context, cfg OpenConfig) (Writer, error)
}

// Writer is one attempt's connection to the destination.
//
// Recover is called first. Write and PreCommit are called from one goroutine
// in epoch order, but Writes for epoch N+1 can arrive before PreCommit(N).
// Commit runs on another goroutine, concurrently with later Writes and
// PreCommits but never with another Commit.
type Writer interface {
	// Write adds a batch to epoch e. The batch is released after Write
	// returns; retain it to keep it longer.
	Write(ctx context.Context, e Epoch, stream string, batch arrow.RecordBatch) error
	// PreCommit makes epoch e durable but not visible.
	PreCommit(ctx context.Context, e Epoch) (Staged, error)
	// Commit runs after e's checkpoint record is in the log. It makes the
	// epoch visible with an (e.Generation, e.N) marker per stream, behind the
	// fence: in one transaction, or one per stream with ScopeStream.
	// Idempotent.
	Commit(ctx context.Context, e Epoch, s Staged) error
	// Recover raises the fence to gen, then reports Applied, applies the
	// streams whose marker is missing and reports Redone, or reports Lost.
	// With a zero e it only raises the fence and reports Applied. Otherwise it
	// may then drop whatever earlier generations staged, since none of it can
	// be applied any more, and, unless e is lost, the markers of epochs before
	// e.N, which no recovery checks again.
	Recover(ctx context.Context, gen uint64, e Epoch, s Staged) (Recovery, error)
	Close(ctx context.Context) error
}

// StreamSchema is a stream's schema as the dataflow shaped it: destination
// names, the metadata columns, and its table in the "openetl.table" schema
// metadata.
type StreamSchema struct {
	Name   string
	Schema *arrow.Schema
}

// Checker is implemented by destinations that can confirm their config
// reaches the destination with the access writes need, without writing
// records.
type Checker interface {
	Check(ctx context.Context) error
}

// SchemaApplier is implemented by writers whose destination has a schema to
// change, such as a database. ApplySchema makes each stream's table hold its
// schema: it creates tables, adds columns and widens types, and never drops
// or narrows them. It is idempotent, and isn't fenced.
//
// It runs before Recover with the restored epoch's schemas, and before
// Commit for each epoch whose schemas changed.
type SchemaApplier interface {
	ApplySchema(ctx context.Context, streams []StreamSchema) error
}

// ErrFenced is returned when a newer generation owns the pipeline.
var ErrFenced = errors.New("destination: fenced by a newer generation")
