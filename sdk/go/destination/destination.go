// Package destination defines what a destination connector implements.
package destination

import (
	"context"
	"errors"
	"slices"
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
	// Overwrite replaces a stream's table with what one read wrote, once the
	// read ends.
	Overwrite
	// Merge upserts on the primary key, guarded by _openetl_seq.
	Merge
)

// DeleteMode is how merge applies a record whose _openetl_op is delete.
type DeleteMode int

const (
	// SoftDelete keeps the row, with _openetl_deleted true and its other
	// columns as they were.
	SoftDelete DeleteMode = iota + 1
	HardDelete
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
	// Nil: append only.
	WriteModes []WriteMode
}

func (c Capabilities) SupportsScope(s Scope) bool {
	return slices.Contains(c.Scopes, s)
}

func (c Capabilities) SupportsMode(m WriteMode) bool {
	if len(c.WriteModes) == 0 {
		return m == Append
	}
	return slices.Contains(c.WriteModes, m)
}

// Dedupe is which append and overwrite rows an apply skips by _openetl_id.
type Dedupe int

const (
	// DedupeNone inserts every staged row.
	DedupeNone Dedupe = iota
	// DedupeTarget inserts one row per ID in the epoch, and only IDs the
	// target doesn't have yet.
	DedupeTarget
	// DedupeEpoch inserts one row per ID in the epoch.
	DedupeEpoch
)

type Stream struct {
	Name       string
	Mode       WriteMode
	PrimaryKey []string
	// Only set for sinks with DedupeAtApply.
	Dedupe Dedupe
	// Merge only; zero: SoftDelete.
	Deletes DeleteMode
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
	Commit(ctx context.Context, e Epoch, s Staged) (Committed, error)
	// Recover raises the fence to gen, then reports Applied, applies the
	// streams whose marker is missing and reports Redone, or reports Lost.
	// With a zero e it only raises the fence and reports Applied. Otherwise it
	// may then drop whatever earlier generations staged, since none of it can
	// be applied any more, and, unless e is lost, the markers of epochs before
	// e.N, which no recovery checks again.
	Recover(ctx context.Context, gen uint64, e Epoch, s Staged) (Recovery, error)
	Close(ctx context.Context) error
}

// ReadTracker is implemented by writers that need to know where each
// stream's read starts and ends, such as overwrite writes, which replace a
// table with what one read wrote. Its calls come from the goroutine that
// calls Write and PreCommit.
type ReadTracker interface {
	// StartReads comes after Recover and before the first Write, with when
	// each stream the attempt reads started its read. A read an earlier
	// attempt began keeps its start, so a stream whose start changed began a
	// new read.
	StartReads(ctx context.Context, starts map[string]time.Time) error
	// EndRead says stream's read ended in epoch e: its last batch came
	// before this call, and PreCommit(e) after it.
	EndRead(ctx context.Context, e Epoch, stream string) error
}

// Committed is what a Commit applied.
type Committed struct {
	// By stream: staged rows the apply didn't insert, because of Dedupe.
	// Streams it applied in an earlier call have none. A Commit that fails
	// returns those it applied before failing.
	Collapsed map[string]uint64
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
