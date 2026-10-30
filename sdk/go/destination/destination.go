// Package destination defines what a destination connector implements.
//
// The platform drives a destination through the commit protocol: records for an
// epoch are written, staged durably but invisibly in PreCommit, and made visible
// in Commit only after the platform has recorded the epoch's checkpoint. On
// restart, Recover finishes or abandons the epoch the checkpoint log points to.
package destination

import (
	"context"
	"errors"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// Epoch identifies one checkpoint: N within the generation that wrote it.
// Every name a sink derives for an epoch (markers, staging) includes both.
type Epoch struct {
	Generation uint64
	N          uint64
}

// IsZero reports whether e is the zero epoch, which means "no checkpoint yet".
func (e Epoch) IsZero() bool { return e.N == 0 }

// Staged is a small description of what PreCommit staged: file URIs, a staging
// table name, a producer slot. The platform stores it in the epoch's checkpoint
// record and passes it back to Commit and Recover, so a sink needs no state of
// its own to finish an epoch. Keep it to a few KB.
type Staged []byte

// Recovery is what Recover found for the epoch the checkpoint log points to.
type Recovery int

const (
	// Applied: every stream of the epoch was already visible.
	Applied Recovery = iota + 1
	// Redone: some streams weren't visible, and Recover applied them now.
	Redone
	// Lost: the epoch was not applied and can't be. The platform falls back to
	// the previous checkpoint, which is safe because nothing of this epoch is
	// visible and upstream acknowledgements trail by one checkpoint.
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

// Class is how a sink makes an epoch visible. It sets the best delivery
// guarantee the sink can offer.
type Class int

const (
	// StageAndApply sinks stage in PreCommit and apply in Commit, with a marker
	// per stream behind a fence (warehouses, Postgres, Parquet). Exactly-once.
	StageAndApply Class = iota + 1
	// TransactionalStream sinks flush into an open transaction in PreCommit and
	// commit it in Commit (Kafka). Exactly-once.
	TransactionalStream
	// DeliverAndAck sinks deliver every record during the epoch, and PreCommit
	// returns once all of them are acknowledged (webhooks, HTTP APIs).
	// At-least-once.
	DeliverAndAck
)

// Scope is whether every stream's epoch becomes visible at the same moment
// (Pipeline) or each stream's separately (Stream).
type Scope int

const (
	ScopePipeline Scope = iota + 1
	ScopeStream
)

// WriteMode is how records land in the destination.
type WriteMode int

const (
	Append WriteMode = iota + 1
	Overwrite
	Merge
)

// Capabilities is what a destination declares in Spec. The planner uses it to
// compute each stream's delivery guarantee and to check a pipeline's settings.
type Capabilities struct {
	Class  Class
	Scopes []Scope
	// DedupeAtApply means the sink can insert append and overwrite rows only for
	// _openetl_id values not already in the target.
	DedupeAtApply         bool
	DefaultCommitInterval time.Duration
	MinCommitInterval     time.Duration
}

// SupportsScope reports whether c lists s.
func (c Capabilities) SupportsScope(s Scope) bool {
	for _, x := range c.Scopes {
		if x == s {
			return true
		}
	}
	return false
}

// Stream is one stream the writer receives, as configured for this attempt.
type Stream struct {
	Name       string
	Mode       WriteMode
	PrimaryKey []string
	// Dedupe asks the sink to dedupe append and overwrite writes of this stream
	// on _openetl_id when it applies an epoch. The platform sets it for streams
	// that can repeat records. Sinks without DedupeAtApply never see it set.
	Dedupe bool
}

// OpenConfig configures a writer for one attempt.
type OpenConfig struct {
	Pipeline string
	// Generation is the attempt's generation. Applies from this writer check it
	// against the destination fence.
	Generation uint64
	Scope      Scope
	Streams    []Stream
}

// Destination opens writers. It is the destination connector itself.
type Destination interface {
	Capabilities() Capabilities
	Open(ctx context.Context, cfg OpenConfig) (Writer, error)
}

// Writer is one attempt's connection to the destination.
//
// The platform calls Recover first, then Write and PreCommit from one
// goroutine, in epoch order. Write calls for epoch N+1 can arrive before
// PreCommit(N), because each stream crosses barrier N at its own resume point.
// Commit runs on another goroutine, concurrently with Write and PreCommit of
// later epochs, but never concurrently with another Commit.
type Writer interface {
	// Write adds a batch of stream's records to epoch e. The batch is released
	// after Write returns; retain it to keep it longer.
	Write(ctx context.Context, e Epoch, stream string, batch arrow.RecordBatch) error
	// PreCommit makes everything written in epoch e durable but not visible,
	// and returns a small descriptor of what it staged.
	PreCommit(ctx context.Context, e Epoch) (Staged, error)
	// Commit runs after epoch e's checkpoint record is in the checkpoint log.
	// It makes the epoch visible together with an (e.Generation, e.N) marker per
	// stream, behind the destination fence: in one destination transaction, or
	// one per stream when the scope is ScopeStream. Running it twice is a no-op.
	Commit(ctx context.Context, e Epoch, s Staged) error
	// Recover runs once per attempt, before any Write, for the epoch the
	// checkpoint log points to. It raises the destination fence to gen, then
	// reports Applied, applies the streams whose marker is missing and reports
	// Redone, or reports Lost. With a zero e there is no checkpoint yet: it only
	// raises the fence and reports Applied.
	Recover(ctx context.Context, gen uint64, e Epoch, s Staged) (Recovery, error)
	Close(ctx context.Context) error
}

// ErrFenced is returned by sinks when a newer generation owns the pipeline.
var ErrFenced = errors.New("destination: fenced by a newer generation")
