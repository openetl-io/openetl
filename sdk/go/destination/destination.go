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
	// everything is acknowledged: webhooks, HTTP APIs. With DeliveryStaged
	// they deliver in Commit instead, from the staged files.
	DeliverAndAck
)

// Delivery is how a destination receives each epoch's rows. Zero is
// DeliveryStreamed.
type Delivery int

const (
	// DeliveryStreamed: batches arrive through Write while the epoch is read,
	// and the sink stages them its own way, if at all.
	DeliveryStreamed Delivery = iota + 1
	// DeliveryStaged: the platform stages each epoch as Parquet files, and
	// hands them over through StagedWriter. It keeps them while the epoch can
	// be recovered.
	DeliveryStaged
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
	// 0: no maximum.
	MaxCommitInterval time.Duration
	// The longest table or column name, in bytes; 0 for no limit.
	MaxIdentifierLength int
	// Timestamps keep nanoseconds; otherwise only microseconds.
	NanosecondTimestamps bool
	// Nil: append only.
	WriteModes []WriteMode
	Delivery   Delivery
	// Zero: the destination takes no holds.
	Holds Holds
}

// Holds is what a destination does with columns and cells the platform
// holds back (ADR-0025). A held cell arrives as null, or as the pipeline's
// held value, and HeldColumn lists its column in the row. A Patcher fills
// it in later.
type Holds struct {
	// Patches a merge stream's rows by their key.
	ByKey bool
	// Patches an append stream's rows by their _openetl_id.
	ByID bool
	// A held cell of a merge stream's column whose field has HeldKey "last"
	// in its metadata keeps the target's value.
	Last bool
	// A patch fills a cell only where the target still holds the version
	// the patch comes from.
	Guarded bool
}

// Takes reports whether the destination takes holds on a stream written
// with m.
func (h Holds) Takes(m WriteMode) bool {
	return h.Guarded && (m == Merge && h.ByKey || m == Append && h.ByID)
}

const (
	// HeldColumn lists a row's held columns, by destination name, as a list
	// of strings. In a patch, the columns the row fills.
	HeldColumn = "_openetl_held"
	// HeldKey is field metadata: "last" on a column whose held cells keep
	// the target's value.
	HeldKey = "openetl.held"
	// FillsKey is field metadata on a patch's column that fills another
	// held column than its own: col__v2 fills col.
	FillsKey = "openetl.fills"
)

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
	// How often epochs are committed, for sinks whose transactions must
	// outlast one; 0 if the platform doesn't say.
	CommitInterval time.Duration
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
	// PreCommit makes epoch e durable but not visible. A staged epoch is
	// durable already.
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

// StagedWriter is implemented by the writers of destinations with
// DeliveryStaged, whose Write is never called. WriteStaged gets each
// stream's files of epoch e before PreCommit(e), and the files of the epoch
// being recovered before Recover. Streams without rows in the epoch get no
// call. Its calls come from the goroutine that calls PreCommit.
type StagedWriter interface {
	WriteStaged(ctx context.Context, e Epoch, stream string, files []StagedFile) error
}

// StagedFile is one Parquet file of a staged epoch. The staging package
// reads it.
type StagedFile struct {
	// Below the pipeline's staging prefix.
	Path  string
	Rows  uint64
	Bytes uint64
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
// It runs before Recover with the restored epoch's schemas, and before each
// epoch whose schemas changed is recorded.
//
// A destination that takes holds answers with the columns it didn't add or
// change, having applied the rest, instead of failing, and with what each
// stream's table can't take as it stands. The platform holds those columns.
// Others return the zero SchemaApplied.
type SchemaApplier interface {
	ApplySchema(ctx context.Context, streams []StreamSchema) (SchemaApplied, error)
}

type SchemaApplied struct {
	Refused []RefusedColumn
	// By stream.
	Limits map[string]TableLimits
}

type RefusedColumn struct {
	Stream string
	// Its destination name.
	Column string
	Reason string
}

// TableLimits name columns by their destination names.
type TableLimits struct {
	// Columns that can't take a null, which a held cell is unless the
	// pipeline gives a held value.
	NotNull []string
	// Columns that can't be held, such as partition columns.
	Unholdable []string
	// Columns whose type can't change, such as those a view depends on.
	Fixed []string
	// The columns the table has, as its limit counts them, and that limit;
	// 0 for none.
	Columns, MaxColumns int
}

// Patcher is implemented by the writers of destinations that take holds.
// Patch adds a batch of patches to epoch e: each row fills the cells its
// HeldColumn lists, in the target row its key (Merge) or its _openetl_id
// (Append) names, where the target still holds the version the row's
// _openetl_id names. Its other columns are the stream's key, _openetl_id,
// _openetl_seq, _openetl_synced_at and the columns it fills, each filling
// the held column of its name or the one its FillsKey metadata names. It is
// called from the goroutine that calls Write, and the batch is released
// after it returns.
type Patcher interface {
	Patch(ctx context.Context, e Epoch, stream string, batch arrow.RecordBatch) error
}

// ErrFenced is returned when a newer generation owns the pipeline.
var ErrFenced = errors.New("destination: fenced by a newer generation")
