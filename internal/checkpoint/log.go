package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrFenced: a newer generation has claimed the pipeline. The attempt must
// stop and never claim again.
var ErrFenced = errors.New("checkpoint: fenced by a newer generation")

const claimAttempts = 16

type RecordRef struct {
	Epoch      uint64 `json:"epoch"`
	Generation uint64 `json:"generation"`
}

type Head struct {
	Generation uint64 `json:"generation"`
	// Owner is unique per attempt, so every HEAD write is distinct and an
	// unknown outcome can be resolved by reading HEAD back.
	Owner string `json:"owner"`
	// Nil before the first checkpoint.
	Record *RecordRef `json:"record,omitempty"`
}

func (h Head) equal(o Head) bool {
	if h.Generation != o.Generation || h.Owner != o.Owner {
		return false
	}
	if h.Record == nil || o.Record == nil {
		return h.Record == nil && o.Record == nil
	}
	return *h.Record == *o.Record
}

type StreamState struct {
	State []byte `json:"state,omitempty"`
	// The engine's schema for the stream at this checkpoint: an Arrow IPC
	// stream holding only the schema.
	Schema []byte `json:"schema,omitempty"`
	// When the stream's current read started, in microseconds since the Unix
	// epoch. Rows read without a cursor carry it as their sequence.
	ReadStart int64 `json:"read_start,omitempty"`
	// The stream completed, and State is where its next sync starts.
	Complete bool `json:"complete,omitempty"`
	// Rows the source emitted in the epoch, quarantined ones included.
	Rows uint64 `json:"rows,omitempty"`
	// The schema the destination has, where columns are held whole; nil
	// where it's Schema.
	Delivered []byte `json:"delivered,omitempty"`
	// Columns and cells held back (ADR-0025).
	Holds *Holds `json:"holds,omitempty"`
}

// Holds are a stream's held columns and cells, where their values are, and
// how far a release has got.
type Holds struct {
	Columns []ColumnHold `json:"columns,omitempty"`
	// By column.
	Cells map[string]CellHold `json:"cells,omitempty"`
	// The epochs with held files, by generation. Records are pruned, so a
	// release finds the files by these, and a file outside them is an
	// orphan.
	Epochs []HeldEpochs `json:"epochs,omitempty"`
	Replay *Replay      `json:"replay,omitempty"`
}

// ColumnHold is a column held whole.
type ColumnHold struct {
	Column string `json:"column"`
	// The field the destination has, as an Arrow IPC stream holding a
	// schema with only it; nil if it has none.
	Delivered []byte    `json:"delivered,omitempty"`
	Reason    string    `json:"reason"`
	Since     RecordRef `json:"since"`
	Started   time.Time `json:"started,omitzero"`
	// Its values are dropped, and the column stays as the destination has
	// it.
	Discarded bool       `json:"discarded,omitempty"`
	Cause     *HoldCause `json:"cause,omitempty"`
	Refused   *Refused   `json:"refused,omitempty"`
}

// CellHold is a column's held cells.
type CellHold struct {
	Cells uint64 `json:"cells"`
	// The first cell's.
	Reason  string    `json:"reason"`
	Since   RecordRef `json:"since"`
	Started time.Time `json:"started,omitzero"`
	Refused *Refused  `json:"refused,omitempty"`
}

// HoldCause is why a column is held whole.
type HoldCause struct {
	Kind HoldKind `json:"kind,omitempty"`
	// HoldPolicy's schema policy.
	Policy string `json:"policy,omitempty"`
	// HoldGroup's column.
	With string `json:"with,omitempty"`
	// The column's source name.
	Source string `json:"source,omitempty"`
	// The change, in logical types. From is empty for a column the
	// destination doesn't have.
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
	// From the destination's refusal: what it can't do, the change in its
	// type names, and a statement that makes it take the change.
	Refusal  string `json:"refusal,omitempty"`
	FromType string `json:"from_type,omitempty"`
	ToType   string `json:"to_type,omitempty"`
	Fix      string `json:"fix,omitempty"`
}

type HoldKind string

const (
	HoldPolicy      HoldKind = "policy"
	HoldRefused     HoldKind = "refused"
	HoldFixed       HoldKind = "fixed"
	HoldColumnLimit HoldKind = "column_limit"
	HoldGroup       HoldKind = "group"
)

// Refused is a release that didn't clear its hold: the destination refused
// the column again, or released cells still don't fit. The hold stays, and
// the release isn't taken again.
type Refused struct {
	// The release's ID.
	Release string `json:"release"`
	Reason  string `json:"reason"`
}

// HeldEpochs are generation Generation's epochs From to To, inclusive.
type HeldEpochs struct {
	Generation uint64 `json:"generation"`
	From       uint64 `json:"from"`
	To         uint64 `json:"to"`
}

// Replay is a release being patched in: the released columns, the held
// files read in order, and where the replay has got.
type Replay struct {
	Columns []string `json:"columns"`
	Files   []string `json:"files"`
	File    int      `json:"file,omitempty"`
	Row     uint64   `json:"row,omitempty"`
	// Cells read that still don't fit, which stay held.
	StillHeld uint64 `json:"still_held,omitempty"`
	// The released columns' cells held since the replay began, which aren't
	// in Files and stay held once it ends.
	Held map[string]CellHold `json:"held,omitempty"`
	// Released column holds whose values go to a new version of the
	// column, col__v2; the column keeps its delivered type.
	Versioned []ColumnHold `json:"versioned,omitempty"`
	// The IDs of the releases of cell holds, by column.
	CellReleases map[string]string `json:"cell_releases,omitempty"`
}

// HeldDir holds a pipeline's held values, next to its checkpoints:
// {org}/{pipeline}/_held/{g}/{N}/{stream}/part-{k}.parquet, with the
// stream's name percent-encoded.
const HeldDir = "_held"

// OutputState is one of a transform's outputs at a checkpoint.
type OutputState struct {
	// As StreamState's.
	Schema []byte `json:"schema,omitempty"`
	// Rows the transform output in the epoch, quarantined ones included.
	Rows uint64 `json:"rows,omitempty"`
}

// QuarantineFile holds the records of one stream the engine quarantined in
// an epoch.
type QuarantineFile struct {
	Stream string `json:"stream"`
	// Stream is one of a transform's outputs.
	Output bool   `json:"output,omitempty"`
	URL    string `json:"url"`
	Rows   uint64 `json:"rows"`
}

// Snapshot is one operator's state at a checkpoint.
type Snapshot struct {
	Operator string `json:"operator"`
	URL      string `json:"url"`
}

// Record is one checkpoint. Records are immutable once written.
type Record struct {
	Epoch      uint64 `json:"epoch"`
	Generation uint64 `json:"generation"`
	// What HEAD pointed to before this record. Recovery rewinds to it when the
	// epoch is lost.
	Previous *RecordRef `json:"previous,omitempty"`
	// The source's streams. With a transform, they're its inputs.
	Streams map[string]StreamState `json:"streams"`
	// A transform's outputs, which are the destination's streams. Nil
	// without a transform.
	Outputs map[string]OutputState `json:"outputs,omitempty"`
	Staged  []byte                 `json:"staged,omitempty"`
	// The files the dataflow staged for a destination that loads them, by
	// the destination's stream.
	Files     map[string][]StagedFile `json:"files,omitempty"`
	Snapshots []Snapshot              `json:"snapshots,omitempty"`
	// Records quarantined in this epoch. Files no record lists are orphans.
	Quarantine []QuarantineFile `json:"quarantine,omitempty"`
	// Streams whose barrier was placed without a resume point. Restoring from
	// this record re-reads rows that may already be applied.
	Forced []string `json:"forced,omitempty"`
	// The sync that wrote it. A later attempt of the same sync doesn't read
	// the streams it completed.
	Sync string `json:"sync,omitempty"`
	// The record the sync started after, the zero ref if none, and the rows
	// each stream's source emitted in the sync so far. Pruning drops the
	// sync's earlier records, so this record alone says what the sync
	// committed. Nil in records from before them, and without a sync.
	SyncStart *RecordRef        `json:"sync_start,omitempty"`
	SyncRows  map[string]uint64 `json:"sync_rows,omitempty"`
	// In a Flink pipeline, the Flink checkpoint the epoch ends at.
	Flink     *FlinkCheckpoint `json:"flink,omitempty"`
	CreatedAt time.Time        `json:"created_at"`
}

// FlinkCheckpoint is where a Flink pipeline's record stands in Flink's
// checkpoints. IDs repeat across jobs, so Job qualifies Checkpoint.
type FlinkCheckpoint struct {
	Job        string `json:"job"`
	Checkpoint int64  `json:"checkpoint"`
	// A stop's savepoint: the directory it's written below. Empty for a
	// checkpoint.
	Savepoint string `json:"savepoint,omitempty"`
	// The path Job was deployed from; empty for none.
	From string `json:"from,omitempty"`
}

// StagedFile is one of an epoch's staged Parquet files, its path below the
// pipeline's staging prefix.
type StagedFile struct {
	Path  string `json:"path"`
	Rows  uint64 `json:"rows"`
	Bytes uint64 `json:"bytes"`
}

func (r *Record) Ref() RecordRef { return RecordRef{Epoch: r.Epoch, Generation: r.Generation} }

// Log is one pipeline's checkpoint log, under {org}/{pipeline}/_checkpoints/.
type Log struct {
	store Store
	// {org}/{pipeline}/
	root   string
	prefix string
}

func NewLog(store Store, org, pipeline string) *Log {
	root := org + "/" + pipeline + "/"
	return &Log{store: store, root: root, prefix: root + "_checkpoints/"}
}

func (l *Log) headKey() string { return l.prefix + "HEAD" }

// Zero-padded, so listings sort by epoch.
func (l *Log) recordKey(ref RecordRef) string {
	return fmt.Sprintf("%s%020d-g%020d.json", l.prefix, ref.Epoch, ref.Generation)
}

func (l *Log) readHead(ctx context.Context) (Head, Version, error) {
	data, v, err := l.store.Get(ctx, l.headKey())
	if err != nil {
		return Head{}, "", err
	}
	var h Head
	if err := json.Unmarshal(data, &h); err != nil {
		return Head{}, "", fmt.Errorf("checkpoint: decode HEAD: %w", err)
	}
	return h, v, nil
}

// Latest is HEAD's record, without claiming the pipeline; nil before the
// first checkpoint.
func (l *Log) Latest(ctx context.Context) (*Record, error) {
	h, _, err := l.readHead(ctx)
	if errors.Is(err, ErrNotFound) || (err == nil && h.Record == nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return l.Record(ctx, *h.Record)
}

func (l *Log) Record(ctx context.Context, ref RecordRef) (*Record, error) {
	data, _, err := l.store.Get(ctx, l.recordKey(ref))
	if err != nil {
		return nil, fmt.Errorf("checkpoint: read record %d-g%d: %w", ref.Epoch, ref.Generation, err)
	}
	var r Record
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("checkpoint: decode record %d-g%d: %w", ref.Epoch, ref.Generation, err)
	}
	return &r, nil
}

// Claim takes the pipeline for owner under HEAD's generation plus one. owner
// must be unique per attempt.
func (l *Log) Claim(ctx context.Context, owner string) (sess *Session, err error) {
	defer func(start time.Time) { recordWrite(ctx, "claim", start, err) }(time.Now())
	for range claimAttempts {
		cur, v, err := l.readHead(ctx)
		var (
			next  Head
			nv    Version
			werr  error
			fresh = errors.Is(err, ErrNotFound)
		)
		switch {
		case fresh:
			next = Head{Generation: 1, Owner: owner}
		case err != nil:
			return nil, fmt.Errorf("checkpoint: claim: %w", err)
		default:
			next = Head{Generation: cur.Generation + 1, Owner: owner, Record: cur.Record}
		}
		data, err := json.Marshal(next)
		if err != nil {
			return nil, err
		}
		if fresh {
			nv, werr = l.store.Create(ctx, l.headKey(), data)
		} else {
			nv, werr = l.store.Swap(ctx, l.headKey(), data, v)
		}
		if werr == nil {
			return &Session{log: l, owner: owner, head: next, version: nv}, nil
		}
		if errors.Is(werr, ErrPrecondition) {
			recordConflict(ctx, "claim")
			continue // lost a race; claim on top of the winner
		}
		// Unknown outcome. Only our write carries our owner.
		got, gv, rerr := l.readHead(ctx)
		if errors.Is(rerr, ErrNotFound) {
			continue // the first create didn't land
		}
		if rerr != nil {
			return nil, fmt.Errorf("checkpoint: claim outcome unknown (%w), re-read failed: %w", werr, rerr)
		}
		if got.equal(next) {
			return &Session{log: l, owner: owner, head: next, version: gv}, nil
		}
	}
	return nil, fmt.Errorf("checkpoint: claim: gave up after %d attempts", claimAttempts)
}

// Session is an attempt's ownership of a pipeline's log. Its methods must not
// be called concurrently.
type Session struct {
	log     *Log
	owner   string
	head    Head
	version Version
}

func (s *Session) Generation() uint64 { return s.head.Generation }

// Head returns HEAD as this session last wrote or read it.
func (s *Session) Head() Head { return s.head }

// Load returns the record HEAD points to, or nil if there is none.
func (s *Session) Load(ctx context.Context) (*Record, error) {
	if s.head.Record == nil {
		return nil, nil
	}
	return s.log.Record(ctx, *s.head.Record)
}

// Commit writes rec, setting its Generation and Previous, and moves HEAD to
// it. On an error other than ErrFenced the outcome is unknown, and calling
// Commit again with the same rec is safe.
func (s *Session) Commit(ctx context.Context, rec *Record) (err error) {
	defer func(start time.Time) { recordWrite(ctx, "commit", start, err) }(time.Now())
	rec.Generation = s.head.Generation
	rec.Previous = s.head.Record
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	ref := rec.Ref()
	key := s.log.recordKey(ref)
	if _, werr := s.log.store.Create(ctx, key, data); werr != nil {
		// It may exist from an earlier call with the same rec.
		existing, _, gerr := s.log.store.Get(ctx, key)
		switch {
		case gerr == nil && !bytes.Equal(existing, data):
			return fmt.Errorf("checkpoint: record %d-g%d exists with different content", ref.Epoch, ref.Generation)
		case gerr != nil:
			return fmt.Errorf("checkpoint: write record %d-g%d (%w), re-read: %w", ref.Epoch, ref.Generation, werr, gerr)
		}
	}
	return s.swapHead(ctx, Head{Generation: s.head.Generation, Owner: s.owner, Record: &ref})
}

// Check reads HEAD, and returns ErrFenced if another attempt has claimed the
// pipeline since this session did.
func (s *Session) Check(ctx context.Context) error {
	cur, _, err := s.log.readHead(ctx)
	if err != nil {
		return fmt.Errorf("checkpoint: read HEAD: %w", err)
	}
	if cur.Generation != s.head.Generation || cur.Owner != s.owner {
		return fmt.Errorf("%w: HEAD is generation %d (%s), this session is %d", ErrFenced, cur.Generation, cur.Owner, s.head.Generation)
	}
	return nil
}

// Rewind moves HEAD back to an earlier record, or to none.
func (s *Session) Rewind(ctx context.Context, to *RecordRef) (err error) {
	defer func(start time.Time) { recordWrite(ctx, "rewind", start, err) }(time.Now())
	return s.swapHead(ctx, Head{Generation: s.head.Generation, Owner: s.owner, Record: to})
}

func (s *Session) swapHead(ctx context.Context, next Head) error {
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	v, werr := s.log.store.Swap(ctx, s.log.headKey(), data, s.version)
	if werr == nil {
		s.head, s.version = next, v
		return nil
	}
	if errors.Is(werr, ErrPrecondition) {
		recordConflict(ctx, "swap")
	}
	// Rejected or unknown: HEAD says which.
	cur, cv, err := s.log.readHead(ctx)
	if err != nil {
		return fmt.Errorf("checkpoint: HEAD swap failed (%w), re-read failed: %w", werr, err)
	}
	switch {
	case cur.equal(next): // our write landed
		s.head, s.version = next, cv
		return nil
	case cur.Generation != s.head.Generation || cur.Owner != s.owner: // replaced
		return fmt.Errorf("%w: HEAD is generation %d (%s), this session is %d", ErrFenced, cur.Generation, cur.Owner, s.head.Generation)
	case cur.equal(s.head): // our write didn't land; the caller may retry
		return fmt.Errorf("checkpoint: HEAD swap not applied: %w", werr)
	default:
		return fmt.Errorf("checkpoint: HEAD changed under this session to %+v", cur)
	}
}
