package checkpoint

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/openetl-io/openetl/internal/platform/errs"
)

// ErrFenced: a newer generation has claimed the pipeline. The attempt must
// stop and never claim again.
var ErrFenced = errors.New("checkpoint: fenced by a newer generation")

// ErrUnreadable: a record HEAD or another record points to is missing, or
// isn't a record of that epoch and generation.
var ErrUnreadable = errors.New("checkpoint: record unreadable")

// ErrNewerFormat: a newer version of openetld wrote HEAD. Reading on would
// drop the fields this version doesn't know, and the next commit would write
// them away for good, so it's refused.
var ErrNewerFormat = errors.New("checkpoint: the log was written by a newer version")

// currentFormat is the checkpoint log's format. It goes up when an older
// version reading what's written would lose something.
const currentFormat = 1

const claimAttempts = 16

type RecordRef struct {
	Epoch      uint64 `json:"epoch"`
	Generation uint64 `json:"generation"`
}

type Head struct {
	// The format HEAD and its records are written in; 0 before it was
	// recorded.
	Format     uint32 `json:"format,omitempty"`
	Generation uint64 `json:"generation"`
	// Owner is unique per attempt, so every HEAD write is distinct and an
	// unknown outcome can be resolved by reading HEAD back.
	Owner string `json:"owner"`
	// Nil before the first checkpoint.
	Record *RecordRef `json:"record,omitempty"`
	// The record's Previous, so a record that can't be read can be stepped
	// back from. Nil with First, or when HEAD doesn't know, as one written
	// before it did.
	Previous *RecordRef `json:"previous,omitempty"`
	// Record is the log's first.
	First bool `json:"first,omitempty"`
}

func (h Head) equal(o Head) bool {
	if h.Generation != o.Generation || h.Owner != o.Owner || h.First != o.First {
		return false
	}
	return sameRef(h.Record, o.Record) && sameRef(h.Previous, o.Previous)
}

func sameRef(a, b *RecordRef) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// Before is the record before HEAD's, and whether HEAD knows it: nil with
// true when HEAD's is the log's first.
func (h Head) Before() (*RecordRef, bool) {
	return h.Previous, h.Previous != nil || h.First
}

type StreamState struct {
	// The state of the stream's one split, the whole stream; nil with
	// Splits.
	State []byte `json:"state,omitempty"`
	// The stream's splits, where it has any but the whole stream.
	Splits []SplitState `json:"splits,omitempty"`
	// The engine's schema for the stream at this checkpoint: an Arrow IPC
	// stream holding only the schema.
	Schema []byte `json:"schema,omitempty"`
	// When the stream's current read started, in microseconds since the Unix
	// epoch. Rows read without a cursor carry it as their sequence.
	ReadStart int64 `json:"read_start,omitempty"`
	// The stream completed: every split did, and their states are where its
	// next sync starts.
	Complete bool `json:"complete,omitempty"`
	// Rows the source emitted in the epoch, quarantined ones included.
	Rows uint64 `json:"rows,omitempty"`
	// The schema the destination has, where columns are held whole; nil
	// where it's Schema.
	Delivered []byte `json:"delivered,omitempty"`
	// Columns and cells held back.
	Holds *Holds `json:"holds,omitempty"`
}

// WholeStream is the ID of the split that is the whole stream. A stream
// with only it keeps its state in StreamState.State, as records from before
// splits do.
const WholeStream = ""

// SplitState is one of a stream's splits: a resumable slice of its read,
// such as a key range or a partition.
type SplitState struct {
	ID    string `json:"id"`
	State []byte `json:"state,omitempty"`
	// The split completed, and State is where its part of the next sync
	// starts.
	Complete bool `json:"complete,omitempty"`
	// As Record.Forced and Record.Reread, for the split.
	Forced bool `json:"forced,omitempty"`
	Reread bool `json:"reread,omitempty"`
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

// Snapshot is one operator's state at a checkpoint, for the key groups it
// covers.
type Snapshot struct {
	Operator string `json:"operator"`
	URL      string `json:"url"`
	// Nil: all of them.
	KeyGroups *KeyGroupRange `json:"key_groups,omitempty"`
}

// KeyGroups is how many key groups a pipeline has, unless its record says
// otherwise: records from before them have this many.
const KeyGroups = 256

// KeyGroupRange is key groups First to Last, inclusive.
type KeyGroupRange struct {
	First int `json:"first"`
	Last  int `json:"last"`
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
	// The pipeline's key groups; 0 in records from before them, which have
	// KeyGroups.
	KeyGroups int `json:"key_groups,omitempty"`
	// What the destination's writer lanes staged for the epoch, one each,
	// which commit and recovery get. Records from before them hold one.
	Committables [][]byte `json:"committables"`
	// The files the dataflow staged for a destination that loads them, by
	// the destination's stream.
	Files     map[string][]StagedFile `json:"files,omitempty"`
	Snapshots []Snapshot              `json:"snapshots,omitempty"`
	// Records quarantined in this epoch. Files no record lists are orphans.
	Quarantine []QuarantineFile `json:"quarantine,omitempty"`
	// Streams whose barrier was placed without a resume point, on any of
	// their splits. Restoring from this record re-reads rows that may already
	// be applied.
	Forced []string `json:"forced,omitempty"`
	// Streams that read rows again in this epoch, on any of their splits:
	// after a restore from a forced barrier, before their next resume point.
	// Those rows may repeat rows an earlier epoch committed.
	Reread []string `json:"reread,omitempty"`
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
	Flink *FlinkCheckpoint `json:"flink,omitempty"`
	// The newest reset the log has taken, carried from record to record: an
	// attempt reads again the streams of resets newer than it.
	Reset     string    `json:"reset,omitempty"`
	CreatedAt time.Time `json:"created_at"`
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

// UnmarshalJSON reads a record from before committables as holding one.
func (r *Record) UnmarshalJSON(data []byte) error {
	type record Record
	var v struct {
		record
		Staged []byte `json:"staged"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	*r = Record(v.record)
	if len(r.Committables) == 0 {
		r.Committables = [][]byte{v.Staged}
	}
	return nil
}

// KeyGroupCount is the pipeline's key groups as the record has them.
func (r *Record) KeyGroupCount() int { return cmp.Or(r.KeyGroups, KeyGroups) }

// Splits returns a stream's splits: its one split, the whole stream, where
// it has no others. Nil if the record doesn't have the stream.
func (r *Record) Splits(stream string) []SplitState {
	st, ok := r.Streams[stream]
	switch {
	case !ok:
		return nil
	case len(st.Splits) > 0:
		return st.Splits
	}
	return []SplitState{{ID: WholeStream, State: st.State, Complete: st.Complete,
		Forced: slices.Contains(r.Forced, stream), Reread: slices.Contains(r.Reread, stream)}}
}

// SetSplits sets a stream's splits, and the record's marks for it, keeping
// its other fields. A stream with only the whole stream keeps its state in
// State.
func (r *Record) SetSplits(stream string, splits []SplitState) {
	st := r.Streams[stream]
	st.State, st.Splits, st.Complete = nil, nil, len(splits) > 0
	forced, reread := false, false
	for _, sp := range splits {
		st.Complete = st.Complete && sp.Complete
		forced, reread = forced || sp.Forced, reread || sp.Reread
	}
	if len(splits) == 1 && splits[0].ID == WholeStream {
		st.State = splits[0].State
	} else {
		st.Splits = splits
	}
	r.Streams[stream] = st
	r.Forced = slices.DeleteFunc(r.Forced, func(s string) bool { return s == stream })
	r.Reread = slices.DeleteFunc(r.Reread, func(s string) bool { return s == stream })
	if forced {
		r.Forced = append(r.Forced, stream)
		slices.Sort(r.Forced)
	}
	if reread {
		r.Reread = append(r.Reread, stream)
		slices.Sort(r.Reread)
	}
}

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
	if h.Format > currentFormat {
		return Head{}, "", errs.Wrap(errs.ConfigInvalid, fmt.Errorf("%w: it's format %d, and this version reads up to format %d; run this pipeline on the newer version", ErrNewerFormat, h.Format, currentFormat), "")
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

// Record reads a record. One that's missing, can't be decoded or is of
// another epoch or generation fails with ErrUnreadable.
func (l *Log) Record(ctx context.Context, ref RecordRef) (*Record, error) {
	data, _, err := l.store.Get(ctx, l.recordKey(ref))
	if errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("%w: record %d-g%d is missing (%w)", ErrUnreadable, ref.Epoch, ref.Generation, err)
	}
	if err != nil {
		return nil, fmt.Errorf("checkpoint: read record %d-g%d: %w", ref.Epoch, ref.Generation, err)
	}
	var r Record
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("%w: decode record %d-g%d: %w", ErrUnreadable, ref.Epoch, ref.Generation, err)
	}
	if r.Ref() != ref {
		return nil, fmt.Errorf("%w: record %d-g%d holds %d-g%d", ErrUnreadable, ref.Epoch, ref.Generation, r.Epoch, r.Generation)
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
			next = Head{Generation: cur.Generation + 1, Owner: owner, Record: cur.Record, Previous: cur.Previous, First: cur.First}
		}
		next.Format = currentFormat
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
	return s.swapHead(ctx, Head{Generation: s.head.Generation, Owner: s.owner, Record: &ref, Previous: rec.Previous, First: rec.Previous == nil})
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

// Rewind moves HEAD back to an earlier record, or to none. HEAD then
// doesn't know the record before it, as RewindTo's does.
func (s *Session) Rewind(ctx context.Context, to *RecordRef) (err error) {
	defer func(start time.Time) { recordWrite(ctx, "rewind", start, err) }(time.Now())
	return s.swapHead(ctx, Head{Generation: s.head.Generation, Owner: s.owner, Record: to})
}

// RewindTo moves HEAD back to an earlier record, or to none.
func (s *Session) RewindTo(ctx context.Context, to *Record) (err error) {
	defer func(start time.Time) { recordWrite(ctx, "rewind", start, err) }(time.Now())
	next := Head{Generation: s.head.Generation, Owner: s.owner}
	if to != nil {
		ref := to.Ref()
		next.Record, next.Previous, next.First = &ref, to.Previous, to.Previous == nil
	}
	return s.swapHead(ctx, next)
}

func (s *Session) swapHead(ctx context.Context, next Head) error {
	next.Format = currentFormat
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
