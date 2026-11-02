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
}

// QuarantineFile holds the records of one stream the engine quarantined in
// an epoch.
type QuarantineFile struct {
	Stream string `json:"stream"`
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
	Previous  *RecordRef             `json:"previous,omitempty"`
	Streams   map[string]StreamState `json:"streams"`
	Staged    []byte                 `json:"staged,omitempty"`
	Snapshots []Snapshot             `json:"snapshots,omitempty"`
	// Records quarantined in this epoch. Files no record lists are orphans.
	Quarantine []QuarantineFile `json:"quarantine,omitempty"`
	// Streams whose barrier was placed without a resume point. Restoring from
	// this record re-reads rows that may already be applied.
	Forced []string `json:"forced,omitempty"`
	// The sync that wrote it. A later attempt of the same sync doesn't read
	// the streams it completed.
	Sync      string    `json:"sync,omitempty"`
	CreatedAt time.Time `json:"created_at"`
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
