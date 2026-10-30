package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrFenced is returned when a newer generation has claimed the pipeline. The
// attempt that gets it must stop and never claim again.
var ErrFenced = errors.New("checkpoint: fenced by a newer generation")

// claimAttempts bounds how often Claim retries after losing a race.
const claimAttempts = 16

// RecordRef points to one checkpoint record.
type RecordRef struct {
	Epoch      uint64 `json:"epoch"`
	Generation uint64 `json:"generation"`
}

// Head is the content of the log's HEAD object.
type Head struct {
	// Generation is the generation that owns the pipeline. Claiming raises it.
	Generation uint64 `json:"generation"`
	// Owner identifies the attempt that claimed the generation. It makes every
	// HEAD write unique, so a write with an unknown outcome can be resolved by
	// reading HEAD back even when versions are derived from content.
	Owner string `json:"owner"`
	// Record is the committed checkpoint, or nil before the first one.
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

// StreamState is one stream's resume state in a checkpoint.
type StreamState struct {
	State []byte `json:"state,omitempty"`
}

// Record is one checkpoint. Records are immutable once written.
type Record struct {
	Epoch      uint64 `json:"epoch"`
	Generation uint64 `json:"generation"`
	// Previous is the record HEAD pointed to when this one was committed. On
	// a lost epoch, recovery moves HEAD back to it.
	Previous *RecordRef             `json:"previous,omitempty"`
	Streams  map[string]StreamState `json:"streams"`
	// Staged is the sink's staged descriptor for this epoch.
	Staged []byte `json:"staged,omitempty"`
	// Snapshots are the engine's operator state snapshot URIs.
	Snapshots []string `json:"snapshots,omitempty"`
	// Forced lists the streams whose barrier was placed without a resume point.
	// Their state is the last resume point before the barrier, so a restore
	// from this record re-reads records that are already applied.
	Forced    []string  `json:"forced,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// Ref returns a pointer to r.
func (r *Record) Ref() RecordRef { return RecordRef{Epoch: r.Epoch, Generation: r.Generation} }

// Log is one pipeline's checkpoint log, under {org}/{pipeline}/_checkpoints/.
type Log struct {
	store  Store
	prefix string
}

// NewLog returns the checkpoint log for a pipeline.
func NewLog(store Store, org, pipeline string) *Log {
	return &Log{store: store, prefix: org + "/" + pipeline + "/_checkpoints/"}
}

func (l *Log) headKey() string { return l.prefix + "HEAD" }

// Record keys are zero-padded so that listing returns them in epoch order.
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

// Record reads one checkpoint record.
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

// Claim makes owner the owner of the pipeline under a new generation, one
// higher than HEAD's. Claiming needs only the log, never the control plane.
// owner must be unique per attempt.
func (l *Log) Claim(ctx context.Context, owner string) (*Session, error) {
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
			continue // lost a race; claim on top of the winner
		}
		// The outcome is unknown. Only our write can have produced next,
		// because it carries our owner.
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

// Generation returns the session's generation.
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

// Commit records checkpoint rec and moves HEAD to it. It sets rec.Generation
// and rec.Previous from the session. It returns ErrFenced if a newer
// generation has claimed the pipeline. On any other error the checkpoint may
// or may not be committed; calling Commit again with the same rec is safe.
func (s *Session) Commit(ctx context.Context, rec *Record) error {
	rec.Generation = s.head.Generation
	rec.Previous = s.head.Record
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	ref := rec.Ref()
	key := s.log.recordKey(ref)
	if _, werr := s.log.store.Create(ctx, key, data); werr != nil {
		// Rejected because it exists, or outcome unknown. Either way the record
		// may be ours: from this call, or an earlier one with the same rec.
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

// Rewind moves HEAD back to an earlier record (nil: no record), for an epoch
// the sink reported lost.
func (s *Session) Rewind(ctx context.Context, to *RecordRef) error {
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
	// The swap didn't report success: it was rejected, or its outcome is
	// unknown. Read HEAD to find out which of three things happened.
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
