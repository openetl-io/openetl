package checkpoint

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

// QuarantineDir holds a pipeline's quarantined records, next to its
// checkpoints: {org}/{pipeline}/_quarantine/{g}/{N}/{stream}.jsonl.
const QuarantineDir = "_quarantine"

// QuarantineOutputs holds, below QuarantineDir, what a transform's outputs
// quarantined, at {g}/{N}/{output}.jsonl, apart from its inputs'.
const QuarantineOutputs = "_outputs"

// How many earlier generations Prune looks through for quarantined files of
// epochs that never committed.
const orphanGenerations = 16

// Prune deletes what no later attempt can need, once the session has
// recovered to restored (nil if it restored nothing):
//   - records more than keep back along the chain from restored;
//   - records and quarantined files that earlier generations wrote for
//     epochs after restored. Those epochs never committed.
//
// Quarantined files of committed epochs are kept. keep must be at least 2,
// because an attempt reads HEAD and the record before it. Prune stops at the
// first error, and a later attempt prunes what it left.
func (s *Session) Prune(ctx context.Context, restored *Record, keep int) (deleted int, err error) {
	if keep < 2 {
		return 0, fmt.Errorf("checkpoint: prune must keep at least 2 records, not %d", keep)
	}
	gen := s.head.Generation
	var after uint64
	if restored != nil {
		after = restored.Epoch
	}
	keys, err := s.log.store.List(ctx, s.log.prefix)
	if err != nil {
		return 0, fmt.Errorf("checkpoint: prune: %w", err)
	}
	var doomed []string
	type kept struct {
		key   string
		epoch uint64
	}
	var older []kept
	for _, k := range keys {
		ref, ok := parseRecordKey(strings.TrimPrefix(k, s.log.prefix))
		switch {
		case !ok:
		case ref.Epoch > after:
			if ref.Generation < gen {
				doomed = append(doomed, k)
			}
		default:
			older = append(older, kept{k, ref.Epoch})
		}
	}
	if restored != nil && len(older) > keep {
		floor, err := s.log.floor(ctx, restored, keep)
		if err != nil {
			return 0, fmt.Errorf("checkpoint: prune: %w", err)
		}
		for _, r := range older {
			if r.epoch < floor {
				doomed = append(doomed, r.key)
			}
		}
	}

	lo := uint64(1)
	if gen > orphanGenerations {
		lo = gen - orphanGenerations
	}
	if restored != nil {
		lo = max(lo, restored.Generation)
	}
	for g := lo; g < gen; g++ {
		for _, dir := range []string{QuarantineDir, QuarantineDir + "/" + QuarantineOutputs} {
			prefix := fmt.Sprintf("%s%s/%d/", s.log.root, dir, g)
			keys, err := s.log.store.List(ctx, prefix)
			if err != nil {
				return 0, fmt.Errorf("checkpoint: prune: %w", err)
			}
			for _, k := range keys {
				n, _, ok := strings.Cut(strings.TrimPrefix(k, prefix), "/")
				if epoch, err := strconv.ParseUint(n, 10, 64); ok && err == nil && epoch > after {
					doomed = append(doomed, k)
				}
			}
		}
	}

	for _, k := range doomed {
		if err := s.log.store.Delete(ctx, k); err != nil {
			return deleted, fmt.Errorf("checkpoint: prune %s: %w", k, err)
		}
		deleted++
	}
	return deleted, nil
}

// floor returns the epoch of the keep-th record along the chain from r, or
// of the oldest one left.
func (l *Log) floor(ctx context.Context, r *Record, keep int) (uint64, error) {
	for i := 1; i < keep && r.Previous != nil; i++ {
		prev, err := l.Record(ctx, *r.Previous)
		if errors.Is(err, ErrNotFound) {
			break
		}
		if err != nil {
			return 0, err
		}
		r = prev
	}
	return r.Epoch, nil
}

func parseRecordKey(name string) (RecordRef, bool) {
	e, g, ok := strings.Cut(strings.TrimSuffix(name, ".json"), "-g")
	if !ok || !strings.HasSuffix(name, ".json") {
		return RecordRef{}, false
	}
	epoch, err1 := strconv.ParseUint(e, 10, 64)
	gen, err2 := strconv.ParseUint(g, 10, 64)
	return RecordRef{Epoch: epoch, Generation: gen}, err1 == nil && err2 == nil
}

// heldFile is one held file's place: its key and its path below the held
// prefix, its generation and epoch, and its stream.
type heldFile struct {
	key, rel string
	g, n     uint64
	stream   string
}

func (l *Log) heldFiles(ctx context.Context, prefix string) ([]heldFile, error) {
	keys, err := l.store.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	root := l.root + HeldDir + "/"
	var out []heldFile
	for _, k := range keys {
		rel := strings.TrimPrefix(k, root)
		parts := strings.Split(rel, "/")
		if len(parts) != 4 {
			continue
		}
		g, err1 := strconv.ParseUint(parts[0], 10, 64)
		n, err2 := strconv.ParseUint(parts[1], 10, 64)
		stream, err3 := url.PathUnescape(parts[2])
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}
		out = append(out, heldFile{key: k, rel: rel, g: g, n: n, stream: stream})
	}
	return out, nil
}

func (h HeldEpochs) holds(g, n uint64) bool { return h.Generation == g && n >= h.From && n <= h.To }

// HeldFiles lists a stream's held files in epochs, by their paths below the
// held prefix, in epoch order.
func (l *Log) HeldFiles(ctx context.Context, stream string, epochs []HeldEpochs) ([]string, error) {
	gens := map[uint64]bool{}
	for _, e := range epochs {
		gens[e.Generation] = true
	}
	var files []heldFile
	for _, g := range slices.Sorted(maps.Keys(gens)) {
		fs, err := l.heldFiles(ctx, fmt.Sprintf("%s%s/%d/", l.root, HeldDir, g))
		if err != nil {
			return nil, fmt.Errorf("checkpoint: list held files: %w", err)
		}
		for _, f := range fs {
			if f.stream == stream && slices.ContainsFunc(epochs, func(e HeldEpochs) bool { return e.holds(f.g, f.n) }) {
				files = append(files, f)
			}
		}
	}
	slices.SortStableFunc(files, func(a, b heldFile) int {
		return cmp.Or(cmp.Compare(a.g, b.g), cmp.Compare(a.n, b.n), strings.Compare(a.rel, b.rel))
	})
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = f.rel
	}
	return out, nil
}

// PruneHeld deletes the held files of earlier generations that no hold in
// restored names: those of epochs that never committed, and those of holds
// that ended. Like Prune, it stops at the first error.
func (s *Session) PruneHeld(ctx context.Context, restored *Record) (deleted int, err error) {
	files, err := s.log.heldFiles(ctx, s.log.root+HeldDir+"/")
	if err != nil {
		return 0, fmt.Errorf("checkpoint: prune held files: %w", err)
	}
	for _, f := range files {
		if f.g >= s.head.Generation {
			continue
		}
		if restored != nil {
			if h := restored.Streams[f.stream].Holds; h != nil && slices.ContainsFunc(h.Epochs, func(e HeldEpochs) bool { return e.holds(f.g, f.n) }) {
				continue
			}
		}
		if err := s.log.store.Delete(ctx, f.key); err != nil {
			return deleted, fmt.Errorf("checkpoint: prune %s: %w", f.key, err)
		}
		deleted++
	}
	return deleted, nil
}
