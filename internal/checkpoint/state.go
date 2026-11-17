package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// pruneState deletes the state objects that none of the records Prune keeps
// names, keep of them along the chain from restored, as the log holds them:
// those of earlier generations, and the session's own of epochs up to
// restored's. It lists all of the pipeline's state, as a record goes on
// naming a run for any number of generations after the one that wrote it.
func (s *Session) pruneState(ctx context.Context, restored *Record, keep int) (deleted int, err error) {
	prefix := s.log.root + StateDir + "/"
	keys, err := s.log.store.List(ctx, prefix)
	if err != nil {
		return 0, fmt.Errorf("checkpoint: prune state: %w", err)
	}
	if len(keys) == 0 {
		return 0, nil
	}
	named, err := s.log.namedState(ctx, restored, keep)
	if err != nil {
		return 0, fmt.Errorf("checkpoint: prune state: %w", err)
	}
	gen := s.head.Generation
	var upto uint64
	if restored != nil {
		upto = restored.Epoch
	}
	for _, k := range keys {
		rel := strings.TrimPrefix(k, prefix)
		if named[rel] {
			continue
		}
		g, n, ok := parseStateKey(rel)
		if !ok || g > gen || g == gen && n > upto {
			continue
		}
		if err := s.log.store.Delete(ctx, k); err != nil {
			return deleted, fmt.Errorf("checkpoint: prune %s: %w", k, err)
		}
		deleted++
	}
	return deleted, nil
}

// namedState is the state objects that keep records along the chain from r
// name, read from the log: r may be an attempt's copy that names less, as
// one that takes a reset does. r itself must read, or nothing is named and
// what HEAD's fallback restores could go. A record further on that can't
// read ends the chain, as nothing restores from it or from those before
// it.
func (l *Log) namedState(ctx context.Context, r *Record, keep int) (map[string]bool, error) {
	named := map[string]bool{}
	if r == nil {
		return named, nil
	}
	start := r.Ref()
	ref := &start
	for i := 0; ref != nil && i < keep; i++ {
		rec, err := l.Record(ctx, *ref)
		if i > 0 && errors.Is(err, ErrUnreadable) {
			break
		}
		if err != nil {
			return nil, err
		}
		for _, s := range rec.Snapshots {
			for _, run := range s.Runs {
				named[run] = true
			}
		}
		ref = rec.Previous
	}
	return named, nil
}

// parseStateKey is the generation and epoch of a run's name below StateDir:
// {dir}/g{g}/e{N}/kg{first}-{last}.run, or a merge's, ….merge.run.
func parseStateKey(rel string) (gen, epoch uint64, ok bool) {
	parts := strings.Split(rel, "/")
	if len(parts) < 4 || !strings.HasPrefix(parts[len(parts)-1], "kg") || !strings.HasSuffix(parts[len(parts)-1], ".run") {
		return 0, 0, false
	}
	g, gok := strings.CutPrefix(parts[len(parts)-3], "g")
	n, nok := strings.CutPrefix(parts[len(parts)-2], "e")
	if !gok || !nok {
		return 0, 0, false
	}
	gen, err1 := strconv.ParseUint(g, 10, 64)
	epoch, err2 := strconv.ParseUint(n, 10, 64)
	return gen, epoch, err1 == nil && err2 == nil
}
