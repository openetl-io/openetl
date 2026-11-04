package runner

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/unsuitable001/openetl/sdk/go/destination"
)

// sinkPump feeds the dataflow's output to the writer, and calls PreCommit(N)
// once every stream has crossed barrier N. An engine sink's epochs arrive
// staged, in their FrameReady. So do a staged destination's, whose files the
// writer gets before PreCommit(N). A writer that tracks reads gets their
// starts first, and each read's end when the barrier that ends it comes out,
// or with a staged destination, at the end of its epoch.
type sinkPump struct {
	w          destination.Writer
	gen        uint64
	engineSink bool
	staging    Staging // set for a staged destination
	in         <-chan Frame
	events     chan<- event
	starts     map[string]time.Time // the streams the attempt reads
	ends       *readEnds

	epochOf   map[string]uint64 // each stream's current epoch
	seen      map[uint64]int    // streams that crossed each barrier
	nextReady uint64
}

func newSinkPump(w destination.Writer, gen, firstEpoch uint64, engineSink bool, staging Staging, streams []Stream, starts map[string]time.Time, ends *readEnds, in <-chan Frame, events chan<- event) *sinkPump {
	s := &sinkPump{w: w, gen: gen, engineSink: engineSink, staging: staging, in: in, events: events, starts: starts, ends: ends, epochOf: make(map[string]uint64), seen: make(map[uint64]int), nextReady: firstEpoch}
	for _, st := range streams {
		s.epochOf[st.Name] = firstEpoch
	}
	return s
}

func (s *sinkPump) run(ctx context.Context) error {
	rt, _ := s.w.(destination.ReadTracker)
	if rt != nil && len(s.starts) > 0 {
		if err := rt.StartReads(ctx, s.starts); err != nil {
			return fmt.Errorf("runner: start reads: %w", err)
		}
	}
	for {
		var f Frame
		select {
		case fr, ok := <-s.in:
			if !ok {
				return nil
			}
			f = fr
		case <-ctx.Done():
			return ctx.Err()
		}
		if f.Kind == FrameReady {
			if f.Epoch != s.nextReady || f.Ready == nil {
				return fmt.Errorf("runner: the dataflow reported epoch %d ready, expected %d", f.Epoch, s.nextReady)
			}
			s.nextReady++
			switch {
			case s.staging != nil:
				if err := s.preCommitStaged(ctx, rt, f.Epoch, f.Ready.Files); err != nil {
					return err
				}
			case s.engineSink:
				if err := s.staged(ctx, f.Epoch, f.Ready.Staged); err != nil {
					return err
				}
			}
			if err := s.send(ctx, event{kind: evReady, epoch: f.Epoch, ready: f.Ready}); err != nil {
				return err
			}
			continue
		}
		n, ok := s.epochOf[f.Stream]
		if !ok {
			release(f)
			return fmt.Errorf("runner: dataflow produced unknown stream %q", f.Stream)
		}
		switch f.Kind {
		case FrameBatch:
			err := s.w.Write(ctx, destination.Epoch{Generation: s.gen, N: n}, f.Stream, f.Batch)
			f.Batch.Release()
			if err != nil {
				return fmt.Errorf("runner: write %s in epoch %d: %w", f.Stream, n, err)
			}
		case FrameBarrier:
			if f.Epoch != n {
				return fmt.Errorf("runner: stream %s crossed barrier %d while in epoch %d", f.Stream, f.Epoch, n)
			}
			s.epochOf[f.Stream] = n + 1
			if rt != nil && s.ends.endsAt(f.Stream, n) {
				if err := rt.EndRead(ctx, destination.Epoch{Generation: s.gen, N: n}, f.Stream); err != nil {
					return fmt.Errorf("runner: end the read of %s in epoch %d: %w", f.Stream, n, err)
				}
			}
			s.seen[n]++
			if s.seen[n] < len(s.epochOf) {
				continue
			}
			delete(s.seen, n)
			staged, err := s.w.PreCommit(ctx, destination.Epoch{Generation: s.gen, N: n})
			if err != nil {
				return fmt.Errorf("runner: pre-commit epoch %d: %w", n, err)
			}
			if err := s.staged(ctx, n, staged); err != nil {
				return err
			}
		default:
			release(f)
			return fmt.Errorf("runner: unexpected frame kind %d from the dataflow", f.Kind)
		}
	}
}

// preCommitStaged hands the writer epoch n's files, ends the reads that
// ended in it, and pre-commits it.
func (s *sinkPump) preCommitStaged(ctx context.Context, rt destination.ReadTracker, n uint64, files map[string][]destination.StagedFile) error {
	e := destination.Epoch{Generation: s.gen, N: n}
	if err := handOver(ctx, s.staging, s.w, e, files); err != nil {
		return err
	}
	if rt != nil {
		for _, stream := range slices.Sorted(maps.Keys(s.epochOf)) {
			if !s.ends.endsAt(stream, n) {
				continue
			}
			if err := rt.EndRead(ctx, e, stream); err != nil {
				return fmt.Errorf("runner: end the read of %s in epoch %d: %w", stream, n, err)
			}
		}
	}
	staged, err := s.w.PreCommit(ctx, e)
	if err != nil {
		return fmt.Errorf("runner: pre-commit epoch %d: %w", n, err)
	}
	return s.staged(ctx, n, staged)
}

func (s *sinkPump) staged(ctx context.Context, n uint64, staged destination.Staged) error {
	return s.send(ctx, event{kind: evStaged, epoch: n, staged: staged})
}

func (s *sinkPump) send(ctx context.Context, ev event) error {
	select {
	case s.events <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func release(f Frame) {
	if f.Batch != nil {
		f.Batch.Release()
	}
}
