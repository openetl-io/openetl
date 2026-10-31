package runner

import (
	"context"
	"fmt"

	"github.com/unsuitable001/openetl/sdk/go/destination"
)

// sinkPump feeds the dataflow's output to the writer, and calls PreCommit(N)
// once every stream has crossed barrier N. An engine sink's epochs arrive
// staged, in their FrameReady.
type sinkPump struct {
	w          destination.Writer
	gen        uint64
	engineSink bool
	in         <-chan Frame
	events     chan<- event

	epochOf   map[string]uint64 // each stream's current epoch
	seen      map[uint64]int    // streams that crossed each barrier
	nextReady uint64
}

func newSinkPump(w destination.Writer, gen, firstEpoch uint64, engineSink bool, streams []Stream, in <-chan Frame, events chan<- event) *sinkPump {
	s := &sinkPump{w: w, gen: gen, engineSink: engineSink, in: in, events: events, epochOf: make(map[string]uint64), seen: make(map[uint64]int), nextReady: firstEpoch}
	for _, st := range streams {
		s.epochOf[st.Name] = firstEpoch
	}
	return s
}

func (s *sinkPump) run(ctx context.Context) error {
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
			if s.engineSink {
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
