package runner

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// sinkPump feeds one writer lane's part of the dataflow's output to its
// writer, and calls PreCommit(N) once every stream has crossed barrier N on
// the lane. An engine sink's epochs arrive staged, in their FrameReady. So
// do a staged destination's, whose files the writer gets before
// PreCommit(N). A writer that tracks reads gets their starts first, and
// before PreCommit(N) the ends of the reads known to end in N, which the
// writer of a read's last lane always knows. Lane 0's pump also reports each
// epoch ready.
type sinkPump struct {
	w          destination.Writer
	lane       int
	gen        uint64
	engineSink bool
	staging    Staging // set for a staged destination
	in         <-chan Frame
	events     chan<- event
	starts     map[string]time.Time // the streams the attempt reads
	ends       readEnder

	epochOf   map[string]uint64 // each stream's current epoch
	seen      map[uint64]int    // streams that crossed each barrier
	nextReady uint64
}

func newSinkPump(w destination.Writer, lane int, gen, firstEpoch uint64, engineSink bool, staging Staging, streams []Stream, starts map[string]time.Time, ends readEnder, in <-chan Frame, events chan<- event) *sinkPump {
	s := &sinkPump{w: w, lane: lane, gen: gen, engineSink: engineSink, staging: staging, in: in, events: events, starts: starts, ends: ends, epochOf: make(map[string]uint64), seen: make(map[uint64]int), nextReady: firstEpoch}
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
			var lane LaneReady
			if s.staging != nil || s.engineSink {
				if s.lane >= len(f.Ready.Lanes) {
					return fmt.Errorf("runner: the dataflow reported epoch %d ready on %d writer lanes, not lane %d", f.Epoch, len(f.Ready.Lanes), s.lane)
				}
				lane = f.Ready.Lanes[s.lane]
			}
			switch {
			case s.staging != nil:
				if err := s.preCommitStaged(ctx, rt, f.Epoch, lane.Files); err != nil {
					return err
				}
			case s.engineSink:
				if err := s.staged(ctx, f.Epoch, lane.Staged); err != nil {
					return err
				}
			}
			if s.lane > 0 {
				continue
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
		case FramePatch:
			pw, ok := s.w.(destination.Patcher)
			if !ok {
				release(f)
				return fmt.Errorf("runner: patches for %s, and the destination's writer takes none", f.Stream)
			}
			err := pw.Patch(ctx, destination.Epoch{Generation: s.gen, N: n}, f.Stream, f.Batch)
			f.Batch.Release()
			if err != nil {
				return fmt.Errorf("runner: patch %s in epoch %d: %w", f.Stream, n, err)
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
			if err := s.endReads(ctx, rt, n); err != nil {
				return err
			}
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
	if err := s.endReads(ctx, rt, n); err != nil {
		return err
	}
	staged, err := s.w.PreCommit(ctx, e)
	if err != nil {
		return fmt.Errorf("runner: pre-commit epoch %d: %w", n, err)
	}
	return s.staged(ctx, n, staged)
}

// endReads ends the reads that end in epoch n.
func (s *sinkPump) endReads(ctx context.Context, rt destination.ReadTracker, n uint64) error {
	if rt == nil {
		return nil
	}
	e := destination.Epoch{Generation: s.gen, N: n}
	for _, stream := range slices.Sorted(maps.Keys(s.epochOf)) {
		if !s.ends.endsAt(stream, n) {
			continue
		}
		if err := rt.EndRead(ctx, e, stream); err != nil {
			return fmt.Errorf("runner: end the read of %s in epoch %d: %w", stream, n, err)
		}
	}
	return nil
}

func (s *sinkPump) staged(ctx context.Context, n uint64, staged destination.Staged) error {
	return s.send(ctx, event{kind: evStaged, epoch: n, lane: s.lane, staged: staged})
}

// demux sends each of the dataflow's frames to its writer lane's pump, and
// each FrameReady to every lane's.
func demux(ctx context.Context, from <-chan Frame, lanes []chan Frame) error {
	defer func() {
		for _, l := range lanes {
			close(l)
		}
	}()
	for {
		var f Frame
		select {
		case fr, ok := <-from:
			if !ok {
				return nil
			}
			f = fr
		case <-ctx.Done():
			return ctx.Err()
		}
		if f.Kind == FrameReady {
			for _, l := range lanes {
				if err := send(ctx, l, f); err != nil {
					return err
				}
			}
			continue
		}
		if f.Lane < 0 || f.Lane >= len(lanes) {
			release(f)
			return fmt.Errorf("runner: the dataflow sent %s to writer lane %d of %d", f.Stream, f.Lane, len(lanes))
		}
		if err := send(ctx, lanes[f.Lane], f); err != nil {
			return err
		}
	}
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
