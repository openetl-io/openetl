package plan

import "fmt"

// KeyGroupRange is key groups First to Last, inclusive.
type KeyGroupRange struct{ First, Last int }

func (r KeyGroupRange) size() int { return r.Last - r.First + 1 }

// Layout is the key groups an attempt's lanes and writer lanes own, picked
// together so that lanes feed writer lanes by range. Writer lane w owns the
// w-th of the writer lanes' contiguous ranges, as an exchange before the
// writer sends it rows. Each writer lane's range is cut again among the
// lanes that feed it, at least one, in lane order: a lane's range is always
// inside its writer lane's, so more lanes feed fewer by merging ranges,
// never by hashing again. The engine's layout is the same.
type Layout struct {
	Lanes   []KeyGroupRange
	Writers []KeyGroupRange
	// Each lane's writer lane.
	WriterOf []int
}

// NewLayout lays lanes and writers out over groups key groups. Each writer
// lane gets one lane, and the others go one at a time to the writer lane
// with the most key groups per lane, the first of those that tie.
func NewLayout(groups, lanes, writers int) (Layout, error) {
	l, err := NewWorkerLayout(groups, []int{lanes}, []int{writers})
	if err != nil {
		return Layout{}, fmt.Errorf("plan: %d lanes and %d writer lanes over %d key groups: each writer lane needs a lane, and each lane a key group", lanes, writers, groups)
	}
	return l, nil
}

// NewWorkerLayout lays out an attempt on workers, worker i with lanes[i]
// lanes and writers[i] writer lanes, each worker's after the worker
// before it's: the attempt's writer lanes own their ranges of groups as
// NewLayout's do, and each worker cuts its own writer lanes' among its
// lanes as NewLayout does. So a lane feeds a writer lane of its own worker,
// and an exchange sends a row to whichever worker owns its key group. On
// one worker it's NewLayout's.
func NewWorkerLayout(groups int, lanes, writers []int) (Layout, error) {
	if len(lanes) != len(writers) || len(lanes) == 0 {
		return Layout{}, fmt.Errorf("plan: lanes %v and writer lanes %v of workers", lanes, writers)
	}
	total := 0
	for _, w := range writers {
		total += w
	}
	var l Layout
	for w := range total {
		l.Writers = append(l.Writers, KeyGroupRange{First: ceilDiv(w*groups, total), Last: ceilDiv((w+1)*groups, total) - 1})
	}
	first := 0
	for i, ws := range writers {
		n := lanes[i]
		if ws < 1 || n < ws {
			return Layout{}, fmt.Errorf("plan: worker %d has %d lanes and %d writer lanes: each worker needs a writer lane, and each writer lane a lane", i, n, ws)
		}
		own := l.Writers[first : first+ws]
		owned := 0
		for _, r := range own {
			if r.size() < 1 {
				return Layout{}, fmt.Errorf("plan: %d writer lanes over %d key groups: each writer lane needs a key group", total, groups)
			}
			owned += r.size()
		}
		if owned < n {
			return Layout{}, fmt.Errorf("plan: worker %d has %d lanes over %d key groups: each lane needs one", i, n, owned)
		}
		feeders := make([]int, len(own))
		for w := range feeders {
			feeders[w] = 1
		}
		for range n - ws {
			best := -1
			for w, r := range own {
				if feeders[w] < r.size() && (best < 0 || r.size()*feeders[best] > own[best].size()*feeders[w]) {
					best = w
				}
			}
			feeders[best]++
		}
		for w, r := range own {
			for j := range feeders[w] {
				l.Lanes = append(l.Lanes, KeyGroupRange{First: r.First + ceilDiv(j*r.size(), feeders[w]), Last: r.First + ceilDiv((j+1)*r.size(), feeders[w]) - 1})
				l.WriterOf = append(l.WriterOf, first+w)
			}
		}
		first += ws
	}
	return l, nil
}

// Part is the layout of the lanes from firstLane and the writer lanes from
// firstWriter that a worker of l has, by its own indexes: its lanes'
// writer lanes as its own, and the ranges l gives them.
func (l Layout) Part(firstLane, lanes, firstWriter, writers int) (Layout, error) {
	if firstLane < 0 || firstWriter < 0 || firstLane+lanes > len(l.Lanes) || firstWriter+writers > len(l.Writers) {
		return Layout{}, fmt.Errorf("plan: lanes %d to %d and writer lanes %d to %d of a layout of %d and %d", firstLane, firstLane+lanes-1, firstWriter, firstWriter+writers-1, len(l.Lanes), len(l.Writers))
	}
	p := Layout{Lanes: l.Lanes[firstLane : firstLane+lanes], Writers: l.Writers[firstWriter : firstWriter+writers]}
	for _, w := range l.WriterOf[firstLane : firstLane+lanes] {
		if w < firstWriter || w >= firstWriter+writers {
			return Layout{}, fmt.Errorf("plan: a lane of the part feeds writer lane %d, outside it", w)
		}
		p.WriterOf = append(p.WriterOf, w-firstWriter)
	}
	return p, nil
}

func ceilDiv(a, b int) int { return (a + b - 1) / b }
