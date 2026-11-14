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
	if writers < 1 || lanes < writers || groups < lanes {
		return Layout{}, fmt.Errorf("plan: %d lanes and %d writer lanes over %d key groups: each writer lane needs a lane, and each lane a key group", lanes, writers, groups)
	}
	var l Layout
	feeders := make([]int, writers)
	for w := range writers {
		l.Writers = append(l.Writers, KeyGroupRange{First: ceilDiv(w*groups, writers), Last: ceilDiv((w+1)*groups, writers) - 1})
		feeders[w] = 1
	}
	for range lanes - writers {
		best := -1
		for w, r := range l.Writers {
			if feeders[w] < r.size() && (best < 0 || r.size()*feeders[best] > l.Writers[best].size()*feeders[w]) {
				best = w
			}
		}
		feeders[best]++
	}
	for w, r := range l.Writers {
		for j := range feeders[w] {
			l.Lanes = append(l.Lanes, KeyGroupRange{First: r.First + ceilDiv(j*r.size(), feeders[w]), Last: r.First + ceilDiv((j+1)*r.size(), feeders[w]) - 1})
			l.WriterOf = append(l.WriterOf, w)
		}
	}
	return l, nil
}

func ceilDiv(a, b int) int { return (a + b - 1) / b }
