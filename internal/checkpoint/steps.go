package checkpoint

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/openetl-io/openetl/internal/platform/ints"
)

// LaneSteps are the steps of a group window's watermark that one keyed
// lane's rows met in an epoch, by the split they were read from. A redo of
// the epoch replays them, so each row meets the watermark it met.
type LaneSteps struct {
	// The keyed lane's.
	KeyGroups KeyGroupRange
	Splits    []SplitSteps
}

// SplitSteps are where a keyed lane's rows of one split met a later
// watermark. A place counts the rows a lane read of the split in the epoch,
// from 0: the rows from a step's place on, before the next step's, met its
// watermark, and those before the first step the window's watermark at the
// epoch's start. Places and watermarks ascend.
type SplitSteps struct {
	Split string
	Steps []Step
}

type Step struct {
	Place uint64
	// In microseconds since the Unix epoch.
	Watermark int64
}

// errSteps: steps that can't be encoded or decoded.
var errSteps = errors.New("checkpoint: window steps")

// EncodeSteps is lanes as a record keeps them, nil for none: the splits by
// their index in splits, the stream's splits in the record, and the
// watermarks as steps up from floor, the window's watermark at the epoch's
// start. A split that splits doesn't name is left out, as a redo doesn't
// read it.
//
// It's varints: the number of distinct watermarks, then each less the one
// before it, from floor; the number of lanes, then of each, in order, its
// first key group less the one after the lane before's last, its last less
// its first, and its number of splits; of each split, in order, its index
// less the one after the split before's, times two, plus one where it has
// one step, and otherwise its number of steps less two; and of each step
// its place less the one after the step before's, and the index of its
// watermark less the one after the step before's.
func EncodeSteps(lanes []LaneSteps, splits []string, floor int64) ([]byte, error) {
	index := make(map[string]int, len(splits))
	for i, s := range splits {
		index[s] = i
	}
	type split struct {
		at    int
		steps []Step
	}
	type lane struct {
		kg     KeyGroupRange
		splits []split
	}
	var ls []lane
	var marks []int64
	for _, l := range lanes {
		if l.KeyGroups.First < 0 || l.KeyGroups.Last < l.KeyGroups.First {
			return nil, fmt.Errorf("%w: a keyed lane of key groups %d to %d", errSteps, l.KeyGroups.First, l.KeyGroups.Last)
		}
		out := lane{kg: l.KeyGroups}
		for _, s := range l.Splits {
			at, ok := index[s.Split]
			if !ok || len(s.Steps) == 0 {
				continue
			}
			for i := 1; i < len(s.Steps); i++ {
				if s.Steps[i].Place <= s.Steps[i-1].Place || s.Steps[i].Watermark <= s.Steps[i-1].Watermark {
					return nil, fmt.Errorf("%w: split %q's on key groups %d to %d don't ascend", errSteps, s.Split, l.KeyGroups.First, l.KeyGroups.Last)
				}
			}
			for _, st := range s.Steps {
				marks = append(marks, st.Watermark)
			}
			out.splits = append(out.splits, split{at: at, steps: s.Steps})
		}
		if len(out.splits) == 0 {
			continue
		}
		slices.SortFunc(out.splits, func(a, b split) int { return a.at - b.at })
		ls = append(ls, out)
	}
	if len(ls) == 0 {
		return nil, nil
	}
	slices.SortFunc(ls, func(a, b lane) int { return a.kg.First - b.kg.First })
	for i := 1; i < len(ls); i++ {
		if ls[i].kg.First <= ls[i-1].kg.Last {
			return nil, fmt.Errorf("%w: keyed lanes of key groups %d to %d and %d to %d overlap", errSteps, ls[i-1].kg.First, ls[i-1].kg.Last, ls[i].kg.First, ls[i].kg.Last)
		}
	}
	slices.Sort(marks)
	marks = slices.Compact(marks)
	rank := make(map[int64]uint64, len(marks))
	b := binary.AppendUvarint(nil, uint64(len(marks)))
	prev := floor
	for i, m := range marks {
		b = binary.AppendVarint(b, m-prev)
		prev, rank[m] = m, uint64(i)
	}
	b = binary.AppendUvarint(b, uint64(len(ls)))
	next := 0
	for _, l := range ls {
		b = binary.AppendUvarint(b, ints.Uint64(int64(l.kg.First-next)))
		b = binary.AppendUvarint(b, ints.Uint64(int64(l.kg.Last-l.kg.First)))
		b = binary.AppendUvarint(b, uint64(len(l.splits)))
		next = l.kg.Last + 1
		nextSplit := 0
		for _, s := range l.splits {
			one := uint64(0)
			if len(s.steps) == 1 {
				one = 1
			}
			b = binary.AppendUvarint(b, ints.Uint64(int64(s.at-nextSplit))<<1|one)
			if one == 0 {
				b = binary.AppendUvarint(b, ints.Uint64(int64(len(s.steps)-2)))
			}
			nextSplit = s.at + 1
			var nextPlace, nextMark uint64
			for _, st := range s.steps {
				b = binary.AppendUvarint(b, st.Place-nextPlace)
				b = binary.AppendUvarint(b, rank[st.Watermark]-nextMark)
				nextPlace, nextMark = st.Place+1, rank[st.Watermark]+1
			}
		}
	}
	return b, nil
}

// DecodeSteps is what EncodeSteps encoded of the same splits and floor.
func DecodeSteps(b []byte, splits []string, floor int64) ([]LaneSteps, error) {
	if len(b) == 0 {
		return nil, nil
	}
	r := stepsReader{b: b}
	n := r.count()
	marks := make([]int64, 0, n)
	prev := floor
	for range n {
		prev += r.varint()
		marks = append(marks, prev)
	}
	var out []LaneSteps
	next := uint64(0)
	for range r.count() {
		first := next + r.uvarint()
		last := first + r.uvarint()
		if first > math.MaxInt32 || last > math.MaxInt32 {
			return nil, fmt.Errorf("%w: key groups up to %d", errSteps, last)
		}
		l := LaneSteps{KeyGroups: KeyGroupRange{First: int(first), Last: int(last)}}
		next = last + 1
		nextSplit := uint64(0)
		for range r.count() {
			v := r.uvarint()
			at := nextSplit + v>>1
			if at >= uint64(len(splits)) {
				return nil, fmt.Errorf("%w: split %d of %d", errSteps, at, len(splits))
			}
			nextSplit = at + 1
			s := SplitSteps{Split: splits[at]}
			steps := 1
			if v&1 == 0 {
				steps = 2 + r.count()
			}
			var nextPlace, nextMark uint64
			for range steps {
				place, mark := nextPlace+r.uvarint(), nextMark+r.uvarint()
				if mark >= uint64(len(marks)) {
					return nil, fmt.Errorf("%w: watermark %d of %d", errSteps, mark, len(marks))
				}
				s.Steps = append(s.Steps, Step{Place: place, Watermark: marks[mark]})
				nextPlace, nextMark = place+1, mark+1
			}
			l.Splits = append(l.Splits, s)
		}
		out = append(out, l)
	}
	if r.err != nil {
		return nil, r.err
	}
	if len(r.b) > 0 {
		return nil, fmt.Errorf("%w: %d bytes after them", errSteps, len(r.b))
	}
	return out, nil
}

// stepsReader reads varints, and keeps the first error.
type stepsReader struct {
	b   []byte
	err error
}

func (r *stepsReader) uvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *stepsReader) varint() int64 {
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return v
}

// count is how many of something follow, each at least a byte.
func (r *stepsReader) count() int {
	v := r.uvarint()
	if v > math.MaxInt32 || v > uint64(len(r.b)) {
		r.fail()
		return 0
	}
	return int(v)
}

func (r *stepsReader) fail() {
	if r.err == nil {
		r.err = fmt.Errorf("%w: cut short", errSteps)
	}
	r.b = nil
}
