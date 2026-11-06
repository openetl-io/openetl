// Package resources holds jobs' and runners' sizes (ADR-0027 §3).
package resources

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	MiB = int64(1) << 20
	GiB = int64(1) << 30
)

// Size is an amount of CPU and memory.
type Size struct {
	// Millicores.
	CPU int64
	// Bytes.
	Memory int64
}

var (
	XS = Size{CPU: 500, Memory: GiB}
	S  = Size{CPU: 1000, Memory: 2 * GiB}
	M  = Size{CPU: 2000, Memory: 4 * GiB}
	L  = Size{CPU: 4000, Memory: 8 * GiB}
	XL = Size{CPU: 8000, Memory: 16 * GiB}
)

// Default is the size of a job nothing says more about.
var Default = S

// Fits reports whether s fits in room.
func (s Size) Fits(room Size) bool { return s.CPU <= room.CPU && s.Memory <= room.Memory }

func (s Size) Add(o Size) Size { return Size{CPU: s.CPU + o.CPU, Memory: s.Memory + o.Memory} }

// Sub is what's left of s after o, which is never negative.
func (s Size) Sub(o Size) Size {
	return Size{CPU: max(0, s.CPU-o.CPU), Memory: max(0, s.Memory-o.Memory)}
}

// Weight puts CPU and memory on one scale, where S weighs 2, for comparing
// how much room runners have.
func (s Size) Weight() float64 {
	return float64(s.CPU)/float64(S.CPU) + float64(s.Memory)/float64(S.Memory)
}

// Name is the preset s is, or its amounts.
func (s Size) Name() string {
	switch s {
	case XS:
		return "xs"
	case S:
		return "s"
	case M:
		return "m"
	case L:
		return "l"
	case XL:
		return "xl"
	}
	return s.String()
}

func (s Size) String() string {
	cpu := strconv.FormatFloat(float64(s.CPU)/1000, 'f', -1, 64)
	if s.Memory%GiB == 0 {
		return fmt.Sprintf("%s vCPU, %d GiB", cpu, s.Memory/GiB)
	}
	return fmt.Sprintf("%s vCPU, %d MiB", cpu, s.Memory/MiB)
}

// Parse reads "CPU,MEMORY", such as "0.5,512Mi" or "2,4Gi".
func Parse(v string) (Size, error) {
	c, m, ok := strings.Cut(v, ",")
	if !ok {
		return Size{}, fmt.Errorf("%q isn't CPU,MEMORY, such as 0.5,512Mi", v)
	}
	cpu, err := strconv.ParseFloat(strings.TrimSpace(c), 64)
	if err != nil || cpu < 0 {
		return Size{}, fmt.Errorf("%q's CPU isn't a number of vCPUs", v)
	}
	mem, err := parseMemory(strings.TrimSpace(m))
	if err != nil {
		return Size{}, fmt.Errorf("%q's memory: %w", v, err)
	}
	return Size{CPU: int64(cpu * 1000), Memory: mem}, nil
}

func parseMemory(v string) (int64, error) {
	unit := int64(1)
	for suffix, u := range map[string]int64{"Ki": 1 << 10, "Mi": MiB, "Gi": GiB, "Ti": 1 << 40} {
		if strings.HasSuffix(v, suffix) {
			v, unit = strings.TrimSuffix(v, suffix), u
			break
		}
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%q isn't an amount such as 512Mi or 4Gi", v)
	}
	return n * unit, nil
}
