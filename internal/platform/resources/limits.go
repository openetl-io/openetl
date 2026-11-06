package resources

import (
	"os"
	"runtime"
	"strconv"
)

// A job's processes share its size (ADR-0027 §4). Each one's limit says what
// it may use alone, so together they may add up to more than the size; what
// holds them all to it is MemoryCeiling.

// Threads is how many threads each of a job's processes runs: its CPU, rounded
// up.
func (s Size) Threads() int {
	return max(1, int((s.CPU+999)/1000))
}

// EngineMemory is what the engine's allocations may hold: most of the size, as
// a job sized from its history peaks at up to 4/5 of it, and that's resident
// memory, which also holds what allocations don't.
func (s Size) EngineMemory() int64 { return s.Memory / 4 * 3 }

// ConnectorEnv is a connector process's environment in a job of this size;
// nothing for no size. GOMEMLIMIT only makes a Go connector collect sooner
// instead of growing, and never fails it.
func (s Size) ConnectorEnv() []string {
	if s == (Size{}) {
		return nil
	}
	return []string{"GOMAXPROCS=" + strconv.Itoa(s.Threads()), "GOMEMLIMIT=" + strconv.FormatInt(s.Memory/2, 10)}
}

// MemoryCeiling is what a job's processes may hold together before they're
// stopped: its size and a margin, since resident memory rises and falls with
// what the allocators and the kernel haven't given back yet.
func (s Size) MemoryCeiling() int64 { return s.Memory + s.Memory/10 }

// KillFirst has the kernel kill this process before the runner if the machine
// runs out of memory, on Linux. Children inherit it. Raising it needs no
// privilege; if /proc won't take it, the process just keeps the default.
func KillFirst(pid int) {
	if runtime.GOOS != "linux" {
		return
	}
	_ = os.WriteFile("/proc/"+strconv.Itoa(pid)+"/oom_score_adj", []byte("1000"), 0)
}
