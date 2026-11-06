package resources

import (
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Usage is what an attempt has used so far.
type Usage struct {
	// CPU seconds of its engine and connector processes.
	CPU float64
	// The most those processes held at once, in resident bytes.
	Memory int64
	// Its estimated share of the runner's own work.
	RunnerCPU    float64
	RunnerMemory int64
	// How long it has been running.
	Seconds float64
}

// Meter adds up what one attempt uses (ADR-0027 §3). A nil Meter counts
// nothing. Its methods may be called concurrently.
type Meter struct {
	start time.Time
	rows  atomic.Int64
	bytes atomic.Int64

	mu    sync.Mutex
	live  map[int]*reading
	usage Usage
	// What exited processes used, in CPU seconds and, where a process's peak
	// can be read only once it has exited, resident bytes.
	exitedCPU float64
	exitedMem int64
}

type reading struct {
	cpu float64
	hwm int64
}

// NewMeter starts metering an attempt.
func NewMeter() *Meter {
	return &Meter{start: time.Now(), live: map[int]*reading{}}
}

// Track counts a process the attempt started, until the function it returns
// gets the process's exit state.
func (m *Meter) Track(pid int) func(*os.ProcessState) {
	if m == nil {
		return func(*os.ProcessState) {}
	}
	m.mu.Lock()
	m.live[pid] = &reading{}
	m.mu.Unlock()
	return func(st *os.ProcessState) {
		m.mu.Lock()
		defer m.mu.Unlock()
		r := m.live[pid]
		if r == nil {
			return
		}
		delete(m.live, pid)
		cpu := r.cpu
		if st != nil {
			cpu = max(cpu, st.UserTime().Seconds()+st.SystemTime().Seconds())
		}
		m.exitedCPU += cpu
		if runtime.GOOS != "linux" {
			m.exitedMem += ExitPeak(st, 0)
		}
		m.read()
	}
}

// Sample reads the running processes, as just before stopping them: a process
// that has exited can't be read. It's only needed then; Usage reads them too.
func (m *Meter) Sample() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.read()
}

// read updates the usage from the running processes' readings. Each one's
// high-water mark is the most it has held, so summing the running ones is an
// upper bound on what they held at once. Without /proc, a process counts only
// once it has exited, when its peak can be read.
func (m *Meter) read() {
	cpu, mem := m.exitedCPU, m.exitedMem
	for pid, r := range m.live {
		if c, hwm, ok := readProcess(pid); ok {
			r.cpu, r.hwm = c, max(r.hwm, hwm)
		}
		cpu += r.cpu
		mem += r.hwm
	}
	m.usage.CPU = max(m.usage.CPU, cpu)
	m.usage.Memory = max(m.usage.Memory, mem)
}

// Usage is what the attempt has used so far.
func (m *Meter) Usage() Usage {
	if m == nil {
		return Usage{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.read()
	u := m.usage
	u.Seconds = time.Since(m.start).Seconds()
	return u
}

// Share adds to the attempt's share of the runner's own work: CPU seconds, and
// the bytes of the runner's memory that are its now.
func (m *Meter) Share(cpu float64, memory int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.usage.RunnerCPU += cpu
	m.usage.RunnerMemory = max(m.usage.RunnerMemory, memory)
}

// Moved counts what the attempt's source emitted.
func (m *Meter) Moved(rows, bytes int64) {
	if m == nil {
		return
	}
	m.rows.Add(rows)
	m.bytes.Add(bytes)
}

// Progress is how many rows and bytes the attempt's source has emitted.
func (m *Meter) Progress() (rows, bytes int64) {
	if m == nil {
		return 0, 0
	}
	return m.rows.Load(), m.bytes.Load()
}

// Self is what this process has used: its CPU seconds, and the bytes its
// heap's objects hold now. It's not its resident memory, which also holds what
// earlier work freed.
func Self() (cpu float64, heap int64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err == nil {
		cpu = time.Duration(ru.Utime.Nano() + ru.Stime.Nano()).Seconds()
	}
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() == metrics.KindUint64 {
		heap = int64(sample[0].Value.Uint64()) //nolint:gosec // no heap has 2^63 bytes
	}
	return cpu, heap
}

// clockTicks is USER_HZ, which is 100 on every architecture runners run on.
const clockTicks = 100

// readProcess reads a running process's CPU seconds and peak resident bytes,
// on Linux.
func readProcess(pid int) (cpu float64, hwm int64, ok bool) {
	if runtime.GOOS != "linux" {
		return 0, 0, false
	}
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, 0, false
	}
	cpu, ok = parseStat(string(stat))
	if !ok {
		return 0, 0, false
	}
	return cpu, HighWater(pid), true
}

// parseStat reads the CPU seconds in a /proc/PID/stat line. The command's name
// can hold anything, so fields count from its end.
func parseStat(s string) (float64, bool) {
	f := strings.Fields(s[strings.LastIndexByte(s, ')')+1:])
	const utime, stime = 11, 12 // fields 14 and 15 of the whole line
	if len(f) <= stime {
		return 0, false
	}
	u, err1 := strconv.ParseInt(f[utime], 10, 64)
	k, err2 := strconv.ParseInt(f[stime], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, false
	}
	return float64(u+k) / clockTicks, true
}

// HighWater reads a running process's peak resident memory, on Linux; 0 where
// it can't. A process's rusage won't do there: Go starts processes with
// CLONE_VM, so execve carries the runner's own peak into the process's maxrss.
func HighWater(pid int) int64 {
	if runtime.GOOS != "linux" {
		return 0
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return 0
	}
	for line := range strings.Lines(string(b)) {
		if v, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			kb, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(v), " kB"), 10, 64)
			if err != nil {
				return 0
			}
			return kb << 10
		}
	}
	return 0
}

// ExitPeak is an exited process's peak resident memory in bytes, given what
// HighWater read before it stopped; 0 if unknown.
func ExitPeak(st *os.ProcessState, hwm int64) int64 {
	if runtime.GOOS == "linux" {
		return hwm
	}
	if st == nil {
		return 0
	}
	ru, ok := st.SysUsage().(*syscall.Rusage)
	if !ok {
		return 0
	}
	if runtime.GOOS == "darwin" {
		return ru.Maxrss
	}
	return ru.Maxrss << 10
}
