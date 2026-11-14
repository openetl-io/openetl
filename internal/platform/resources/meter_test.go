package resources

import (
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"
)

func TestMeterCountsAProcess(t *testing.T) {
	m := NewMeter()
	// Burns some CPU, in a shell any machine has, then waits to be told to end.
	cmd := exec.Command("sh", "-c", "i=0; while [ $i -lt 200000 ]; do i=$((i+1)); done; read x; exit 0")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := m.Track(cmd.Process)
	if runtime.GOOS == "linux" {
		// A running process can be read there, which an attempt's reports rely on.
		deadline := time.Now().Add(30 * time.Second)
		for u := m.Usage(); u.CPU <= 0 || u.Memory <= 0; u = m.Usage() {
			if time.Now().After(deadline) {
				t.Fatalf("usage of a running process: %+v", u)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	m.Sample()
	_ = stdin.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	done(cmd.ProcessState)
	u := m.Usage()
	if u.CPU <= 0 || u.Memory <= 0 && runtime.GOOS != "linux" {
		t.Errorf("usage %+v, want some CPU and memory", u)
	}
	if u.Seconds <= 0 {
		t.Errorf("Seconds = %v", u.Seconds)
	}
	if again := m.Usage(); again.CPU != u.CPU || again.Memory != u.Memory {
		t.Errorf("usage changed with nothing running: %+v, then %+v", u, again)
	}
}

func TestParseStat(t *testing.T) {
	line := "1234 (a) (b c) S 1 1234 1234 0 -1 4194304 100 0 0 0 250 50 0 0 20 0 1 0 100 1000 200"
	if cpu, ok := parseStat(line); !ok || cpu != 3 {
		t.Errorf("parseStat = %v, %v, want 3 s", cpu, ok)
	}
	if _, ok := parseStat("1234 (a) S 1"); ok {
		t.Error("parsed a line cut short")
	}
}

func TestMeterSharesAddUp(t *testing.T) {
	m := NewMeter()
	m.Share(1.5, 100)
	m.Share(0.5, 50)
	u := m.Usage()
	if u.RunnerCPU != 2 || u.RunnerMemory != 100 {
		t.Fatalf("usage = %+v, want 2 s of CPU and the larger memory", u)
	}
}

// An attempt on workers uses what its largest worker does, and moves what
// they all do.
func TestMeterOfWorkers(t *testing.T) {
	m := NewMeter()
	m.Share(9, 900)
	m.SetLanes(8, 0)
	m.SetRemote(0, Remote{Usage: Usage{CPU: 3, Memory: 100, RunnerCPU: 1, RunnerMemory: 10}, Lanes: Lanes{Count: 2, Limit: 2, Busy: 1, Rows: 5}, Rows: 5, Bytes: 50})
	m.SetRemote(1, Remote{Usage: Usage{CPU: 2, Memory: 300, RunnerCPU: 2, RunnerMemory: 5}, Lanes: Lanes{Count: 3, Limit: 3, Idle: 2, Rows: 7}, Rows: 7, Bytes: 70})
	m.SetRemote(0, Remote{Usage: Usage{CPU: 4, Memory: 100, RunnerCPU: 1, RunnerMemory: 10}, Lanes: Lanes{Count: 2, Limit: 2, Busy: 2, Rows: 6}, Rows: 6, Bytes: 60})
	u := m.Usage()
	if u.CPU != 4 || u.Memory != 300 || u.RunnerCPU != 2 || u.RunnerMemory != 10 {
		t.Errorf("usage = %+v, want each figure's largest worker's", u)
	}
	if rows, bytes := m.Progress(); rows != 13 || bytes != 130 {
		t.Errorf("progress = %d rows, %d bytes, want 13 and 130", rows, bytes)
	}
	if l := m.Lanes(); l != (Lanes{Count: 5, Limit: 5, Busy: 2, Idle: 2, Rows: 13}) {
		t.Errorf("lanes = %+v", l)
	}
	m.SetRemote(2, Remote{Lanes: Lanes{Count: 1}})
	if l := m.Lanes(); l.Count != 6 || l.Limit != 0 {
		t.Errorf("lanes = %+v, want no limit once a worker has none", l)
	}
}

func TestResidentAndKill(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("needs /proc")
	}
	m := NewMeter()
	cmd := balloon(64)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := m.Track(cmd.Process)
	deadline := time.Now().Add(30 * time.Second)
	for m.Resident() < 64*MiB {
		if time.Now().After(deadline) {
			t.Fatalf("resident = %d, want at least 64 MiB", m.Resident())
		}
		time.Sleep(10 * time.Millisecond)
	}
	m.Kill()
	if err := cmd.Wait(); err == nil {
		t.Error("the process wasn't killed")
	}
	done(cmd.ProcessState)
	if n := m.Resident(); n != 0 {
		t.Errorf("resident = %d once it has exited", n)
	}
}

func TestNilMeter(t *testing.T) {
	var m *Meter
	m.Track(&os.Process{Pid: 1})(nil)
	m.Confine(exec.Command("true"))
	m.Sample()
	m.Kill()
	if n := m.Resident(); n != 0 {
		t.Fatalf("resident = %d", n)
	}
	m.Share(1, 1)
	m.Moved(1, 1)
	if u := m.Usage(); u != (Usage{}) {
		t.Fatalf("usage = %+v", u)
	}
	if r, b := m.Progress(); r != 0 || b != 0 {
		t.Fatalf("progress = %d, %d", r, b)
	}
}

func TestSelf(t *testing.T) {
	cpu, heap := Self()
	if cpu <= 0 || heap <= 0 {
		t.Fatalf("Self = %v, %d", cpu, heap)
	}
}

func TestSizes(t *testing.T) {
	if got := S.Up(); got != M {
		t.Errorf("S.Up = %v", got)
	}
	if got := (Size{CPU: 6000, Memory: 12 * GiB}).Up(); got != XL {
		t.Errorf("up from amounts = %v", got)
	}
	if got := XL.Up(); got != (Size{CPU: 16000, Memory: 32 * GiB}) {
		t.Errorf("XL.Up = %v", got)
	}
	if got := (Size{CPU: 100, Memory: 3 * GiB}).Smallest(); got != M {
		t.Errorf("Smallest = %v", got)
	}
	if got := (Size{CPU: 9000, Memory: GiB}).Smallest(); got != (Size{CPU: 9000, Memory: GiB}) {
		t.Errorf("Smallest above the largest = %v", got)
	}
}
