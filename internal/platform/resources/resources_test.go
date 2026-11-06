package resources

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParse(t *testing.T) {
	for in, want := range map[string]Size{
		"0.5,512Mi": {CPU: 500, Memory: 512 * MiB},
		"2,4Gi":     {CPU: 2000, Memory: 4 * GiB},
		"64, 128Gi": {CPU: 64000, Memory: 128 * GiB},
		"1,1024":    {CPU: 1000, Memory: 1024},
	} {
		got, err := Parse(in)
		if err != nil || got != want {
			t.Errorf("Parse(%q) = %+v, %v; want %+v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "2", "x,1Gi", "1,y", "-1,1Gi", "1,-1Gi"} {
		if got, err := Parse(in); err == nil {
			t.Errorf("Parse(%q) = %+v, want an error", in, got)
		}
	}
}

func TestSubStopsAtZero(t *testing.T) {
	if got := (Size{CPU: 500, Memory: GiB}).Sub(S); got != (Size{}) {
		t.Errorf("got %+v, want nothing left", got)
	}
}

func TestNames(t *testing.T) {
	if S.Name() != "s" || XL.Name() != "xl" {
		t.Errorf("%s, %s", S.Name(), XL.Name())
	}
	if got := (Size{CPU: 1500, Memory: 512 * MiB}).Name(); got != "1.5 vCPU, 512 MiB" {
		t.Errorf("got %q", got)
	}
}

func writeCgroup(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestDetectUsesCgroupLimits(t *testing.T) {
	host := func() (int64, error) { return 64 * GiB, nil }
	got, err := detect(writeCgroup(t, map[string]string{"cpu.max": "50000 100000\n", "memory.max": "1073741824\n"}), host)
	if err != nil || got != (Size{CPU: 500, Memory: GiB}) {
		t.Errorf("cgroup v2: %+v, %v", got, err)
	}
	got, err = detect(writeCgroup(t, map[string]string{"cpu/cpu.cfs_quota_us": "25000", "cpu/cpu.cfs_period_us": "100000", "memory/memory.limit_in_bytes": "2147483648"}), host)
	if err != nil || got != (Size{CPU: 250, Memory: 2 * GiB}) {
		t.Errorf("cgroup v1: %+v, %v", got, err)
	}
	// "max" means no limit, and a limit above the host's doesn't count.
	got, err = detect(writeCgroup(t, map[string]string{"cpu.max": "max 100000\n", "memory.max": "max\n"}), host)
	if err != nil || got.CPU <= 0 || got.Memory != 64*GiB {
		t.Errorf("no limits: %+v, %v", got, err)
	}
}

func TestDetectReadsThisHost(t *testing.T) {
	got, err := Detect()
	if err != nil || got.CPU <= 0 || got.Memory <= 0 {
		t.Fatalf("got %+v, %v", got, err)
	}
}
