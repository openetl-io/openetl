package dbt

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// fakeDbt records how it was run in target/, then acts on the project's
// mode file.
const fakeDbt = `#!/bin/sh
mkdir -p target
printf '%s\n' "$@" > target/args
env | sort > target/env
cp "$3/profiles.yml" target/profiles.yml
echo "12:00:00  Running with dbt=fake"
case "$(cat mode)" in
ok)
	printf '%s' '{"results": [{"unique_id": "model.shop.a", "status": "success", "message": "CREATE VIEW"},
		{"unique_id": "test.shop.t", "status": "pass", "message": null}], "elapsed_time": 1.5}' > target/run_results.json
	exit 0;;
fail)
	printf '%s' '{"results": [{"unique_id": "model.shop.a", "status": "error", "message": "Database Error\n  relation \"x\" does not exist"},
		{"unique_id": "model.shop.b", "status": "skipped", "message": null}], "elapsed_time": 0.5}' > target/run_results.json
	exit 1;;
crash)
	echo "12:00:01  Encountered an error:"
	echo "Compilation Error in model a"
	exit 2;;
hang)
	trap 'echo interrupted; exit 130' INT
	while :; do sleep 0.1; done;;
esac
`

func fakeRun(t *testing.T, mode string, inv Invocation) (*Result, string, error) {
	t.Helper()
	dir := t.TempDir()
	inv.Executable = filepath.Join(dir, "dbt")
	if err := os.WriteFile(inv.Executable, []byte(fakeDbt), 0o700); err != nil {
		t.Fatal(err)
	}
	bundle, err := encode(map[string][]byte{"dbt_project.yml": []byte(projectYAML), "mode": []byte(mode)})
	if err != nil {
		t.Fatal(err)
	}
	if inv.Target == nil {
		if inv.Target, err = DestinationTarget("postgres", []byte(`{"dsn": "postgres://u:pw@db/shop"}`), "raw", ""); err != nil {
			t.Fatal(err)
		}
	}
	work := filepath.Join(dir, "work")
	res, err := Exec(t.Context(), bundle, work, inv)
	return res, filepath.Join(work, "project", "target"), err
}

func TestExec(t *testing.T) {
	t.Setenv("OPENETL_TEST_RUNNER_ONLY", "1")
	res, target, err := fakeRun(t, "ok", Invocation{Command: "build", Select: " tag:a  b+ ", Exclude: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Nodes["success"] != 1 || res.Nodes["pass"] != 1 || res.Elapsed != 1500*time.Millisecond {
		t.Errorf("result = %+v", res)
	}
	read := func(name string) string {
		b, err := os.ReadFile(filepath.Join(target, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	args := strings.Split(strings.TrimSpace(read("args")), "\n")
	want := []string{"build", "--profiles-dir", filepath.Join(filepath.Dir(filepath.Dir(target)), "profiles"), "--select", "tag:a", "b+", "--exclude", "c"}
	if !slices.Equal(args, want) {
		t.Errorf("args = %q, want %q", args, want)
	}
	env := read("env")
	for _, v := range []string{"OPENETL_DBT_HOST=db", "DBT_ENV_SECRET_OPENETL_PASSWORD=pw", "OPENETL_NAMESPACE=raw", "DBT_SEND_ANONYMOUS_USAGE_STATS=false"} {
		if !strings.Contains(env, v+"\n") {
			t.Errorf("dbt's environment lacks %s:\n%s", v, env)
		}
	}
	if strings.Contains(env, "OPENETL_TEST_RUNNER_ONLY") {
		t.Error("dbt got the runner's environment")
	}
	if p := read("profiles.yml"); !strings.Contains(p, "shop:") || strings.Contains(p, "pw") {
		t.Errorf("profiles.yml:\n%s", p)
	}
}

func TestExecFailures(t *testing.T) {
	res, _, err := fakeRun(t, "fail", Invocation{Command: "run"})
	var f *Failure
	if !errors.As(err, &f) || f.Exit != 1 {
		t.Fatalf("err = %v", err)
	}
	if want := `dbt run exited with 1: model.shop.a: Database Error relation "x" does not exist`; err.Error() != want {
		t.Errorf("err = %q, want %q", err, want)
	}
	if res.Nodes["error"] != 1 || res.Nodes["skipped"] != 1 {
		t.Errorf("nodes = %v", res.Nodes)
	}

	_, _, err = fakeRun(t, "crash", Invocation{Command: "build"})
	if !errors.As(err, &f) || f.Exit != 2 || len(f.Failed) != 0 {
		t.Fatalf("err = %v", err)
	}
	if !strings.HasSuffix(err.Error(), "Encountered an error:; Compilation Error in model a") {
		t.Errorf("err = %q", err)
	}

	bundle, _ := encode(map[string][]byte{"dbt_project.yml": []byte(projectYAML)})
	tg, _ := DestinationTarget("postgres", []byte(`{"dsn": "host=db user=u"}`), "", "")
	_, err = Exec(t.Context(), bundle, t.TempDir(), Invocation{Executable: filepath.Join(t.TempDir(), "dbt"), Command: "build", Target: tg})
	if !errors.Is(err, ErrStart) {
		t.Errorf("a missing dbt: %v", err)
	}
}

func TestFailureNamesAFewNodes(t *testing.T) {
	f := &Failure{Command: "build", Exit: 1, Failed: []string{"a: x", "b: y", "c: z", "d: w", "e: v"}}
	if got, want := f.Error(), "dbt build exited with 1: a: x; b: y; c: z; and 2 more"; got != want {
		t.Errorf("%q, want %q", got, want)
	}
	if len(f.Failed) != 5 {
		t.Error("Error changed Failed")
	}
}

// Cancelling interrupts dbt, which gets to stop.
func TestExecCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(500*time.Millisecond, cancel)
	dir := t.TempDir()
	exe := filepath.Join(dir, "dbt")
	if err := os.WriteFile(exe, []byte(fakeDbt), 0o700); err != nil {
		t.Fatal(err)
	}
	bundle, _ := encode(map[string][]byte{"dbt_project.yml": []byte(projectYAML), "mode": []byte("hang")})
	tg, _ := DestinationTarget("postgres", []byte(`{"dsn": "host=db user=u"}`), "", "")
	start := time.Now()
	_, err := Exec(ctx, bundle, filepath.Join(dir, "work"), Invocation{Executable: exe, Command: "build", Target: tg})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("took %v to stop", d)
	}
}
