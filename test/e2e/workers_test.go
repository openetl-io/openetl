//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"gocloud.dev/blob"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/checkpoint"
)

// workersDocs are a Postgres source of the schema, read in key ranges, the
// destination dst, and a scheduled pipeline between them on the data plane,
// at size m and on this many workers; 0 is one pod. destination is the
// pipeline's destination settings besides its ref.
func workersDocs(name, plane, dst, destination string, workers int) []string {
	res := `{"size": "m"}`
	if workers > 0 {
		res = fmt.Sprintf(`{"size": "m", "workers": %d}`, workers)
	}
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "chunkRows": 1000}}`, schema(name))
	d := docs(name, src, dst, `{}`)
	d[2] = fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
		"spec": {"schedule": {"every": "720h"}, "dataPlane": %q, "resources": %s, "source": {"ref": %q},
		"destination": {"ref": %q %s}, "delivery": {"commit": {"maxBytes": "64KB"}}}}`, name, plane, res, name, name, destination)
	return d
}

// workerTables makes the schema's tables, each of rows keyed rows, analyzed
// so the source reads them in key ranges.
func workerTables(t *testing.T, name string, rows int, tables ...string) {
	t.Helper()
	sch := pgx.Identifier{schema(name)}.Sanitize()
	qs := []string{"CREATE SCHEMA " + sch}
	for _, tbl := range tables {
		ident := pgx.Identifier{schema(name), tbl}.Sanitize()
		qs = append(qs,
			"CREATE TABLE "+ident+" (id bigint PRIMARY KEY, name text)",
			fmt.Sprintf("INSERT INTO %s SELECT i, 'n' || i FROM generate_series(1, %d) i", ident, rows),
			"ANALYZE "+ident)
	}
	run(t, qs...)
}

// syncStats is a sync's stats, as its attempt's result gives them.
type syncStats struct {
	Generation, Restored, Last uint64
}

func statsOf(t *testing.T, stats string) syncStats {
	t.Helper()
	var s syncStats
	if err := json.Unmarshal([]byte(stats), &s); err != nil {
		t.Fatalf("the sync's stats %q: %v", stats, err)
	}
	return s
}

// lastAttempt is the latest sync's last attempt.
func lastAttempt(t *testing.T, name string) *apiv1.SyncAttempt {
	t.Helper()
	var last *apiv1.SyncAttempt
	for _, a := range latestSync(t, name).GetAttempts() {
		if last == nil || a.GetAttempt() > last.GetAttempt() {
			last = a
		}
	}
	if last == nil {
		t.Fatalf("%s's latest sync has no attempts", name)
	}
	return last
}

// discover discovers the source on a runner of the data plane, so that the
// plan and the placer know its streams.
func discover(t *testing.T, name, plane string) {
	t.Helper()
	if _, err := apiv1connect.NewSourceServiceClient(env.http, env.apiURL).DiscoverSource(t.Context(), connect.NewRequest(&apiv1.DiscoverSourceRequest{
		Name: "workspaces/default/sources/" + name, DataPlane: plane,
	})); err != nil {
		t.Fatal(err)
	}
}

// lastPlacements is how many times the pipeline's latest attempt was
// placed.
func lastPlacements(t *testing.T, name string) int {
	t.Helper()
	return mustQuery[int](t, `SELECT d.placements FROM dispatches d JOIN attempts a ON a.org_id = d.org_id AND a.id = d.id
		JOIN pipelines p ON p.org_id = a.org_id AND p.id = a.pipeline_id WHERE p.name = $1 ORDER BY a.create_time DESC LIMIT 1`, name)
}

// workersJoined are the workers, by number from 1, that joined the coordinator of
// the pipeline's latest attempt on workers, as the launcher's processes
// logged it.
func workersJoined(t *testing.T, launcher *runner, name string) map[int]bool {
	t.Helper()
	id := mustQuery[string](t, `SELECT a.id FROM attempts a JOIN pipelines p ON p.org_id = a.org_id AND p.id = a.pipeline_id
		WHERE p.name = $1 AND a.workers > 0 ORDER BY a.create_time DESC LIMIT 1`, name)
	re := regexp.MustCompile(`msg="worker joining its coordinator" .*attempt=` + regexp.QuoteMeta(id) + ` worker=(\d+)`)
	out := map[int]bool{}
	for _, m := range re.FindAllStringSubmatch(launcher.out.String(), -1) {
		n, _ := strconv.Atoi(m[1])
		out[n] = true
	}
	return out
}

// coverage is, for the pipeline's checkpoint records of generation gen, the
// committables whose coverage names any split, and the splits of stream
// named among them.
func coverage(t *testing.T, name string, gen uint64, stream string) (committables, splits int) {
	t.Helper()
	dir := mustQuery[string](t, "SELECT org_id || '/' || id FROM pipelines WHERE name = $1", name)
	store, err := checkpoint.NewFSStore(env.checkpoints)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := store.List(t.Context(), dir+"/_checkpoints/")
	if err != nil {
		t.Fatal(err)
	}
	covering, covered := map[int]bool{}, map[string]bool{}
	for _, k := range keys {
		if !strings.HasSuffix(k, ".json") {
			continue
		}
		data, _, err := store.Get(t.Context(), k)
		if err != nil {
			t.Fatal(err)
		}
		var rec checkpoint.Record
		if err := json.Unmarshal(data, &rec); err != nil {
			t.Fatal(err)
		}
		if rec.Generation != gen {
			continue
		}
		for i, c := range rec.Coverage {
			for _, s := range c.Splits {
				covering[i] = true
				if s.Stream == stream {
					covered[s.ID] = true
				}
			}
		}
	}
	return len(covering), len(covered)
}

// checkS3Times fails unless each of ids 1 to rows is in the table's files
// times times, and no other id is.
func checkS3Times(t *testing.T, b *blob.Bucket, table string, rows, times int) {
	t.Helper()
	ids, err := s3IDs(t, b, table)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[int64]int{}
	for _, id := range ids {
		counts[id]++
	}
	var wrong []string
	for _, id := range slices.Sorted(maps.Keys(counts)) {
		if n := counts[id]; n != times || id < 1 || id > int64(rows) {
			wrong = append(wrong, fmt.Sprintf("%d×%d", id, n))
		}
	}
	if len(counts) != rows || len(wrong) > 0 {
		t.Errorf("%s: %d rows, %d distinct ids, want %d ids %d times each; wrong: %v", table, len(ids), len(counts), rows, times, wrong[:min(len(wrong), 10)])
	}
}

// An attempt on workers runs as a coordinator and worker processes of a pod
// launcher. A table read in key ranges is spread across the workers, and
// every row lands once a sync, as the job is rescaled from one pod to three
// workers and back, each sync restoring from the record the one before
// wrote. A Postgres snapshot into Iceberg runs on workers too, each table
// read on one of them, as the plan says once the source is discovered, and
// so does a keyed table appended into Postgres or merged into Iceberg, read
// in key ranges across the workers, which hold columns, and a SQL
// transform. A job that can't run on workers runs on one pod, and the plan
// and the attempt say why.
func TestWorkersInPods(t *testing.T) {
	plane, token := placementPlane(t)
	launcher := startPodLauncher(t, plane, token)
	startPlaneRunner(t, plane, token, "1,2Gi")

	t.Run("rescaled", func(t *testing.T) {
		name := unique("workers-s3")
		dst, b := s3Dest(t)
		const rows = 30_000
		workerTables(t, name, rows, "items")
		apply(t, workersDocs(name, plane, dst, s3Append, 0)...)
		status, stats := waitSync(t, name, 3*time.Minute)
		if status != "succeeded" {
			t.Fatalf("sync on one pod: %s %s", status, stats)
		}
		first := statsOf(t, stats)
		checkS3Times(t, b, "items", rows, 1)
		if a := lastAttempt(t, name); a.GetWorkers() != 0 || a.GetOnePod() != "" {
			t.Errorf("the first sync ran on %d workers (%q), want one pod", a.GetWorkers(), a.GetOnePod())
		}

		res := applyAll(t, false, workersDocs(name, plane, dst, s3Append, 3)...)
		if j := res.GetPlans()[0].GetJob(); j.GetWorkers() != 3 {
			t.Errorf("the plan runs the job on %d workers, want 3; warnings %v", j.GetWorkers(), res.GetPlans()[0].GetWarnings())
		}
		status, stats = syncNow(t, name)
		if status != "succeeded" {
			t.Fatalf("sync on workers: %s %s", status, stats)
		}
		second := statsOf(t, stats)
		checkS3Times(t, b, "items", rows, 2)
		if a := lastAttempt(t, name); a.GetWorkers() != 3 || a.GetOnePod() != "" {
			t.Errorf("the second sync's attempt ran on %d workers (%q), want 3", a.GetWorkers(), a.GetOnePod())
		}
		if got := workersJoined(t, launcher, name); len(got) != 3 || !got[1] || !got[2] || !got[3] {
			t.Errorf("workers %v joined their coordinator, want 1, 2 and 3", slices.Sorted(maps.Keys(got)))
		}
		// Rows of the table's key ranges reached several writer lanes.
		if committables, splits := coverage(t, name, second.Generation, "items"); committables < 2 || splits < 2 {
			t.Errorf("the workers' records cover %d splits in %d committables, want them spread", splits, committables)
		}
		if second.Restored != first.Last || second.Generation <= first.Generation {
			t.Errorf("on workers: generation %d restored epoch %d, after generation %d ended at epoch %d", second.Generation, second.Restored, first.Generation, first.Last)
		}

		apply(t, workersDocs(name, plane, dst, s3Append, 0)...)
		status, stats = syncNow(t, name)
		if status != "succeeded" {
			t.Fatalf("sync back on one pod: %s %s", status, stats)
		}
		third := statsOf(t, stats)
		checkS3Times(t, b, "items", rows, 3)
		if a := lastAttempt(t, name); a.GetWorkers() != 0 {
			t.Errorf("the third sync ran on %d workers, want one pod", a.GetWorkers())
		}
		if third.Restored != second.Last || third.Generation <= second.Generation {
			t.Errorf("back on one pod: generation %d restored epoch %d, after generation %d ended at epoch %d", third.Generation, third.Restored, second.Generation, second.Last)
		}
	})

	t.Run("snapshot into Iceberg", func(t *testing.T) {
		name := unique("workers-ice")
		dst := icebergDest(t, schema(name)+"_out")
		tables := []string{"a", "b", "c"}
		workerTables(t, name, 5000, tables...)
		// Discovered, the source's streams are in the plan, which says which
		// worker reads each table.
		d := workersDocs(name, plane, dst, "", 3)
		apply(t, d[0])
		discover(t, name, plane)
		plan := applyAll(t, false, d...).GetPlans()[0]
		for _, tbl := range tables {
			if !slices.ContainsFunc(plan.GetWarnings(), func(w *apiv1.PlanWarning) bool {
				return w.GetStream() == tbl && strings.Contains(w.GetMessage(), "each attempt reads it on one of its 3 workers: an overwrite's read ends on one worker")
			}) {
				t.Errorf("no warning that %s is read on one worker: %v", tbl, plan.GetWarnings())
			}
		}
		if plan.GetJob().GetWorkers() != 3 {
			t.Errorf("the plan runs the snapshot on %d workers, want 3", plan.GetJob().GetWorkers())
		}
		status, stats := waitSync(t, name, 3*time.Minute)
		if status != "succeeded" {
			t.Fatalf("sync %s: %s", status, stats)
		}
		checkIceberg(t, schema(name)+"_out", 5000, tables...)
		if a := lastAttempt(t, name); a.GetWorkers() != 3 {
			t.Errorf("the snapshot ran on %d workers (%q), want 3", a.GetWorkers(), a.GetOnePod())
		}
	})

	// A keyed table, read in key ranges, appended into Postgres and merged
	// into Iceberg, both of which hold columns: each attempt runs on
	// workers, which read its key ranges apart, and every row lands once.
	for _, k := range []struct{ dest, write string }{{"postgres", "append"}, {"iceberg", "merge"}} {
		t.Run("keyed "+k.write+" into "+k.dest, func(t *testing.T) {
			name := unique("workers-" + k.dest)
			out := schema(name) + "_out"
			dst := `{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}`
			destination := fmt.Sprintf(`, "namespace": %q, "write": %q`, out, k.write)
			if k.dest == "iceberg" {
				dst, destination = icebergDest(t, out), fmt.Sprintf(`, "write": %q`, k.write)
			}
			const rows = 20_000
			workerTables(t, name, rows, "items")
			d := workersDocs(name, plane, dst, destination, 3)
			if k.write == "merge" {
				d[2] = strings.Replace(d[2], `"delivery": {`, `"delivery": {"require": "effectively_once", `, 1)
			}
			apply(t, d[0])
			discover(t, name, plane)
			plan := applyAll(t, false, d...).GetPlans()[0]
			if plan.GetJob().GetWorkers() != 3 || slices.ContainsFunc(plan.GetWarnings(), func(w *apiv1.PlanWarning) bool {
				return strings.Contains(w.GetMessage(), "one of its 3 workers") || strings.Contains(w.GetMessage(), "one pod")
			}) {
				t.Errorf("the plan runs the job on %d workers, and warns %v; want 3, reading items on all", plan.GetJob().GetWorkers(), plan.GetWarnings())
			}
			status, stats := waitSync(t, name, 3*time.Minute)
			if status != "succeeded" {
				t.Fatalf("sync %s: %s", status, stats)
			}
			if k.dest == "iceberg" {
				checkIceberg(t, out, rows, "items")
			} else {
				checkRows(t, out, rows, "items")
			}
			a := lastAttempt(t, name)
			if a.GetWorkers() != 3 || a.GetOnePod() != "" {
				t.Errorf("the sync ran on %d workers (%q), want 3", a.GetWorkers(), a.GetOnePod())
			}
			if committables, splits := coverage(t, name, statsOf(t, stats).Generation, "items"); committables < 2 || splits < 2 {
				t.Errorf("the workers' records cover %d splits in %d committables, want them spread", splits, committables)
			}
		})
	}

	// A SQL transform runs on workers: the statement's output, and the
	// stream it reads, which passes through, land every row once.
	t.Run("sql", func(t *testing.T) {
		name := unique("workers-sql")
		dst, b := s3Dest(t)
		const rows = 20_000
		workerTables(t, name, rows, "items")
		sql, err := json.Marshal("INSERT INTO evens SELECT id, upper(name) AS label FROM items WHERE id % 2 = 0")
		if err != nil {
			t.Fatal(err)
		}
		d := workersDocs(name, plane, dst, s3Append, 3)
		d[2] = strings.Replace(d[2], `"delivery":`, fmt.Sprintf(`"transform": {"sql": %s}, "delivery":`, sql), 1)
		apply(t, d...)
		status, stats := waitSync(t, name, 3*time.Minute)
		if status != "succeeded" {
			t.Fatalf("sync %s: %s", status, stats)
		}
		checkS3Times(t, b, "items", rows, 1)
		ids, err := s3IDs(t, b, "evens")
		if err != nil {
			t.Fatal(err)
		}
		seen := map[int64]bool{}
		for _, id := range ids {
			if seen[id] || id%2 != 0 || id < 1 || id > rows {
				t.Fatalf("evens has id %d, again or that the statement leaves out", id)
			}
			seen[id] = true
		}
		if len(seen) != rows/2 {
			t.Errorf("evens has %d ids, want %d", len(seen), rows/2)
		}
		if a := lastAttempt(t, name); a.GetWorkers() != 3 || a.GetOnePod() != "" {
			t.Errorf("the sync ran on %d workers (%q), want 3", a.GetWorkers(), a.GetOnePod())
		}
	})

	// A job that can't run on workers runs on one pod, and the plan and the
	// attempt say why: the control plane knows ahead, and places the attempt
	// on one pod at once.
	t.Run("one pod", func(t *testing.T) {
		name := unique("workers-one-pod")
		workerTables(t, name, 5000, "items")
		dir := t.TempDir()
		d := workersDocs(name, plane, parquetDest(dir), "", 3)
		const why = "a destination the engine writes runs on one pod"
		plan := planOf(t, d...)
		if plan.GetJob().GetWorkers() != 0 || !slices.ContainsFunc(plan.GetWarnings(), func(w *apiv1.PlanWarning) bool {
			return strings.Contains(w.GetMessage(), "resources.workers asks for 3 workers, and the job runs on one pod: "+why)
		}) {
			t.Errorf("the plan runs the job on %d workers, and warns %v; want one pod, because %s", plan.GetJob().GetWorkers(), plan.GetWarnings(), why)
		}
		apply(t, d...)
		status, stats := waitSync(t, name, 3*time.Minute)
		if status != "succeeded" {
			t.Fatalf("sync %s: %s", status, stats)
		}
		checkParquet(t, dir, 5000, "items")
		if a := lastAttempt(t, name); a.GetWorkers() != 0 || !strings.Contains(a.GetOnePod(), why) {
			t.Errorf("the attempt ran on %d workers, and says %q; want one pod, because %s", a.GetWorkers(), a.GetOnePod(), why)
		}
		if n := lastPlacements(t, name); n != 1 {
			t.Errorf("the attempt was placed %d times, want once, on one pod", n)
		}
	})

	eventually(t, time.Minute, "the launcher's pods to go", func() bool { return launcher.pods(t) == 0 })
}
