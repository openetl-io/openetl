//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

// The retry re-reads 150 rows per stream that are already applied. The
// default, exactly_once, dedupes them at apply.
func TestSupersetKilledMidSync(t *testing.T) {
	r := startRunner(t)
	name := unique("superset-killed")
	faker := `{"seed": 3, "count": 2000, "batchSize": 50, "delay": "50ms", "overlap": 150}`
	apply(t, pipelineDocs(name, faker, `{"commit": {"interval": "1s"}}`)...)
	pid := r.engine(t, name)
	eventually(t, 30*time.Second, "a committed epoch", func() bool { return committedEpochs(t, schema(name)) > 0 })
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	status, stats := waitSync(t, name, 90*time.Second)
	if status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if !strings.Contains(stats, `"Generation": 2`) {
		t.Errorf("stats %s: want the second attempt to have finished", stats)
	}
	checkRows(t, schema(name), 2000, "users", "purchases")
}

// A table without a primary key isn't deduped, so rows it holds twice land
// twice.
func TestDuplicateRowsLand(t *testing.T) {
	startRunner(t)
	name := unique("duplicate-rows")
	in, out := pgx.Identifier{schema(name)}.Sanitize(), schema(name)+"_out"
	conn, err := pgx.Connect(t.Context(), env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, q := range []string{
		"CREATE SCHEMA " + in,
		"CREATE TABLE " + in + ".views (user_id bigint, path text)",
		"INSERT INTO " + in + ".views SELECT i % 350, '/p' || (i % 350) FROM generate_series(1, 700) i",
	} {
		if _, err := conn.Exec(t.Context(), q); err != nil {
			t.Fatal(err)
		}
	}
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q}}`, schema(name))
	dst := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q}}`, out)
	apply(t, docs(name, src, dst, `{}`)...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	got, err := query[string](t, "SELECT count(*) || ' ' || count(DISTINCT (user_id, path)) FROM "+pgx.Identifier{out, "views"}.Sanitize())
	if err != nil {
		t.Fatal(err)
	}
	if got != "700 350" {
		t.Errorf("rows and distinct rows are %s, want 700 350", got)
	}
	if n, err := query[int](t, "SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND tablename = 'views'", out); err != nil || n != 0 {
		t.Errorf("views has %d indexes (%v), want none", n, err)
	}
}

func TestPlanChecksFailTheSync(t *testing.T) {
	startRunner(t)
	name := unique("plan-checks")
	src := `{"connector": "faker", "config": {"count": 10, "overlap": 5}}`
	apply(t, docs(name, src, parquetDest(t.TempDir()), `{}`)...)
	status, msg := waitSync(t, name, 60*time.Second)
	if status != "failed" {
		t.Fatalf("sync %s: %s", status, msg)
	}
	for _, stream := range []string{"users", "purchases"} {
		if want := `stream "` + stream + `" gets at_least_once (superset replay, and the destination can't dedupe), below exactly_once`; !strings.Contains(msg, want) {
			t.Errorf("error %q doesn't say %q", msg, want)
		}
	}
}

// A superset stream without a primary key, whose source holds some rows
// twice, synced twice. The second sync reads every row again, with the same
// IDs, since the stream's cursor is part of them. Each combination of
// collapse and require does what the plan warns it does, and the syncs count
// what collapsed.
func TestCollapse(t *testing.T) {
	startRunner(t)
	// 300 rows a sync, in batches of 50; rows 10, 20, ... 290 copy the row
	// before them, so 271 are distinct. 24 copies are in the same batch as
	// their original, and so in the same epoch.
	const faker = `{"count": 300, "batchSize": 50, "overlap": 1, "keyless": true, "twice": 10}`
	for _, tc := range []struct {
		name, stream, warning string
		indexed               bool
		// Rows in the table after both syncs.
		rows func(n int) bool
	}{
		{name: "default", stream: `{"name": "users"}`, warning: "within each epoch and against the target", indexed: true,
			rows: func(n int) bool { return n == 271 }},
		{name: "epoch", stream: `{"name": "users", "require": "at_least_once", "collapse": true}`, warning: "within each epoch only",
			rows: func(n int) bool { return n >= 2*271 && n <= 600-2*24 }},
		{name: "none", stream: `{"name": "users", "require": "at_least_once"}`, warning: "doesn't collapse, so every row lands",
			rows: func(n int) bool { return n == 600 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := unique("collapse-" + tc.name)
			res := applyAll(t, false, collapseDocs(t, name, faker, tc.stream)...)
			var users []*apiv1.PlanWarning
			for _, w := range res.GetPlans()[0].GetWarnings() {
				if w.GetStream() != "" {
					users = append(users, w)
				}
			}
			if len(users) != 1 || users[0].GetStream() != "users" || !strings.Contains(users[0].GetMessage(), tc.warning) {
				t.Errorf("warnings %v, want one for users that says %q", users, tc.warning)
			}
			if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
				t.Fatalf("first sync %s: %s", status, stats)
			}
			if status, stats := syncNow(t, name); status != "succeeded" {
				t.Fatalf("second sync %s: %s", status, stats)
			}
			table := pgx.Identifier{schema(name), "users"}.Sanitize()
			rows, err := query[int](t, "SELECT count(*) FROM "+table)
			if err != nil {
				t.Fatal(err)
			}
			if !tc.rows(rows) {
				t.Errorf("users has %d rows", rows)
			}
			if n, err := query[int](t, "SELECT count(DISTINCT id) FROM "+table); err != nil || n != 271 {
				t.Errorf("users has %d distinct ids (%v), want 271", n, err)
			}
			if n, err := query[int](t, "SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND tablename = 'users'", schema(name)); err != nil || (n == 1) != tc.indexed {
				t.Errorf("users has %d indexes (%v); want one: %v", n, err, tc.indexed)
			}
			// Each sync emitted 300 rows; what didn't land collapsed.
			var emitted, collapsed uint64
			eventually(t, 10*time.Second, "both syncs' checkpoints in the mirror", func() bool {
				res, err := syncs().ListSyncs(t.Context(), connect.NewRequest(&apiv1.ListSyncsRequest{Parent: "workspaces/default/pipelines/" + name}))
				if err != nil {
					t.Fatal(err)
				}
				emitted, collapsed = 0, 0
				for _, s := range res.Msg.GetSyncs() {
					for _, st := range s.GetStreams() {
						emitted += st.GetRows()
						collapsed += st.GetCollapsedRows()
					}
				}
				return emitted == 600
			})
			if collapsed != 600-uint64(rows) { //nolint:gosec // a row count
				t.Errorf("the syncs collapsed %d rows; %d of 600 landed", collapsed, rows)
			}
		})
	}

	// Without collapsing, exactly_once is out of reach, and Apply refuses.
	name := unique("collapse-refused")
	res := applyAll(t, true, collapseDocs(t, name, faker, `{"name": "users", "collapse": false}`)...)
	if want := `stream "users" gets at_least_once (superset replay, no primary key, and not collapsed), below exactly_once`; !strings.Contains(strings.Join(res.GetPlans()[0].GetProblems(), "\n"), want) {
		t.Errorf("problems %v, want one that says %q", res.GetPlans()[0].GetProblems(), want)
	}
}

// collapseDocs discovers the source first, so that the plan checks streams.
// It appends, so each sync's repeats land on the last's.
func collapseDocs(t *testing.T, name, faker, stream string) []string {
	t.Helper()
	d := pipelineDocs(name, faker, `{}`)
	apply(t, d[0])
	sources := apiv1connect.NewSourceServiceClient(env.http, env.apiURL)
	if _, err := sources.DiscoverSource(t.Context(), connect.NewRequest(&apiv1.DiscoverSourceRequest{Name: "workspaces/default/sources/" + name})); err != nil {
		t.Fatal(err)
	}
	d[2] = fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
		"spec": {"schedule": {"every": "1h"}, "source": {"ref": %q, "streams": [%s]},
		"destination": {"ref": %q, "namespace": %q, "write": "append"}}}`, name, name, stream, name, schema(name))
	return d
}
