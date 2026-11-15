//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
)

// latestRecord is the pipeline's newest checkpoint record.
func latestRecord(t *testing.T, name string) *checkpoint.Record {
	t.Helper()
	ids := mustQuery[string](t, "SELECT org_id || ' ' || id FROM pipelines WHERE name = $1", name)
	var org, id string
	if _, err := fmt.Sscan(ids, &org, &id); err != nil {
		t.Fatal(err)
	}
	store, err := checkpoint.NewFSStore(env.checkpoints)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := checkpoint.NewLog(store, org, id).Latest(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// A keyed table read with cdc, merged into Postgres by an attempt on two
// workers in pods: one worker reads the changes from the replication slot,
// and its engine's exchange sends each row over TLS to the writer lane that
// owns its key group, on either worker, so both workers' committables hold
// the stream. A worker killed mid-stream fails the attempt, the next one
// restores both workers from the record, and every change is merged once.
func TestCDCAcrossWorkers(t *testing.T) {
	plane, token := placementPlane(t)
	launcher := startPodLauncher(t, plane, token)
	startPlaneRunner(t, plane, token, "1,2Gi")
	name := unique("exchange")
	in, out := schema(name), schema(name)+"_out"
	src := pgx.Identifier{in, "users"}.Sanitize()
	dst := pgx.Identifier{out, "users"}.Sanitize()
	run(t, "CREATE SCHEMA "+pgx.Identifier{in}.Sanitize(),
		"CREATE TABLE "+src+" (id bigint PRIMARY KEY, name text)",
		"INSERT INTO "+src+" SELECT i, 'u' || i FROM generate_series(1, 3000) i")
	t.Cleanup(func() {
		if conn, err := pgx.Connect(context.Background(), env.dbURL); err == nil {
			_, _ = conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+pgx.Identifier{in}.Sanitize())
			_ = conn.Close(context.Background())
		}
	})
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "publication": %q, "snapshotChunkRows": 200}}}`, name, in, in),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"mode": "continuous", "dataPlane": %q, "resources": {"size": "m", "workers": 2}, "source": {"ref": %q},
			"destination": {"ref": %q, "namespace": %q}, "delivery": {"commit": {"interval": "1s"}}}}`, name, plane, name, name, out))
	pipeline := "workspaces/default/pipelines/" + name
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: pipeline}))
	})
	matches := func() bool {
		v, err := query[bool](t, `SELECT
			(SELECT count(*) FROM `+dst+` WHERE NOT _openetl_deleted) = (SELECT count(*) FROM `+src+`)
			AND NOT EXISTS (SELECT 1 FROM `+src+` s LEFT JOIN `+dst+` d USING (id)
				WHERE d.id IS NULL OR d._openetl_deleted OR d.name IS DISTINCT FROM s.name)`)
		return err == nil && v
	}
	eventually(t, 2*time.Minute, "the snapshot on two workers", matches)
	first := latestRecord(t, name)
	if committables, splits := coverage(t, name, first.Generation, "users"); committables < 2 || splits != 1 {
		t.Errorf("the records cover %d splits of users in %d committables, want its one in both workers'", splits, committables)
	}

	// The newest worker pod of the pipeline's attempt, as the launcher's
	// pods log it.
	id := mustQuery[string](t, "SELECT id::text FROM pipelines WHERE name = $1", name)
	started := regexp.MustCompile(`msg="runner running one worker of an attempt" .*id=\S+:(\d+) attempt=\S*` + regexp.QuoteMeta(id) + `\S* placement=\d+ worker=(\d+)`)
	m := started.FindAllStringSubmatch(launcher.out.String(), -1)
	if len(m) == 0 {
		t.Fatal("no worker pod of the pipeline")
	}
	pid, err := strconv.Atoi(m[len(m)-1][1])
	if err != nil {
		t.Fatal(err)
	}
	run(t, "UPDATE "+src+" SET name = 'renamed' WHERE id <= 1000")
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("killing worker %s's pod: %v", m[len(m)-1][2], err)
	}
	run(t, "DELETE FROM "+src+" WHERE id BETWEEN 2001 AND 2100",
		"INSERT INTO "+src+" SELECT i, 'new' FROM generate_series(3001, 3500) i")
	eventually(t, 3*time.Minute, "the changes after a worker was killed", matches)
	var last *checkpoint.Record
	eventually(t, time.Minute, "a record of a newer generation", func() bool {
		last = latestRecord(t, name)
		return last.Generation > first.Generation
	})
	if n := mustQuery[int](t, "SELECT count(*) FROM "+dst+" WHERE _openetl_deleted"); n != 100 {
		t.Errorf("%d rows marked deleted, want 100", n)
	}
	if committables, _ := coverage(t, name, last.Generation, "users"); committables < 2 {
		t.Errorf("generation %d's records cover users in %d committables, want both workers'", last.Generation, committables)
	}
}
