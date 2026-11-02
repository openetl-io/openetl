//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

// A continuous pipeline reads a Postgres table's snapshot and then its
// changes through a replication slot, and merges them into Postgres: updates,
// soft deletes, a key's change, and an update that leaves a TOAST value
// alone. A runner killed mid-stream hands the job, and the slot, to another.
// Deleting the pipeline drops its slot.
func TestPostgresCDC(t *testing.T) {
	a, b := startRunner(t), startRunner(t)
	name := unique("cdc")
	in, out := schema(name), schema(name)+"_out"
	src := pgx.Identifier{in, "users"}.Sanitize()
	dst := pgx.Identifier{out, "users"}.Sanitize()
	run(t, "CREATE SCHEMA "+pgx.Identifier{in}.Sanitize(),
		"CREATE TABLE "+src+" (id bigint PRIMARY KEY, name text, doc text)",
		"ALTER TABLE "+src+" ALTER COLUMN doc SET STORAGE EXTERNAL",
		"INSERT INTO "+src+" SELECT i, 'u' || i, NULL FROM generate_series(1, 300) i")
	t.Cleanup(func() {
		if conn, err := pgx.Connect(context.Background(), env.dbURL); err == nil {
			_, _ = conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+pgx.Identifier{in}.Sanitize())
			_ = conn.Close(context.Background())
		}
	})
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "publication": %q, "snapshotChunkRows": 50}}}`, name, in, in),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"mode": "continuous", "source": {"ref": %q}, "destination": {"ref": %q, "namespace": %q},
			"delivery": {"commit": {"interval": "1s"}}}}`, name, name, name, out))
	pipeline := "workspaces/default/pipelines/" + name
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(t.Context(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: pipeline}))
	})
	id := mustQuery[string](t, "SELECT id::text FROM pipelines WHERE name = $1", name)
	slot := "openetl_" + stripDashes(id)

	// What the destination shows of the source: live rows, and deleted ones.
	matches := func() bool {
		v, err := query[bool](t, `SELECT
			(SELECT count(*) FROM `+dst+` WHERE NOT _openetl_deleted) = (SELECT count(*) FROM `+src+`)
			AND NOT EXISTS (SELECT 1 FROM `+src+` s LEFT JOIN `+dst+` d USING (id)
				WHERE d.id IS NULL OR d._openetl_deleted OR d.name IS DISTINCT FROM s.name OR d.doc IS DISTINCT FROM s.doc)`)
		return err == nil && v
	}
	eventually(t, 60*time.Second, "the snapshot", matches)
	if write := mustQuery[string](t, "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = $1::regclass AND attname = '_openetl_seq'", dst); write != "numeric(38,0)" {
		t.Errorf("_openetl_seq is %s", write)
	}

	big := fmt.Sprintf("repeat('d', %d)", 9000)
	run(t, "UPDATE "+src+" SET doc = "+big+" WHERE id <= 10",
		"UPDATE "+src+" SET name = 'renamed' WHERE id <= 20",
		"DELETE FROM "+src+" WHERE id BETWEEN 21 AND 30",
		"UPDATE "+src+" SET id = id + 1000 WHERE id BETWEEN 31 AND 35",
		"INSERT INTO "+src+" SELECT i, 'new', NULL FROM generate_series(301, 350) i")
	eventually(t, 60*time.Second, "the changes", matches)
	if n := mustQuery[int](t, "SELECT count(*) FROM "+dst+" WHERE _openetl_deleted"); n != 15 {
		t.Errorf("%d rows marked deleted, want 15", n)
	}

	host(t, a, b).kill(t)
	run(t, "UPDATE "+src+" SET name = 'after' WHERE id > 300")
	eventually(t, 90*time.Second, "the changes after the runner died", matches)
	if got := statuses(runs(t, name)); got != "running" {
		t.Errorf("runs %q, want one running", got)
	}
	// The slot's lag, as the source reports it, reaches the run.
	eventually(t, 30*time.Second, "the run's progress", func() bool {
		res, err := apiv1connect.NewSyncServiceClient(env.http, env.apiURL).ListSyncs(t.Context(), connect.NewRequest(&apiv1.ListSyncsRequest{Parent: pipeline, PageSize: 1}))
		if err != nil || len(res.Msg.GetSyncs()) == 0 {
			return false
		}
		return slices.ContainsFunc(res.Msg.GetSyncs()[0].GetProgress(), func(p *apiv1.ReadProgress) bool {
			return p.GetStream() == "" && p.LagBytes != nil && p.GetLag() != nil && p.GetTime() != nil
		})
	})

	if _, err := pipelines().DeletePipeline(t.Context(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: pipeline})); err != nil {
		t.Fatal(err)
	}
	eventually(t, 60*time.Second, "the slot to be dropped", func() bool {
		n, err := query[int](t, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", slot)
		return err == nil && n == 0
	})
}

func stripDashes(s string) string {
	out := make([]byte, 0, len(s))
	for i := range len(s) {
		if s[i] != '-' {
			out = append(out, s[i])
		}
	}
	return string(out)
}
