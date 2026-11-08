//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

// A continuous Postgres CDC pipeline whose slot is lost fails with
// POSITION_LOST, and its incident pauses it. A reset and a resume read the
// table again through a new slot, with the rows changed meanwhile, and the
// incident resolves.
func TestResetAfterALostSlot(t *testing.T) {
	startRunner(t)
	name := unique("reset")
	in, out := schema(name), schema(name)+"_out"
	src := pgx.Identifier{in, "users"}.Sanitize()
	dst := pgx.Identifier{out, "users"}.Sanitize()
	run(t, "CREATE SCHEMA "+pgx.Identifier{in}.Sanitize(),
		"CREATE TABLE "+src+" (id bigint PRIMARY KEY, name text)",
		"INSERT INTO "+src+" SELECT i, 'u' || i FROM generate_series(1, 100) i")
	t.Cleanup(func() {
		if conn, err := pgx.Connect(context.Background(), env.dbURL); err == nil {
			_, _ = conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+pgx.Identifier{in}.Sanitize())
			_ = conn.Close(context.Background())
		}
	})
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "publication": %q}}}`, name, in, in),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"mode": "continuous", "source": {"ref": %q}, "destination": {"ref": %q, "namespace": %q},
			"delivery": {"commit": {"interval": "1s"}}}}`, name, name, name, out))
	pipeline := "workspaces/default/pipelines/" + name
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: pipeline}))
	})
	id := mustQuery[string](t, "SELECT id::text FROM pipelines WHERE name = $1", name)
	slot := "openetl_" + stripDashes(id)
	matches := func() bool {
		v, err := query[bool](t, `SELECT
			(SELECT count(*) FROM `+dst+` WHERE NOT _openetl_deleted) = (SELECT count(*) FROM `+src+`)
			AND NOT EXISTS (SELECT 1 FROM `+src+` s LEFT JOIN `+dst+` d USING (id)
				WHERE d.id IS NULL OR d._openetl_deleted OR d.name IS DISTINCT FROM s.name)`)
		return err == nil && v
	}
	eventually(t, 60*time.Second, "the snapshot", matches)

	// The run holds the slot and takes it back, so this ends its connection
	// until the drop wins.
	eventually(t, 60*time.Second, "the slot to be dropped", func() bool {
		_, _ = query[bool](t, "SELECT pg_terminate_backend(active_pid) IS NOT NULL FROM pg_replication_slots WHERE slot_name = $1 AND active", slot)
		_, _ = query[int](t, "SELECT count(*) FROM (SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active) d", slot)
		n, err := query[int](t, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", slot)
		return err == nil && n == 0
	})
	run(t, "UPDATE "+src+" SET name = 'changed' WHERE id <= 10", "INSERT INTO "+src+" SELECT i, 'new' FROM generate_series(101, 120) i")
	incident := func(status string) func() bool {
		return func() bool {
			n, err := query[int](t, "SELECT count(*) FROM incidents WHERE pipeline_id = $1::uuid AND class = 'position_lost' AND status = $2", id, status)
			return err == nil && n == 1
		}
	}
	eventually(t, 90*time.Second, "a position_lost incident", incident("open"))
	eventually(t, 30*time.Second, "the pipeline to be paused", func() bool {
		p, err := query[bool](t, "SELECT paused FROM pipelines WHERE id = $1::uuid", id)
		return err == nil && p
	})

	if _, err := pipelines().ResetPipeline(t.Context(), connect.NewRequest(&apiv1.ResetPipelineRequest{Name: pipeline})); err != nil {
		t.Fatal(err)
	}
	if _, err := pipelines().ResumePipeline(t.Context(), connect.NewRequest(&apiv1.ResumePipelineRequest{Name: pipeline})); err != nil {
		t.Fatal(err)
	}
	eventually(t, 90*time.Second, "the rows after the reset", matches)
	eventually(t, 30*time.Second, "the incident to resolve", incident("resolved"))
	if n := mustQuery[int](t, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", slot); n != 1 {
		t.Errorf("%d slots named %s after the reset, want a new one", n, slot)
	}
	run(t, "UPDATE "+src+" SET name = 'after' WHERE id > 110")
	eventually(t, 60*time.Second, "changes through the new slot", matches)
}
