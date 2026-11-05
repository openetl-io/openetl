//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
)

// Columns added to a source table reach the destination without anyone
// acting, under the default schema policy: on the next scheduled sync of an
// incremental stream, and mid-run through Postgres CDC, into Postgres and,
// with destination-iceberg, into Iceberg. Rows from before the change get the
// new columns empty, unless they changed after it.
func TestAdditiveSchemaChange(t *testing.T) {
	startRunner(t)
	name := unique("addcol")
	in := schema(name)
	inc, cdc := pgx.Identifier{in, "items"}.Sanitize(), pgx.Identifier{in, "users"}.Sanitize()
	next := "nextval('" + pgx.Identifier{in, "items_at"}.Sanitize() + "')"
	run(t, "CREATE SCHEMA "+pgx.Identifier{in}.Sanitize(),
		"CREATE SEQUENCE "+pgx.Identifier{in, "items_at"}.Sanitize(),
		"CREATE TABLE "+inc+" (id bigint PRIMARY KEY, v int NOT NULL, at bigint NOT NULL DEFAULT "+next+")",
		"INSERT INTO "+inc+" (id, v) SELECT i, 1 FROM generate_series(1, 100) i",
		"CREATE TABLE "+cdc+" (id bigint PRIMARY KEY, name text)",
		"INSERT INTO "+cdc+" SELECT i, 'u' || i FROM generate_series(1, 100) i",
		"CREATE PUBLICATION "+pgx.Identifier{in}.Sanitize()+" FOR TABLE "+cdc)
	t.Cleanup(func() {
		if conn, err := pgx.Connect(context.Background(), env.dbURL); err == nil {
			_, _ = conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+pgx.Identifier{in}.Sanitize())
			_ = conn.Close(context.Background())
		}
	})

	scheduled, continuous := name+"-inc", name+"-cdc"
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q}}}`, scheduled, in),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, scheduled),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "1h"}, "source": {"ref": %q, "streams": [{"name": "items", "sync": "incremental", "cursor": "at"}]},
			"destination": {"ref": %q, "namespace": %q, "write": "merge"}, "delivery": {"require": "effectively_once"}}}`,
			scheduled, scheduled, scheduled, in+"_inc"),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "publication": %q}}}`, continuous, in, in),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, continuous),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"mode": "continuous", "source": {"ref": %q}, "destination": {"ref": %q, "namespace": %q},
			"delivery": {"commit": {"interval": "1s"}}}}`, continuous, continuous, continuous, in+"_cdc"))
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + continuous}))
	})
	lake := name + "-ice"
	_, err := os.Stat(filepath.Join(env.connectors, "destination-iceberg"))
	withIceberg := err == nil
	if withIceberg {
		apply(t,
			fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
				"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "publication": %q}}}`, lake, in, in),
			fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q}, "spec": %s}`, lake, icebergDest(t, in+"_ice")),
			fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
				"spec": {"mode": "continuous", "source": {"ref": %q}, "destination": {"ref": %q}, "delivery": {"commit": {"interval": "1s"}}}}`, lake, lake, lake))
		t.Cleanup(func() {
			_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + lake}))
		})
	} else {
		t.Log("OPENETL_CONNECTORS has no destination-iceberg; Postgres only")
	}
	if status, stats := waitSync(t, scheduled, 60*time.Second); status != "succeeded" {
		t.Fatalf("first sync %s: %s", status, stats)
	}
	merged, changes := pgx.Identifier{in + "_inc", "items"}.Sanitize(), pgx.Identifier{in + "_cdc", "users"}.Sanitize()
	// The destination holds the source's rows and columns, the added ones
	// included once they exist.
	same := func(src, dst string, cols ...string) bool {
		t.Helper()
		differ := ""
		for _, c := range cols {
			differ += " OR d." + c + " IS DISTINCT FROM s." + c
		}
		v, err := query[bool](t, `SELECT (SELECT count(*) FROM `+dst+`) = (SELECT count(*) FROM `+src+`)
			AND NOT EXISTS (SELECT 1 FROM `+src+` s LEFT JOIN `+dst+` d USING (id) WHERE d.id IS NULL`+differ+`)`)
		return err == nil && v
	}
	eventually(t, 60*time.Second, "the CDC snapshot", func() bool { return same(cdc, changes, "name") })
	// Iceberg's users, by id: name, score and email, as Postgres prints them.
	sourceUsers := func() map[int64][3]string {
		t.Helper()
		conn, err := pgx.Connect(t.Context(), env.dbURL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close(context.Background()) }()
		cols := "name, '', ''"
		if mustQuery[bool](t, "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = $1 AND table_name = 'users' AND column_name = 'score')", in) {
			cols = "name, coalesce(score::text, ''), coalesce(email, '')"
		}
		rows, err := conn.Query(t.Context(), "SELECT id, "+cols+" FROM "+cdc)
		if err != nil {
			t.Fatal(err)
		}
		out := map[int64][3]string{}
		for rows.Next() {
			var id int64
			var r [3]string
			if err := rows.Scan(&id, &r[0], &r[1], &r[2]); err != nil {
				t.Fatal(err)
			}
			out[id] = r
		}
		return out
	}
	lakeUsers := func() map[int64][3]string {
		t.Helper()
		tbl, err := icebergTable(t, in+"_ice", "users")
		if err != nil {
			return nil
		}
		_, it, err := tbl.Scan().ToArrowRecords(t.Context())
		if err != nil {
			return nil
		}
		out := map[int64][3]string{}
		for b, err := range it {
			if err != nil {
				return nil
			}
			col := func(name string) arrow.Array {
				if i := b.Schema().FieldIndices(name); len(i) == 1 {
					return b.Column(i[0])
				}
				return nil
			}
			ids, deleted := col("id").(*array.Int64), col("_openetl_deleted")
			for i := range ids.Len() {
				if d, ok := deleted.(*array.Boolean); ok && d.Value(i) {
					continue
				}
				var r [3]string
				for k, c := range []string{"name", "score", "email"} {
					if a := col(c); a != nil && a.IsValid(i) {
						r[k] = a.ValueStr(i)
					}
				}
				out[ids.Value(i)] = r
			}
			b.Release()
		}
		return out
	}
	if withIceberg {
		eventually(t, 2*time.Minute, "the CDC snapshot in Iceberg", func() bool { return maps.Equal(lakeUsers(), sourceUsers()) })
	}

	run(t, "ALTER TABLE "+inc+" ADD COLUMN price numeric(10, 2), ADD COLUMN tag text",
		"UPDATE "+inc+" SET price = id / 4.0, at = "+next+" WHERE id % 10 = 0",
		"INSERT INTO "+inc+" (id, v, price, tag) SELECT i, 1, i / 2.0, 't' || i FROM generate_series(101, 150) i",
		"ALTER TABLE "+cdc+" ADD COLUMN score int, ADD COLUMN email text",
		"UPDATE "+cdc+" SET score = id WHERE id % 10 = 0",
		"INSERT INTO "+cdc+" SELECT i, 'u' || i, i * 2, 'u' || i || '@example.com' FROM generate_series(101, 150) i")

	if status, stats := syncNow(t, scheduled); status != "succeeded" {
		t.Fatalf("the sync after the change %s: %s", status, stats)
	}
	if !same(inc, merged, "v", "price", "tag") {
		t.Errorf("the merged items differ from the source's: %s", mustQuery[string](t, `SELECT count(*) || ' rows, ' ||
			count(*) FILTER (WHERE price IS NOT NULL) || ' with a price' FROM `+merged))
	}
	eventually(t, 2*time.Minute, "the changes after the CDC table's new columns", func() bool { return same(cdc, changes, "name", "score", "email") })
	if withIceberg {
		eventually(t, 2*time.Minute, "the new columns in Iceberg", func() bool { return maps.Equal(lakeUsers(), sourceUsers()) })
		if got := statuses(runs(t, lake)); got != "running" {
			t.Errorf("Iceberg's runs %q, want the first still running", got)
		}
	}
	if got := statuses(runs(t, continuous)); got != "running" {
		t.Errorf("runs %q, want the first still running", got)
	}
}

// Into Kafka with a schema registry, columns added to a CDC table register a
// new version of the topic's value schema, which the registry's
// compatibility (BACKWARD, compose's default) accepts, and the run goes on.
// In Protobuf the fields keep their numbers, so a reader with the first
// version still reads later records' fields.
func TestAdditiveSchemaChangeToKafka(t *testing.T) {
	startRunner(t)
	for _, format := range []string{"avro", "protobuf"} {
		t.Run(format, func(t *testing.T) {
			name := unique("addcol-" + format)
			in := schema(name)
			kafkaDest(t, in)
			src := pgx.Identifier{in, "users"}.Sanitize()
			run(t, "CREATE SCHEMA "+pgx.Identifier{in}.Sanitize(),
				"CREATE TABLE "+src+" (id bigint PRIMARY KEY, name text)",
				"INSERT INTO "+src+" SELECT i, 'u' || i FROM generate_series(1, 100) i",
				"CREATE PUBLICATION "+pgx.Identifier{in}.Sanitize()+" FOR TABLE "+src)
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
					"spec": {"connector": "kafka", "config": {"brokers": [%q], "namespace": %q, "format": %q, "schemaRegistryUrl": %q}}}`,
					name, kafkaBrokers(), in, format, registryURL()),
				fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
					"spec": {"mode": "continuous", "source": {"ref": %q}, "destination": {"ref": %q}, "delivery": {"commit": {"interval": "1s"}}}}`, name, name, name))
			t.Cleanup(func() {
				_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
			})
			topic := in + ".users"
			vals := &registryValues{t: t, format: format, schemas: map[uint32]string{}}
			// Each id's last record, and the source's rows: name, score and
			// email.
			latest := func() map[string][3]string {
				out := map[string][3]string{}
				for _, b := range topicRecords(t, topic) {
					v := vals.decode(b)
					out[v["id"]] = [3]string{v["name"], v["score"], v["email"]}
				}
				return out
			}
			want := func() map[string][3]string {
				cols := "name, '', ''"
				if mustQuery[bool](t, "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = $1 AND table_name = 'users' AND column_name = 'score')", in) {
					cols = "name, coalesce(score::text, ''), coalesce(email, '')"
				}
				conn, err := pgx.Connect(t.Context(), env.dbURL)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = conn.Close(context.Background()) }()
				rows, err := conn.Query(t.Context(), "SELECT id::text, "+cols+" FROM "+src)
				if err != nil {
					t.Fatal(err)
				}
				out := map[string][3]string{}
				for rows.Next() {
					var id string
					var r [3]string
					if err := rows.Scan(&id, &r[0], &r[1], &r[2]); err != nil {
						t.Fatal(err)
					}
					out[id] = r
				}
				return out
			}
			eventually(t, 60*time.Second, "the snapshot", func() bool { return maps.Equal(latest(), want()) })

			run(t, "ALTER TABLE "+src+" ADD COLUMN score int, ADD COLUMN email text",
				"UPDATE "+src+" SET score = id WHERE id % 10 = 0",
				"INSERT INTO "+src+" SELECT i, 'u' || i, i * 2, 'u' || i || '@example.com' FROM generate_series(101, 150) i")
			eventually(t, 2*time.Minute, "the changes after the new columns", func() bool { return maps.Equal(latest(), want()) })

			var versions []int
			if err := registryGet(t, "/subjects/"+topic+"-value/versions", &versions); err != nil {
				t.Fatal(err)
			}
			if len(versions) != 2 {
				t.Errorf("%s-value has versions %v, want 2", topic, versions)
			}
			if format == "protobuf" && len(versions) > 0 {
				var first struct{ Schema string }
				if err := registryGet(t, fmt.Sprintf("/subjects/%s-value/versions/%d", topic, versions[0]), &first); err != nil {
					t.Fatal(err)
				}
				for _, b := range topicRecords(t, topic) {
					own, old := vals.decode(b), vals.read(first.Schema, b)
					for _, f := range []string{"id", "name", "_openetl_id", "_openetl_op"} {
						if old[f] != own[f] {
							t.Fatalf("with the first version, %s reads %q, want %q", f, old[f], own[f])
						}
					}
				}
			}
			if got := statuses(runs(t, name)); got != "running" {
				t.Errorf("runs %q, want the first still running", got)
			}
		})
	}
}
