//go:build e2e

package e2e

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/apache/iceberg-go/table"
	"github.com/jackc/pgx/v5"
)

// The compose Iceberg REST catalog, which keeps its tables in the S3 store's
// iceberg bucket.
func icebergURI() string { return cmp.Or(os.Getenv("OPENETL_TEST_ICEBERG_URI"), "http://127.0.0.1:8181") }
func s3Endpoint() string { return cmp.Or(os.Getenv("OPENETL_TEST_S3_ENDPOINT"), "http://127.0.0.1:8333") }

// icebergDest returns a destination spec writing into namespace, once the
// catalog's bucket exists. It skips the test without destination-iceberg.
func icebergDest(t *testing.T, namespace string) string {
	t.Helper()
	if _, err := os.Stat(filepath.Join(env.connectors, "destination-iceberg")); err != nil {
		t.Skip("OPENETL_CONNECTORS has no destination-iceberg")
	}
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPut, s3Endpoint()+"/iceberg", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create the iceberg bucket (make deps-up): %v", err)
	}
	_ = res.Body.Close()
	key := setSecret(t, "openetl")
	return fmt.Sprintf(`{"connector": "iceberg", "config": {"uri": %q, "namespace": %q, "s3Endpoint": %q, "region": "us-east-1",
		"accessKeyId": {"secret": %q}, "secretAccessKey": {"secret": %q}}}`, icebergURI(), namespace, s3Endpoint(), key, key)
}

func icebergDocs(name, src, dst, write, delivery string) []string {
	return []string{
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, src),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q}, "spec": %s}`, name, dst),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "1h"}, "source": {"ref": %q}, "destination": {"ref": %q, "write": %q}, "delivery": %s}}`, name, name, name, write, delivery),
	}
}

func icebergTable(t *testing.T, namespace, name string) (*table.Table, error) {
	t.Helper()
	cat, err := rest.NewCatalog(t.Context(), "e2e", icebergURI(), rest.WithAdditionalProps(iceberg.Properties{
		"s3.endpoint": s3Endpoint(), "s3.region": "us-east-1", "s3.access-key-id": "openetl", "s3.secret-access-key": "openetl",
	}))
	if err != nil {
		t.Fatal(err)
	}
	return cat.LoadTable(t.Context(), table.Identifier{namespace, name})
}

// checkIceberg fails unless each table has want rows, each id once.
func checkIceberg(t *testing.T, namespace string, want int, tables ...string) {
	t.Helper()
	for _, name := range tables {
		tbl, err := icebergTable(t, namespace, name)
		if err != nil {
			t.Fatal(err)
		}
		_, it, err := tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		rows, seen := 0, map[int64]bool{}
		for b, err := range it {
			if err != nil {
				t.Fatal(err)
			}
			col := b.Column(0).(*array.Int64)
			for i := range col.Len() {
				seen[col.Value(i)] = true
				rows++
			}
			b.Release()
		}
		if rows != want || len(seen) != want {
			t.Errorf("%s.%s: %d rows and %d distinct ids, want %d each", namespace, name, rows, len(seen), want)
		}
	}
}

func TestPostgresToIceberg(t *testing.T) {
	startRunner(t)
	name := unique("postgres-iceberg")
	ns := schema(name)
	dst := icebergDest(t, ns)
	sch := pgx.Identifier{ns}.Sanitize()
	conn, err := pgx.Connect(t.Context(), env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, q := range []string{
		"CREATE SCHEMA " + sch,
		"CREATE TABLE " + sch + ".items (id bigint PRIMARY KEY, name text, price numeric(10, 2), at timestamptz, uid uuid)",
		"INSERT INTO " + sch + ".items SELECT i, 'item ' || i, i / 100.0, now(), gen_random_uuid() FROM generate_series(1, 5000) i",
		"CREATE TABLE " + sch + ".events (id bigint, kind text)",
		"INSERT INTO " + sch + ".events SELECT i, 'k' || (i % 3) FROM generate_series(1, 700) i",
	} {
		if _, err := conn.Exec(t.Context(), q); err != nil {
			t.Fatal(err)
		}
	}
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "chunkRows": 1000}}`, ns)
	apply(t, icebergDocs(name, src, dst, "overwrite", `{"commit": {"interval": "1s"}}`)...)
	if status, stats := waitSync(t, name, 90*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkIceberg(t, ns, 5000, "items")
	checkIceberg(t, ns, 700, "events")
	tbl, err := icebergTable(t, ns, "items")
	if err != nil {
		t.Fatal(err)
	}
	if got := tbl.CurrentSnapshot().Summary.Properties["openetl.stream"]; got != "items" {
		t.Errorf("snapshot summary's stream: %q", got)
	}
}

const icebergSlowFaker = `{"connector": "faker", "config": {"seed": 4, "count": 2000, "batchSize": 50, "delay": "100ms"}}`

// checkpointRecords counts the pipeline's checkpoint records.
func checkpointRecords(t *testing.T, pipeline string) int {
	t.Helper()
	id, err := query[string](t, "SELECT id::text FROM pipelines WHERE name = $1", pipeline)
	if err != nil {
		return 0
	}
	m, _ := filepath.Glob(filepath.Join(env.checkpoints, "*", id, "_checkpoints", "*.json"))
	return len(m)
}

// finishedLater fails unless the sync succeeded in an attempt after the
// first. A loaded machine can time out the second one too.
func finishedLater(t *testing.T, status, stats string) {
	t.Helper()
	if status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	var g int
	if m := regexp.MustCompile(`"Generation": (\d+)`).FindStringSubmatch(stats); m != nil {
		g, _ = strconv.Atoi(m[1])
	}
	if g < 2 {
		t.Errorf("stats %s: want a later attempt to have finished", stats)
	}
}

// An overwrite read that spans the engine's death: the next attempt
// continues it, and the table ends with the read's rows once.
func TestIcebergEngineKilledMidSync(t *testing.T) {
	r := startRunner(t)
	name := unique("iceberg-engine-killed")
	ns := schema(name)
	apply(t, icebergDocs(name, icebergSlowFaker, icebergDest(t, ns), "overwrite", `{"commit": {"interval": "1s"}}`)...)
	pid := r.engine(t)
	eventually(t, 30*time.Second, "a checkpoint", func() bool { return checkpointRecords(t, name) > 0 })
	// Gone already if its attempt failed, as a busy Temporal can make it.
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		t.Fatal(err)
	}
	status, stats := waitSync(t, name, 90*time.Second)
	finishedLater(t, status, stats)
	checkIceberg(t, ns, 2000, "users", "purchases")
}

// Appends that land while the runner dies, with its destination process.
func TestIcebergRunnerKilledMidSync(t *testing.T) {
	first := startRunner(t)
	name := unique("iceberg-runner-killed")
	ns := schema(name)
	apply(t, icebergDocs(name, icebergSlowFaker, icebergDest(t, ns), "append", `{"commit": {"interval": "1s"}}`)...)
	dst := first.child(t, "destination-ice")
	eventually(t, 30*time.Second, "a committed epoch", func() bool {
		tbl, err := icebergTable(t, ns, "users")
		if errors.Is(err, catalog.ErrNoSuchTable) || err != nil {
			return false
		}
		return tbl.CurrentSnapshot() != nil
	})
	first.kill(t)
	eventually(t, 10*time.Second, "the orphaned destination to exit", func() bool {
		return errors.Is(syscall.Kill(dst, 0), syscall.ESRCH)
	})
	startRunner(t)
	status, stats := waitSync(t, name, 90*time.Second)
	finishedLater(t, status, stats)
	checkIceberg(t, ns, 2000, "users", "purchases")
}
