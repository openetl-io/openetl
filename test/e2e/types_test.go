//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/unsuitable001/openetl/connectors/native/postgres"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/engineclient"
	runnerpkg "github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
)

// Postgres to Postgres through the engine: declared numerics arrive as
// decimals, values that don't fit are quarantined, and infinities are
// clamped.
func TestPostgresTypesRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	name := unique("types")
	src, dst := schema(name)+"_src", schema(name)+"_dst"
	conn, err := pgx.Connect(ctx, env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, q := range []string{
		"CREATE SCHEMA " + src,
		`CREATE TABLE ` + src + `.items (id int PRIMARY KEY, amount numeric(12, 2), any_num numeric,
			day date, at_time time, tstz timestamptz, uid uuid, doc jsonb)`,
		`INSERT INTO ` + src + `.items VALUES
			(1, 12.5, 1.123456789, '2026-10-31', '12:34:56.789', 'infinity', '0b0e1f2a-3c4d-4e5f-8a9b-0c1d2e3f4a5b', '{"a": 1}'),
			(2, 'NaN', 1, '-infinity', NULL, NULL, NULL, NULL),
			(3, 1, 0.0000000001, NULL, NULL, NULL, NULL, NULL)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	t.Cleanup(func() {
		for _, s := range []string{src, dst} {
			_, _ = conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+s+" CASCADE")
		}
	})

	source, err := postgres.NewSource(fmt.Appendf(nil, `{"dsn": %q, "schema": %q}`, env.dbURL, src))
	if err != nil {
		t.Fatal(err)
	}
	cat, err := source.Discover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pg, err := postgres.NewDestination(fmt.Appendf(nil, `{"dsn": %q, "schema": %q}`, env.dbURL, dst))
	if err != nil {
		t.Fatal(err)
	}
	quarantine := t.TempDir()
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", name)
	cs := cat.Streams[0]
	_, err = runnerpkg.RunAttempt(ctx, runnerpkg.Job{
		Owner:         "a1",
		Pipeline:      name,
		Log:           log,
		Source:        source,
		Streams:       []runnerpkg.Stream{{Name: cs.Name, Replay: cs.Replay, Mode: destination.Append, PrimaryKey: cs.PrimaryKey, Schema: cs.Schema}},
		Destination:   pg,
		Dataflow:      &engineclient.Dataflow{Options: engineclient.Options{Binary: env.engine}},
		QuarantineURL: "file://" + quarantine,
		Settings: runnerpkg.Settings{
			Require: delivery.ExactlyOnce,
			Scope:   destination.ScopePipeline,
			Dedupe:  delivery.DedupeNone,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	types := map[string]string{}
	rows, err := conn.Query(ctx, `SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute
		WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped`, dst+".items")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var n, typ string
		if err := rows.Scan(&n, &typ); err != nil {
			t.Fatal(err)
		}
		types[n] = typ
	}
	for col, want := range map[string]string{
		"amount": "numeric(12,2)", "any_num": "numeric(38,9)", "day": "date", "at_time": "time without time zone",
		"tstz": "timestamp with time zone", "uid": "uuid", "doc": "jsonb",
	} {
		if types[col] != want {
			t.Errorf("%s is %s, want %s", col, types[col], want)
		}
	}

	var got string
	err = conn.QueryRow(ctx, `SELECT string_agg(concat_ws('|', id, amount, any_num, day, at_time, tstz AT TIME ZONE 'UTC', uid, doc), ';' ORDER BY id)
		FROM `+dst+`.items`).Scan(&got)
	if err != nil {
		t.Fatal(err)
	}
	want := "1|12.50|1.123456789|2026-10-31|12:34:56.789|9999-12-31 23:59:59.999999|0b0e1f2a-3c4d-4e5f-8a9b-0c1d2e3f4a5b|{\"a\": 1}"
	if got != want {
		t.Errorf("rows are %q, want %q", got, want)
	}

	var lines []map[string]string
	files, _ := filepath.Glob(filepath.Join(quarantine, "*", "*", "items.jsonl"))
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for l := range strings.Lines(string(b)) {
			var line map[string]string
			if err := json.Unmarshal([]byte(l), &line); err != nil {
				t.Fatal(err)
			}
			lines = append(lines, line)
		}
	}
	if len(lines) != 2 || !strings.HasPrefix(lines[0]["reason"], "nan:") || !strings.HasPrefix(lines[1]["reason"], "precision:") {
		t.Fatalf("quarantined %v", lines)
	}
	if !strings.Contains(lines[0]["record"], `"id":2`) {
		t.Errorf("quarantined record %v", lines[0])
	}
}
