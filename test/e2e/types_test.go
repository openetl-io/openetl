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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/engineclient"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	runnerpkg "github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// Postgres to Postgres through the engine: declared numerics arrive as
// decimals, values that don't fit are quarantined, and infinities are
// clamped.
func TestPostgresTypesRoundTrip(t *testing.T) {
	got := typesRoundTrip(t, nil)
	for col, want := range map[string]string{
		"amount": "numeric(12,2)", "any_num": "numeric(38,9)", "day": "date", "at_time": "time without time zone",
		"tstz": "timestamp with time zone", "uid": "uuid", "doc": "jsonb",
	} {
		if got.types[col] != want {
			t.Errorf("%s is %s, want %s", col, got.types[col], want)
		}
	}
	want := "1|12.50|1.123456789|2026-10-31|12:34:56.789|9999-12-31 23:59:59.999999|0b0e1f2a-3c4d-4e5f-8a9b-0c1d2e3f4a5b|{\"a\": 1}"
	if got.rows != want {
		t.Errorf("rows are %q, want %q", got.rows, want)
	}
	lines := got.quarantined
	if len(lines) != 3 || !strings.HasPrefix(lines[0]["reason"], "nan:") || lines[1]["reason"] != pastArrow || !strings.HasPrefix(lines[2]["reason"], "precision:") {
		t.Fatalf("quarantined %v", lines)
	}
	if !strings.Contains(lines[0]["record"], `"id":2`) {
		t.Errorf("quarantined record %v", lines[0])
	}
	if !strings.Contains(lines[1]["record"], `"tstz":"294250-01-01T00:00:00Z"`) {
		t.Errorf("quarantined record %v", lines[1])
	}
}

// Postgres's timestamps go on past where Arrow's microseconds end.
const pastArrow = `range: column "tstz" is out of range for microseconds`

// Decimal columns overridden to float64 and string keep the values a
// decimal can't hold.
func TestPostgresTypeOverrides(t *testing.T) {
	got := typesRoundTrip(t, func(j *runnerpkg.Job) {
		j.Streams[0].Overrides = map[string]arrow.DataType{"amount": arrow.PrimitiveTypes.Float64, "any_num": arrow.BinaryTypes.String}
	})
	if got.types["amount"] != "double precision" || got.types["any_num"] != "text" {
		t.Errorf("amount is %s and any_num %s, want double precision and text", got.types["amount"], got.types["any_num"])
	}
	want := "1|12.5|1.123456789|2026-10-31|12:34:56.789|9999-12-31 23:59:59.999999|0b0e1f2a-3c4d-4e5f-8a9b-0c1d2e3f4a5b|{\"a\": 1};" +
		"2|NaN|1|0001-01-01;4|1|0.0000000001"
	if got.rows != want {
		t.Errorf("rows are %q, want %q", got.rows, want)
	}
	if len(got.quarantined) != 1 || got.quarantined[0]["reason"] != pastArrow {
		t.Errorf("quarantined %v", got.quarantined)
	}
}

// With types.infinity: quarantine, rows with an infinite date or timestamp
// are quarantined. The overrides keep the other edge cases out.
func TestPostgresInfinitiesQuarantined(t *testing.T) {
	got := typesRoundTrip(t, func(j *runnerpkg.Job) {
		j.Streams[0].Overrides = map[string]arrow.DataType{"amount": arrow.PrimitiveTypes.Float64, "any_num": arrow.BinaryTypes.String}
		j.QuarantineInfinity = true
	})
	if want := "4|1|0.0000000001"; got.rows != want {
		t.Errorf("rows are %q, want %q", got.rows, want)
	}
	lines := got.quarantined
	if len(lines) != 3 || lines[0]["reason"] != `range: column "tstz" is infinite` || lines[1]["reason"] != `range: column "day" is infinite` || lines[2]["reason"] != pastArrow {
		t.Fatalf("quarantined %v", lines)
	}
}

type roundTrip struct {
	// Column types in the destination.
	types map[string]string
	// Each row's non-null values joined with |, rows with ;.
	rows        string
	quarantined []map[string]string
}

// typesRoundTrip syncs a table of edge-case values once, after edit changes
// the job.
func typesRoundTrip(t *testing.T, edit func(*runnerpkg.Job)) roundTrip {
	t.Helper()
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
			(3, 1, 1, NULL, NULL, '294250-01-01 00:00:00+00', NULL, NULL),
			(4, 1, 0.0000000001, NULL, NULL, NULL, NULL, NULL)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	t.Cleanup(func() {
		conn, err := pgx.Connect(context.Background(), env.dbURL)
		if err != nil {
			return
		}
		for _, s := range []string{src, dst} {
			_, _ = conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+s+" CASCADE")
		}
		_ = conn.Close(context.Background())
	})

	source := postgresSource(t, fmt.Appendf(nil, `{"dsn": %q, "schema": %q}`, env.dbURL, src))
	cat, err := source.Discover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pg := postgresDestination(t, fmt.Appendf(nil, `{"dsn": %q, "schema": %q}`, env.dbURL, dst))
	quarantine := t.TempDir()
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", name)
	cs := cat.Streams[0]
	job := runnerpkg.Job{
		Owner:         "a1",
		Pipeline:      name,
		Log:           log,
		Source:        source,
		Streams:       []runnerpkg.Stream{{Name: cs.Name, Replay: cs.Replay, Mode: destination.Append, PrimaryKey: cs.PrimaryKey, Schema: cs.Schema}},
		Destination:   pg,
		Dataflow:      &engineclient.Dataflow{Options: engineclient.Options{Binary: env.engine}},
		QuarantineURL: "file://" + quarantine,
		Settings: delivery.Settings{
			Require: delivery.ExactlyOnce,
			Scope:   destination.ScopePipeline,
		},
	}
	if edit != nil {
		edit(&job)
	}
	if _, err = runnerpkg.RunAttempt(ctx, job); err != nil {
		t.Fatal(err)
	}

	res := roundTrip{types: map[string]string{}}
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
		res.types[n] = typ
	}
	err = conn.QueryRow(ctx, `SELECT coalesce(string_agg(concat_ws('|', id, amount, any_num, day, at_time, tstz AT TIME ZONE 'UTC', uid, doc), ';' ORDER BY id), '')
		FROM `+dst+`.items`).Scan(&res.rows)
	if err != nil {
		t.Fatal(err)
	}

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
			res.quarantined = append(res.quarantined, line)
		}
	}
	return res
}

// A raw column that widens from int64 to float64 to string stops an unkeyed
// stream: Postgres doesn't change a column's type, and nothing can hold it.
// Once the user has altered the column, the sync lands every value, widened
// within its epoch.
func TestWideningIntoPostgresWaitsForTheUser(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	name := unique("widen")
	pg := postgresDestination(t, fmt.Appendf(nil, `{"dsn": %q, "schema": %q}`, env.dbURL, schema(name)))
	t.Cleanup(func() {
		conn, err := pgx.Connect(context.Background(), env.dbURL)
		if err == nil {
			_, _ = conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema(name)+" CASCADE")
			_ = conn.Close(context.Background())
		}
	})
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", name)
	attempt := func(owner string) (runnerpkg.Result, error) {
		return runnerpkg.RunAttempt(ctx, runnerpkg.Job{
			Owner:       owner,
			Pipeline:    name,
			Log:         log,
			Source:      wideningSource{},
			Streams:     []runnerpkg.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append}},
			Destination: pg,
			Dataflow:    &engineclient.Dataflow{Options: engineclient.Options{Binary: env.engine}},
			Settings: delivery.Settings{
				Require: delivery.ExactlyOnce,
				Scope:   destination.ScopePipeline,
			},
		})
	}
	typ := func(table string) string {
		got, err := query[string](t, "SELECT format_type(atttypid, atttypmod) || ' ' || (SELECT string_agg(x::text, ',' ORDER BY id) FROM "+
			table+") FROM pg_attribute WHERE attrelid = $1::regclass AND attname = 'x'", table)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	table := schema(name) + ".s"
	if res, err := attempt("a1"); err != nil || res.Committed == 0 {
		t.Fatalf("the first sync: %+v, %v", res, err)
	}
	_, err := attempt("a2")
	if errs.CodeOf(err) != errs.SchemaDrift || !strings.Contains(err.Error(), "doesn't change a column's type") {
		t.Fatalf("a widening across syncs: %v", err)
	}
	if got, want := typ(table), "bigint 1"; got != want {
		t.Errorf("after the refusal, x is %q, want %q", got, want)
	}
	run(t, "ALTER TABLE "+table+" ALTER COLUMN x TYPE text")
	if res, err := attempt("a3"); err != nil || res.Committed == 0 {
		t.Fatalf("after altering x: %+v, %v", res, err)
	}
	if got, want := typ(table), "text 1,2,2.5,a"; got != want {
		t.Errorf("x is %q, want %q", got, want)
	}
}

// wideningSource reads x = 1 on its first sync, then 2, 2.5 and "a", each a
// batch.
type wideningSource struct{}

func (wideningSource) Read(_ context.Context, req source.ReadRequest, out source.Emitter) error {
	rows := []string{`{"id": 1, "x": 1}`}
	if req.Streams[0].State != nil {
		rows = []string{`{"id": 2, "x": 2}`, `{"id": 3, "x": 2.5}`, `{"id": 4, "x": "a"}`}
	}
	for _, r := range rows {
		if err := out.Rows("s", []json.RawMessage{json.RawMessage(r)}); err != nil {
			return err
		}
	}
	return out.Complete("s", []byte(`"read"`))
}
