//go:build e2e

package e2e

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
)

// duckDB is the DuckDB CLI the tests compare with: OPENETL_DUCKDB, which
// must run, or duckdb on the path. Without either the test skips, but
// fails on CI, where the e2e action installs it.
func duckDB(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("OPENETL_DUCKDB"); p != "" {
		if err := exec.Command(p, "-version").Run(); err != nil {
			t.Fatalf("OPENETL_DUCKDB is %s, which doesn't run: %v", p, err)
		}
		return p
	}
	p, err := exec.LookPath("duckdb")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("no DuckDB CLI: set OPENETL_DUCKDB")
		}
		t.Skip("no duckdb on the path, and no OPENETL_DUCKDB")
	}
	return p
}

// duckRows runs query in DuckDB, in UTC, and gives its rows, each value as
// DuckDB writes it to CSV, a null as NULL.
func duckRows(t *testing.T, bin, query string) [][]string {
	t.Helper()
	dir := t.TempDir()
	out := filepath.Join(dir, "out.csv")
	init := filepath.Join(dir, "init.sql")
	if err := os.WriteFile(init, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	script := fmt.Sprintf("SET TimeZone = 'UTC';\nCOPY (%s) TO '%s' (HEADER false, NULLSTR 'NULL');\n", query, strings.ReplaceAll(out, "'", "''"))
	// Its own empty init file, so no ~/.duckdbrc applies.
	cmd := exec.Command(bin, "-bail", "-init", init)
	cmd.Stdin = strings.NewReader(script)
	if b, err := cmd.CombinedOutput(); err != nil || len(strings.TrimSpace(string(b))) > 0 {
		t.Fatalf("duckdb: %v: %s\n%s", err, b, script)
	}
	f, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// A running total's test records: record i is at t0 + i*overStep, of
// key k{i % overKeys}, on partition i % the topic's partitions, so each
// partition reads its records in the order of their times, none is late,
// and no two of a key share a time.
const (
	overKeys    = 5
	overStep    = 25 * time.Millisecond
	overRecords = 900
)

// The statement's functions, as ours and as DuckDB's SQL, over its
// columns k, p, o and ts.
const overColumns = `sum(o) OVER (PARTITION BY k ORDER BY ts) AS total,
  count(*) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS recent,
  max(o) OVER (PARTITION BY k ORDER BY ts RANGE BETWEEN INTERVAL '1 second' PRECEDING AND CURRENT ROW) AS top,
  min(o) OVER (PARTITION BY k ORDER BY ts ROWS UNBOUNDED PRECEDING) AS lowest,
  lag(o) OVER (PARTITION BY k ORDER BY ts) AS prev,
  lag(o, 3, -1) OVER (PARTITION BY k ORDER BY ts) AS third,
  first_value(p) OVER (PARTITION BY k ORDER BY ts) AS first_p`

// TestRunningTotalFromKafkaMatchesDuckDB runs a scheduled pipeline from
// Kafka's partitions into Postgres whose statement gives each record its
// key's running total of offsets, and the other functions of overColumns,
// by the records' timestamps, on one pod and on two workers. The input is
// bounded, its times half an hour back, so the sync's end moves every
// partition's watermark past them and every row is written once. DuckDB
// runs the same query over the same records, which the test writes as CSV
// from what Kafka acknowledged, and the table matches it row for row.
func TestRunningTotalFromKafkaMatchesDuckDB(t *testing.T) {
	bin := duckDB(t)
	t.Run("one pod", func(t *testing.T) {
		startRunner(t)
		runningTotal(t, bin, "", 0)
	})
	t.Run("workers", func(t *testing.T) {
		plane, token := placementPlane(t)
		startPodLauncher(t, plane, token)
		startPlaneRunner(t, plane, token, "1,2Gi")
		// A pipeline applied before the launcher reports runs on one pod.
		eventually(t, 30*time.Second, "the pod launcher to report", func() bool {
			n, err := query[int](t, "SELECT count(*) FROM runners WHERE data_plane_id = $1 AND launcher AND attempt = ''", plane)
			return err == nil && n == 1
		})
		runningTotal(t, bin, plane, 2)
	})
}

// runningTotal is TestRunningTotalFromKafkaMatchesDuckDB on the data plane,
// the default for "", on this many workers, or one pod for 0.
func runningTotal(t *testing.T, bin, plane string, workers int) {
	t.Helper()
	name := unique("over-kafka")
	topic, out := schema(name), schema(name)+"_out"
	const partitions = 6
	anyTime := "9223372036854775807"
	created, err := kadm.NewClient(kafkaClient(t)).CreateTopic(t.Context(), partitions, 1, map[string]*string{
		"message.timestamp.type":          kadm.StringPtr("CreateTime"),
		"message.timestamp.before.max.ms": &anyTime,
	}, topic)
	if err == nil {
		err = created.Err
	}
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Minute)
	var recs []*kgo.Record
	for i := range overRecords {
		recs = append(recs, &kgo.Record{Topic: topic, Partition: int32(i % partitions), Key: fmt.Appendf(nil, "k%d", i%overKeys),
			Value: fmt.Appendf(nil, `{"i": %d}`, i), Timestamp: t0.Add(time.Duration(i) * overStep)})
	}
	cl := kafkaClient(t, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	// Each record has its offset once Kafka has acknowledged it.
	if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(t.TempDir(), "input.csv")
	var lines []string
	lines = append(lines, "k,p,o,ts")
	for _, r := range recs {
		lines = append(lines, fmt.Sprintf("%s,%d,%d,%s", r.Key, r.Partition, r.Offset, r.Timestamp.UTC().Format("2006-01-02 15:04:05.000000+00:00")))
	}
	if err := os.WriteFile(input, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ours := strings.NewReplacer("(o)", "(_kafka_offset)", "(o,", "(_kafka_offset,", "(p)", "(_kafka_partition)",
		"PARTITION BY k", "PARTITION BY _kafka_key", "ORDER BY ts", "ORDER BY _kafka_timestamp").Replace(overColumns)
	sql, err := json.Marshal(fmt.Sprintf("INSERT INTO running\nSELECT _kafka_key AS k, _kafka_partition AS p, _kafka_offset AS o,\n  %s\nFROM %q", ours, topic))
	if err != nil {
		t.Fatal(err)
	}
	placed := ""
	if workers > 0 {
		placed = fmt.Sprintf(`"dataPlane": %q, "resources": {"size": "m", "workers": %d}, `, plane, workers)
	}
	docs := []string{
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, kafkaSource(t, topic)),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "720h"}, %s
			"source": {"ref": %q, "streams": [{"name": %q, "eventTime": "_kafka_timestamp"}]},
			"destination": {"ref": %q, "namespace": %q, "write": "append"},
			"delivery": {"commit": {"interval": "1s"}}, "transform": {"passthrough": "none", "sql": %s}}}`, name, placed, name, topic, name, out, sql),
	}
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
	})
	if workers > 0 {
		// The plan reads the partitions from the source's discovery.
		apply(t, docs[0])
		discover(t, name, plane)
		plan := planOf(t, docs...)
		if plan.GetJob().GetWorkers() != uint32(workers) || len(plan.GetProblems()) > 0 || slices.ContainsFunc(plan.GetWarnings(), func(w *apiv1.PlanWarning) bool {
			return strings.Contains(w.GetMessage(), "one pod") || strings.Contains(w.GetMessage(), fmt.Sprintf("one of its %d workers", workers))
		}) {
			t.Fatalf("the plan runs the job on %d workers, with problems %v and warnings %v; want %d workers that read every partition between them",
				plan.GetJob().GetWorkers(), plan.GetProblems(), plan.GetWarnings(), workers)
		}
		// One exchange, by the partition's key, before the statement.
		var exchanges []string
		for _, st := range plan.GetStreams() {
			for _, op := range st.GetOperators() {
				if op.GetExchange() {
					exchanges = append(exchanges, fmt.Sprintf("%s %s %v", st.GetName(), op.GetKind(), op.GetNeeds()))
				}
			}
		}
		if want := []string{"running PLAN_OPERATOR_KIND_STATEMENT [_kafka_key]"}; !slices.Equal(exchanges, want) {
			t.Errorf("the plan exchanges %v; want %v", exchanges, want)
		}
	}
	apply(t, docs...)
	if status, stats := waitSync(t, name, 3*time.Minute); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if workers > 0 {
		if n := mustQuery[int](t, `SELECT count(*) FROM attempts a JOIN pipelines p ON p.org_id = a.org_id AND p.id = a.pipeline_id
			WHERE p.name = $1 AND a.workers = $2`, name, workers); n == 0 {
			t.Errorf("no attempt ran on %d workers", workers)
		}
	}

	want := duckRows(t, bin, fmt.Sprintf("SELECT k, p, o,\n  %s\nFROM read_csv('%s', header = true, columns = {'k': 'VARCHAR', 'p': 'INTEGER', 'o': 'BIGINT', 'ts': 'TIMESTAMPTZ'})\nORDER BY p, o",
		overColumns, strings.ReplaceAll(input, "'", "''")))
	// Ordered by the table's numbers, as DuckDB's rows are, not by their
	// text, which the select list's labels would sort.
	got, err := rowsOf(t, `SELECT r.k, r.p::text, r.o::text, r.total::text, r.recent::text, r.top::text, r.lowest::text, coalesce(r.prev::text, 'NULL'), r.third::text, r.first_p::text FROM `+
		pgx.Identifier{out, "running"}.Sanitize()+` AS r ORDER BY r.p, r.o`, func(row pgx.CollectableRow) ([]string, error) {
		r := make([]string, 10)
		ptrs := make([]any, len(r))
		for i := range r {
			ptrs[i] = &r[i]
		}
		return r, row.Scan(ptrs...)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(want) != overRecords {
		t.Fatalf("DuckDB gave %d rows, want one for each of the %d records", len(want), overRecords)
	}
	differ := 0
	for i := range max(len(got), len(want)) {
		var g, w []string
		if i < len(got) {
			g = got[i]
		}
		if i < len(want) {
			w = want[i]
		}
		if !slices.Equal(g, w) {
			if differ++; differ <= 5 {
				t.Errorf("row %d: got %v, DuckDB gives %v", i, g, w)
			}
		}
	}
	if differ > 0 || len(got) != len(want) {
		t.Fatalf("%d of %d rows differ from DuckDB's; the table has %d", differ, len(want), len(got))
	}
	t.Logf("%d rows match DuckDB's, on %d workers", len(got), workers)
	if q := quarantined(t, name, "running"); len(q) != 0 {
		t.Errorf("quarantined %v", q)
	}
	// The operator took in each record once, wrote each once and found none
	// late, over every checkpoint of the sync, once the control plane has
	// them all: a sync can end before its last checkpoint's report.
	wantOps := fmt.Sprintf("one operator: in %d, %d inserts, 0 updates, 0 deletes, 0 late", overRecords, overRecords)
	var gotOps string
	for deadline := time.Now().Add(30 * time.Second); gotOps != wantOps; time.Sleep(500 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the operator's numbers over every checkpoint: got %s, want %s", gotOps, wantOps)
		}
		ops := operatorTotals(t, name, deadline)
		gotOps = fmt.Sprintf("%d operators", len(ops))
		for _, o := range ops {
			if len(ops) == 1 {
				gotOps = fmt.Sprintf("one operator: in %d, %d inserts, %d updates, %d deletes, %d late", o.in, o.inserts, o.updates, o.deletes, o.late)
			}
		}
	}
}

// opTotals are what an operator did over a pipeline's checkpoints.
type opTotals struct{ in, inserts, updates, deletes, late, repeats uint64 }

// operatorTotals sums, by operator ID, what every checkpoint of the pipeline
// the control plane has says each operator did, from one listing, each of
// its pages by the caller's deadline.
func operatorTotals(t *testing.T, name string, deadline time.Time) map[string]opTotals {
	t.Helper()
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	out := map[string]opTotals{}
	token := ""
	for {
		res, err := pipelines().ListCheckpoints(ctx, connect.NewRequest(&apiv1.ListCheckpointsRequest{
			Parent: "workspaces/default/pipelines/" + name, PageSize: 500, PageToken: token,
		}))
		if err != nil {
			t.Fatalf("listing %s's checkpoints: %v", name, err)
		}
		for _, c := range res.Msg.GetCheckpoints() {
			for _, o := range c.GetOperators() {
				s := out[o.GetOperator()]
				s.in += o.GetRowsIn()
				s.inserts += o.GetInserts()
				s.updates += o.GetUpdates()
				s.deletes += o.GetDeletes()
				s.late += o.GetLateRows()
				s.repeats += o.GetRepeats()
				out[o.GetOperator()] = s
			}
		}
		if token = res.Msg.GetNextPageToken(); token == "" {
			return out
		}
	}
}

// killEngine kills the engine the runner runs for the pipeline, and fails
// unless the signal reached a live one.
func killEngine(t *testing.T, r *runner, name string) int {
	t.Helper()
	var pid int
	eventually(t, 30*time.Second, "a live engine of "+name+" to kill", func() bool {
		pid = r.engine(t, name)
		switch err := syscall.Kill(pid, syscall.SIGKILL); {
		case err == nil:
			return true
		case errors.Is(err, syscall.ESRCH):
			// Gone already: the runner starts another, whose ready line
			// comes next.
			return false
		default:
			t.Fatal(err)
			return false
		}
	})
	return pid
}

// A dedupe test's records: record i is at t0 + i*dedupeStep, of key
// k{i % dedupeTestKeys}, on partition i % 2, so each partition reads its
// records in the order of their times and none is late. A key's records
// are dedupeTestKeys steps apart, so a retention of dedupeRetention takes
// several of them, and a key's records span it many times over.
const (
	dedupeTestKeys  = 7
	dedupeStep      = 250 * time.Millisecond
	dedupeRetention = 10 * time.Second
	// The key of the records that move the watermark past every other.
	dedupeEnd = "~end"
)

// TestDedupesThroughAKilledEngine runs two dedupes over one Kafka topic in
// a continuous pipeline into Postgres: one keeps each key's first record
// within a retention of 10 seconds of event time, appended, and one each
// key's latest record, merged. The engine is killed once a checkpoint
// holds the dedupes' state, and the next attempt restores it: each key's
// first records land once each, by their times alone, a record 10 seconds
// or more after the key's last first record written again as a new one;
// each key's row of the latest is its latest record. The dedupes'
// numbers, over every checkpoint, count each record once.
func TestDedupesThroughAKilledEngine(t *testing.T) {
	r := startRunner(t)
	name := unique("dedupe-kafka")
	topic, out := schema(name), schema(name)+"_out"
	// Far enough ahead that an idle partition's watermark, which follows the
	// clock, can't reach the records while the test runs; the topic takes
	// records a day ahead.
	t0 := time.Now().UTC().Truncate(time.Minute).Add(20 * time.Minute)
	ahead := "86400000"
	if _, err := kadm.NewClient(kafkaClient(t)).CreateTopic(t.Context(), 2, 1, map[string]*string{
		"message.timestamp.type": kadm.StringPtr("CreateTime"), "message.timestamp.after.max.ms": &ahead,
	}, topic); err != nil {
		t.Fatal(err)
	}
	cl := kafkaClient(t, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	produced := 0
	produce := func(n int) {
		t.Helper()
		var recs []*kgo.Record
		for i := produced; i < produced+n; i++ {
			recs = append(recs, &kgo.Record{Topic: topic, Partition: int32(i % 2), Key: fmt.Appendf(nil, "k%d", i%dedupeTestKeys),
				Value: fmt.Appendf(nil, `{"i": %d}`, i), Timestamp: t0.Add(time.Duration(i) * dedupeStep)})
		}
		if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
			t.Fatal(err)
		}
		produced += n
	}

	sql, err := json.Marshal(fmt.Sprintf(`INSERT INTO firsts
SELECT _kafka_key AS k, _kafka_timestamp AS at, _kafka_partition AS p
FROM %[1]q
QUALIFY row_number() OVER (PARTITION BY _kafka_key ORDER BY _kafka_timestamp) = 1;

INSERT INTO latest
SELECT k, at, p FROM (
  SELECT _kafka_key AS k, _kafka_timestamp AS at, _kafka_partition AS p,
    row_number() OVER (PARTITION BY _kafka_key ORDER BY _kafka_timestamp DESC) AS rn
  FROM %[1]q
) AS t
WHERE rn = 1`, topic))
	if err != nil {
		t.Fatal(err)
	}
	docs := []string{
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, kafkaSource(t, topic)),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		// No destination.write: the first rows are appended, and the latest
		// merged, as the plan says.
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"mode": "continuous",
			"source": {"ref": %q, "streams": [{"name": %q, "eventTime": "_kafka_timestamp", "dedupe": {"retention": %q}}]},
			"destination": {"ref": %q, "namespace": %q}, "delivery": {"commit": {"interval": "1s"}},
			"transform": {"passthrough": "none", "sql": %s}}}`, name, name, topic, dedupeRetention.String(), name, out, sql),
	}
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
	})
	apply(t, docs[0])
	// On the API's default data plane, which the test's runner serves.
	discover(t, name, "")
	plan := planOf(t, docs...)
	ops := map[string]string{}
	for _, op := range plan.GetTransform().GetOperators() {
		ops[op.GetOutput()] = op.GetId()
	}
	writes, levels := map[string]string{}, map[string]apiv1.DeliveryLevel{}
	for _, st := range plan.GetStreams() {
		writes[st.GetName()], levels[st.GetName()] = st.GetWrite().String(), st.GetLevel()
	}
	if len(plan.GetProblems()) > 0 || !strings.HasPrefix(ops["firsts"], "dedupe-") || !strings.HasPrefix(ops["latest"], "dedupe-") ||
		writes["firsts"] != apiv1.WriteMode_WRITE_MODE_APPEND.String() || writes["latest"] != apiv1.WriteMode_WRITE_MODE_MERGE.String() {
		t.Fatalf("the plan has problems %v, operators %v and writes %v; want a dedupe for each output, firsts appended and latest merged", plan.GetProblems(), ops, writes)
	}
	// The topic replays exactly, so the retention leaves firsts exactly
	// once, and the default require takes it.
	if levels["firsts"] != apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE || levels["latest"] != apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE {
		t.Fatalf("levels %v, want both exactly once", levels)
	}
	apply(t, docs...)

	// Fed 20 records at a time, until a checkpoint holds state and the
	// output has first rows; then the engine dies.
	produce(20)
	firsts := pgx.Identifier{out, "firsts"}.Sanitize()
	var killed *checkpoint.Record
	eventually(t, 90*time.Second, "a checkpoint that holds state, and first rows written", func() bool {
		if killed = latestRecord(t, name); holdsState(killed) {
			if n, err := query[int](t, "SELECT count(*) FROM "+firsts); err == nil && n > 0 {
				return true
			}
		}
		if produced < 200 {
			produce(20)
		}
		return false
	})
	pid := killEngine(t, r, name)
	t.Logf("killed engine %d after a record of generation %d, %d records written", pid, killed.Generation, produced)
	// A minute more of event time, past several retentions of each key.
	for range 12 {
		produce(20)
	}
	// A record a minute past the last on each partition moves the watermark
	// past every other; its own first row waits.
	end := t0.Add(time.Duration(produced)*dedupeStep + time.Minute)
	if err := cl.ProduceSync(t.Context(),
		&kgo.Record{Topic: topic, Partition: 0, Key: []byte(dedupeEnd), Value: []byte(`{"i": -1}`), Timestamp: end},
		&kgo.Record{Topic: topic, Partition: 1, Key: []byte(dedupeEnd), Value: []byte(`{"i": -1}`), Timestamp: end}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	// Each key's first records, by their times alone; and its latest.
	type row struct {
		k  string
		at int64
		p  int32
	}
	wantFirst, wantLatest, last := map[row]bool{}, map[string]row{}, map[string]time.Time{}
	for i := range produced {
		k, at := fmt.Sprintf("k%d", i%dedupeTestKeys), t0.Add(time.Duration(i)*dedupeStep)
		r := row{k, at.UnixMicro(), int32(i % 2)}
		wantLatest[k] = r
		if l, ok := last[k]; ok && at.Sub(l) < dedupeRetention {
			continue
		}
		last[k] = at
		wantFirst[r] = true
	}
	// The first rows are read whole: the closing records' wait, and a row of
	// theirs is extra. The latest has theirs, at one time on two
	// partitions, and leaves it out.
	read := func(table, where string) []row {
		got, err := rowsOf(t, `SELECT k, (extract(epoch FROM at) * 1000000)::bigint, p FROM `+pgx.Identifier{out, table}.Sanitize()+where,
			func(rows pgx.CollectableRow) (row, error) {
				var r row
				return r, rows.Scan(&r.k, &r.at, &r.p)
			})
		if err != nil {
			return nil
		}
		return got
	}
	eventually(t, 3*time.Minute, "every first row", func() bool {
		if failed := runFailure(t, name); failed != "" {
			t.Fatalf("the pipeline's run failed: %s", failed)
		}
		return len(read("firsts", "")) >= len(wantFirst)
	})
	// A moment more shows nothing else lands.
	time.Sleep(3 * time.Second)
	seen, again := map[row]bool{}, map[string]int{}
	for _, r := range read("firsts", "") {
		switch {
		case seen[r]:
			t.Errorf("first row %+v landed twice", r)
		case !wantFirst[r]:
			t.Errorf("first row %+v isn't a key's first within %s", r, dedupeRetention)
		}
		seen[r] = true
		again[r.k]++
	}
	if len(seen) != len(wantFirst) {
		t.Errorf("%d first rows landed, want %d", len(seen), len(wantFirst))
	}
	// The retention passed for every key many times over.
	for k, n := range again {
		if n < 4 {
			t.Errorf("key %s has %d first rows, want one each time the retention passed", k, n)
		}
	}
	var latest []row
	eventually(t, time.Minute, "every key's latest row", func() bool {
		latest = read("latest", " WHERE k <> '"+dedupeEnd+"' AND NOT _openetl_deleted")
		got := map[string]row{}
		for _, r := range latest {
			got[r.k] = r
		}
		return len(latest) == len(wantLatest) && maps.Equal(got, wantLatest)
	})
	t.Logf("%d first rows of %d records, and %d latest rows, through a killed engine", len(seen), produced, len(latest))

	// A later attempt restored the state.
	if rec := latestRecord(t, name); rec.Generation <= killed.Generation {
		t.Errorf("the latest record is of generation %d, the killed attempt's %d", rec.Generation, killed.Generation)
	}
	// Each record counted once, in the epoch that committed it: the first
	// rows' dedupe wrote each first row and dropped the rest as repeats,
	// but the closing records, which wait; the latest's wrote each key's
	// first record as an insert and each later one as an update, and the
	// second closing record, at the first's time, as an update or a
	// repeat.
	all := uint64(produced + 2)
	want := fmt.Sprintf("firsts: in %d, %d written, %d repeats, 0 late; latest: in %d, %d inserts, %d updates and repeats, 0 late",
		all, len(wantFirst), produced-len(wantFirst), all, dedupeTestKeys+1, all-dedupeTestKeys-1)
	var got string
	for deadline := time.Now().Add(time.Minute); got != want; time.Sleep(500 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the dedupes' numbers over every checkpoint: got %s, want %s", got, want)
		}
		totals := operatorTotals(t, name, deadline)
		f, l := totals[ops["firsts"]], totals[ops["latest"]]
		got = fmt.Sprintf("firsts: in %d, %d written, %d repeats, %d late; latest: in %d, %d inserts, %d updates and repeats, %d late",
			f.in, f.inserts, f.repeats, f.late, l.in, l.inserts, l.updates+l.repeats, l.late)
	}
}

// rowsOf are what scan gives of each row of query.
func rowsOf[T any](t *testing.T, query string, scan pgx.RowToFunc[T]) ([]T, error) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), env.dbURL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	rows, err := conn.Query(t.Context(), query)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scan)
}
