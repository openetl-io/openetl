//go:build e2e

package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

// windowRows is a merged group window's live rows, as start/key/n/seq, the
// start in UTC as HH:MI:SS, in order.
func windowRows(t *testing.T, table, key string) []string {
	t.Helper()
	v, err := query[string](t, `SELECT coalesce(string_agg(to_char(window_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') || '/' || `+key+` || '/' || n || '/' || _openetl_seq,
		',' ORDER BY window_start, `+key+`), '') FROM `+table+` WHERE NOT _openetl_deleted`)
	if err != nil || v == "" {
		return nil
	}
	return strings.Split(v, ",")
}

// quarantined is every record the pipeline quarantined as output's, each
// as the JSON of the columns its statement reads.
func quarantined(t *testing.T, pipeline, output string) []map[string]any {
	t.Helper()
	id, err := query[string](t, "SELECT id::text FROM pipelines WHERE name = $1", pipeline)
	if err != nil {
		return nil
	}
	dirs, _ := filepath.Glob(filepath.Join(env.checkpoints, "*", id, "_quarantine"))
	var out []map[string]any
	for _, dir := range dirs {
		_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			name := filepath.Base(path)
			if err != nil || d.IsDir() || !strings.HasPrefix(name, output+".") || !strings.HasSuffix(name, ".jsonl") {
				return nil
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = f.Close() }()
			lines := bufio.NewScanner(f)
			for lines.Scan() {
				var line struct{ Stream, Reason, Record string }
				var rec map[string]any
				if err := json.Unmarshal(lines.Bytes(), &line); err != nil || json.Unmarshal([]byte(line.Record), &rec) != nil {
					t.Fatalf("%s: %q", path, lines.Text())
				}
				rec["reason"] = line.Reason
				out = append(out, rec)
			}
			return nil
		})
	}
	return out
}

// A continuous pipeline with a tumbling window over a Kafka topic's two
// partitions, by the records' timestamps, merged into Postgres. A window's
// row lands once both partitions have passed its end. A record that comes
// after its window closed, within the stream's lateness, writes the
// window's row again, as an update; one past the lateness is quarantined.
// The records' times are a quarter of an hour ahead of the clock, which
// the topic is set to take, so a partition that is idle for a minute moves
// to the clock and closes no window.
func TestGroupWindowFromKafka(t *testing.T) {
	startRunner(t)
	name := unique("window-kafka")
	topic := schema(name)
	src := kafkaSource(t, topic)
	anyTime := "9223372036854775807"
	created, err := kadm.NewClient(kafkaClient(t)).CreateTopic(t.Context(), 2, 1, map[string]*string{
		"message.timestamp.type":          kadm.StringPtr("CreateTime"),
		"message.timestamp.after.max.ms":  &anyTime,
		"message.timestamp.before.max.ms": &anyTime,
	}, topic)
	if err == nil {
		err = created.Err
	}
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().Add(15 * time.Minute).Truncate(time.Minute)
	// Each record goes to the partition the test names.
	cl := kafkaClient(t, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	// produce writes a record to partition p for each key and second past
	// base, as key@second.
	produce := func(p int32, records ...string) {
		t.Helper()
		var recs []*kgo.Record
		for _, r := range records {
			key, sec, _ := strings.Cut(r, "@")
			var s int
			if _, err := fmt.Sscan(sec, &s); err != nil {
				t.Fatal(err)
			}
			recs = append(recs, &kgo.Record{Topic: topic, Partition: p, Key: []byte(key), Value: []byte(`{"x": 1}`),
				Timestamp: base.Add(time.Duration(s) * time.Second)})
		}
		if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	// The rows the test expects, as windowRows has them: the window's
	// minute past base, the key, the count and the seq.
	minute := func(m int) string {
		return base.Add(time.Duration(m) * time.Minute).UTC().Format("2006-01-02T15:04:05")
	}
	row := func(m int, key string, n, seq int) string { return fmt.Sprintf("%s/%s/%d/%d", minute(m), key, n, seq) }
	table := pgx.Identifier{topic, "per_minute"}.Sanitize()
	waitRows := func(what string, want ...string) {
		t.Helper()
		slices.Sort(want)
		var got []string
		for deadline := time.Now().Add(90 * time.Second); !slices.Equal(got, want); time.Sleep(200 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s: the table has %v, want %v", what, got, want)
			}
			got = windowRows(t, table, "region")
		}
	}

	produce(0, "eu@5", "eu@10", "us@20")
	produce(1, "us@30", "eu@50")
	sql, err := json.Marshal(fmt.Sprintf("INSERT INTO per_minute\nSELECT window_start, _kafka_key AS region, count(*) AS n\nFROM %s\n"+
		"GROUP BY tumble(_kafka_timestamp, INTERVAL '1 minute'), _kafka_key", topic))
	if err != nil {
		t.Fatal(err)
	}
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, src),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"mode": "continuous",
			"source": {"ref": %q, "streams": [{"name": %q, "eventTime": "_kafka_timestamp", "watermark": {"lateness": "1m"}}]},
			"destination": {"ref": %q, "namespace": %q, "write": "merge"},
			"delivery": {"commit": {"interval": "1s"}}, "transform": {"passthrough": "none", "sql": %s}}}`, name, name, topic, name, topic, sql))
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
	})

	// Both partitions pass a minute: its windows close.
	produce(0, "eu@65")
	produce(1, "us@70")
	waitRows("the first minute's windows", row(0, "eu", 3, 1), row(0, "us", 2, 1))
	if rows := windowRows(t, table, "region"); len(rows) != 2 {
		t.Fatalf("rows %v before the second minute closed", rows)
	}

	// 40 seconds is late, within the minute's lateness: the first minute's
	// eu row again.
	produce(1, "eu@40")
	waitRows("the late row's update", row(0, "eu", 4, 2), row(0, "us", 2, 1))
	op := mustQuery[string](t, "SELECT _openetl_op FROM "+table+" WHERE region = 'eu' AND window_start = $1", base)
	if op != "update" {
		t.Errorf("the late row's window row is an %s, want an update", op)
	}

	// Both partitions pass the second minute, which also ends the first
	// minute's lateness.
	produce(0, "us@75", "eu@121")
	produce(1, "us@122")
	waitRows("the second minute's windows",
		row(0, "eu", 4, 2), row(0, "us", 2, 1), row(1, "eu", 1, 1), row(1, "us", 2, 1))

	// 15 seconds is past the first minute's lateness, and is quarantined;
	// both partitions pass the third minute.
	produce(0, "us@15", "eu@185")
	produce(1, "eu@190")
	waitRows("the third minute's windows",
		row(0, "eu", 4, 2), row(0, "us", 2, 1), row(1, "eu", 1, 1), row(1, "us", 2, 1), row(2, "eu", 1, 1), row(2, "us", 1, 1))
	var q []map[string]any
	eventually(t, 30*time.Second, "the late record in quarantine", func() bool {
		q = quarantined(t, name, "per_minute")
		return len(q) > 0
	})
	late := base.Add(15 * time.Second).UTC().Format("2006-01-02T15:04:05")
	if len(q) != 1 || q[0]["_kafka_key"] != "us" || !strings.HasPrefix(fmt.Sprint(q[0]["_kafka_timestamp"]), late) ||
		!strings.Contains(fmt.Sprint(q[0]["reason"]), "late") {
		t.Errorf("quarantined %v, want the us record at %s, late", q, late)
	}
	// The fourth minute is open: nothing past the third.
	time.Sleep(3 * time.Second)
	if rows := windowRows(t, table, "region"); len(rows) != 6 {
		t.Errorf("rows %v, want the first three minutes'", rows)
	}
	operatorStats(t, name, 14, 6, 1, 1, 3, base.Add(185*time.Second))
}

// operatorStats waits until the pipeline's checkpoints say its one stateful
// operator took rows records in, wrote inserts and updates and no deletes,
// and quarantined late rows, and that the newest keeps windows at
// watermark.
func operatorStats(t *testing.T, name string, rows, inserts, updates, late uint64, windows int64, watermark time.Time) {
	t.Helper()
	var got string
	want := fmt.Sprintf("in %d, %d/%d/0, late %d; newest: %d windows at %s", rows, inserts, updates, late, windows, watermark.UTC().Format(time.RFC3339))
	read := func() bool {
		res, err := pipelines().ListCheckpoints(t.Context(), connect.NewRequest(&apiv1.ListCheckpointsRequest{Parent: "workspaces/default/pipelines/" + name, PageSize: 1000}))
		if err != nil {
			t.Fatal(err)
		}
		cps := res.Msg.GetCheckpoints()
		var in, ins, upd, del, lt uint64
		ids := map[string]bool{}
		for _, c := range cps {
			for _, o := range c.GetOperators() {
				ids[o.GetOperator()] = true
				in, ins, upd, del, lt = in+o.GetRowsIn(), ins+o.GetInserts(), upd+o.GetUpdates(), del+o.GetDeletes(), lt+o.GetLateRows()
			}
		}
		if len(cps) == 0 || len(ids) != 1 || len(cps[0].GetOperators()) != 1 {
			got = fmt.Sprintf("%d checkpoints, operators %v", len(cps), ids)
			return false
		}
		newest := cps[0].GetOperators()[0]
		got = fmt.Sprintf("in %d, %d/%d/%d, late %d; newest: %d windows at %s", in, ins, upd, del, lt, newest.GetWindows(), newest.GetWatermark().AsTime().UTC().Format(time.RFC3339))
		return got == want
	}
	for deadline := time.Now().Add(30 * time.Second); !read(); time.Sleep(500 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the operator's numbers: got %s, want %s", got, want)
		}
	}
}

// A scheduled pipeline with a tumbling window over a table read with
// full_refresh, merged into Postgres: the stream's watermark holds until
// the read ends, so every window closes then, each with every row of it,
// and none is late. The next sync reads the table again and starts its
// windows again, from every row, and its rows replace the last sync's.
func TestGroupWindowFullRefresh(t *testing.T) {
	startRunner(t)
	name := unique("window-full")
	in, out := schema(name), schema(name)+"_out"
	src := pgx.Identifier{in, "orders"}.Sanitize()
	table := pgx.Identifier{out, "per_minute"}.Sanitize()
	type order struct {
		region string
		amount int
		at     time.Time
	}
	base := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	var orders []order
	// 600 orders over ten minutes, out of order by id.
	for i := range 600 {
		orders = append(orders, order{[]string{"eu", "us", "apac"}[i%3], i%17 - 3, base.Add(time.Duration((i*7919)%600) * time.Second)})
	}
	insert := func(from int, batch []order) {
		t.Helper()
		var vals []string
		for i, o := range batch {
			vals = append(vals, fmt.Sprintf("(%d, '%s', %d, '%s')", from+i, o.region, o.amount, o.at.Format(time.RFC3339)))
		}
		run(t, "INSERT INTO "+src+" VALUES "+strings.Join(vals, ", "))
	}
	// want is each window's row, as start/region/n/total.
	want := func() []string {
		type agg struct{ n, total int }
		w := map[string]agg{}
		for _, o := range orders {
			k := o.at.Truncate(time.Minute).Format("2006-01-02T15:04:05") + "/" + o.region
			a := w[k]
			a.n++
			a.total += o.amount
			w[k] = a
		}
		var out []string
		for k, a := range w {
			out = append(out, fmt.Sprintf("%s/%d/%d", k, a.n, a.total))
		}
		slices.Sort(out)
		return out
	}
	got := func() []string {
		v := mustQuery[string](t, `SELECT coalesce(string_agg(to_char(window_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') || '/' || region || '/' || n || '/' || total,
			',' ORDER BY to_char(window_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') || '/' || region), '') FROM `+table+` WHERE NOT _openetl_deleted`)
		return strings.Split(v, ",")
	}
	run(t, "CREATE SCHEMA "+pgx.Identifier{in}.Sanitize(),
		"CREATE TABLE "+src+" (id bigint PRIMARY KEY, region text NOT NULL, amount int NOT NULL, at timestamptz NOT NULL)")
	insert(1, orders)
	sql, err := json.Marshal("INSERT INTO per_minute\nSELECT window_start, region, count(*) AS n, sum(amount) AS total\nFROM orders\n" +
		"GROUP BY tumble(at, INTERVAL '1 minute'), region")
	if err != nil {
		t.Fatal(err)
	}
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q}}}`, name, in),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "720h"},
			"source": {"ref": %q, "streams": [{"name": "orders", "sync": "full_refresh", "eventTime": "at"}]},
			"destination": {"ref": %q, "namespace": %q, "write": "merge"},
			"delivery": {"commit": {"interval": "1s"}}, "transform": {"passthrough": "none", "sql": %s}}}`, name, name, name, out, sql))
	if status, stats := waitSync(t, name, 90*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	w := want()
	if g := got(); !slices.Equal(g, w) {
		t.Fatalf("after the first sync:\n got %v\nwant %v", g, w)
	}
	if len(w) != 30 {
		t.Fatalf("%d windows, want ten minutes of three regions", len(w))
	}
	if q := quarantined(t, name, "per_minute"); len(q) != 0 {
		t.Errorf("quarantined %v", q)
	}

	// More orders: into windows the first sync wrote, and a new one.
	more := []order{{"eu", 100, base.Add(30 * time.Second)}, {"us", 5, base.Add(9*time.Minute + 59*time.Second)}, {"apac", 7, base.Add(10 * time.Minute)}}
	insert(len(orders)+1, more)
	orders = append(orders, more...)
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("second sync %s: %s", status, stats)
	}
	if g, w := got(), want(); !slices.Equal(g, w) {
		t.Errorf("after the second sync:\n got %v\nwant %v", g, w)
	}
}
