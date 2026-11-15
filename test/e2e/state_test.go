//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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

// produceAt writes n records to topic, keyed by keys in turn, the i-th at
// at(i).
func produceAt(t *testing.T, topic string, keys []string, n int, at func(i int) time.Time) {
	t.Helper()
	var recs []*kgo.Record
	for i := range n {
		recs = append(recs, &kgo.Record{Topic: topic, Key: []byte(keys[i%len(keys)]), Value: fmt.Appendf(nil, `{"i": %d}`, i), Timestamp: at(i)})
	}
	if err := kafkaClient(t).ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// stateRuns are the runs of state the record names.
func stateRuns(rec *checkpoint.Record) []string {
	var out []string
	if rec == nil {
		return nil
	}
	for _, s := range rec.Snapshots {
		out = append(out, s.Runs...)
	}
	return out
}

// A continuous windowed count from Kafka into Postgres keeps its state
// through the engine's death in the middle of a minute: the next attempt
// restores the windows from the runs its checkpoint names, and the
// minute's row counts each of its records once, those read before the
// death and those after.
func TestWindowStateOutlivesTheEngine(t *testing.T) {
	r := startRunner(t)
	name := unique("window-state")
	topic, out := schema(name), schema(name)+"_out"
	if _, err := kadm.NewClient(kafkaClient(t)).CreateTopic(t.Context(), 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	// A minute far enough ahead that an idle partition's watermark, which
	// follows the clock, can't reach it while the test runs.
	minute := time.Now().UTC().Truncate(time.Minute).Add(time.Hour)
	at := func(sec int) func(int) time.Time {
		return func(i int) time.Time { return minute.Add(time.Duration(sec+i) * time.Second) }
	}
	produceAt(t, topic, []string{"a", "b"}, 30, at(1))

	sql, err := json.Marshal("INSERT INTO counts\nSELECT window_start, _kafka_key AS k, count(*) AS n\nFROM " + topic + "\nGROUP BY tumble(_kafka_timestamp, INTERVAL '1 minute'), _kafka_key")
	if err != nil {
		t.Fatal(err)
	}
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, kafkaSource(t, topic)),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"mode": "continuous", "source": {"ref": %q, "streams": [{"name": %q, "eventTime": "_kafka_timestamp"}]},
			"destination": {"ref": %q, "namespace": %q, "write": "merge"}, "delivery": {"commit": {"interval": "1s"}},
			"transform": {"sql": %s, "passthrough": "none"}}}`, name, name, topic, name, out, sql))
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
	})

	// The runner's engine dies once a checkpoint names the minute's state,
	// with the minute open.
	var killed *checkpoint.Record
	eventually(t, 60*time.Second, "a checkpoint that names state", func() bool {
		killed = latestRecord(t, name)
		return len(stateRuns(killed)) > 0
	})
	if err := syscall.Kill(r.engine(t, name), syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		t.Fatal(err)
	}
	// The rest of the minute, and a record of key c two minutes on, whose
	// time moves the watermark past the minute's end.
	produceAt(t, topic, []string{"a", "b"}, 20, at(31))
	produceAt(t, topic, []string{"c"}, 1, at(180))

	counts := pgx.Identifier{out, "counts"}.Sanitize()
	got := func() string {
		rows, err := query[string](t, `SELECT coalesce(string_agg(k || '=' || n, ',' ORDER BY k), '') FROM `+counts+` WHERE window_start = $1`, minute)
		if err != nil {
			return ""
		}
		return rows
	}
	eventually(t, 120*time.Second, "the minute's rows", func() bool { return got() != "" })
	// A merge writes the minute's row once per key; a moment more shows
	// no other write follows.
	time.Sleep(3 * time.Second)
	if g, want := got(), "a=25,b=25"; g != want {
		t.Errorf("the minute's counts are %s, want %s", g, want)
	}
	if n := mustQuery[int](t, "SELECT count(*) FROM "+counts); n != 2 {
		t.Errorf("%s has %d rows, want the minute's two", counts, n)
	}

	// A later attempt restored the runs the killed one's record named.
	rec := latestRecord(t, name)
	if rec.Generation <= killed.Generation {
		t.Fatalf("the latest record is of generation %d, the killed attempt's %d", rec.Generation, killed.Generation)
	}
	before := stateRuns(killed)
	if after := stateRuns(rec); !slices.ContainsFunc(after, func(r string) bool { return slices.Contains(before, r) }) {
		t.Errorf("generation %d's record names %v, none of %v that it restored", rec.Generation, after, before)
	}
}
