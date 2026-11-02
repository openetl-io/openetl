//go:build e2e

package e2e

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
)

// The compose broker.
func kafkaBrokers() string { return cmp.Or(os.Getenv("OPENETL_TEST_KAFKA_BROKERS"), "127.0.0.1:9092") }

// kafkaDest returns a destination spec writing topics namespace.<table>.
// It skips the test without destination-kafka.
func kafkaDest(t *testing.T, namespace string) string {
	t.Helper()
	if _, err := os.Stat(filepath.Join(env.connectors, "destination-kafka")); err != nil {
		t.Skip("OPENETL_CONNECTORS has no destination-kafka")
	}
	return fmt.Sprintf(`{"connector": "kafka", "config": {"brokers": [%q], "namespace": %q}}`, kafkaBrokers(), namespace)
}

func kafkaSource(t *testing.T, topics ...string) string {
	t.Helper()
	if _, err := os.Stat(filepath.Join(env.connectors, "source-kafka")); err != nil {
		t.Skip("OPENETL_CONNECTORS has no source-kafka")
	}
	b, _ := json.Marshal(topics)
	return fmt.Sprintf(`{"connector": "kafka", "config": {"brokers": [%q], "topics": %s}}`, kafkaBrokers(), b)
}

func kafkaClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(kafkaBrokers())}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// topicValues reads every committed record of topic, as JSON objects.
func topicValues(t *testing.T, topic string) []map[string]any {
	t.Helper()
	ends, err := kadm.NewClient(kafkaClient(t)).ListCommittedOffsets(t.Context(), topic)
	if err != nil {
		// Not created yet.
		return nil
	}
	starts, left := map[int32]kgo.Offset{}, map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err == nil && o.Offset > 0 {
			starts[o.Partition], left[o.Partition] = kgo.NewOffset().AtStart(), o.Offset
		}
	})
	if len(left) == 0 {
		return nil
	}
	cl := kafkaClient(t, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: starts}), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords())
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	var out []map[string]any
	for len(left) > 0 {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %s: %v", topic, ctx.Err())
		}
		for _, r := range fs.Records() {
			if r.Offset+1 >= left[r.Partition] {
				delete(left, r.Partition)
			}
			if r.Attrs.IsControl() {
				continue
			}
			var v map[string]any
			if err := json.Unmarshal(r.Value, &v); err != nil {
				t.Fatalf("%s: value %q: %v", topic, r.Value, err)
			}
			out = append(out, v)
		}
	}
	return out
}

// checkTopics fails unless each table's topic holds want records, each id
// once.
func checkTopics(t *testing.T, namespace string, want int, tables ...string) {
	t.Helper()
	for _, table := range tables {
		vals := topicValues(t, namespace+"."+table)
		seen := map[float64]bool{}
		for _, v := range vals {
			id, _ := v["id"].(float64)
			seen[id] = true
		}
		if len(vals) != want || len(seen) != want {
			t.Errorf("%s.%s: %d records and %d distinct ids, want %d each", namespace, table, len(vals), len(seen), want)
		}
	}
}

func TestPostgresToKafka(t *testing.T) {
	startRunner(t)
	name := unique("postgres-kafka")
	ns := schema(name)
	dst := kafkaDest(t, ns)
	sch := pgx.Identifier{ns}.Sanitize()
	run(t, "CREATE SCHEMA "+sch,
		"CREATE TABLE "+sch+".items (id bigint PRIMARY KEY, name text, price numeric(10, 2), at timestamptz)",
		"INSERT INTO "+sch+".items SELECT i, 'item ' || i, i / 100.0, now() FROM generate_series(1, 3000) i")
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "chunkRows": 500}}`, ns)
	apply(t, docs(name, src, dst, `{"commit": {"interval": "1s"}}`)...)
	if status, stats := waitSync(t, name, 90*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkTopics(t, ns, 3000, "items")
	v := topicValues(t, ns+".items")[0]
	for _, col := range []string{"name", "price", "at", "_openetl_id", "_openetl_op", "_openetl_seq", "_openetl_synced_at"} {
		if _, ok := v[col]; !ok {
			t.Errorf("record %v lacks %s", v, col)
		}
	}
}

// A continuous CDC pipeline appends the table's snapshot, then its changes,
// to the topic, each record once.
func TestPostgresCDCToKafka(t *testing.T) {
	startRunner(t)
	name := unique("cdc-kafka")
	in := schema(name)
	dst := kafkaDest(t, in)
	src := pgx.Identifier{in, "users"}.Sanitize()
	run(t, "CREATE SCHEMA "+pgx.Identifier{in}.Sanitize(),
		"CREATE TABLE "+src+" (id bigint PRIMARY KEY, name text)",
		"INSERT INTO "+src+" SELECT i, 'u' || i FROM generate_series(1, 200) i")
	t.Cleanup(func() {
		if conn, err := pgx.Connect(context.Background(), env.dbURL); err == nil {
			_, _ = conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+pgx.Identifier{in}.Sanitize())
			_ = conn.Close(context.Background())
		}
	})
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "publication": %q, "snapshotChunkRows": 50}}}`, name, in, in),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q}, "spec": %s}`, name, dst),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"mode": "continuous", "source": {"ref": %q}, "destination": {"ref": %q}, "delivery": {"commit": {"interval": "1s"}}}}`, name, name, name))
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(t.Context(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
	})
	ops := func() map[string]int {
		out := map[string]int{}
		for _, v := range topicValues(t, in+".users") {
			op, _ := v["_openetl_op"].(string)
			out[op]++
		}
		return out
	}
	eventually(t, 60*time.Second, "the snapshot", func() bool { return ops()["insert"] == 200 })
	run(t, "UPDATE "+src+" SET name = 'renamed' WHERE id <= 20",
		"DELETE FROM "+src+" WHERE id BETWEEN 21 AND 30",
		"INSERT INTO "+src+" SELECT i, 'new' FROM generate_series(201, 250) i")
	eventually(t, 60*time.Second, "the changes", func() bool {
		o := ops()
		return o["insert"] == 250 && o["update"] == 20 && o["delete"] == 10
	})
	ids := map[string]int{}
	for _, v := range topicValues(t, in+".users") {
		id, _ := v["_openetl_id"].(string)
		ids[id]++
	}
	for id, n := range ids {
		if n != 1 {
			t.Errorf("_openetl_id %s appears %d times", id, n)
		}
	}
}

// produceJSON writes ids from..to-1 to topic, outside a transaction.
func produceJSON(t *testing.T, topic string, from, to int) {
	t.Helper()
	cl := kafkaClient(t, kgo.DefaultProduceTopic(topic))
	var recs []*kgo.Record
	for i := from; i < to; i++ {
		recs = append(recs, &kgo.Record{Key: []byte(strconv.Itoa(i)), Value: []byte(fmt.Sprintf(`{"id": %d, "name": "e%d"}`, i, i))})
	}
	if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// Scheduled syncs read a topic to where it ended when each started, and
// each picks up where the last stopped.
func TestKafkaToPostgres(t *testing.T) {
	startRunner(t)
	name := unique("kafka-postgres")
	topic := schema(name)
	if _, err := kadm.NewClient(kafkaClient(t)).CreateTopic(t.Context(), 3, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	produceJSON(t, topic, 0, 500)
	src := kafkaSource(t, topic)
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, src),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "1h"}, "source": {"ref": %q}, "destination": {"ref": %q, "namespace": %q},
			"delivery": {"commit": {"interval": "1s"}}}}`, name, name, name, topic))
	if status, stats := waitSync(t, name, 90*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkRows(t, topic, 500, topic)
	produceJSON(t, topic, 500, 650)
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("second sync %s: %s", status, stats)
	}
	checkRows(t, topic, 650, topic)
	if n := mustQuery[int](t, "SELECT count(DISTINCT (_kafka_partition, _kafka_offset)) FROM "+pgx.Identifier{topic, topic}.Sanitize()); n != 650 {
		t.Errorf("%d distinct coordinates, want 650", n)
	}
}

// An epoch's transaction left open by a killed engine is aborted, and the
// next attempt writes each record once.
func TestKafkaEngineKilledMidSync(t *testing.T) {
	r := startRunner(t)
	name := unique("kafka-engine-killed")
	ns := schema(name)
	apply(t, docs(name, icebergSlowFaker, kafkaDest(t, ns), `{"commit": {"interval": "1s"}}`)...)
	pid := r.engine(t)
	eventually(t, 30*time.Second, "a checkpoint", func() bool { return checkpointRecords(t, name) > 0 })
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		t.Fatal(err)
	}
	status, stats := waitSync(t, name, 90*time.Second)
	finishedLater(t, status, stats)
	checkTopics(t, ns, 2000, "users", "purchases")
}

// A killed runner's destination process dies with it; the next attempt's
// producers fence its transactions.
func TestKafkaRunnerKilledMidSync(t *testing.T) {
	first := startRunner(t)
	name := unique("kafka-runner-killed")
	ns := schema(name)
	apply(t, docs(name, icebergSlowFaker, kafkaDest(t, ns), `{"commit": {"interval": "1s"}}`)...)
	dst := first.child(t, "destination-kaf")
	eventually(t, 30*time.Second, "a committed epoch", func() bool { return len(topicValues(t, ns+".users")) > 0 })
	first.kill(t)
	eventually(t, 10*time.Second, "the orphaned destination to exit", func() bool {
		return errors.Is(syscall.Kill(dst, 0), syscall.ESRCH)
	})
	startRunner(t)
	status, stats := waitSync(t, name, 90*time.Second)
	finishedLater(t, status, stats)
	checkTopics(t, ns, 2000, "users", "purchases")
}
