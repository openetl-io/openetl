//go:build e2e

package e2e

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/jackc/pgx/v5"
)

// The compose kinesis-mock.
func kinesisEndpoint() string {
	return cmp.Or(os.Getenv("OPENETL_TEST_KINESIS_ENDPOINT"), "http://127.0.0.1:4568")
}

// kinesisConfig is a connector config's connection properties. It skips the
// test without the connector.
func kinesisConfig(t *testing.T, exe string) string {
	t.Helper()
	if _, err := os.Stat(filepath.Join(env.connectors, exe)); err != nil {
		t.Skip("OPENETL_CONNECTORS has no " + exe)
	}
	key := setSecret(t, "openetl")
	return fmt.Sprintf(`"region": "us-east-1", "endpoint": %q, "accessKeyId": {"secret": %q}, "secretAccessKey": {"secret": %q}`, kinesisEndpoint(), key, key)
}

func kinesisDest(t *testing.T, namespace string) string {
	t.Helper()
	return fmt.Sprintf(`{"connector": "kinesis", "config": {%s, "namespace": %q, "shards": 2}}`, kinesisConfig(t, "destination-kinesis"), namespace)
}

func kinesisSource(t *testing.T, streams ...string) string {
	t.Helper()
	b, _ := json.Marshal(streams)
	return fmt.Sprintf(`{"connector": "kinesis", "config": {%s, "streams": %s}}`, kinesisConfig(t, "source-kinesis"), b)
}

// At least once, as Kinesis needs.
const kinesisDelivery = `{"require": "at_least_once", "commit": {"interval": "1s"}}`

func kinesisClient() *kinesis.Client {
	return kinesis.New(kinesis.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(kinesisEndpoint()),
		Credentials: credentials.NewStaticCredentialsProvider("openetl", "openetl", ""),
	})
}

func kinesisActive(t *testing.T, stream string) {
	t.Helper()
	eventually(t, 30*time.Second, stream+" to be active", func() bool {
		d, err := kinesisClient().DescribeStreamSummary(t.Context(), &kinesis.DescribeStreamSummaryInput{StreamName: aws.String(stream)})
		return err == nil && d.StreamDescriptionSummary.StreamStatus == types.StreamStatusActive
	})
}

// kinesisData reads every record of a stream, as JSON objects; nil if the
// stream doesn't exist yet, or is still being created.
func kinesisData(t *testing.T, stream string) []map[string]any {
	t.Helper()
	cl := kinesisClient()
	d, err := cl.DescribeStreamSummary(t.Context(), &kinesis.DescribeStreamSummaryInput{StreamName: aws.String(stream)})
	if err != nil || d.StreamDescriptionSummary.StreamStatus == types.StreamStatusCreating {
		return nil
	}
	shards, err := cl.ListShards(t.Context(), &kinesis.ListShardsInput{StreamName: aws.String(stream)})
	if err != nil {
		return nil
	}
	var out []map[string]any
	for _, sh := range shards.Shards {
		it, err := cl.GetShardIterator(t.Context(), &kinesis.GetShardIteratorInput{StreamName: aws.String(stream), ShardId: sh.ShardId, ShardIteratorType: types.ShardIteratorTypeTrimHorizon})
		if err != nil {
			t.Fatal(err)
		}
		for next := it.ShardIterator; next != nil; {
			res, err := cl.GetRecords(t.Context(), &kinesis.GetRecordsInput{ShardIterator: next})
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range res.Records {
				var v map[string]any
				if err := json.Unmarshal(r.Data, &v); err != nil {
					t.Fatalf("%s: data %q: %v", stream, r.Data, err)
				}
				v["partitionKey"] = aws.ToString(r.PartitionKey)
				out = append(out, v)
			}
			if len(res.Records) == 0 && aws.ToInt64(res.MillisBehindLatest) == 0 {
				break
			}
			next = res.NextShardIterator
		}
	}
	return out
}

// checkKinesis fails unless each table's stream holds each of want ids, at
// least once, keyed by its id.
func checkKinesis(t *testing.T, namespace string, want int, tables ...string) {
	t.Helper()
	for _, table := range tables {
		vals := kinesisData(t, namespace+"."+table)
		seen := map[float64]bool{}
		for _, v := range vals {
			id, _ := v["id"].(float64)
			seen[id] = true
			if v["partitionKey"] != strconv.Itoa(int(id)) {
				t.Fatalf("%s.%s: id %v has partition key %v", namespace, table, id, v["partitionKey"])
			}
		}
		if len(seen) != want {
			t.Errorf("%s.%s: %d records with %d distinct ids, want %d ids", namespace, table, len(vals), len(seen), want)
		}
	}
}

func TestPostgresToKinesis(t *testing.T) {
	startRunner(t)
	name := unique("postgres-kinesis")
	ns := schema(name)
	dst := kinesisDest(t, ns)
	sch := pgx.Identifier{ns}.Sanitize()
	run(t, "CREATE SCHEMA "+sch,
		"CREATE TABLE "+sch+".items (id bigint PRIMARY KEY, name text, price numeric(10, 2), at timestamptz)",
		"INSERT INTO "+sch+".items SELECT i, 'item ' || i, i / 100.0, now() FROM generate_series(1, 3000) i")
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "chunkRows": 500}}`, ns)
	apply(t, docs(name, src, dst, kinesisDelivery)...)
	if status, stats := waitSync(t, name, 90*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkKinesis(t, ns, 3000, "items")
	v := kinesisData(t, ns+".items")[0]
	for _, col := range []string{"name", "price", "at", "_openetl_id", "_openetl_op", "_openetl_seq", "_openetl_synced_at"} {
		if _, ok := v[col]; !ok {
			t.Errorf("record %v lacks %s", v, col)
		}
	}
}

// putKinesis puts ids from..to-1, each keyed k<id mod 10>.
func putKinesis(t *testing.T, stream string, from, to int) {
	t.Helper()
	for i := from; i < to; i += 500 {
		var entries []types.PutRecordsRequestEntry
		for j := i; j < min(i+500, to); j++ {
			entries = append(entries, types.PutRecordsRequestEntry{
				Data: fmt.Appendf(nil, `{"id": %d, "name": "e%d"}`, j, j), PartitionKey: aws.String("k" + strconv.Itoa(j%10)),
			})
		}
		res, err := kinesisClient().PutRecords(t.Context(), &kinesis.PutRecordsInput{StreamName: aws.String(stream), Records: entries})
		if err != nil {
			t.Fatal(err)
		}
		if aws.ToInt32(res.FailedRecordCount) > 0 {
			t.Fatalf("%d records failed", aws.ToInt32(res.FailedRecordCount))
		}
	}
}

// Scheduled syncs read a stream to where it caught up when each started,
// each picking up where the last stopped, across a split.
func TestKinesisToPostgres(t *testing.T) {
	startRunner(t)
	name := unique("kinesis-postgres")
	stream := schema(name)
	src := kinesisSource(t, stream)
	if _, err := kinesisClient().CreateStream(t.Context(), &kinesis.CreateStreamInput{StreamName: aws.String(stream), ShardCount: aws.Int32(2)}); err != nil {
		t.Fatal(err)
	}
	kinesisActive(t, stream)
	putKinesis(t, stream, 0, 500)
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, src),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "1h"}, "source": {"ref": %q}, "destination": {"ref": %q, "namespace": %q},
			"delivery": {"commit": {"interval": "1s"}}}}`, name, name, name, stream))
	if status, stats := waitSync(t, name, 90*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkRows(t, stream, 500, stream)
	quarter := new(big.Int).Lsh(big.NewInt(1), 126)
	if _, err := kinesisClient().SplitShard(t.Context(), &kinesis.SplitShardInput{StreamName: aws.String(stream), ShardToSplit: aws.String("shardId-000000000000"), NewStartingHashKey: aws.String(quarter.String())}); err != nil {
		t.Fatal(err)
	}
	kinesisActive(t, stream)
	putKinesis(t, stream, 500, 650)
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("second sync %s: %s", status, stats)
	}
	checkRows(t, stream, 650, stream)
	if n := mustQuery[int](t, "SELECT count(DISTINCT (_kinesis_shard, _kinesis_sequence)) FROM "+pgx.Identifier{stream, stream}.Sanitize()); n != 650 {
		t.Errorf("%d distinct coordinates, want 650", n)
	}
}

// A killed runner's destination process dies with it; the next attempt
// sends the epochs that weren't checkpointed again, and every row arrives.
func TestKinesisRunnerKilledMidSync(t *testing.T) {
	first := startRunner(t)
	name := unique("kinesis-runner-killed")
	ns := schema(name)
	apply(t, docs(name, icebergSlowFaker, kinesisDest(t, ns), kinesisDelivery)...)
	dst := first.child(t, "destination-kin")
	eventually(t, 30*time.Second, "a delivered record", func() bool { return len(kinesisData(t, ns+".users")) > 0 })
	first.kill(t)
	eventually(t, 10*time.Second, "the orphaned destination to exit", func() bool {
		return errors.Is(syscall.Kill(dst, 0), syscall.ESRCH)
	})
	startRunner(t)
	status, stats := waitSync(t, name, 90*time.Second)
	finishedLater(t, status, stats)
	checkKinesis(t, ns, 2000, "users", "purchases")
}
