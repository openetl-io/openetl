//go:build e2e

package e2e

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/engineclient"
	"github.com/unsuitable001/openetl/internal/platform/objectstore"
	runnerpkg "github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// The engine quarantines a malformed row into S3, and the checkpoint lists
// the file.
func TestQuarantineInS3(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	endpoint := cmp.Or(os.Getenv("OPENETL_TEST_S3_ENDPOINT"), "http://127.0.0.1:8333")
	bucket := "openetl-e2e-" + strings.ToLower(rand.Text())[:16]
	storage := "s3://" + bucket + "?endpoint=" + url.QueryEscape(endpoint)
	u, err := url.Parse(storage)
	if err != nil {
		t.Fatal(err)
	}
	c, err := objectstore.S3Client(u)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	quarantine := *u
	quarantine.Path = "/org/pipe/_quarantine"

	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "pipe")
	res, err := runnerpkg.RunAttempt(ctx, runnerpkg.Job{
		Owner:         "a1",
		Pipeline:      "pipe",
		Log:           log,
		Source:        badRowSource{},
		Streams:       []runnerpkg.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append}},
		Destination:   &rowCounter{},
		Dataflow:      &engineclient.Dataflow{Options: engineclient.Options{Binary: env.engine}},
		QuarantineURL: quarantine.String(),
		Settings: delivery.Settings{
			Require: delivery.ExactlyOnce,
			Scope:   destination.ScopePipeline,
			Dedupe:  delivery.DedupeNone,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := log.Record(ctx, checkpoint.RecordRef{Epoch: res.Last, Generation: res.Generation})
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Quarantine) != 1 || rec.Quarantine[0].Rows != 1 {
		t.Fatalf("the record lists %+v", rec.Quarantine)
	}
	key, ok := strings.CutPrefix(rec.Quarantine[0].URL, "s3://"+bucket+"/")
	if !ok {
		t.Fatalf("quarantine file %s isn't in the bucket", rec.Quarantine[0].URL)
	}
	b, err := objectstore.Open(ctx, storage)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	body, err := b.ReadAll(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	var line struct{ Stream, Reason, Record string }
	if err := json.Unmarshal(body, &line); err != nil || line.Record != "[2]" || line.Stream != "s" {
		t.Errorf("the quarantine file holds %s (%v)", body, err)
	}
}

type badRowSource struct{}

func (badRowSource) Read(_ context.Context, _ source.ReadRequest, out source.Emitter) error {
	if err := out.Rows("s", []json.RawMessage{json.RawMessage(`{"id": 1}`), json.RawMessage(`[2]`), json.RawMessage(`{"id": 3}`)}); err != nil {
		return err
	}
	if err := out.ResumePoint("s", []byte(`"end"`)); err != nil {
		return err
	}
	return out.Complete("s")
}

type rowCounter struct{ rows int64 }

func (d *rowCounter) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline}}
}

func (d *rowCounter) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *rowCounter) Write(_ context.Context, _ destination.Epoch, _ string, b arrow.RecordBatch) error {
	d.rows += b.NumRows()
	return nil
}

func (d *rowCounter) PreCommit(context.Context, destination.Epoch) (destination.Staged, error) {
	return nil, nil
}

func (d *rowCounter) Commit(context.Context, destination.Epoch, destination.Staged) error {
	return nil
}

func (d *rowCounter) Recover(context.Context, uint64, destination.Epoch, destination.Staged) (destination.Recovery, error) {
	return destination.Applied, nil
}

func (d *rowCounter) Close(context.Context) error { return nil }
