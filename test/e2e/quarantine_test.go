//go:build e2e

package e2e

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
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

// The engine quarantines a malformed row into each provider's object
// storage, and the checkpoint lists the file.
func TestQuarantineInObjectStorage(t *testing.T) {
	for _, p := range []struct {
		scheme string
		create func(t *testing.T, bucket string) string // the storage URL
	}{
		{"s3", func(t *testing.T, bucket string) string {
			endpoint := cmp.Or(os.Getenv("OPENETL_TEST_S3_ENDPOINT"), "http://127.0.0.1:8333")
			storage := "s3://" + bucket + "?endpoint=" + url.QueryEscape(endpoint)
			c, err := objectstore.S3Client(mustParse(t, storage))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
				t.Fatalf("create bucket: %v", err)
			}
			return storage
		}},
		{"gs", func(t *testing.T, bucket string) string {
			t.Setenv("STORAGE_EMULATOR_HOST", xmlUploads(t, cmp.Or(os.Getenv("STORAGE_EMULATOR_HOST"), "127.0.0.1:4443")))
			storage := "gs://" + bucket
			c, err := objectstore.GCSClient(t.Context(), mustParse(t, storage))
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Bucket(bucket).Create(t.Context(), "openetl", nil); err != nil {
				t.Fatalf("create bucket: %v", err)
			}
			return storage
		}},
		{"azblob", func(t *testing.T, bucket string) string {
			t.Setenv("AZURE_STORAGE_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
			endpoint := cmp.Or(os.Getenv("OPENETL_TEST_AZURE_ENDPOINT"), "http://127.0.0.1:10000/devstoreaccount1")
			storage := "azblob://" + bucket + "?account=devstoreaccount1&endpoint=" + url.QueryEscape(endpoint)
			c, err := objectstore.AzureClient(mustParse(t, storage))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Create(t.Context(), nil); err != nil {
				t.Fatalf("create container: %v", err)
			}
			return storage
		}},
	} {
		t.Run(p.scheme, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			bucket := "openetl-e2e-" + strings.ToLower(rand.Text())[:16]
			storage := p.create(t, bucket)
			quarantine := *mustParse(t, storage)
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
			key, ok := strings.CutPrefix(rec.Quarantine[0].URL, p.scheme+"://"+bucket+"/")
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
		})
	}
}

// xmlUploads serves the GCS emulator at host, turning XML-API uploads
// (PUT /{bucket}/{object}), which object_store sends and fake-gcs-server
// lacks, into JSON-API media uploads, with the XML API's ETag header. It
// returns the host it listens on.
func xmlUploads(t *testing.T, host string) string {
	target := &url.URL{Scheme: "http", Host: host}
	proxy := &httputil.ReverseProxy{}
	proxy.ModifyResponse = func(resp *http.Response) error {
		if !strings.HasPrefix(resp.Request.URL.Path, "/upload/") || resp.StatusCode != http.StatusOK {
			return nil
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return err
		}
		var obj struct{ Etag string }
		if err := json.Unmarshal(body, &obj); err != nil {
			return err
		}
		resp.Header.Set("ETag", obj.Etag)
		resp.Body = io.NopCloser(bytes.NewReader(body))
		return nil
	}
	proxy.Rewrite = func(r *httputil.ProxyRequest) {
		r.SetURL(target)
		if r.In.Method != http.MethodPut {
			return
		}
		bucket, object, ok := strings.Cut(strings.TrimPrefix(r.In.URL.EscapedPath(), "/"), "/")
		b, berr := url.PathUnescape(bucket)
		o, oerr := url.PathUnescape(object)
		if !ok || berr != nil || oerr != nil {
			return
		}
		r.Out.Method = http.MethodPost
		r.Out.URL.Path, r.Out.URL.RawPath = "/upload/storage/v1/b/"+b+"/o", ""
		r.Out.URL.RawQuery = url.Values{"uploadType": {"media"}, "name": {o}}.Encode()
	}
	srv := httptest.NewServer(proxy)
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

func mustParse(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
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
