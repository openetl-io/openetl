package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/conformance"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
	"github.com/openetl-io/openetl/sdk/go/staging"
)

// api is a fake API that keeps every record delivered to it, repeats
// included, by namespace and stream.
type api struct {
	mu   sync.Mutex
	rows map[string][]map[string]any
}

// stagedAPI delivers each staged epoch to the API in Commit, a file at a
// time, and saves a delivery cursor once the API has each.
type stagedAPI struct {
	c   memConfig
	h   host.Host
	api *api
	bug string
}

var stagedCapabilities = destination.Capabilities{
	Class:    destination.DeliverAndAck,
	Scopes:   []destination.Scope{destination.ScopePipeline},
	Delivery: destination.DeliveryStaged,
}

func (d *stagedAPI) Capabilities() destination.Capabilities { return stagedCapabilities }
func (d *stagedAPI) Check(context.Context) error            { return d.c.check() }

func (d *stagedAPI) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	if err := d.c.check(); err != nil {
		return nil, err
	}
	return &stagedAPIWriter{d: d, ns: cfg.Namespace, files: map[destination.Epoch]map[string][]destination.StagedFile{}}, nil
}

type stagedAPIWriter struct {
	d     *stagedAPI
	ns    string
	mu    sync.Mutex
	files map[destination.Epoch]map[string][]destination.StagedFile
}

// apiCursor is the next file to deliver: the stream's, in name order, and
// the file's.
type apiCursor struct {
	Stream, File int
	Done         bool
}

func (w *stagedAPIWriter) Write(context.Context, destination.Epoch, string, arrow.RecordBatch) error {
	return errors.New("a staged destination got a batch")
}

func (w *stagedAPIWriter) WriteStaged(_ context.Context, e destination.Epoch, stream string, files []destination.StagedFile) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.files[e] == nil {
		w.files[e] = map[string][]destination.StagedFile{}
	}
	w.files[e][stream] = files
	return nil
}

func (w *stagedAPIWriter) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	return destination.Staged(fmt.Sprintf("%d/%d", e.Generation, e.N)), nil
}

func (w *stagedAPIWriter) Commit(ctx context.Context, e destination.Epoch, _ []destination.Staged) (destination.Committed, error) {
	from, err := w.cursor(ctx, e)
	if err != nil || from.Done {
		return destination.Committed{}, err
	}
	return destination.Committed{}, w.deliver(ctx, e, from)
}

func (w *stagedAPIWriter) Recover(ctx context.Context, _ uint64, e destination.Epoch, _ []destination.Staged) (destination.Recovery, error) {
	if e.IsZero() {
		return destination.Applied, nil
	}
	from, err := w.cursor(ctx, e)
	if err != nil {
		return 0, err
	}
	if w.d.bug == "ignore-cursor" {
		from = apiCursor{}
	}
	if from.Done {
		return destination.Applied, nil
	}
	return destination.Redone, w.deliver(ctx, e, from)
}

func (w *stagedAPIWriter) cursor(ctx context.Context, e destination.Epoch) (apiCursor, error) {
	var c apiCursor
	b, ok, err := w.d.h.GetDeliveryCursor(ctx, e)
	if err != nil || !ok {
		return c, err
	}
	return c, json.Unmarshal(b, &c)
}

func (w *stagedAPIWriter) save(ctx context.Context, e destination.Epoch, c apiCursor) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return w.d.h.PutDeliveryCursor(ctx, e, b)
}

func (w *stagedAPIWriter) deliver(ctx context.Context, e destination.Epoch, from apiCursor) error {
	w.mu.Lock()
	files := w.files[e]
	w.mu.Unlock()
	streams := slices.Sorted(maps.Keys(files))
	for si, stream := range streams {
		for fi, f := range files[stream] {
			if si < from.Stream || si == from.Stream && fi < from.File {
				continue
			}
			next := apiCursor{Stream: si, File: fi + 1}
			if w.d.bug == "cursor-leads" {
				if err := w.save(ctx, e, next); err != nil {
					return err
				}
			}
			var rows []map[string]any
			err := staging.Read(ctx, w.d.h, f, staging.Position{}, func(_ staging.Position, b arrow.RecordBatch) error {
				ids, vs := b.Column(b.Schema().FieldIndices("id")[0]).(*array.Int64), b.Column(b.Schema().FieldIndices("v")[0]).(*array.String)
				for i := range int(b.NumRows()) {
					rows = append(rows, map[string]any{"id": ids.Value(i), "v": vs.Value(i)})
				}
				return nil
			})
			if err != nil {
				return err
			}
			// The call is cut off with the attempt.
			if err := ctx.Err(); err != nil {
				return err
			}
			w.d.api.mu.Lock()
			w.d.api.rows[w.ns+"."+stream] = append(w.d.api.rows[w.ns+"."+stream], rows...)
			w.d.api.mu.Unlock()
			if w.d.bug != "cursor-leads" {
				if err := w.save(ctx, e, next); err != nil {
					return err
				}
			}
		}
	}
	return w.save(ctx, e, apiCursor{Done: true})
}

func (w *stagedAPIWriter) Close(context.Context) error { return nil }

func stagedSuite(bug string) conformance.DestinationSuite {
	a := &api{rows: map[string][]map[string]any{}}
	return conformance.DestinationSuite{
		Launch: conformance.InProcess(sdk.Connector{
			Name: "staged", ConfigSchema: configSchema, Egress: sdk.Egress{Tier: sdk.EgressRaw},
			Destination: func(config []byte, h host.Host) (destination.Destination, error) {
				c, err := bind(config)
				return &stagedAPI{c: c, h: h, api: a, bug: bug}, err
			},
			Capabilities: stagedCapabilities,
		}),
		Config: good, BadCredentials: badCredentials, Unreachable: unreachable,
		Read: func(_ context.Context, namespace, stream string) ([]map[string]any, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			return slices.Clone(a.rows[namespace+"."+stream]), nil
		},
	}
}

func TestStagedDestinationPasses(t *testing.T) {
	stagedSuite("").Test(t)
}

func TestStagedDestinationBugsFail(t *testing.T) {
	cases := map[string][]string{
		"ignore-cursor": {"write/crash-after-precommit", "write/crash-after-commit", "write/staged-resume"},
		"cursor-leads":  {"write/staged-resume"},
	}
	for bug, checks := range cases {
		t.Run(bug, func(t *testing.T) {
			failed := runFailed(t, func(ctx context.Context, report func(conformance.Result)) { stagedSuite(bug).Run(ctx, report) })
			if !slices.Equal(failed, checks) {
				t.Errorf("failed checks %v, want %v", failed, checks)
			}
		})
	}
}
