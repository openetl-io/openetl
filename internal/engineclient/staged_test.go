package engineclient

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/connectorhost/hostservices"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/internal/staging"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/host"
	"github.com/unsuitable001/openetl/sdk/go/source"
	sdkstaging "github.com/unsuitable001/openetl/sdk/go/staging"
)

// stagedDest reads each epoch's staged files back in Commit, as an API
// destination does, through host services.
type stagedDest struct {
	h       host.Host
	mu      sync.Mutex
	files   map[destination.Epoch]map[string][]destination.StagedFile
	applied map[string][]int64
}

func (d *stagedDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.DeliverAndAck, Scopes: []destination.Scope{destination.ScopePipeline}, Delivery: destination.DeliveryStaged}
}

func (d *stagedDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *stagedDest) Write(context.Context, destination.Epoch, string, arrow.RecordBatch) error {
	return errors.New("a staged destination got a batch")
}

func (d *stagedDest) WriteStaged(_ context.Context, e destination.Epoch, stream string, files []destination.StagedFile) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.files[e] == nil {
		d.files[e] = map[string][]destination.StagedFile{}
	}
	d.files[e][stream] = files
	return nil
}

func (d *stagedDest) PreCommit(context.Context, destination.Epoch) (destination.Staged, error) {
	return nil, nil
}

func (d *stagedDest) Commit(ctx context.Context, e destination.Epoch, _ []destination.Staged) (destination.Committed, error) {
	d.mu.Lock()
	files := d.files[e]
	d.mu.Unlock()
	for _, stream := range slices.Sorted(maps.Keys(files)) {
		for _, f := range files[stream] {
			err := sdkstaging.Read(ctx, d.h, f, sdkstaging.Position{}, func(_ sdkstaging.Position, b arrow.RecordBatch) error {
				ids := b.Column(b.Schema().FieldIndices("id")[0]).(*array.Int64)
				d.mu.Lock()
				d.applied[stream] = append(d.applied[stream], ids.Int64Values()...)
				d.mu.Unlock()
				return nil
			})
			if err != nil {
				return destination.Committed{}, err
			}
		}
	}
	return destination.Committed{}, d.h.PutDeliveryCursor(ctx, e, []byte("done"))
}

func (d *stagedDest) Recover(context.Context, uint64, destination.Epoch, []destination.Staged) (destination.Recovery, error) {
	return destination.Applied, nil
}

func (d *stagedDest) Close(context.Context) error { return nil }

func TestRunAttemptIntoAStagedDestination(t *testing.T) {
	runStaged(t, t.TempDir())
}

// runStaged runs an attempt whose engine stages below location, and checks
// that the destination read every row back, and that only the last
// recorded epoch's files are left.
func runStaged(t *testing.T, location string) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	opts := options(t, mem)
	defer mem.AssertSize(t, 0)
	ctx := testContext(t)

	area, err := staging.Open(ctx, location, "org", "pipe", staging.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = area.Close() }()
	hs := hostservices.New(hostservices.Options{})
	hs.SetStaging(area)
	dest := &stagedDest{h: hs, files: map[destination.Epoch]map[string][]destination.StagedFile{}, applied: map[string][]int64{}}
	src := &pagedSource{
		pages: map[string][][]string{
			"users/eu": {
				{`{"id": 1, "name": "a"}`, `{"id": 2, "name": "b"}`},
				{`{"id": 3, "name": null}`},
			},
			"orders": {
				{`{"id": 10}`},
			},
		},
		afterPage: func(context.Context, int) error { return nil },
	}
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "pipe")
	job := runner.Job{
		Owner:       "a1",
		Pipeline:    "pipe",
		Log:         log,
		Source:      src,
		Destination: dest,
		Dataflow:    &Dataflow{Options: opts},
		Staging:     area,
		Settings: delivery.Settings{
			Require:        delivery.AtLeastOnce,
			Scope:          destination.ScopePipeline,
			CommitMaxBytes: 1,
		},
	}
	for _, s := range []string{"users/eu", "orders"} {
		job.Streams = append(job.Streams, runner.Stream{Name: s, Replay: source.Exact, Mode: destination.Append})
	}
	res, err := runner.RunAttempt(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if got := dest.applied["users/eu"]; !slices.Equal(got, []int64{1, 2, 3}) {
		t.Errorf("users/eu got %v", got)
	}
	if got := dest.applied["orders"]; !slices.Equal(got, []int64{10}) {
		t.Errorf("orders got %v", got)
	}

	// Only HEAD's epoch can be recovered, so only its files and cursor are
	// left: none if it staged nothing.
	head, err := log.Record(ctx, checkpoint.RecordRef{Epoch: res.Last, Generation: res.Generation})
	if err != nil {
		t.Fatal(err)
	}
	want := 0
	for _, fs := range head.Files {
		want += len(fs)
	}
	if want > 0 {
		want++
	}
	n, err := area.Sweep(ctx, destination.Epoch{Generation: res.Generation, N: res.Last + 1}, res.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if n != want {
		t.Errorf("%d objects were left after the attempt, want %d: HEAD's files and cursor", n, want)
	}
}
