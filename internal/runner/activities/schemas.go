package activities

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/sdk/go/destination"
)

// schemaReporter sends a pipeline's schemas to the control plane's schema
// history, in order and off the commit path. A report it can't send is
// logged and dropped; the next attempt reports every schema again.
type schemaReporter struct {
	api      dataplanev1connect.DataPlaneServiceClient
	pipeline string
	log      *slog.Logger
	ctx      context.Context
	cancel   context.CancelFunc
	queue    chan *dataplanev1.ReportSchemasRequest
	done     chan struct{}
}

func newSchemaReporter(ctx context.Context, api dataplanev1connect.DataPlaneServiceClient, pipeline string, log *slog.Logger) *schemaReporter {
	r := &schemaReporter{
		api:      api,
		pipeline: pipeline,
		log:      log,
		queue:    make(chan *dataplanev1.ReportSchemasRequest, 16),
		done:     make(chan struct{}),
	}
	r.ctx, r.cancel = context.WithCancel(context.WithoutCancel(ctx))
	go r.run()
	return r
}

// report queues a report without blocking.
func (r *schemaReporter) report(e destination.Epoch, schemas map[string][]byte) {
	req := &dataplanev1.ReportSchemasRequest{PipelineId: r.pipeline, Generation: e.Generation, Epoch: e.N, Schemas: schemas}
	select {
	case r.queue <- req:
	default:
		r.log.Warn("schema history report dropped: too many queued", "epoch", e.N)
	}
}

func (r *schemaReporter) run() {
	defer close(r.done)
	for req := range r.queue {
		ctx, cancel := context.WithTimeout(r.ctx, 10*time.Second)
		_, err := r.api.ReportSchemas(ctx, connect.NewRequest(req))
		cancel()
		if err != nil {
			r.log.Warn("schema history report failed", "epoch", req.GetEpoch(), "err", err)
		}
	}
}

// close sends what is queued, for up to grace.
func (r *schemaReporter) close(grace time.Duration) {
	close(r.queue)
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-r.done:
	case <-t.C:
	}
	r.cancel()
}
