package openstore

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/openetl-io/openetl/internal/checkpoint"
)

var (
	meter       = otel.Meter("github.com/openetl-io/openetl/internal/checkpoint/openstore")
	calls, _    = meter.Int64Counter("openetl.objectstore.calls", metric.WithUnit("{call}"), metric.WithDescription("Object-store calls for checkpoints and logs, by operation and outcome"))
	duration, _ = meter.Float64Histogram("openetl.objectstore.call.duration", metric.WithUnit("s"), metric.WithDescription("How long an object-store call took"))
)

// instrumented counts and times a store's calls.
type instrumented struct {
	s        checkpoint.Store
	provider attribute.KeyValue
}

func instrument(s checkpoint.Store, provider string) checkpoint.Store {
	return instrumented{s: s, provider: attribute.String("openetl.objectstore.provider", provider)}
}

func (i instrumented) record(ctx context.Context, op string, start time.Time, err error) {
	outcome := "ok"
	switch {
	case errors.Is(err, checkpoint.ErrNotFound):
		outcome = "not_found"
	case errors.Is(err, checkpoint.ErrPrecondition):
		outcome = "precondition"
	case err != nil:
		outcome = "error"
	}
	attrs := metric.WithAttributes(i.provider, attribute.String("openetl.objectstore.op", op), attribute.String("openetl.outcome", outcome))
	calls.Add(ctx, 1, attrs)
	duration.Record(ctx, time.Since(start).Seconds(), attrs)
}

func (i instrumented) Get(ctx context.Context, key string) ([]byte, checkpoint.Version, error) {
	start := time.Now()
	b, v, err := i.s.Get(ctx, key)
	i.record(ctx, "get", start, err)
	return b, v, err
}

func (i instrumented) Create(ctx context.Context, key string, data []byte) (checkpoint.Version, error) {
	start := time.Now()
	v, err := i.s.Create(ctx, key, data)
	i.record(ctx, "create", start, err)
	return v, err
}

func (i instrumented) Swap(ctx context.Context, key string, data []byte, prev checkpoint.Version) (checkpoint.Version, error) {
	start := time.Now()
	v, err := i.s.Swap(ctx, key, data, prev)
	i.record(ctx, "swap", start, err)
	return v, err
}

func (i instrumented) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := i.s.Delete(ctx, key)
	i.record(ctx, "delete", start, err)
	return err
}

func (i instrumented) List(ctx context.Context, prefix string) ([]string, error) {
	start := time.Now()
	keys, err := i.s.List(ctx, prefix)
	i.record(ctx, "list", start, err)
	return keys, err
}
