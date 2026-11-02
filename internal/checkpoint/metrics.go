package checkpoint

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	meter          = otel.Meter("github.com/unsuitable001/openetl/internal/checkpoint")
	headWrites, _  = meter.Int64Counter("openetl.checkpoint.writes", metric.WithUnit("{write}"), metric.WithDescription("Claims, commits and rewinds of checkpoint logs' HEADs, by outcome"))
	conflicts, _   = meter.Int64Counter("openetl.checkpoint.conflicts", metric.WithUnit("{conflict}"), metric.WithDescription("Compare-and-swaps on HEAD that another writer won"))
	writeLength, _ = meter.Float64Histogram("openetl.checkpoint.write.duration", metric.WithUnit("s"), metric.WithDescription("How long a claim, commit or rewind of HEAD took"))
)

func recordWrite(ctx context.Context, op string, start time.Time, err error) {
	outcome := "ok"
	switch {
	case errors.Is(err, ErrFenced):
		outcome = "fenced"
	case err != nil:
		outcome = "error"
	}
	attrs := metric.WithAttributes(attribute.String("openetl.checkpoint.op", op), attribute.String("openetl.outcome", outcome))
	headWrites.Add(ctx, 1, attrs)
	writeLength.Record(ctx, time.Since(start).Seconds(), attrs)
}

func recordConflict(ctx context.Context, op string) {
	conflicts.Add(ctx, 1, metric.WithAttributes(attribute.String("openetl.checkpoint.op", op)))
}
