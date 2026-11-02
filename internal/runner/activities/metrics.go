package activities

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner"
)

var attempts, _ = otel.Meter("github.com/unsuitable001/openetl/internal/runner/activities").Int64Counter("openetl.job.attempts",
	metric.WithUnit("{attempt}"), metric.WithDescription("Sync attempts and stream jobs that ended, by outcome; retries are those after a sync's or run's first"))

// countAttempt counts an attempt that ended with err: kind is sync or
// stream, and stopped says it stopped because it was asked to.
func countAttempt(ctx context.Context, org, pipeline, kind string, attempt int, stopped bool, err error) {
	outcome, code := "succeeded", ""
	switch {
	case errors.Is(err, runner.ErrDrained) && !stopped:
		outcome = "drained"
	case stopped:
		outcome = "stopped"
	case err != nil:
		outcome, code = "failed", errs.CodeOf(err).String()
	}
	attempts.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(
		attribute.String("openetl.org", org),
		attribute.String("openetl.pipeline", pipeline),
		attribute.String("openetl.attempt.kind", kind),
		attribute.Bool("openetl.retry", attempt > 1),
		attribute.String("openetl.outcome", outcome),
		attribute.String("openetl.error.code", code),
	))
}
