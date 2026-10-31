// Package temporalx creates Temporal clients. All namespace-aware code gets
// its client here.
package temporalx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"go.temporal.io/sdk/client"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"

	"github.com/unsuitable001/openetl/internal/platform/errs"
)

type Options struct {
	// host:port of the frontend.
	Address   string
	Namespace string
	Logger    *slog.Logger
}

func Dial(ctx context.Context, o Options) (client.Client, error) {
	log := o.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	c, err := client.DialContext(ctx, client.Options{
		HostPort:  o.Address,
		Namespace: o.Namespace,
		Logger:    tlog.NewStructuredLogger(log.With("component", "temporal")),
	})
	if err != nil {
		return nil, fmt.Errorf("temporalx: dial %s: %w", o.Address, err)
	}
	return c, nil
}

// ApplicationError returns err as a Temporal application error whose type is
// its taxonomy code, non-retryable where the taxonomy says so. Context
// cancellation passes through, so Temporal sees the activity cancelled.
func ApplicationError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return err
	}
	if _, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		return err
	}
	code := errs.CodeOf(err)
	opts := temporal.ApplicationErrorOptions{NonRetryable: !code.Retryable(), Cause: err}
	if e, ok := errors.AsType[*errs.Error](err); ok && e.RetryAfter > 0 {
		opts.NextRetryDelay = e.RetryAfter
	}
	return temporal.NewApplicationErrorWithOptions(err.Error(), code.String(), opts)
}
