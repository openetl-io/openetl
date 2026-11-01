// Package temporalx creates Temporal clients. All namespace-aware code gets
// its client here.
package temporalx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
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
	if d := errs.RetryAfterOf(err); d > 0 {
		opts.NextRetryDelay = d
	}
	return temporal.NewApplicationErrorWithOptions(err.Error(), code.String(), opts)
}

// EnsureSearchAttributes adds the custom search attributes the namespace
// lacks. It fails if one exists with another type.
func EnsureSearchAttributes(ctx context.Context, c client.Client, namespace string, keys []temporal.SearchAttributeKey) error {
	have, err := c.OperatorService().ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{Namespace: namespace})
	if err != nil {
		return fmt.Errorf("temporalx: list search attributes: %w", err)
	}
	missing := map[string]enumspb.IndexedValueType{}
	for _, k := range keys {
		got, ok := have.GetCustomAttributes()[k.GetName()]
		switch {
		case !ok:
			missing[k.GetName()] = k.GetValueType()
		case got != k.GetValueType():
			return fmt.Errorf("temporalx: search attribute %s is a %s, not a %s", k.GetName(), got, k.GetValueType())
		}
	}
	if len(missing) == 0 {
		return nil
	}
	_, err = c.OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{Namespace: namespace, SearchAttributes: missing})
	if _, ok := errors.AsType[*serviceerror.AlreadyExists](err); ok {
		return nil
	}
	if err != nil {
		return fmt.Errorf("temporalx: add search attributes: %w", err)
	}
	return nil
}
