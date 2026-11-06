package errs

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// Status returns err as a gRPC status error that carries its classification
// in the details, as the connector protocol does. It returns nil if err is
// nil.
func Status(err error) error {
	if err == nil {
		return nil
	}
	c := codes.Unknown
	switch {
	case errors.Is(err, context.Canceled):
		c = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		c = codes.DeadlineExceeded
	}
	p := ToProto(err)
	st, derr := status.New(c, p.GetMessage()).WithDetails(p)
	if derr != nil {
		return status.Error(c, p.GetMessage())
	}
	return st.Err()
}

// FromStatus returns the classified error a gRPC status carries. A status
// without one is Internal. It returns nil if err is nil.
func FromStatus(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return Wrap(Internal, err, "")
	}
	for _, d := range st.Details() {
		if p, ok := d.(*protocolv1.Error); ok {
			return FromProto(p)
		}
	}
	switch st.Code() {
	case codes.Canceled:
		return Wrap(Internal, context.Canceled, st.Message())
	case codes.DeadlineExceeded:
		return Wrap(Internal, context.DeadlineExceeded, st.Message())
	}
	return New(Internal, "%s", st.Message())
}
