// Package rpcerr carries classified errors in gRPC statuses, as the
// connector protocol does.
package rpcerr

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

type coded interface {
	error
	ErrorCode() protocolv1.ErrorCode
}

type delayed interface {
	error
	RetryAfter() time.Duration
}

// Proto classifies err: INTERNAL unless something in its chain has a code.
func Proto(err error) *protocolv1.Error {
	if err == nil {
		return nil
	}
	p := &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_INTERNAL, Message: err.Error()}
	if c, ok := errors.AsType[coded](err); ok {
		p.Code = c.ErrorCode()
	}
	if d, ok := errors.AsType[delayed](err); ok && d.RetryAfter() > 0 {
		p.RetryAfter = durationpb.New(d.RetryAfter())
	}
	return p
}

// Status returns err as a status error with its classification in the
// details.
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
	p := Proto(err)
	st, derr := status.New(c, p.GetMessage()).WithDetails(p)
	if derr != nil {
		return status.Error(c, p.GetMessage())
	}
	return st.Err()
}

// Error is a classified error from the other side.
type Error struct {
	Code    protocolv1.ErrorCode
	Message string
	After   time.Duration
	err     error
}

func FromProto(p *protocolv1.Error) *Error {
	return &Error{Code: p.GetCode(), Message: p.GetMessage(), After: p.GetRetryAfter().AsDuration()}
}

func (e *Error) Error() string                   { return e.Message }
func (e *Error) Unwrap() error                   { return e.err }
func (e *Error) ErrorCode() protocolv1.ErrorCode { return e.Code }
func (e *Error) RetryAfter() time.Duration       { return e.After }

// FromStatus returns the classified error a status carries; INTERNAL if it
// carries none.
func FromStatus(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return &Error{Code: protocolv1.ErrorCode_ERROR_CODE_INTERNAL, Message: err.Error(), err: err}
	}
	for _, d := range st.Details() {
		if p, ok := d.(*protocolv1.Error); ok {
			return FromProto(p)
		}
	}
	e := &Error{Code: protocolv1.ErrorCode_ERROR_CODE_INTERNAL, Message: st.Message()}
	switch st.Code() {
	case codes.Canceled:
		e.err = context.Canceled
	case codes.DeadlineExceeded:
		e.err = context.DeadlineExceeded
	}
	return e
}
