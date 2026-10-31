// Package errs is the error taxonomy. Every error that crosses a connector,
// activity, engine or API boundary carries a Code, and retries, pauses and
// heal playbooks key off it rather than off messages.
//
// The codes mirror openetl.connector.v1.ErrorCode, which is the one
// definition; the engine mirrors it too.
package errs

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

// Code classifies an error. Its values are the protocol's ErrorCode values.
type Code int32

const (
	Unspecified            = Code(protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED)
	Transient              = Code(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT)
	RateLimited            = Code(protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED)
	AuthExpired            = Code(protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED)
	AuthInvalid            = Code(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID)
	PermissionDenied       = Code(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED)
	ConfigInvalid          = Code(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID)
	SchemaDrift            = Code(protocolv1.ErrorCode_ERROR_CODE_SCHEMA_DRIFT)
	ContractChanged        = Code(protocolv1.ErrorCode_ERROR_CODE_CONTRACT_CHANGED)
	DataInvalid            = Code(protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID)
	DestinationUnavailable = Code(protocolv1.ErrorCode_ERROR_CODE_DESTINATION_UNAVAILABLE)
	DestinationQuota       = Code(protocolv1.ErrorCode_ERROR_CODE_DESTINATION_QUOTA)
	ResourceExhausted      = Code(protocolv1.ErrorCode_ERROR_CODE_RESOURCE_EXHAUSTED)
	Internal               = Code(protocolv1.ErrorCode_ERROR_CODE_INTERNAL)
	Fenced                 = Code(protocolv1.ErrorCode_ERROR_CODE_FENCED)
)

// retryable lists every code and whether an attempt that failed with it is
// retried. A test checks that it covers the protocol's enum.
var retryable = map[Code]bool{
	Transient:              true,
	RateLimited:            true,
	AuthExpired:            true,
	AuthInvalid:            false,
	PermissionDenied:       false,
	ConfigInvalid:          false,
	SchemaDrift:            true,
	ContractChanged:        false,
	DataInvalid:            true,
	DestinationUnavailable: true,
	DestinationQuota:       false,
	ResourceExhausted:      true,
	Internal:               true,
	Fenced:                 false,
}

// Retryable reports whether an attempt that failed with c is retried.
// Internal is retried a limited number of times, then raises an incident.
func (c Code) Retryable() bool { return retryable[c] }

// String returns the code's name without the enum prefix, such as
// "RATE_LIMITED".
func (c Code) String() string {
	name, ok := protocolv1.ErrorCode_name[int32(c)]
	if !ok {
		return fmt.Sprintf("Code(%d)", int32(c))
	}
	return name[len("ERROR_CODE_"):]
}

// Error is an error with a code.
type Error struct {
	Code Code
	// Msg is for people: no secrets, no record contents.
	Msg string
	// RetryAfter is how long to wait before retrying, when the cause says.
	RetryAfter time.Duration
	// Err is the cause, if any.
	Err error
}

func (e *Error) Error() string { return e.Code.String() + ": " + e.text() }

// text is the message without the code.
func (e *Error) text() string {
	switch {
	case e.Err == nil:
		return e.Msg
	case e.Msg == "":
		return e.Err.Error()
	default:
		return e.Msg + ": " + e.Err.Error()
	}
}

func (e *Error) Unwrap() error { return e.Err }

// New returns an error with code c.
func New(c Code, format string, args ...any) error {
	return &Error{Code: c, Msg: fmt.Sprintf(format, args...)}
}

// Wrap classifies err as c. It returns nil if err is nil.
func Wrap(c Code, err error, msg string) error {
	if err == nil {
		return nil
	}
	return &Error{Code: c, Msg: msg, Err: err}
}

// CodeOf returns the code of the first *Error in err's chain. An error without
// one is Internal, and a nil error is Unspecified.
func CodeOf(err error) Code {
	if err == nil {
		return Unspecified
	}
	if e, ok := errors.AsType[*Error](err); ok {
		return e.Code
	}
	return Internal
}

// ToProto returns err as a protocol Error, or nil if err is nil.
func ToProto(err error) *protocolv1.Error {
	if err == nil {
		return nil
	}
	p := &protocolv1.Error{Code: protocolv1.ErrorCode(CodeOf(err)), Message: err.Error()}
	if e, ok := err.(*Error); ok { //nolint:errorlint // only the outermost error's text starts with its code
		// The code travels in its own field.
		p.Message = e.text()
	}
	if e, ok := errors.AsType[*Error](err); ok && e.RetryAfter > 0 {
		p.RetryAfter = durationpb.New(e.RetryAfter)
	}
	return p
}

// FromProto returns p as an error, or nil if p is nil.
func FromProto(p *protocolv1.Error) error {
	if p == nil {
		return nil
	}
	return &Error{Code: Code(p.GetCode()), Msg: p.GetMessage(), RetryAfter: p.GetRetryAfter().AsDuration()}
}
