// Package errs is the error taxonomy. Its codes mirror
// openetl.connector.v1.ErrorCode.
package errs

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

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
	Draining               = Code(protocolv1.ErrorCode_ERROR_CODE_DRAINING)
)

// SCHEMA_DRIFT comes only from a schema policy pausing a stream, which no
// retry changes until approvals exist.
var retryable = map[Code]bool{
	Transient:              true,
	RateLimited:            true,
	AuthExpired:            true,
	AuthInvalid:            false,
	PermissionDenied:       false,
	ConfigInvalid:          false,
	SchemaDrift:            false,
	ContractChanged:        false,
	DataInvalid:            true,
	DestinationUnavailable: true,
	DestinationQuota:       false,
	ResourceExhausted:      true,
	Internal:               true,
	Fenced:                 false,
	Draining:               true,
}

// Retryable reports whether an attempt that failed with c is retried.
func (c Code) Retryable() bool { return retryable[c] }

// NonRetryable returns the names of the codes that aren't retried, sorted.
func NonRetryable() []string {
	var names []string
	for c, r := range retryable {
		if !r {
			names = append(names, c.String())
		}
	}
	slices.Sort(names)
	return names
}

// String returns the name without the enum prefix, such as "RATE_LIMITED".
func (c Code) String() string {
	name, ok := protocolv1.ErrorCode_name[int32(c)]
	if !ok {
		return fmt.Sprintf("Code(%d)", int32(c))
	}
	return name[len("ERROR_CODE_"):]
}

type Error struct {
	Code Code
	// No secrets or record contents.
	Msg        string
	RetryAfter time.Duration
	Err        error
}

func (e *Error) Error() string { return e.Code.String() + ": " + e.text() }

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

// CodeOf returns the code of the first *Error in err's chain: Internal if
// there is none, Unspecified if err is nil.
func CodeOf(err error) Code {
	if err == nil {
		return Unspecified
	}
	if e, ok := errors.AsType[*Error](err); ok {
		return e.Code
	}
	return Internal
}

func ToProto(err error) *protocolv1.Error {
	if err == nil {
		return nil
	}
	p := &protocolv1.Error{Code: protocolv1.ErrorCode(CodeOf(err)), Message: err.Error()}
	if e, ok := err.(*Error); ok { //nolint:errorlint // the code has its own field, so drop it from the outermost message
		p.Message = e.text()
	}
	if e, ok := errors.AsType[*Error](err); ok && e.RetryAfter > 0 {
		p.RetryAfter = durationpb.New(e.RetryAfter)
	}
	return p
}

func FromProto(p *protocolv1.Error) error {
	if p == nil {
		return nil
	}
	return &Error{Code: Code(p.GetCode()), Msg: p.GetMessage(), RetryAfter: p.GetRetryAfter().AsDuration()}
}
