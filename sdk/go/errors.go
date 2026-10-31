package sdk

import protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"

// WithCode classifies err with a code of the error taxonomy, which decides
// whether the platform retries it. Unclassified errors count as INTERNAL. It
// returns nil if err is nil.
func WithCode(code protocolv1.ErrorCode, err error) error {
	if err == nil {
		return nil
	}
	return &codedError{code: code, err: err}
}

type codedError struct {
	code protocolv1.ErrorCode
	err  error
}

func (e *codedError) Error() string                   { return e.err.Error() }
func (e *codedError) Unwrap() error                   { return e.err }
func (e *codedError) ErrorCode() protocolv1.ErrorCode { return e.code }
