package errs

import (
	"errors"
	"fmt"
	"testing"
	"time"

	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/host"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

func TestCoversProtocol(t *testing.T) {
	for v, name := range protocolv1.ErrorCode_name {
		c := Code(v)
		if c == Unspecified {
			if _, ok := retryable[c]; ok {
				t.Errorf("%s has a retry decision", name)
			}
			continue
		}
		if _, ok := retryable[c]; !ok {
			t.Errorf("%s has no retry decision", name)
		}
		if "ERROR_CODE_"+c.String() != name {
			t.Errorf("Code(%d).String() = %q, want the suffix of %s", v, c, name)
		}
		if Named(c.String()) != c {
			t.Errorf("Named(%q) = %s", c.String(), Named(c.String()))
		}
	}
	for c := range retryable {
		if _, ok := protocolv1.ErrorCode_name[int32(c)]; !ok {
			t.Errorf("%s isn't in the protocol", c)
		}
	}
	// A stream job retries FENCED, which a sync attempt doesn't.
	for c := range recurs {
		if !c.Retryable() && c != Fenced {
			t.Errorf("%s recurs, and isn't retried", c)
		}
	}
	if Named("NO_RUNNER") != Unspecified || DataInvalid.Recurs() || Internal.Recurs() {
		t.Error("a code that isn't the protocol's, DATA_INVALID or INTERNAL recurs")
	}
}

func TestCodeOf(t *testing.T) {
	limited := New(RateLimited, "slow down")
	tests := []struct {
		err  error
		want Code
	}{
		{nil, Unspecified},
		{errors.New("plain"), Internal},
		{limited, RateLimited},
		{fmt.Errorf("read orders: %w", limited), RateLimited},
		{Wrap(AuthInvalid, errors.New("401"), "token revoked"), AuthInvalid},
		{fmt.Errorf("check: %w", sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, errors.New("42501"))), PermissionDenied},
	}
	for _, tt := range tests {
		if got := CodeOf(tt.err); got != tt.want {
			t.Errorf("CodeOf(%v) = %s, want %s", tt.err, got, tt.want)
		}
	}
}

func TestProtoRoundTrip(t *testing.T) {
	in := &Error{Code: RateLimited, Msg: "429 from api.example.com", RetryAfter: 30 * time.Second}
	p := ToProto(in)
	if p.GetMessage() != in.Msg {
		t.Errorf("message = %q, want %q", p.GetMessage(), in.Msg)
	}
	out := FromProto(p)
	if out.Error() != in.Error() {
		t.Errorf("round trip: %q, want %q", out, in)
	}
	if e, ok := errors.AsType[*Error](out); !ok || e.RetryAfter != in.RetryAfter {
		t.Errorf("round trip lost RetryAfter: %v", out)
	}

	wrapped := fmt.Errorf("read orders: %w", in)
	if p := ToProto(wrapped); p.GetCode() != protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED || p.GetRetryAfter().AsDuration() != in.RetryAfter {
		t.Errorf("ToProto(wrapped) = %v", p)
	}
	if ToProto(nil) != nil || FromProto(nil) != nil {
		t.Error("nil doesn't map to nil")
	}
}

// A check reads a platform error's code as it reads a connector's, so a
// built-in connector's check that failed with one keeps it.
func TestCheckReadsCode(t *testing.T) {
	f := check.Classify(fmt.Errorf("parquet: %w", Wrap(PermissionDenied, errors.New("mkdir /data: permission denied"), "")))
	if f.Code != protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED || f.Reason != check.PermissionDenied.Name {
		t.Errorf("Classify = %+v", f)
	}
}

// A host.StatusError carries its code and wait through the SDK's methods.
func TestStatusErrorToProto(t *testing.T) {
	err := fmt.Errorf("list orders: %w", &host.StatusError{Status: 429, Code: protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED, After: time.Minute})
	p := ToProto(err)
	if p.GetCode() != protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED || p.GetRetryAfter().AsDuration() != time.Minute {
		t.Errorf("ToProto = %v", p)
	}
}
