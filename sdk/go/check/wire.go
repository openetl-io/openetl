package check

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/openetl-io/openetl/sdk/go/internal/ints"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// Of runs c's check, a bound source's or destination's: Run of the checks
// its Checks() []Check method lists, when it lists any; or else FromError of
// its Check(context.Context) error method's error; or else an empty report.
// A panic in Checks or Check is the report's error.
func Of(ctx context.Context, c any) Report {
	if l, ok := c.(interface{ Checks() []Check }); ok {
		checks, err := build(l.Checks)
		if err != nil {
			return Report{err: err}
		}
		if len(checks) > 0 {
			return Run(ctx, checks)
		}
	}
	if ck, ok := c.(interface{ Check(context.Context) error }); ok {
		return FromError(checked(ctx, ck.Check))
	}
	return Report{}
}

// RunList is Run of the checks that list returns, such as a connector's
// preflight for one request. A panic in list is the report's error, with no
// items.
func RunList(ctx context.Context, list func() []Check) Report {
	checks, err := build(list)
	if err != nil {
		return Report{err: err}
	}
	return Run(ctx, checks)
}

func build(list func() []Check) (checks []Check, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &Failure{Reason: Unknown.Name, Code: Unknown.Code, Message: "the connector panicked listing its checks", Detail: clean(fmt.Sprint(r))}
		}
	}()
	return list(), nil
}

func checked(ctx context.Context, check func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &Failure{Reason: Unknown.Name, Code: Unknown.Code, Message: "the connector's check panicked", Detail: clean(fmt.Sprint(r))}
		}
	}()
	return check(ctx)
}

// FromError is the report of a connector that gives an error and no items:
// no items, and err as Err. It's OK for a nil err.
func FromError(err error) Report {
	if isNil(err) {
		return Report{}
	}
	return Report{err: err}
}

// FromProto reads a connector's answer: a CheckResponse,
// PreflightSourceResponse or PreflightDestinationResponse. The answer's
// error fails the report whether an item failed or not: without a failed
// item, it's the error of checks that couldn't finish or couldn't run. It
// fails closed: an item whose status is none of PASSED, WARNING and SKIPPED
// is Failed, and a Failed item without an error gets Unknown's. Without
// items, it's FromError of the answer's error.
func FromProto(resp interface {
	GetError() *protocolv1.Error
	GetItems() []*protocolv1.CheckItem
}) Report {
	var r Report
	if e := resp.GetError(); e != nil {
		r.err = &protoError{p: e}
	}
	for _, p := range resp.GetItems() {
		r.Items = append(r.Items, itemOf(p))
	}
	return r
}

func itemOf(p *protocolv1.CheckItem) Item {
	it := Item{
		Name: p.GetName(), Kind: Kind(p.GetKind()).orCustom(), Title: p.GetTitle(), Description: p.GetDescription(),
		Field: p.GetField(), Status: Status(p.GetStatus()), Message: p.GetMessage(), Params: Params(maps.Clone(p.GetParams())),
		Duration: p.GetDuration().AsDuration(),
	}
	if it.Params == nil {
		it.Params = Params{}
	}
	switch it.Status {
	case Passed, Skipped:
		return it
	case Warning:
		if p.GetError() == nil && p.GetReason() == "" && p.GetFix() == "" && p.GetDetail() == "" {
			return it
		}
	case Failed:
	default:
		it.Status = Failed
	}
	f := &Failure{
		Reason: p.GetReason(), Code: p.GetError().GetCode(), Message: p.GetMessage(), Fix: p.GetFix(), Detail: p.GetDetail(),
		Params: it.Params, After: p.GetError().GetRetryAfter().AsDuration(),
	}
	if m := p.GetError().GetMessage(); f.Message == "" && m != "" {
		f.Message = m
	}
	if f.Detail == "" && it.Status != Status(p.GetStatus()) {
		f.Detail = "the connector gave the item the status " + p.GetStatus().String()
	}
	it.Failure = normal(f)
	if it.Status == Warning && it.Failure.Reason == Unknown.Name && it.Failure.Message == Unknown.Message {
		// Unknown's message says the check failed, which a warning didn't,
		// as Run has it.
		it.Failure.Message, it.Message = unknownWarning, unknownWarning
	}
	if it.Message == "" {
		it.Message = it.Failure.Message
	}
	return it
}

// protoError is a connector's answer's error.
type protoError struct{ p *protocolv1.Error }

func (e *protoError) Error() string { return e.p.GetMessage() }

// ErrorCode is INTERNAL for an error without a code.
func (e *protoError) ErrorCode() protocolv1.ErrorCode {
	if c := e.p.GetCode(); c != protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED {
		return c
	}
	return protocolv1.ErrorCode_ERROR_CODE_INTERNAL
}

func (e *protoError) RetryAfter() time.Duration { return e.p.GetRetryAfter().AsDuration() }

// Proto is r as a connector answers: its items, with their templates
// unfilled and their strings valid UTF-8, and the answer's error, which is
// Err's: the first failed item's, or else, with none failed, that of a run
// its context cut short or of checks that couldn't run. An item's own error
// has as message the item's message filled in, without the detail, which has
// its own field. An item whose status is none of the four goes as FAILED, and
// a failing item without a Failure gets Unknown's.
func (r Report) Proto() ([]*protocolv1.CheckItem, *protocolv1.Error) {
	var items []*protocolv1.CheckItem
	for _, it := range r.Items {
		it = it.valid()
		p := &protocolv1.CheckItem{
			Name: it.Name, Title: it.Title, Status: ints.Enum[protocolv1.CheckStatus](it.Status), Message: it.Message,
			Field: it.Field, Kind: ints.Enum[protocolv1.CheckKind](it.Kind.orCustom()), Description: it.Description,
		}
		if len(it.Params) > 0 {
			p.Params = it.Params
		}
		if it.Duration > 0 {
			p.Duration = durationpb.New(it.Duration)
		}
		if failing(it.Status) {
			p.Status = protocolv1.CheckStatus_CHECK_STATUS_FAILED
		}
		if failing(it.Status) || it.Status == Warning && it.Failure != nil {
			f := it.failure()
			if p.Message == "" {
				p.Message = f.Message
			}
			p.Error = &protocolv1.Error{Code: f.Code, Message: Render(p.Message, it.Params)}
			if f.After > 0 {
				p.Error.RetryAfter = durationpb.New(f.After)
			}
			p.Fix, p.Reason, p.Detail = f.Fix, f.Reason, f.Detail
		}
		items = append(items, p)
	}
	return items, errorProto(r.Err())
}

// Redact puts "[redacted]" in place of each of secrets in an answer: in its
// items' titles, descriptions, messages, fixes, details and fields, their
// params' names and values, and its errors' messages. It finds a secret as
// written and as a URL's query, path and userinfo escape it, the longest form
// first, and changes the answer in place. It's for the SDK's serving code and
// hosts, with the values of the secrets they gave the connector, not for a
// connector's checks.
func Redact(secrets []string, items []*protocolv1.CheckItem, e *protocolv1.Error) {
	var forms []string
	for _, s := range secrets {
		userinfo := strings.TrimPrefix(url.UserPassword("", s).String(), ":")
		for _, v := range []string{s, url.QueryEscape(s), url.PathEscape(s), userinfo} {
			if v != "" && !slices.Contains(forms, v) {
				forms = append(forms, v)
			}
		}
	}
	if len(forms) == 0 {
		return
	}
	slices.SortStableFunc(forms, func(a, b string) int { return len(b) - len(a) })
	redact := func(s string) string {
		for _, v := range forms {
			s = strings.ReplaceAll(s, v, redacted)
		}
		return s
	}
	for _, it := range items {
		it.Title, it.Description, it.Message, it.Fix, it.Detail, it.Field = redact(it.Title), redact(it.Description), redact(it.Message), redact(it.Fix), redact(it.Detail), redact(it.Field)
		if len(it.Params) > 0 {
			p := make(map[string]string, len(it.Params))
			for k, v := range it.Params {
				p[redact(k)] = redact(v)
			}
			it.Params = p
		}
		if it.Error != nil {
			it.Error.Message = redact(it.Error.Message)
		}
	}
	if e != nil {
		e.Message = redact(e.Message)
	}
}

const redacted = "[redacted]"

func errorProto(err error) *protocolv1.Error {
	if err == nil {
		return nil
	}
	p := &protocolv1.Error{Code: codeIn(err), Message: detailOf(err, err.Error())}
	if p.Code == protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED {
		p.Code = protocolv1.ErrorCode_ERROR_CODE_INTERNAL
	}
	if d := afterIn(err); d > 0 {
		p.RetryAfter = durationpb.New(d)
	}
	return p
}
