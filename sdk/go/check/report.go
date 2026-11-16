package check

import (
	"time"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// Item is how one check went.
type Item struct {
	Name string
	Kind Kind
	// Templates.
	Title       string
	Description string
	// A dotted path in the config; empty for none.
	Field  string
	Status Status
	// A template: what a Passed check found, the failure's message for a
	// Failed or Warning one, and why a Skipped one didn't run.
	Message string
	// Set when Failed, and when Warning other than from a connector that
	// gave nothing more than a message.
	Failure *Failure
	// The values of the placeholders of the item's templates and its
	// Failure's.
	Params Params
	// Zero for a check that didn't run.
	Duration time.Duration
}

// Rendered is it with its templates and its Failure's filled in from
// Params. Its Failure has no Params, so that its Error doesn't fill in what
// a value put there; the item's Params keep them.
func (it Item) Rendered() Item {
	it = it.valid()
	it.Title, it.Description, it.Message = Render(it.Title, it.Params), Render(it.Description, it.Params), Render(it.Message, it.Params)
	if f := it.Failure; f != nil {
		f.Message, f.Fix, f.Params = Render(f.Message, it.Params), Render(f.Fix, it.Params), nil
	}
	return it
}

// valid is a copy of it with its strings valid UTF-8, and its Failure's
// params among its own: where both have a name, the item's value.
func (it Item) valid() Item {
	it.Name, it.Title, it.Description, it.Field, it.Message = clean(it.Name), clean(it.Title), clean(it.Description), clean(it.Field), clean(it.Message)
	it.Params = cleanParams(it.Params)
	if it.Failure != nil {
		f := *it.Failure
		for k, v := range cleanParams(f.Params) {
			if _, ok := it.Params[k]; !ok {
				it.Params[k] = v
			}
		}
		f.Reason, f.Message, f.Fix, f.Detail, f.Params = clean(f.Reason), clean(f.Message), clean(f.Fix), clean(f.Detail), it.Params
		it.Failure = &f
	}
	return it
}

// failure is it's Failure as Err and the protocol give it: filled in as a
// hand-built one is, Unknown's for an item without one, and with it's
// params.
func (it Item) failure() *Failure {
	it = it.valid()
	if it.Failure == nil {
		return &Failure{Reason: Unknown.Name, Code: Unknown.Code, Message: Unknown.Message, Params: it.Params}
	}
	return normal(it.Failure)
}

// Report is a run's items, in the order the checks ran.
type Report struct {
	Items []Item
	// What failed besides an item: the list of checks, the run's context,
	// or a connector that gave an error and no items.
	err error
}

// OK reports whether Err is nil.
func (r Report) OK() bool { return r.Err() == nil }

// Err is the first failing item as an error: "title: message: detail", its
// templates filled in, with its failure's ErrorCode and RetryAfter. An item
// fails when it's Failed, or when its status is none of the four. Without
// one, Err is the error of what failed besides the items: a list Validate
// refused, a panic listing the checks, a run cut short by its context, or
// the error of a connector's answer; or nil.
func (r Report) Err() error {
	for _, it := range r.Items {
		if !failing(it.Status) {
			continue
		}
		f := it.failure()
		text := f.Error()
		if t := Render(clean(it.Title), f.Params); t != "" {
			text = t + ": " + text
		}
		return &itemError{text: text, code: f.Code, after: f.After}
	}
	return r.err
}

// itemError is a failing item's. It doesn't unwrap: its code is the item's,
// whatever the failure's cause says.
type itemError struct {
	text  string
	code  protocolv1.ErrorCode
	after time.Duration
}

func (e *itemError) Error() string                   { return e.text }
func (e *itemError) ErrorCode() protocolv1.ErrorCode { return e.code }
func (e *itemError) RetryAfter() time.Duration       { return e.after }
