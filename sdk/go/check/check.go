// Package check is a report of what a check or a preflight looked at, item
// by item, with what to do about each that fails.
//
// A check is about a connection: whether a config reaches the source or the
// destination, such as signing in. A source or destination reports one by
// implementing source.CheckReporter or destination.CheckReporter. A
// preflight is about a pipeline: whether the streams it reads or writes
// would work, such as each stream's permissions, and it's reported by
// source.Preflighter or destination.Preflighter.
//
// Item text is shown to people and kept in results, so it never holds
// secrets or records.
package check

import (
	"errors"
	"fmt"
)

// Status is how an item went.
type Status int

const (
	Passed Status = iota + 1
	// It works, but not as configured or not as well as it could: a pipeline
	// can still run.
	Warning
	Failed
	// Not checked: an item it needs failed, or it can't be checked without
	// changing something, such as writing a record.
	Skipped
)

func (s Status) String() string {
	switch s {
	case Passed:
		return "passed"
	case Warning:
		return "warning"
	case Failed:
		return "failed"
	case Skipped:
		return "skipped"
	}
	return "unspecified"
}

// Item is one thing a check looked at.
type Item struct {
	// Unique within the report and the same from one check to the next,
	// such as stream.clicks.read.
	Name string
	// What was checked, for people: "Read stream clicks".
	Title  string
	Status Status
	// What it found. For a failure, Err's message when it's empty.
	Message string
	// A failure's error, classified with sdk.WithCode.
	Err error
	// What to do about a failure or a warning, such as the IAM action to
	// allow.
	Fix string
	// The config field it concerns, as a dotted path such as consumer.name.
	Field string
}

// Report collects a check's items in the order they ran. Its methods aren't
// safe for concurrent use.
type Report struct {
	items []Item
	names map[string]bool
}

// Option sets more of an item.
type Option func(*Item)

// Field names the config field an item concerns, as a dotted path.
func Field(path string) Option { return func(it *Item) { it.Field = path } }

// WithFix says what to do about a failure or a warning. A fix that err
// carries, from Fix, takes its place.
func WithFix(text string) Option { return func(it *Item) { it.Fix = text } }

// Run runs fn as the item name and records whether it passed. A failed item
// takes fn's error, its message, and the fix the error carries. It reports
// whether fn passed, so the items that need it can be skipped.
func (r *Report) Run(name, title string, fn func() error, opts ...Option) bool {
	err := fn()
	if err == nil {
		r.add(Item{Name: name, Title: title, Status: Passed}, opts)
		return true
	}
	r.Fail(name, title, err, opts...)
	return false
}

// Pass records an item that passed, with what it found.
func (r *Report) Pass(name, title, message string, opts ...Option) {
	r.add(Item{Name: name, Title: title, Status: Passed, Message: message}, opts)
}

// Fail records an item that failed with err.
func (r *Report) Fail(name, title string, err error, opts ...Option) {
	if err == nil {
		err = errors.New("failed")
	}
	it := Item{Name: name, Title: title, Status: Failed, Message: err.Error(), Err: err}
	r.add(it, append(opts, func(it *Item) {
		if f := fixOf(err); f != "" {
			it.Fix = f
		}
	}))
}

// Warn records an item that works, but not as well as it could.
func (r *Report) Warn(name, title, message string, opts ...Option) {
	r.add(Item{Name: name, Title: title, Status: Warning, Message: message}, opts)
}

// Skip records an item that wasn't checked, and why.
func (r *Report) Skip(name, title, why string, opts ...Option) {
	r.add(Item{Name: name, Title: title, Status: Skipped, Message: why}, opts)
}

func (r *Report) add(it Item, opts []Option) {
	for _, o := range opts {
		o(&it)
	}
	if r.names == nil {
		r.names = map[string]bool{}
	}
	if r.names[it.Name] {
		for i := 2; ; i++ {
			if n := fmt.Sprintf("%s.%d", it.Name, i); !r.names[n] {
				it.Name = n
				break
			}
		}
	}
	r.names[it.Name] = true
	r.items = append(r.items, it)
}

// Items are the report's items, in the order they ran.
func (r *Report) Items() []Item { return r.items }

// Err is the first failed item's error, or nil if none failed.
func (r *Report) Err() error {
	for _, it := range r.items {
		if it.Status == Failed {
			return it.Err
		}
	}
	return nil
}

// Fix attaches what to do about err, which a failed item shows. It returns
// nil if err is nil.
func Fix(err error, text string) error {
	if err == nil {
		return nil
	}
	return &fixed{err: err, fix: text}
}

type fixed struct {
	err error
	fix string
}

func (e *fixed) Error() string { return e.err.Error() }
func (e *fixed) Unwrap() error { return e.err }

func fixOf(err error) string {
	if f, ok := errors.AsType[*fixed](err); ok {
		return f.fix
	}
	return ""
}
