package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"

	"github.com/unsuitable001/openetl/sdk/go/source"
)

type streamKind uint8

const (
	full streamKind = iota + 1
	incremental
	resumable
)

// Stream is one stream of a Source. Build it with Full, Incremental or
// Resumable. Every stream's replay is superset: a retry can repeat records,
// and the sink deduplicates them on _openetl_id.
type Stream[C any] struct {
	name       string
	kind       streamKind
	primaryKey []string
	cursor     string
	ordered    bool
	run        func(c *Ctx[C], state []byte, out *Out) error
}

func (s Stream[C]) Name() string { return s.name }

type StreamOption func(*streamOptions)

type streamOptions struct {
	primaryKey []string
	ordered    bool
}

func PrimaryKey(cols ...string) StreamOption {
	return func(o *streamOptions) { o.primaryKey = cols }
}

// Ordered says the API returns an incremental stream's records sorted by
// its cursor, so the cursor floor can advance after every page.
func Ordered() StreamOption { return func(o *streamOptions) { o.ordered = true } }

func options(opts []StreamOption) streamOptions {
	var o streamOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Full declares a stream read whole on every sync. Paginate marks a resume
// point after each page; otherwise a retry reads it from the start.
func Full[C any](name string, read func(c *Ctx[C], out *Out) error, opts ...StreamOption) Stream[C] {
	o := options(opts)
	return Stream[C]{name: name, kind: full, primaryKey: o.primaryKey, run: func(c *Ctx[C], _ []byte, out *Out) error {
		return read(c, out)
	}}
}

// Incremental declares a stream read from a cursor floor onwards. read gets
// the floor, which is zero on the first sync, and must return every record
// whose cursor field is at or after it: records at the floor are read again.
// The floor advances when the stream completes, or after every page with
// Ordered. A resumed read gets the floor its page token belongs to.
func Incremental[C any](name, cursor string, read func(c *Ctx[C], since Cursor, out *Out) error, opts ...StreamOption) Stream[C] {
	o := options(opts)
	return Stream[C]{name: name, kind: incremental, primaryKey: o.primaryKey, cursor: cursor, ordered: o.ordered, run: func(c *Ctx[C], _ []byte, out *Out) error {
		return read(c, out.since(), out)
	}}
}

// Resumable declares a stream that keeps its own state. read gets the state
// of the last committed resume point, nil at first, and marks resume points
// with Out.Checkpoint.
func Resumable[C any](name string, read func(c *Ctx[C], state json.RawMessage, out *Out) error, opts ...StreamOption) Stream[C] {
	o := options(opts)
	return Stream[C]{name: name, kind: resumable, primaryKey: o.primaryKey, run: func(c *Ctx[C], state []byte, out *Out) error {
		return read(c, state, out)
	}}
}

// streamState is a Full or Incremental stream's resume point.
type streamState struct {
	// Where a read without a page token starts.
	Cursor json.RawMessage `json:"cursor,omitempty"`
	// With Page in an incremental stream: the floor the page token belongs
	// to; null for none.
	Since json.RawMessage `json:"since,omitempty"`
	// The highest cursor emitted by the read Page belongs to.
	Max  json.RawMessage `json:"max,omitempty"`
	Page string          `json:"page,omitempty"`
}

const (
	flushRecords = 1000
	flushBytes   = 1 << 20
)

// Out sends one stream's records. Its methods block under backpressure and
// fail once the read is cancelled.
type Out struct {
	ctx      context.Context
	stream   string
	kind     streamKind
	emit     source.Emitter
	buf      []json.RawMessage
	bufBytes int

	cursor  string
	ordered bool
	state   streamState // as restored
	max     json.RawMessage
	resumed string // the restored page token, until Paginate uses it
	expired bool   // the restored page token was rejected
}

func newOut[C any](ctx context.Context, st *Stream[C], emit source.Emitter, s streamState) *Out {
	return &Out{ctx: ctx, stream: st.name, kind: st.kind, emit: emit, cursor: st.cursor, ordered: st.ordered, state: s, max: s.Max, resumed: s.Page}
}

func (o *Out) since() Cursor {
	raw := o.state.Cursor
	if o.state.Page != "" {
		raw = o.state.Since
	}
	if string(raw) == "null" {
		raw = nil
	}
	return Cursor{raw: raw}
}

// Emit sends records, each encoded as a JSON object.
func (o *Out) Emit(records ...any) error {
	for _, r := range records {
		b, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if err := o.EmitJSON(b); err != nil {
			return err
		}
	}
	return nil
}

// EmitJSON sends records that are JSON objects.
func (o *Out) EmitJSON(records ...json.RawMessage) error {
	for _, r := range records {
		r = bytes.TrimSpace(r)
		if len(r) == 0 || r[0] != '{' {
			return errors.New("a record must be a JSON object")
		}
		if o.kind == incremental {
			if err := o.track(r); err != nil {
				return err
			}
		}
		o.buf = append(o.buf, r)
		o.bufBytes += len(r)
		if len(o.buf) >= flushRecords || o.bufBytes >= flushBytes {
			if err := o.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *Out) track(r json.RawMessage) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(r, &fields); err != nil {
		return err
	}
	v, ok := fields[o.cursor]
	if !ok || string(v) == "null" {
		return nil
	}
	if o.max == nil {
		o.max = v
		return nil
	}
	c, err := compareCursors(v, o.max)
	if err != nil {
		return fmt.Errorf("cursor %q: %w", o.cursor, err)
	}
	if c > 0 {
		o.max = v
	}
	return nil
}

func (o *Out) flush() error {
	if len(o.buf) == 0 {
		return nil
	}
	err := o.emit.Rows(o.stream, o.buf)
	o.buf, o.bufBytes = nil, 0
	return err
}

// Checkpoint marks a resume point in a Resumable stream: a retry passes
// state to its read function.
func (o *Out) Checkpoint(state any) error {
	if o.kind != resumable {
		return errors.New("Checkpoint is for Resumable streams; Paginate marks resume points for the others")
	}
	b, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return o.resumePoint(b)
}

func (o *Out) resumePoint(state []byte) error {
	if err := o.flush(); err != nil {
		return err
	}
	return o.emit.ResumePoint(o.stream, state)
}

// page marks a resume point after a page whose successor is next.
func (o *Out) page(next string) error {
	s := streamState{Page: next}
	if o.kind == incremental {
		s.Cursor, s.Since = o.state.Cursor, o.since().raw
		if s.Since == nil {
			s.Since = json.RawMessage("null")
		}
		if o.ordered {
			floor, err := o.highest()
			if err != nil {
				return err
			}
			s.Cursor = floor
		} else {
			s.Max = o.max
		}
	}
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return o.resumePoint(b)
}

// highest is the larger of the restored floor and the highest cursor
// emitted.
func (o *Out) highest() (json.RawMessage, error) {
	if o.max == nil {
		return o.state.Cursor, nil
	}
	if o.state.Cursor == nil {
		return o.max, nil
	}
	c, err := compareCursors(o.max, o.state.Cursor)
	if err != nil {
		return nil, fmt.Errorf("cursor %q: %w", o.cursor, err)
	}
	if c > 0 {
		return o.max, nil
	}
	return o.state.Cursor, nil
}

// complete marks the final resume point and ends the stream. A Full stream
// ends with no state, so the next sync reads it whole again.
func (o *Out) complete() error {
	var state []byte
	switch o.kind {
	case incremental:
		floor, err := o.highest()
		if err != nil {
			return err
		}
		if state, err = json.Marshal(streamState{Cursor: floor}); err != nil {
			return err
		}
	case resumable:
		if err := o.flush(); err != nil {
			return err
		}
		return o.emit.Complete(o.stream)
	}
	if err := o.resumePoint(state); err != nil {
		return err
	}
	return o.emit.Complete(o.stream)
}

// ErrPageTokenExpired is what a fetch returns, wrapped or not, when the API
// rejects a page token. If it rejects the token a read resumed from, the
// stream starts over from its cursor floor instead of failing.
var ErrPageTokenExpired = errors.New("sdk: page token expired")

// Paginate fetches pages, starting where a resumed read stopped, and yields
// each one. After the loop body returns from a page, Paginate marks a resume
// point, then fetches the next. An empty next token ends pagination. A read
// runs at most one Paginate loop. Resumable streams mark their own resume
// points and can't use it.
func Paginate[P any](out *Out, fetch func(ctx context.Context, token string) (page P, next string, err error)) iter.Seq2[P, error] {
	return func(yield func(P, error) bool) {
		var zero P
		if out.kind == resumable {
			yield(zero, errors.New("Paginate can't mark resume points in a Resumable stream; use Checkpoint"))
			return
		}
		token := out.resumed
		out.resumed = ""
		first := token != ""
		for {
			p, next, err := fetch(out.ctx, token)
			if err != nil {
				out.expired = first && errors.Is(err, ErrPageTokenExpired)
				yield(zero, err)
				return
			}
			first = false
			if !yield(p, nil) {
				return
			}
			if next == "" {
				return
			}
			if err := out.page(next); err != nil {
				yield(zero, err)
				return
			}
			token = next
		}
	}
}
