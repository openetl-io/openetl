package sdk_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// recorder writes each call as one line: rows ids, resume state, complete
// and the next sync's state.
type recorder struct{ calls []string }

func (r *recorder) Rows(stream string, rows []json.RawMessage) error {
	var ids []string
	for _, row := range rows {
		var v struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.Unmarshal(row, &v); err != nil {
			return err
		}
		ids = append(ids, string(v.ID))
	}
	r.calls = append(r.calls, fmt.Sprintf("%s rows %s", stream, strings.Join(ids, ",")))
	return nil
}

func (r *recorder) Records(string, arrow.RecordBatch) error { return errors.New("unexpected batch") }

func (r *recorder) ResumePoint(stream string, state []byte) error {
	r.calls = append(r.calls, fmt.Sprintf("%s resume %s", stream, state))
	return nil
}

func (r *recorder) Split(stream, id string) source.SplitEmitter {
	return source.WholeSplit(r, stream, id)
}

func (r *recorder) Complete(stream string, next []byte) error {
	r.calls = append(r.calls, fmt.Sprintf("%s complete %s", stream, next))
	return nil
}

type config struct {
	Limit int `json:"limit"`
}

func (c *config) Validate() error {
	if c.Limit < 0 {
		return errors.New("limit must not be negative")
	}
	return nil
}

func read(t *testing.T, src *sdk.Source[config], stream, state string) []string {
	t.Helper()
	s, err := src.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	r := &recorder{}
	cs := source.ConfiguredStream{Name: stream}
	if state != "" {
		cs.State = []byte(state)
	}
	if err := s.Read(context.Background(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, r); err != nil {
		t.Fatal(err)
	}
	return r.calls
}

func expect(t *testing.T, got []string, want ...string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("got\n  %s\nwant\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// pages serves records in pages chained by tokens "", "t1", "t2", …, and
// records each token it was asked for. Each expired token is rejected once.
type pages struct {
	records [][]string
	tokens  []string
	expired map[string]bool
}

func (p *pages) fetch(_ context.Context, token string) ([]json.RawMessage, string, error) {
	p.tokens = append(p.tokens, token)
	if p.expired[token] {
		delete(p.expired, token)
		return nil, "", fmt.Errorf("HTTP 400: %w", sdk.ErrPageTokenExpired)
	}
	i := 0
	if token != "" {
		if _, err := fmt.Sscanf(token, "t%d", &i); err != nil {
			return nil, "", err
		}
	}
	var out []json.RawMessage
	for _, r := range p.records[i] {
		out = append(out, json.RawMessage(r))
	}
	if i == len(p.records)-1 {
		return out, "", nil
	}
	return out, fmt.Sprintf("t%d", i+1), nil
}

func paginated[C any](p *pages, since *[]string) func(c *sdk.Ctx[C], s sdk.Cursor, out *sdk.Out) error {
	return func(_ *sdk.Ctx[C], s sdk.Cursor, out *sdk.Out) error {
		*since = append(*since, s.String())
		for page, err := range sdk.Paginate(out, p.fetch) {
			if err != nil {
				return err
			}
			if err := out.EmitJSON(page...); err != nil {
				return err
			}
		}
		return nil
	}
}

func TestNewChecksConfigAndStreams(t *testing.T) {
	src := &sdk.Source[config]{Name: "acme", Streams: []sdk.Stream[config]{
		sdk.Full("a", func(*sdk.Ctx[config], *sdk.Out) error { return nil }),
	}}
	if _, err := src.New([]byte(`{"limit": 3}`)); err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []string{`{"limt": 3}`, `{"limit": -1}`, `[]`} {
		if _, err := src.New([]byte(cfg)); err == nil {
			t.Errorf("config %s accepted", cfg)
		}
	}
	src.Streams = append(src.Streams, src.Streams[0])
	if _, err := src.New(nil); err == nil {
		t.Error("a repeated stream name was accepted")
	}
}

func TestCheckAndDiscover(t *testing.T) {
	var checked int
	src := &sdk.Source[config]{
		Name:  "acme",
		Check: func(_ context.Context, cfg *config) error { checked = cfg.Limit; return nil },
		Streams: []sdk.Stream[config]{
			sdk.Full("a", func(*sdk.Ctx[config], *sdk.Out) error { return nil }, sdk.PrimaryKey("id")),
			sdk.Incremental("b", "updated_at", func(*sdk.Ctx[config], sdk.Cursor, *sdk.Out) error { return nil }),
		},
	}
	s, err := src.New([]byte(`{"limit": 7}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.(source.Checker).Check(context.Background()); err != nil || checked != 7 {
		t.Fatalf("Check: %v, saw limit %d", err, checked)
	}
	cat, err := s.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []source.CatalogStream{{Name: "a", PrimaryKey: []string{"id"}, Replay: source.Superset}, {Name: "b", Replay: source.Superset}}
	if len(cat.Streams) != 2 || cat.Streams[0].Name != "a" || !slices.Equal(cat.Streams[0].PrimaryKey, want[0].PrimaryKey) || cat.Streams[1].Replay != source.Superset {
		t.Fatalf("catalog %+v", cat)
	}
}

func TestFullPaginates(t *testing.T) {
	p := &pages{records: [][]string{{`{"id":1}`, `{"id":2}`}, {`{"id":3}`}, {`{"id":4}`}}}
	src := &sdk.Source[config]{Name: "acme", Streams: []sdk.Stream[config]{
		sdk.Full("a", func(c *sdk.Ctx[config], out *sdk.Out) error {
			return paginated[config](p, new([]string))(c, sdk.Cursor{}, out)
		}),
	}}
	expect(t, read(t, src, "a", ""),
		"a rows 1,2", `a resume {"page":"t1"}`,
		"a rows 3", `a resume {"page":"t2"}`,
		"a rows 4", "a complete ")

	p.tokens = nil
	expect(t, read(t, src, "a", `{"page":"t2"}`), "a rows 4", "a complete ")
	expect(t, p.tokens, "t2")
}

func TestIncrementalUnordered(t *testing.T) {
	p := &pages{records: [][]string{
		{`{"id":1,"u":"2026-01-05T00:00:00Z"}`, `{"id":2,"u":"2026-01-03T00:00:00Z"}`},
		{`{"id":3,"u":"2026-01-04T01:00:00+02:00"}`},
	}}
	var since []string
	src := &sdk.Source[config]{Name: "acme", Streams: []sdk.Stream[config]{
		sdk.Incremental("b", "u", paginated[config](p, &since)),
	}}
	// The floor stays put until the stream completes.
	expect(t, read(t, src, "b", ""),
		"b rows 1,2", `b resume {"since":null,"max":"2026-01-05T00:00:00Z","page":"t1"}`,
		"b rows 3", `b complete {"cursor":"2026-01-05T00:00:00Z"}`)

	// A resumed read keeps the highest cursor the first part emitted.
	since = nil
	expect(t, read(t, src, "b", `{"cursor":"2026-01-01T00:00:00Z","since":"2026-01-01T00:00:00Z","max":"2026-01-05T00:00:00Z","page":"t1"}`),
		"b rows 3", `b complete {"cursor":"2026-01-05T00:00:00Z"}`)
	expect(t, since, "2026-01-01T00:00:00Z")
}

func TestIncrementalOrdered(t *testing.T) {
	p := &pages{records: [][]string{{`{"id":1,"u":1}`, `{"id":2,"u":2}`}, {`{"id":3,"u":2}`, `{"id":4,"u":10}`}}}
	var since []string
	src := &sdk.Source[config]{Name: "acme", Streams: []sdk.Stream[config]{
		sdk.Incremental("b", "u", paginated[config](p, &since), sdk.Ordered()),
	}}
	expect(t, read(t, src, "b", `{"cursor":1}`),
		"b rows 1,2", `b resume {"cursor":2,"since":1,"page":"t1"}`,
		"b rows 3,4", `b complete {"cursor":10}`)
	// The page token belongs to the read from 1.
	since = nil
	read(t, src, "b", `{"cursor":2,"since":1,"page":"t1"}`)
	expect(t, since, "1")
}

// An ordered stream whose cursor goes backwards fails rather than move its
// floor past records it hasn't read. A tie doesn't.
func TestOrderedCursorGoingBackwards(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pages [][]string
		state string
		fails bool
	}{
		{name: "within a page", pages: [][]string{{`{"id":1,"u":5}`, `{"id":2,"u":3}`}}, fails: true},
		{name: "between pages", pages: [][]string{{`{"id":1,"u":5}`}, {`{"id":2,"u":3}`}}, fails: true},
		{name: "ties", pages: [][]string{{`{"id":1,"u":5}`, `{"id":2,"u":5}`}, {`{"id":3,"u":5}`}}},
		{name: "a token's records below the floor", pages: [][]string{{`{"id":1,"u":5}`}, {`{"id":2,"u":3}`}}, state: `{"cursor":5,"since":null,"page":"t1"}`, fails: true},
		{name: "a token's records at the floor", pages: [][]string{{`{"id":1,"u":5}`}, {`{"id":2,"u":5}`}}, state: `{"cursor":5,"since":null,"page":"t1"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &pages{records: tc.pages}
			src := &sdk.Source[config]{Name: "acme", Streams: []sdk.Stream[config]{
				sdk.Incremental("b", "u", paginated[config](p, new([]string)), sdk.Ordered()),
			}}
			s, err := src.New(nil)
			if err != nil {
				t.Fatal(err)
			}
			cs := source.ConfiguredStream{Name: "b"}
			if tc.state != "" {
				cs.State = []byte(tc.state)
			}
			err = s.Read(context.Background(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, &recorder{})
			var coded interface{ ErrorCode() protocolv1.ErrorCode }
			changed := errors.As(err, &coded) && coded.ErrorCode() == protocolv1.ErrorCode_ERROR_CODE_CONTRACT_CHANGED
			if changed != tc.fails || (!tc.fails && err != nil) {
				t.Errorf("Read: %v", err)
			}
		})
	}
}

// A rejected token restarts from the floor, without what the read it belonged
// to had seen.
func TestRestartForgetsTheHighestCursor(t *testing.T) {
	p := &pages{records: [][]string{{`{"id":1,"u":3}`}, {`{"id":2,"u":4}`}}, expired: map[string]bool{"t1": true}}
	src := &sdk.Source[config]{Name: "acme", Streams: []sdk.Stream[config]{
		sdk.Incremental("b", "u", paginated[config](p, new([]string)), sdk.Ordered()),
	}}
	read(t, src, "b", `{"cursor":9,"since":null,"page":"t1"}`)
}

func TestExpiredPageTokenRestartsFromTheFloor(t *testing.T) {
	p := &pages{records: [][]string{{`{"id":1,"u":3}`}, {`{"id":2,"u":4}`}}, expired: map[string]bool{"t1": true}}
	var since []string
	src := &sdk.Source[config]{Name: "acme", Streams: []sdk.Stream[config]{
		sdk.Incremental("b", "u", paginated[config](p, &since), sdk.Ordered()),
	}}
	expect(t, read(t, src, "b", `{"cursor":3,"since":null,"page":"t1"}`),
		"b rows 1", `b resume {"cursor":3,"since":3,"page":"t1"}`,
		"b rows 2", `b complete {"cursor":4}`)
	expect(t, since, "", "3")
	expect(t, p.tokens, "t1", "", "t1")
}

func TestExpiredPageTokenFailsOutsideAResume(t *testing.T) {
	p := &pages{records: [][]string{{`{"id":1}`}, {`{"id":2}`}}, expired: map[string]bool{"t1": true}}
	src := &sdk.Source[config]{Name: "acme", Streams: []sdk.Stream[config]{
		sdk.Full("a", func(c *sdk.Ctx[config], out *sdk.Out) error {
			return paginated[config](p, new([]string))(c, sdk.Cursor{}, out)
		}),
	}}
	s, err := src.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	err = s.Read(context.Background(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "a"}}}, &recorder{})
	if !errors.Is(err, sdk.ErrPageTokenExpired) {
		t.Fatalf("Read: %v", err)
	}
}

func TestResumable(t *testing.T) {
	type st struct {
		Offset int `json:"offset"`
	}
	var restored []string
	src := &sdk.Source[config]{Name: "acme", Streams: []sdk.Stream[config]{
		sdk.Resumable("c", func(_ *sdk.Ctx[config], state json.RawMessage, out *sdk.Out) error {
			restored = append(restored, string(state))
			var s st
			if state != nil {
				if err := json.Unmarshal(state, &s); err != nil {
					return err
				}
			}
			for i := s.Offset; i < 4; i += 2 {
				if err := out.Emit(map[string]int{"id": i}, map[string]int{"id": i + 1}); err != nil {
					return err
				}
				if err := out.Checkpoint(st{Offset: i + 2}); err != nil {
					return err
				}
			}
			return nil
		}),
	}}
	expect(t, read(t, src, "c", ""),
		"c rows 0,1", `c resume {"offset":2}`, "c rows 2,3", `c resume {"offset":4}`, `c complete {"offset":4}`)
	expect(t, read(t, src, "c", `{"offset":2}`), "c rows 2,3", `c resume {"offset":4}`, `c complete {"offset":4}`)
	expect(t, restored, "", `{"offset":2}`)
}

func TestMisuse(t *testing.T) {
	for name, stream := range map[string]sdk.Stream[config]{
		"checkpoint in a full stream": sdk.Full("s", func(_ *sdk.Ctx[config], out *sdk.Out) error {
			return out.Checkpoint(1)
		}),
		"paginate in a resumable stream": sdk.Resumable("s", func(_ *sdk.Ctx[config], _ json.RawMessage, out *sdk.Out) error {
			for _, err := range sdk.Paginate(out, (&pages{records: [][]string{{}}}).fetch) {
				return err
			}
			return nil
		}),
		"a record that isn't an object": sdk.Full("s", func(_ *sdk.Ctx[config], out *sdk.Out) error {
			return out.Emit([]int{1})
		}),
		"mixed cursor types": sdk.Incremental("s", "u", func(_ *sdk.Ctx[config], _ sdk.Cursor, out *sdk.Out) error {
			return out.EmitJSON(json.RawMessage(`{"u":1}`), json.RawMessage(`{"u":"2"}`))
		}),
	} {
		t.Run(name, func(t *testing.T) {
			s, err := (&sdk.Source[config]{Name: "acme", Streams: []sdk.Stream[config]{stream}}).New(nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Read(context.Background(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "s"}}}, &recorder{}); err == nil {
				t.Fatal("Read succeeded")
			}
		})
	}
	s, err := (&sdk.Source[config]{Name: "acme"}).New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Read(context.Background(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "nope"}}}, &recorder{}); err == nil {
		t.Fatal("read an unknown stream")
	}
}

func TestCursorOrder(t *testing.T) {
	for _, tc := range []struct {
		records string
		want    string
	}{
		{`{"u":9},{"u":10},{"u":2}`, `10`},
		{`{"u":1.5},{"u":12},{"u":1e1}`, `12`},
		{`{"u":"b"},{"u":"ab"},{"u":"c"}`, `"c"`},
		{`{"u":"2026-01-01T10:00:00+05:30"},{"u":"2026-01-01T05:00:00Z"}`, `"2026-01-01T05:00:00Z"`},
		{`{"u":null},{"id":1},{"u":3}`, `3`},
	} {
		var records []json.RawMessage
		if err := json.Unmarshal([]byte("["+tc.records+"]"), &records); err != nil {
			t.Fatal(err)
		}
		src := &sdk.Source[config]{Name: "acme", Streams: []sdk.Stream[config]{
			sdk.Incremental("s", "u", func(_ *sdk.Ctx[config], _ sdk.Cursor, out *sdk.Out) error {
				return out.EmitJSON(records...)
			}),
		}}
		got := read(t, src, "s", "")
		if want := fmt.Sprintf(`s complete {"cursor":%s}`, tc.want); got[len(got)-1] != want {
			t.Errorf("%s: %s, want %s", tc.records, got[len(got)-1], want)
		}
	}
}
