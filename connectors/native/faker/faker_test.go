package faker

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/unsuitable001/openetl/sdk/go/source"
)

type recorder struct {
	rows     map[string][]string
	resumes  map[string][][]byte
	complete []string
}

func newRecorder() *recorder {
	return &recorder{rows: map[string][]string{}, resumes: map[string][][]byte{}}
}

func (r *recorder) Rows(stream string, rows []json.RawMessage) error {
	for _, row := range rows {
		r.rows[stream] = append(r.rows[stream], string(row))
	}
	return nil
}

func (r *recorder) Records(string, arrow.RecordBatch) error { panic("faker emits rows") }

func (r *recorder) ResumePoint(stream string, state []byte) error {
	r.resumes[stream] = append(r.resumes[stream], state)
	return nil
}

func (r *recorder) Complete(stream string, next []byte) error {
	if next != nil {
		return fmt.Errorf("%s: a full refresh completed with next state %s", stream, next)
	}
	r.complete = append(r.complete, stream)
	return nil
}

func read(t *testing.T, s *Source, states map[string][]byte) *recorder {
	t.Helper()
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var req source.ReadRequest
	for _, st := range cat.Streams {
		req.Streams = append(req.Streams, source.ConfiguredStream{Name: st.Name, State: states[st.Name]})
	}
	rec := newRecorder()
	if err := s.Read(t.Context(), req, rec); err != nil {
		t.Fatal(err)
	}
	return rec
}

func TestReadIsDeterministic(t *testing.T) {
	s, err := New([]byte(`{"seed": 7, "count": 25, "batchSize": 10}`))
	if err != nil {
		t.Fatal(err)
	}
	a, b := read(t, s, nil), read(t, s, nil)
	for _, name := range []string{"users", "purchases"} {
		if len(a.rows[name]) != 25 {
			t.Errorf("%s: %d rows, want 25", name, len(a.rows[name]))
		}
		if !slices.Equal(a.rows[name], b.rows[name]) {
			t.Errorf("%s: two reads differ", name)
		}
		if len(a.resumes[name]) != 3 {
			t.Errorf("%s: %d resume points, want 3", name, len(a.resumes[name]))
		}
	}
	if !slices.Equal(a.complete, []string{"users", "purchases"}) {
		t.Errorf("complete = %v", a.complete)
	}

	other, err := New([]byte(`{"seed": 8, "count": 25, "batchSize": 10}`))
	if err != nil {
		t.Fatal(err)
	}
	if slices.Equal(read(t, other, nil).rows["users"], a.rows["users"]) {
		t.Error("a different seed produced the same rows")
	}
}

func TestResumeYieldsTheRest(t *testing.T) {
	s, err := New([]byte(`{"seed": 1, "count": 25, "batchSize": 10}`))
	if err != nil {
		t.Fatal(err)
	}
	full := read(t, s, nil)
	resumed := read(t, s, map[string][]byte{"users": full.resumes["users"][0]})
	if !slices.Equal(resumed.rows["users"], full.rows["users"][10:]) {
		t.Error("resuming users after the first batch didn't yield exactly the remaining rows")
	}
	if !slices.Equal(resumed.rows["purchases"], full.rows["purchases"]) {
		t.Error("purchases without state didn't start from the beginning")
	}
	done := read(t, s, map[string][]byte{"users": full.resumes["users"][2]})
	if len(done.rows["users"]) != 0 || !slices.Contains(done.complete, "users") {
		t.Errorf("resuming at the end: %d rows, complete %v", len(done.rows["users"]), done.complete)
	}
}

func TestOverlapRepeatsRowsBeforeTheResumePoint(t *testing.T) {
	s, err := New([]byte(`{"seed": 1, "count": 25, "batchSize": 10, "overlap": 4}`))
	if err != nil {
		t.Fatal(err)
	}
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if cat.Streams[0].Replay != source.Superset {
		t.Errorf("replay = %v, want superset", cat.Streams[0].Replay)
	}
	full := read(t, s, nil)
	if len(full.rows["users"]) != 25 {
		t.Errorf("a fresh read has %d rows, want 25", len(full.rows["users"]))
	}
	resumed := read(t, s, map[string][]byte{"users": full.resumes["users"][1]})
	if !slices.Equal(resumed.rows["users"], full.rows["users"][16:]) {
		t.Errorf("resuming after row 20 yielded %d rows, want rows 17 to 25", len(resumed.rows["users"]))
	}
}

func TestKeylessRowsHeldTwice(t *testing.T) {
	s, err := New([]byte(`{"count": 25, "keyless": true, "twice": 10}`))
	if err != nil {
		t.Fatal(err)
	}
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, cs := range cat.Streams {
		if len(cs.PrimaryKey) != 0 || cs.CursorField != "id" {
			t.Errorf("%s: primary key %v, cursor %q", cs.Name, cs.PrimaryKey, cs.CursorField)
		}
	}
	rows := read(t, s, nil).rows["users"]
	for i := 1; i < len(rows); i++ {
		if same, want := rows[i] == rows[i-1], i%10 == 0; same != want {
			t.Errorf("row %d is a copy of row %d: %v, want %v", i, i-1, same, want)
		}
	}
}

func TestConfigErrors(t *testing.T) {
	for _, cfg := range []string{`{"count": -1}`, `{"batchSize": 0}`, `{"delay": "soon"}`, `{"rows": 5}`, `{"overlap": -1}`, `{"twice": 3}`} {
		if _, err := New([]byte(cfg)); err == nil {
			t.Errorf("New(%s) succeeded", cfg)
		}
	}
	if _, err := New(nil); err != nil {
		t.Errorf("New(nil): %v", err)
	}
}
