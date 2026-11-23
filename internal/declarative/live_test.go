package declarative

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// liveAPI serves records sorted by id, not by their cursor u, one a page by
// offset, to requests for u at or after since. It reads its records at each
// request, and after serving request n it applies change[n], as writes
// landing while a read is under way. A datetime API writes u as minutes past
// midnight.
type liveAPI struct {
	mu       sync.Mutex
	datetime bool
	u        map[int]int
	change   map[int]map[int]int
	served   int
}

func (a *liveAPI) value(u int) any {
	if a.datetime {
		return fmt.Sprintf("2026-01-01T00:%02d:00Z", u)
	}
	return u
}

func (a *liveAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	q := r.URL.Query()
	var ids []int
	since := q.Get("since")
	for id, u := range a.u {
		if since != "" {
			n, err := strconv.Atoi(since)
			if a.datetime && fmt.Sprint(a.value(u)) < since || !a.datetime && (err != nil || u < n) {
				continue
			}
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	off, _ := strconv.Atoi(q.Get("offset"))
	page := []map[string]any{}
	if off < len(ids) {
		id := ids[off]
		page = append(page, map[string]any{"id": id, "version": fmt.Sprintf("%d@%d", id, a.u[id]), "u": a.value(a.u[id])})
	}
	a.served++
	for k, u := range a.change[a.served] {
		a.u[k] = u
	}
	writeJSON(w, page)
}

const liveStream = `
streams:
  - name: items
    primary_key: [id]
    request:
      path: /items
      params: { since: "{has(state.cursor) ? state.cursor : null}" }
    paginator: { type: offset, size: 1 }
    incremental: { cursor_field: u, type: %s }
`

// A record updated after its page was read, with a later page carrying a
// newer cursor, is read again by a later sync: the floor doesn't pass it.
func TestIncrementalUnorderedReadsUpdatesMadeDuringARead(t *testing.T) {
	for _, tc := range []struct {
		name  string
		kind  string
		state string
		// Fail the first read after this many pages, and read the rest from
		// its last resume point, as a retry does. 0: read it whole.
		stopAfter int
	}{
		{name: "the first sync", kind: "integer"},
		{name: "a saved floor", kind: "integer", state: `{"cursor":1}`},
		{name: "a floor and the highest of the reads before", kind: "integer", state: `{"cursor":1,"high":2}`},
		{name: "a datetime cursor", kind: "datetime", state: `{"cursor":"2026-01-01T00:01:00Z"}`},
		{name: "a resumed read", kind: "integer", state: `{"cursor":1}`, stopAfter: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Record 1 changes to 4 after its page is read, and record 3,
			// on a later page, to 5.
			api := &liveAPI{datetime: tc.kind == "datetime", u: map[int]int{1: 1, 2: 2, 3: 3}, change: map[int]map[int]int{1: {1: 4, 3: 5}}}
			srv := httptest.NewServer(api)
			defer srv.Close()
			s := bind(t, testManifest("", fmt.Sprintf(liveStream, tc.kind)), srv, nil)
			state := []byte(tc.state)
			var got []string
			for i := range 3 {
				r, err := read(t, s, source.ConfiguredStream{Name: "items", State: state})
				if err != nil {
					t.Fatal(err)
				}
				for _, m := range r.rows["items"] {
					got = append(got, fmt.Sprint(m["version"]))
				}
				state = r.next["items"]
				if i == 0 && tc.stopAfter > 0 {
					got = got[:tc.stopAfter]
					state = []byte(r.resume["items"][tc.stopAfter-1])
				}
			}
			if !slices.Contains(got, "1@4") {
				t.Errorf("record 1's update was never read: read %v", got)
			}
		})
	}
}

// The same holds for a child stream, read once per parent, whose resume
// points carry the highest cursor of the reads before.
func TestIncrementalChildReadsUpdatesMadeDuringARead(t *testing.T) {
	var mu sync.Mutex
	// Each repo has one issue; a's changes to 4 after a's issues are read,
	// and b's to 5.
	u := map[string]int{"a": 1, "b": 2}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/repos" {
			writeJSON(w, []any{map[string]any{"name": "a"}, map[string]any{"name": "b"}})
			return
		}
		repo := strings.Split(r.URL.Path, "/")[2]
		page := []any{}
		if n, err := strconv.Atoi(r.URL.Query().Get("since")); err != nil || u[repo] >= n {
			page = append(page, map[string]any{"id": repo, "version": fmt.Sprintf("%s@%d", repo, u[repo]), "u": u[repo]})
		}
		if repo == "a" && u["a"] == 1 {
			u["a"], u["b"] = 4, 5
		}
		writeJSON(w, page)
	}))
	defer srv.Close()
	s := bind(t, testManifest("", `
streams:
  - name: repos
    primary_key: [name]
    request: { path: /repos }
  - name: issues
    primary_key: [id]
    parent: { stream: repos }
    request:
      path: "/repos/{parent.name}/issues"
      params: { since: "{has(state.cursor) ? state.cursor : null}" }
    incremental: { cursor_field: u, type: integer }
`), srv, nil)
	state := []byte(`{"cursor":1,"high":1}`)
	var got []string
	for i := range 3 {
		r, err := read(t, s, source.ConfiguredStream{Name: "issues", State: state})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range r.rows["issues"] {
			got = append(got, fmt.Sprint(m["version"]))
		}
		if i == 0 {
			if want := `{"cursor":1,"max":1,"high":1,"parent":"","done":[["a"]]}`; r.resume["issues"][0] != want {
				t.Errorf("resume point %s, want %s", r.resume["issues"][0], want)
			}
		}
		state = r.next["issues"]
	}
	if !slices.Contains(got, "a@4") {
		t.Errorf("a's update was never read: read %v", got)
	}
}
