package declarative

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/internal/connectorhost/hostservices"
	"github.com/openetl-io/openetl/internal/netguard"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// recorder records what a source emits, in order.
type recorder struct {
	mu     sync.Mutex
	events []string
	rows   map[string][]map[string]any
	raw    map[string][]string
	resume map[string][]string
	next   map[string][]byte
}

func newRecorder() *recorder {
	return &recorder{rows: map[string][]map[string]any{}, raw: map[string][]string{}, resume: map[string][]string{}, next: map[string][]byte{}}
}

func (r *recorder) Rows(stream string, rows []json.RawMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, row := range rows {
		var m map[string]any
		if err := json.Unmarshal(row, &m); err != nil {
			return err
		}
		r.rows[stream] = append(r.rows[stream], m)
		r.raw[stream] = append(r.raw[stream], string(row))
		r.events = append(r.events, "row "+stream)
	}
	return nil
}

func (r *recorder) Records(string, arrow.RecordBatch) error { panic("declarative sources send rows") }

func (r *recorder) ResumePoint(stream string, state []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resume[stream] = append(r.resume[stream], string(state))
	r.events = append(r.events, "resume "+stream)
	return nil
}

func (r *recorder) Split(stream, id string) source.SplitEmitter {
	return source.WholeSplit(r, stream, id)
}

func (r *recorder) Complete(stream string, next []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.next[stream] = next
	r.events = append(r.events, "complete "+stream)
	return nil
}

// ids is a stream's records' "id"s.
func (r *recorder) ids(stream string) []string {
	var out []string
	for _, m := range r.rows[stream] {
		out = append(out, fmt.Sprint(m["id"]))
	}
	return out
}

var loopback = netguard.Policy{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}}

// bind loads manifest and binds it, with the server's URL as base_url, to a
// source whose host is real host services.
func bind(t *testing.T, manifest string, srv *httptest.Server, config map[string]any) source.Source {
	t.Helper()
	c, err := Load([]byte(manifest))
	if err != nil {
		t.Fatal(err)
	}
	cfg := map[string]any{"base_url": srv.URL}
	for k, v := range config {
		cfg[k] = v
	}
	raw, _ := json.Marshal(cfg)
	b, err := c.Bind(raw)
	if err != nil {
		t.Fatal(err)
	}
	h := hostservices.New(hostservices.Options{Policy: loopback, Secrets: hostservices.StaticSecrets(b.Secrets), RateLimits: b.RateLimits})
	t.Cleanup(h.Close)
	return b.Source(h)
}

func read(t *testing.T, s source.Source, streams ...source.ConfiguredStream) (*recorder, error) {
	t.Helper()
	r := newRecorder()
	return r, s.Read(t.Context(), source.ReadRequest{Streams: streams}, r)
}

// testManifest builds a manifest whose config has base_url, plus the given
// properties, and whose spec has the given YAML after http's base_url.
func testManifest(props, rest string) string {
	return `
apiVersion: openetl.io/v1alpha1
kind: DeclarativeSource
metadata: { name: acme, version: 0.1.0 }
spec:
  config:
    type: object
    properties:
      base_url: { type: string }
` + indent(props, "      ") + `
  http:
    base_url: "{config.base_url}"
` + indent(rest, "  ")
}

func indent(s, prefix string) string {
	lines := strings.Split(strings.Trim(s, "\n"), "\n")
	for i, l := range lines {
		if l != "" {
			lines[i] = prefix + l
		}
	}
	return strings.Join(lines, "\n")
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// items serves the records with IDs from to to, inclusive.
func items(from, to int) []map[string]any {
	var out []map[string]any
	for i := from; i <= to; i++ {
		out = append(out, map[string]any{"id": i})
	}
	return out
}

func TestPaginators(t *testing.T) {
	// Seven records, three a page.
	page := func(n int) []map[string]any {
		return items(3*n+1, min(3*n+3, 7))
	}
	cases := []struct {
		name, paginator, records string
		handler                  func(srv **httptest.Server) http.HandlerFunc
	}{{
		name:      "cursor",
		paginator: `{ type: cursor, next: "response.body.has_more ? records[size(records)-1].id : null", param: starting_after }`,
		records:   "response.body.data",
		handler: func(**httptest.Server) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				n := 0
				if a := r.URL.Query().Get("starting_after"); a != "" {
					id, _ := strconv.Atoi(a)
					n = id / 3
				}
				writeJSON(w, map[string]any{"data": page(n), "has_more": n < 2})
			}
		},
	}, {
		name:      "next_url",
		paginator: `{ type: next_url, next: "response.body.next" }`,
		records:   "response.body.results",
		handler: func(**httptest.Server) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				n, _ := strconv.Atoi(r.URL.Query().Get("p"))
				var next any
				if n < 2 {
					next = fmt.Sprintf("/items?p=%d&limit=3", n+1)
				}
				writeJSON(w, map[string]any{"results": page(n), "next": next})
			}
		},
	}, {
		name:      "link",
		paginator: `{ type: link }`,
		handler: func(srv **httptest.Server) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				n, _ := strconv.Atoi(r.URL.Query().Get("page"))
				if n < 2 {
					w.Header().Set("Link", fmt.Sprintf(`<%s/items?page=%d>; rel="next", <%s/items?page=2>; rel="last"`, (*srv).URL, n+1, (*srv).URL))
				}
				writeJSON(w, page(n))
			}
		},
	}, {
		name:      "offset",
		paginator: `{ type: offset, size: 3, size_param: limit }`,
		handler: func(**httptest.Server) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				off, _ := strconv.Atoi(r.URL.Query().Get("offset"))
				if r.URL.Query().Get("limit") != "3" {
					http.Error(w, "no limit", http.StatusBadRequest)
					return
				}
				writeJSON(w, page(off/3))
			}
		},
	}, {
		name:      "page",
		paginator: `{ type: page, param: p, start: 0, size: 3 }`,
		handler: func(**httptest.Server) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				n, _ := strconv.Atoi(r.URL.Query().Get("p"))
				if n > 2 {
					writeJSON(w, []any{})
					return
				}
				writeJSON(w, page(n))
			}
		},
	}, {
		name:      "stop",
		paginator: `{ type: page, stop: "size(records) < 3" }`,
		handler: func(**httptest.Server) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				n, _ := strconv.Atoi(r.URL.Query().Get("page"))
				if n > 3 {
					http.Error(w, "past the end", http.StatusBadRequest)
					return
				}
				writeJSON(w, page(n-1))
			}
		},
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var srv *httptest.Server
			srv = httptest.NewServer(tc.handler(&srv))
			defer srv.Close()
			rest := "streams:\n  - name: items\n    request: { path: /items }\n    paginator: " + tc.paginator
			if tc.records != "" {
				rest += "\n    records: " + tc.records
			}
			r, err := read(t, bind(t, testManifest("", rest), srv, nil), source.ConfiguredStream{Name: "items"})
			if err != nil {
				t.Fatal(err)
			}
			if got, want := r.ids("items"), []string{"1", "2", "3", "4", "5", "6", "7"}; !slices.Equal(got, want) {
				t.Errorf("ids %v, want %v", got, want)
			}
			// A resume point after each page but the last, then completion.
			want := slices.Concat(slices.Repeat([]string{"row items"}, 3), []string{"resume items"}, slices.Repeat([]string{"row items"}, 3), []string{"resume items", "row items", "complete items"})
			if !slices.Equal(r.events, want) {
				t.Errorf("events %v, want %v", r.events, want)
			}
		})
	}
}

func TestResumeFromAPageToken(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := r.URL.Query().Get("cursor")
		got = append(got, c)
		next := map[string]any{"": "b", "b": "c", "c": nil}[c]
		writeJSON(w, map[string]any{"data": []any{map[string]any{"id": c}}, "next": next})
	}))
	defer srv.Close()
	s := bind(t, testManifest("", `
streams:
  - name: items
    request: { path: /items }
    records: response.body.data
    paginator: { type: cursor, next: response.body.next, param: cursor }
`), srv, nil)
	r, err := read(t, s, source.ConfiguredStream{Name: "items", State: []byte(`{"page":"b"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"b", "c"}) {
		t.Errorf("requested cursors %q, want b then c", got)
	}
	if want := []string{`{"page":"c"}`}; !slices.Equal(r.resume["items"], want) {
		t.Errorf("resume points %v, want %v", r.resume["items"], want)
	}
}

// cursorAPI serves records updated at the given minutes past midnight,
// sorted, to requests whose since is at or before them, two a page.
func cursorAPI(minutes []int, sinces *[]string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		since := r.URL.Query().Get("since")
		*sinces = append(*sinces, since)
		var recs []map[string]any
		for i, m := range minutes {
			ts := fmt.Sprintf("2026-01-01T00:%02d:00Z", m)
			if since == "" || ts >= since {
				recs = append(recs, map[string]any{"id": i, "updated_at": ts})
			}
		}
		start, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		end := min(start+2, len(recs))
		writeJSON(w, recs[min(start, end):end])
	}))
}

const incrementalStream = `
streams:
  - name: items
    primary_key: [id]
    request:
      path: /items
      params: { since: "{has(state.cursor) ? state.cursor : null}" }
    paginator: { type: offset, size: 2 }
    incremental: { cursor_field: updated_at, type: datetime, lookback: 5m%s }
`

func TestIncrementalOrdered(t *testing.T) {
	var sinces []string
	srv := cursorAPI([]int{10, 20, 30, 40, 50}, &sinces)
	defer srv.Close()
	s := bind(t, testManifest("", fmt.Sprintf(incrementalStream, ", ordered: true")), srv, nil)
	r, err := read(t, s, source.ConfiguredStream{Name: "items"})
	if err != nil {
		t.Fatal(err)
	}
	// The floor advances with each page, and the token stays with the floor
	// its requests used.
	want := []string{
		`{"cursor":"2026-01-01T00:20:00Z","since":null,"page":"2"}`,
		`{"cursor":"2026-01-01T00:40:00Z","since":null,"page":"4"}`,
	}
	if !slices.Equal(r.resume["items"], want) {
		t.Errorf("resume points\n%s\nwant\n%s", strings.Join(r.resume["items"], "\n"), strings.Join(want, "\n"))
	}
	if got := string(r.next["items"]); got != `{"cursor":"2026-01-01T00:50:00Z"}` {
		t.Errorf("next %s", got)
	}

	// The next sync starts at the floor less the lookback.
	sinces = nil
	if _, err := read(t, s, source.ConfiguredStream{Name: "items", State: r.next["items"]}); err != nil {
		t.Fatal(err)
	}
	if sinces[0] != "2026-01-01T00:45:00Z" {
		t.Errorf("since %q, want 00:45 (00:50 less the lookback)", sinces[0])
	}

	// A token that expires restarts an ordered read from the highest cursor
	// it had seen.
	sinces = nil
	expired := testManifest("", fmt.Sprintf(incrementalStream, ", ordered: true")+`
    errors:
      - { when: "response.status == 400", action: expired }
`)
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("offset") == "4" {
			http.Error(w, "stale offset", http.StatusBadRequest)
			return
		}
		srv.Config.Handler.ServeHTTP(w, r)
	}))
	defer srv2.Close()
	r, err = read(t, bind(t, expired, srv2, nil), source.ConfiguredStream{Name: "items", State: []byte(want[1])})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sinces, []string{"2026-01-01T00:35:00Z", "2026-01-01T00:35:00Z"}) {
		t.Errorf("sinces %q, want a restart from 00:40 less the lookback", sinces)
	}
	if got := r.ids("items"); !slices.Equal(got, []string{"3", "4"}) {
		t.Errorf("ids %v, want the records from 00:40", got)
	}
}

func TestIncrementalUnordered(t *testing.T) {
	var sinces []string
	srv := cursorAPI([]int{30, 10, 50, 20, 40}, &sinces)
	defer srv.Close()
	s := bind(t, testManifest("", fmt.Sprintf(incrementalStream, "")), srv, nil)
	r, err := read(t, s, source.ConfiguredStream{Name: "items", State: []byte(`{"cursor":"2026-01-01T00:05:00Z"}`)})
	if err != nil {
		t.Fatal(err)
	}
	// The floor stays put until the stream completes; the highest cursor
	// seen travels with the resume points.
	want := []string{
		`{"cursor":"2026-01-01T00:05:00Z","since":"2026-01-01T00:05:00Z","max":"2026-01-01T00:30:00Z","page":"2"}`,
		`{"cursor":"2026-01-01T00:05:00Z","since":"2026-01-01T00:05:00Z","max":"2026-01-01T00:50:00Z","page":"4"}`,
	}
	if !slices.Equal(r.resume["items"], want) {
		t.Errorf("resume points\n%s\nwant\n%s", strings.Join(r.resume["items"], "\n"), strings.Join(want, "\n"))
	}
	if got := string(r.next["items"]); got != `{"cursor":"2026-01-01T00:50:00Z"}` {
		t.Errorf("next %s", got)
	}
	if sinces[0] != "2026-01-01T00:00:00Z" {
		t.Errorf("since %q, want the floor less the lookback", sinces[0])
	}
}

func TestChildStreams(t *testing.T) {
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.URL.RequestURI())
		q := r.URL.Query()
		if r.URL.Path == "/repos" {
			// Two pages of repos: a and b, then c.
			if q.Get("page") == "2" {
				writeJSON(w, []any{map[string]any{"name": "c"}})
				return
			}
			w.Header().Set("Link", `</repos?page=2>; rel="next"`)
			writeJSON(w, []any{map[string]any{"name": "a"}, map[string]any{"name": "b"}})
			return
		}
		repo := strings.Split(r.URL.Path, "/")[2]
		if repo == "b" {
			http.NotFound(w, r)
			return
		}
		// Two pages of issues per repo.
		n, _ := strconv.Atoi(q.Get("page"))
		writeJSON(w, []any{map[string]any{"id": fmt.Sprintf("%s-%d", repo, n)}})
	}))
	defer srv.Close()
	m := testManifest("", `
streams:
  - name: repos
    primary_key: [name]
    request: { path: /repos }
    paginator: { type: link }
  - name: issues
    parent: { stream: repos }
    request: { path: "/repos/{parent.name}/issues" }
    paginator: { type: page, stop: "has(page.token) && page.token == '2'" }
    errors:
      - { when: "response.status == 404", action: ignore }
`)
	s := bind(t, m, srv, nil)
	r, err := read(t, s, source.ConfiguredStream{Name: "issues"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := r.ids("issues"), []string{"a-1", "a-2", "c-1", "c-2"}; !slices.Equal(got, want) {
		t.Errorf("ids %v, want %v", got, want)
	}
	want := []string{
		`{"page":"2","parent":"","current":["a"]}`,
		`{"parent":"","done":[["a"]]}`,
		`{"parent":"","done":[["a"],["b"]]}`,
		`{"parent":"` + "/repos?page=2" + `"}`,
		`{"page":"2","parent":"/repos?page=2","current":["c"]}`,
		`{"parent":"/repos?page=2","done":[["c"]]}`,
	}
	if !slices.Equal(r.resume["issues"], want) {
		t.Errorf("resume points\n%s\nwant\n%s", strings.Join(r.resume["issues"], "\n"), strings.Join(want, "\n"))
	}

	// Resumed partway through repo a's issues on the first page: a carries
	// on from its token, and b, being done, isn't read again.
	calls = nil
	r, err = read(t, s, source.ConfiguredStream{Name: "issues", State: []byte(`{"page":"2","parent":"","done":[["b"]],"current":["a"]}`)})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := r.ids("issues"), []string{"a-2", "c-1", "c-2"}; !slices.Equal(got, want) {
		t.Errorf("ids %v, want %v", got, want)
	}
	if want := []string{"/repos", "/repos/a/issues?page=2", "/repos?page=2", "/repos/c/issues?page=1", "/repos/c/issues?page=2"}; !slices.Equal(calls, want) {
		t.Errorf("calls %v, want %v", calls, want)
	}
}

func TestErrorRules(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/gone":
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": map[string]any{"code": "resource_missing"}})
		case "/ok-but-not":
			writeJSON(w, map[string]any{"error": "maintenance"})
		case "/html":
			_, _ = w.Write([]byte("<html>down for maintenance</html>"))
		case "/forbidden":
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	defer srv.Close()
	m := testManifest("", `
  errors:
    - { when: "response.body == '<html>down for maintenance</html>'", action: fail, code: TRANSIENT }
streams:
  - name: gone
    request: { path: /gone }
    errors:
      - when: "response.body.error.code == 'resource_missing'"
        action: fail
        code: CONFIG_INVALID
        message: "the account has no {response.body.error.code}"
  - name: maintenance
    request: { path: /ok-but-not }
    errors:
      # Fails to evaluate on a list, so it doesn't match.
      - { when: "response.body[0] == 1", action: ignore }
      - { when: "has(response.body.error)", action: fail }
  - name: html
    request: { path: /html }
  - name: forbidden
    request: { path: /forbidden }
    errors:
      - { when: "response.status == 403", action: ignore }
`)
	s := bind(t, m, srv, nil)
	cases := []struct {
		stream string
		code   errs.Code
		msg    string
	}{
		{"gone", errs.ConfigInvalid, "the account has no resource_missing"},
		{"maintenance", errs.ContractChanged, "matched an error rule"},
		{"html", errs.Transient, "matched an error rule"},
	}
	for _, tc := range cases {
		_, err := read(t, s, source.ConfiguredStream{Name: tc.stream})
		if errs.CodeOf(err) != tc.code || err == nil || !strings.Contains(err.Error(), tc.msg) {
			t.Errorf("%s: %v, want %s with %q", tc.stream, err, tc.code, tc.msg)
		}
	}
	r, err := read(t, s, source.ConfiguredStream{Name: "forbidden"})
	if err != nil || len(r.rows["forbidden"]) != 0 || r.next["forbidden"] != nil || r.events[len(r.events)-1] != "complete forbidden" {
		t.Errorf("an ignored 403: %v, %v", err, r.events)
	}
}

func TestAuthAndCheck(t *testing.T) {
	const token = "sk_live_secret"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		writeJSON(w, []any{})
	}))
	defer srv.Close()
	m := testManifest(`api_key: { type: string, x-secret: true, pattern: "^sk_" }`, `
  auth: { type: bearer, token: { secret: api_key } }
streams:
  - name: items
    request: { path: /items }
`)
	if err := bind(t, m, srv, map[string]any{"api_key": token}).(source.Checker).Check(t.Context()); err != nil {
		t.Errorf("check with the right token: %v", err)
	}
	err := bind(t, m, srv, map[string]any{"api_key": "sk_wrong"}).(source.Checker).Check(t.Context())
	if errs.CodeOf(err) != errs.AuthInvalid {
		t.Errorf("check with a wrong token: %v, want AUTH_INVALID", err)
	}

	// A config that fails validation doesn't show its secrets.
	c, err := Load([]byte(m))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Bind([]byte(`{"base_url": "https://api.example", "api_key": "pk_live_not_a_secret_key"}`))
	if errs.CodeOf(err) != errs.ConfigInvalid || strings.Contains(err.Error(), "pk_live") {
		t.Errorf("bind with a bad key: %v", err)
	}
	if _, err := c.Bind([]byte(`{"base_url": "https://api.example", "api_kye": "x"}`)); err == nil || !strings.Contains(err.Error(), `no property "api_kye"`) {
		t.Errorf("bind with an unknown property: %v", err)
	}
}

// Expressions can't reach secrets at run time either: config holds only
// the other properties.
func TestConfigHasNoSecrets(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"path": r.URL.EscapedPath()})
	}))
	defer srv.Close()
	m := testManifest(`
api_key: { type: string, x-secret: true }
org: { type: string }`, `
streams:
  - name: items
    request: { path: "/orgs/{config.org}/items" }
    records: "[{'keys': config.map(k, k), 'path': response.body.path}]"
`)
	r, err := read(t, bind(t, m, srv, map[string]any{"api_key": "sk_live_secret", "org": "a b/c?d"}), source.ConfiguredStream{Name: "items"})
	if err != nil {
		t.Fatal(err)
	}
	rec := r.rows["items"][0]
	var keys []string
	for _, k := range rec["keys"].([]any) {
		keys = append(keys, k.(string))
	}
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"base_url", "org"}) {
		t.Errorf("config has %v, want base_url and org only", keys)
	}
	// Holes in a path are escaped as segments.
	if rec["path"] != "/orgs/a%20b%2Fc%3Fd/items" {
		t.Errorf("path %v", rec["path"])
	}
}

func TestHMACSigning(t *testing.T) {
	key := "signing-key"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mac := hmac.New(sha256.New, []byte(key))
		mac.Write([]byte(r.Method + " " + r.URL.RequestURI()))
		if r.Header.Get("X-Signature") != "v1="+hex.EncodeToString(mac.Sum(nil)) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		writeJSON(w, []any{map[string]any{"id": 1}})
	}))
	defer srv.Close()
	m := testManifest(`secret: { type: string, x-secret: true }`, `
  sign:
    type: hmac
    key: { secret: secret }
    hash: sha256
    message: "request.method + ' ' + request.path + '?' + request.query"
    header: X-Signature
    prefix: v1=
streams:
  - name: items
    request: { path: /items, params: { limit: "10" } }
`)
	r, err := read(t, bind(t, m, srv, map[string]any{"secret": key}), source.ConfiguredStream{Name: "items"})
	if err != nil || len(r.rows["items"]) != 1 {
		t.Errorf("a signed read: %v, %v", err, r.rows)
	}
}

func TestRecordsKeepTheirValues(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":123456789012345678901234567890,"amount":1.10,"note":"<a & b>"}]}`))
	}))
	defer srv.Close()
	r, err := read(t, bind(t, testManifest("", `
streams:
  - name: items
    request: { path: /items }
    records: response.body.data
`), srv, nil), source.ConfiguredStream{Name: "items"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := r.raw["items"][0], `{"amount":1.10,"id":123456789012345678901234567890,"note":"<a & b>"}`; got != want {
		t.Errorf("record %s, want %s", got, want)
	}
}

func TestNextURLStaysOnTheBaseURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"data": []any{}, "next": "https://evil.example/items?page=2"})
	}))
	defer srv.Close()
	_, err := read(t, bind(t, testManifest("", `
streams:
  - name: items
    request: { path: /items }
    records: response.body.data
    paginator: { type: next_url, next: response.body.next }
`), srv, nil), source.ConfiguredStream{Name: "items"})
	if errs.CodeOf(err) != errs.ContractChanged || !strings.Contains(err.Error(), "leaves") {
		t.Errorf("a next URL elsewhere: %v", err)
	}
}

// An API that hands back the token it was given would loop forever.
func TestRepeatedPageTokens(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls++; calls > 3 {
			http.Error(w, "still looping", http.StatusBadRequest)
			return
		}
		writeJSON(w, map[string]any{"data": []any{map[string]any{"id": 1}}, "next": "same"})
	}))
	defer srv.Close()
	_, err := read(t, bind(t, testManifest("", `
streams:
  - name: items
    request: { path: /items }
    records: response.body.data
    paginator: { type: cursor, next: response.body.next, param: cursor }
`), srv, nil), source.ConfiguredStream{Name: "items"})
	if errs.CodeOf(err) != errs.ContractChanged || !strings.Contains(err.Error(), "page token it was given") {
		t.Errorf("a repeated token: %v", err)
	}
}

func TestDiscover(t *testing.T) {
	c, err := Load([]byte(testManifest("", `
streams:
  - name: charges
    primary_key: [id]
    request: { path: /charges }
    incremental: { cursor_field: created, type: unix }
    schema:
      properties:
        id: { type: string }
        amount: { type: integer }
        created_at: { type: [string, "null"], format: date-time }
        metadata: { type: object }
`)))
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Bind([]byte(`{"base_url": "https://api.example"}`))
	if err != nil {
		t.Fatal(err)
	}
	cat, err := b.Source(nil).Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cs := cat.Streams[0]
	if cs.Name != "charges" || cs.CursorField != "created" || cs.Replay != source.Superset || !slices.Equal(cs.PrimaryKey, []string{"id"}) {
		t.Errorf("stream %+v", cs)
	}
	want := "amount: int64, created_at: timestamp[us, tz=UTC], id: utf8, metadata: extension<arrow.json[storage_type=utf8]>"
	var got []string
	for _, f := range cs.Schema.Fields() {
		got = append(got, f.Name+": "+f.Type.String())
	}
	if strings.Join(got, ", ") != want {
		t.Errorf("schema %s, want %s", strings.Join(got, ", "), want)
	}
	if len(b.RateLimits) != 0 {
		t.Errorf("rate limits %v, want none", b.RateLimits)
	}
}

func TestLoadErrors(t *testing.T) {
	secret := `api_key: { type: string, x-secret: true }`
	cases := []struct {
		name, props, rest, want string
	}{
		{"no streams", "", "streams: []", "minItems"},
		{"unknown field", "", "streams:\n  - { name: a, request: { path: /a }, recrods: x }", "recrods"},
		{"bearer without a token", "", "  auth: { type: bearer }\nstreams:\n  - { name: a, request: { path: /a } }", "token"},
		{"api key in two places", secret, "  auth: { type: api_key, key: { secret: api_key }, header: X, query: y }\nstreams:\n  - { name: a, request: { path: /a } }", "oneOf"},
		{"cursor without next", "", "streams:\n  - { name: a, request: { path: /a }, paginator: { type: cursor } }", "next"},
		{"a field of another paginator", "", "streams:\n  - { name: a, request: { path: /a }, paginator: { type: link, param: x } }", "param"},
		{"bad duration", "", "streams:\n  - { name: a, request: { path: /a }, incremental: { cursor_field: t, type: datetime, lookback: 5 minutes } }", "lookback"},
		{"syntax", "", "streams:\n  - { name: a, request: { path: /a }, records: 'response.body.' }", "records"},
		{"records not a list", "", "streams:\n  - { name: a, request: { path: /a }, records: '1' }", "not a list"},
		{"when not a bool", "", "streams:\n  - { name: a, request: { path: /a }, errors: [{ when: \"'x'\", action: ignore }] }", "not a bool"},
		{"a secret in a param", secret, "streams:\n  - { name: a, request: { path: /a, params: { k: '{config.api_key}' } } }", "config.api_key is a secret"},
		{"a secret by index", secret, "streams:\n  - { name: a, request: { path: /a }, records: \"[config['api_key']]\" }", "config.api_key is a secret"},
		{"an unknown property", "", "streams:\n  - { name: a, request: { path: '/{config.org}' } }", `no property "org"`},
		{"a misspelt field", "", "streams:\n  - { name: a, request: { path: /a, params: { s: '{state.curser}' } } }", `state has no field "curser"`},
		{"parent in a stream without one", "", "streams:\n  - { name: a, request: { path: '/{parent.id}' } }", "undeclared reference to 'parent'"},
		{"auth from a plain property", "", "  auth: { type: bearer, token: { secret: base_url } }\nstreams:\n  - { name: a, request: { path: /a } }", "isn't marked x-secret"},
		{"a missing parent", "", "streams:\n  - { name: a, parent: { stream: b }, request: { path: /a } }", `no parent stream "b"`},
		{"a parent without a key", "", "streams:\n  - { name: p, request: { path: /p } }\n  - { name: a, parent: { stream: p }, request: { path: /a } }", "needs a primary key"},
		{"a grandchild", "", "streams:\n  - { name: p, primary_key: [id], request: { path: /p } }\n  - { name: c, primary_key: [id], parent: { stream: p }, request: { path: /c } }\n  - { name: g, parent: { stream: c }, request: { path: /g } }", "has a parent of its own"},
		{"an ordered child", "", "streams:\n  - { name: p, primary_key: [id], request: { path: /p } }\n  - { name: c, parent: { stream: p }, request: { path: /c }, incremental: { cursor_field: t, type: datetime, ordered: true } }", "can't be ordered"},
		{"a lone brace", "", "streams:\n  - { name: a, request: { path: '/a}' } }", "without its {"},
		{"an open hole", "", "streams:\n  - { name: a, request: { path: '/{config.base_url' } }", "without its }"},
		{"lookback on integers", "", "streams:\n  - { name: a, request: { path: /a }, incremental: { cursor_field: n, type: integer, lookback: 5m } }", "lookback is for"},
		{"jwt and bearer", secret, "  auth: { type: bearer, token: { secret: api_key } }\n  sign: { type: jwt, key: { secret: api_key }, algorithm: HS256, claims: '{}' }\nstreams:\n  - { name: a, request: { path: /a } }", "both auth and sign"},
		{"a secret that isn't text", `n: { type: integer, x-secret: true }`, "streams:\n  - { name: a, request: { path: /a } }", "a secret is a string"},
		{"two streams with one name", "", "streams:\n  - { name: a, request: { path: /a } }\n  - { name: a, request: { path: /b } }", "declared twice"},
		{"expired without a paginator", "", "streams:\n  - { name: a, request: { path: /a }, errors: [{ when: 'true', action: expired }] }", "needs a paginator"},
		{"a body on a GET", "", "streams:\n  - { name: a, request: { path: /a, body: '{}' } }", "no body"},
		{"a code on ignore", "", "streams:\n  - { name: a, request: { path: /a }, errors: [{ when: 'true', action: ignore, code: TRANSIENT }] }", "for fail"},
		{"a schema type without a logical type", "", "streams:\n  - { name: a, request: { path: /a }, schema: { properties: { x: { type: string, format: color } } } }", `format "color"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load([]byte(testManifest(tc.props, tc.rest)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want an error with %q", err, tc.want)
			}
		})
	}
}

// Every field in the manifest JSON Schema decodes into the manifest's Go
// types, which reject unknown fields.
func TestEveryFieldLoads(t *testing.T) {
	props := `
api_key: { type: string, x-secret: true }
client_id: { type: string }
client_secret: { type: string, x-secret: true }
refresh: { type: string, x-secret: true }
start_date: { type: string, format: date-time, default: "2020-01-01T00:00:00Z" }`
	streams := `
check: { stream: a }
streams:
  - name: a
    primary_key: [id]
    request:
      method: POST
      path: /a
      params: { since: "{has(state.cursor) ? state.cursor : config.start_date}", t: "{page.token}" }
      headers: { X-Now: "{now}" }
      body: "{'q': 1}"
      idempotent: true
    records: response.body.data
    paginator: { type: cursor, next: response.body.next, param: after, stop: "size(records) == 0" }
    incremental: { cursor_field: updated, type: datetime, lookback: 1h30m, ordered: true }
    errors:
      - { when: "response.status == 410", action: fail, code: CONTRACT_CHANGED, message: "gone: {response.status}" }
      - { when: "response.status == 409", action: expired }
    schema: { properties: { id: { type: string, format: uuid }, n: { type: number } } }
  - { name: b, request: { path: /b }, paginator: { type: next_url, next: response.body.next } }
  - { name: c, request: { path: /c }, paginator: { type: link } }
  - { name: d, request: { path: /d }, paginator: { type: offset, param: o, start: 0, size: 10, size_param: n } }
  - { name: e, primary_key: [id], request: { path: /e }, paginator: { type: page, param: p, start: 1, size: 10, size_param: n } }
  - { name: f, parent: { stream: e }, request: { path: "/e/{parent.id}" } }`
	auths := []string{
		`  auth: { type: bearer, token: { secret: api_key } }`,
		`  auth: { type: basic, username: "{config.client_id}", password: { secret: api_key } }`,
		`  auth: { type: api_key, key: { secret: api_key }, header: X-Key, prefix: "Token " }`,
		`  auth: { type: api_key, key: { secret: api_key }, query: key }`,
		`  auth: { type: oauth2, token_url: https://auth.example/token, client_id: "{config.client_id}", client_secret: { secret: client_secret }, refresh_token: { secret: refresh }, scopes: [read], auth_style: header }`,
		`  auth: { type: oauth2, token_url: https://auth.example/token, client_id: { secret: api_key }, client_secret: { secret: client_secret } }`,
		`  auth: { type: aws_sigv4, access_key_id: "{config.client_id}", secret_access_key: { secret: client_secret }, session_token: { secret: refresh }, region: us-east-1, service: execute-api }`,
		`  sign: { type: hmac, key: { secret: api_key }, hash: sha512, message: "request.body", encoding: base64, query: sig, prefix: "" }`,
		`  sign: { type: jwt, key: { secret: api_key }, algorithm: RS256, claims: "{'iat': int(now)}", jose_header: "{'kid': 'k1'}", prefix: "JWT " }
  auth: { type: api_key, key: { secret: client_secret }, query: key }`,
	}
	for i, a := range auths {
		m := testManifest(props, a+`
  headers: { Accept: application/json }
  rate_limit: { rps: 2.5 }
  errors:
    - { when: "response.status == 404", action: ignore }
`+streams)
		c, err := Load([]byte(m))
		if err != nil {
			t.Fatalf("manifest %d: %v", i, err)
		}
		b, err := c.Bind([]byte(`{"base_url": "https://API.example:8443/v1", "api_key": "k", "client_id": "id", "client_secret": "s", "refresh": "r"}`))
		if err != nil {
			t.Fatalf("manifest %d: %v", i, err)
		}
		if got := b.RateLimits["api.example:8443"]; got != 2.5 {
			t.Errorf("manifest %d: rate limits %v", i, b.RateLimits)
		}
		if b.config["start_date"] != "2020-01-01T00:00:00Z" {
			t.Errorf("manifest %d: the default start_date is missing: %v", i, b.config)
		}
	}
}
