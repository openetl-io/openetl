package declarative

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/sdk/go/host"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type src struct {
	b *Binding
	h host.Host
}

var (
	_ source.Source  = (*src)(nil)
	_ source.Checker = (*src)(nil)
)

// Every stream is superset: cursor floors and lookback windows read records
// again, and so does a retry after a resume point.
func (s *src) Discover(context.Context) (source.Catalog, error) {
	var cat source.Catalog
	for _, st := range s.b.c.streams {
		cs := source.CatalogStream{Name: st.name, PrimaryKey: st.primaryKey, Replay: source.Superset, Schema: st.schema, SyncModes: []source.SyncMode{source.FullRefresh}}
		if st.inc != nil {
			cs.CursorField, cs.SyncModes = st.inc.field, []source.SyncMode{source.Incremental}
		}
		cat.Streams = append(cat.Streams, cs)
	}
	return cat, nil
}

// Check fetches the first page of the check stream.
func (s *src) Check(ctx context.Context) error {
	if s.b.c.check == nil {
		return nil
	}
	_, err := s.fetch(ctx, s.b.c.check, call{})
	return err
}

// Read reads the requested streams one after another.
func (s *src) Read(ctx context.Context, req source.ReadRequest, emit source.Emitter) error {
	for _, cs := range req.Streams {
		st := s.b.c.stream(cs.Name)
		if st == nil {
			return errs.New(errs.ConfigInvalid, "%s: no stream %q", s.b.c.name, cs.Name)
		}
		var ss streamState
		if len(cs.State) > 0 {
			if err := json.Unmarshal(cs.State, &ss); err != nil {
				return errs.Wrap(errs.ConfigInvalid, err, fmt.Sprintf("%s: %s: state", s.b.c.name, st.name))
			}
		}
		out := &rows{stream: st.name, emit: emit}
		var err error
		if st.parent != nil {
			err = s.readChild(ctx, st, ss, out)
		} else {
			err = s.readStream(ctx, st, ss, out)
		}
		if err != nil {
			return fmt.Errorf("%s: %s: %w", s.b.c.name, st.name, err)
		}
	}
	return nil
}

func (c *Connector) stream(name string) *stream {
	for _, st := range c.streams {
		if st.name == name {
			return st
		}
	}
	return nil
}

// streamState is a resume point.
type streamState struct {
	// Where a read without a page token starts.
	Cursor json.RawMessage `json:"cursor,omitempty"`
	// With Page: the floor the page token's requests use; null for none.
	Since json.RawMessage `json:"since,omitempty"`
	// The highest cursor the read has seen, unless it's ordered.
	Max  json.RawMessage `json:"max,omitempty"`
	Page string          `json:"page,omitempty"`
	// A child stream's place among its parents: the token of the parents'
	// page, the keys of the parents read on it, and the parent Page belongs
	// to.
	Parent  *string           `json:"parent,omitempty"`
	Done    []json.RawMessage `json:"done,omitempty"`
	Current json.RawMessage   `json:"current,omitempty"`
}

var errTokenRejected = errs.New(errs.ContractChanged, "the API rejected a page token it had just given")

func (s *src) readStream(ctx context.Context, st *stream, ss streamState, out *rows) error {
	t := newTracker(st.inc, ss)
	token := ss.Page
	resumed := token != ""
	for {
		sv, err := t.stateVar()
		if err != nil {
			return err
		}
		p, err := s.fetch(ctx, st, call{state: sv, token: token})
		if err != nil {
			return err
		}
		if p.expired {
			if !resumed {
				return errTokenRejected
			}
			token, resumed = "", false
			t.restart()
			continue
		}
		resumed = false
		for _, rec := range p.records {
			if err := t.see(rec); err != nil {
				return err
			}
			if err := out.add(rec); err != nil {
				return err
			}
		}
		if p.next == "" {
			break
		}
		if p.next == token {
			return errs.New(errs.ContractChanged, "the paginator returned the page token it was given")
		}
		state, err := t.pageState(p.next)
		if err != nil {
			return err
		}
		if err := out.resumePoint(state); err != nil {
			return err
		}
		token = p.next
	}
	next, err := t.completion()
	if err != nil {
		return err
	}
	return out.complete(next)
}

// readChild reads st once per record of its parent, which it reads whole.
// A resume point follows each child page and each parent.
func (s *src) readChild(ctx context.Context, st *stream, ss streamState, out *rows) error {
	t := newTracker(st.inc, streamState{Cursor: ss.Cursor, Max: ss.Max})
	ptoken := ""
	if ss.Parent != nil {
		ptoken = *ss.Parent
	}
	presumed := ptoken != ""
	done := map[string]bool{}
	var doneList []json.RawMessage
	for _, k := range ss.Done {
		done[string(k)] = true
		doneList = append(doneList, k)
	}
	current, ctoken := string(ss.Current), ss.Page
	for {
		pp, err := s.fetch(ctx, st.parent, call{token: ptoken})
		if err != nil {
			return fmt.Errorf("parent %s: %w", st.parent.name, err)
		}
		if pp.expired {
			if !presumed {
				return fmt.Errorf("parent %s: %w", st.parent.name, errTokenRejected)
			}
			ptoken, presumed = "", false
			done, doneList, current, ctoken = map[string]bool{}, nil, "", ""
			continue
		}
		presumed = false
		for _, prec := range pp.records {
			key, err := parentKey(prec, st.parent.primaryKey)
			if err != nil {
				return fmt.Errorf("parent %s: %w", st.parent.name, err)
			}
			if done[string(key)] {
				continue
			}
			tok := ""
			if string(key) == current {
				tok = ctoken
			}
			place := streamState{Parent: &ptoken, Done: doneList, Current: key}
			if err := s.readForParent(ctx, st, t, prec, tok, place, out); err != nil {
				return err
			}
			done[string(key)] = true
			doneList = append(doneList, key)
			if err := out.resumePoint(t.childState(streamState{Parent: &ptoken, Done: doneList})); err != nil {
				return err
			}
		}
		if pp.next == "" {
			break
		}
		if pp.next == ptoken {
			return fmt.Errorf("parent %s: %w", st.parent.name, errs.New(errs.ContractChanged, "the paginator returned the page token it was given"))
		}
		ptoken, done, doneList = pp.next, map[string]bool{}, nil
		if err := out.resumePoint(t.childState(streamState{Parent: &ptoken})); err != nil {
			return err
		}
	}
	next, err := t.completion()
	if err != nil {
		return err
	}
	return out.complete(next)
}

func (s *src) readForParent(ctx context.Context, st *stream, t *tracker, parent map[string]any, token string, place streamState, out *rows) error {
	resumed := token != ""
	for {
		sv, err := t.stateVar()
		if err != nil {
			return err
		}
		p, err := s.fetch(ctx, st, call{state: sv, parent: parent, token: token})
		if err != nil {
			return err
		}
		if p.expired {
			if !resumed {
				return errTokenRejected
			}
			token, resumed = "", false
			continue
		}
		resumed = false
		for _, rec := range p.records {
			if err := t.see(rec); err != nil {
				return err
			}
			if err := out.add(rec); err != nil {
				return err
			}
		}
		if p.next == "" {
			return nil
		}
		if p.next == token {
			return errs.New(errs.ContractChanged, "the paginator returned the page token it was given")
		}
		place.Page = p.next
		if err := out.resumePoint(t.childState(place)); err != nil {
			return err
		}
		token = p.next
	}
}

// parentKey is a parent record's primary key, as a JSON array.
func parentKey(rec map[string]any, pk []string) (json.RawMessage, error) {
	vals := make([]any, len(pk))
	for i, f := range pk {
		v, ok := rec[f]
		if !ok || v == nil {
			return nil, errs.New(errs.ContractChanged, "a record has no primary key %q", f)
		}
		vals[i] = v
	}
	return marshal(vals)
}

// call is what a request's expressions see besides the config.
type call struct {
	// state.cursor, if the stream has a floor.
	state  map[string]any
	parent map[string]any
	// Empty: the first page.
	token string
}

// page is one fetched page.
type page struct {
	records []map[string]any
	// Empty: the last page.
	next string
	// An expired rule matched.
	expired bool
}

func (s *src) fetch(ctx context.Context, st *stream, c call) (*page, error) {
	now, err := s.h.Now(ctx)
	if err != nil {
		return nil, err
	}
	pv := map[string]any{}
	if c.token != "" {
		pv["token"] = c.token
	}
	vars := map[string]any{varConfig: s.b.config, varNow: now, varState: orEmpty(c.state), varPage: pv, varParent: orEmpty(c.parent)}
	req, err := s.request(ctx, st, vars, c.token)
	if err != nil {
		return nil, err
	}
	resp, err := s.h.HTTP(ctx, req)
	if err != nil {
		se, ok := errors.AsType[*host.StatusError](err)
		if !ok {
			return nil, err
		}
		vars[varResponse], _ = responseVar(se.Status, se.Header, se.Body)
		if p, ok, err := s.decide(st, vars, se); ok {
			return p, err
		}
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return nil, err
	}
	rv, isJSON := responseVar(resp.Status, resp.Header, body)
	vars[varResponse] = rv
	if p, ok, err := s.decide(st, vars, nil); ok {
		return p, err
	}
	if !isJSON {
		return nil, errs.New(errs.ContractChanged, "HTTP %d: the response isn't JSON", resp.Status)
	}
	var recs ref.Val
	if st.records != nil {
		if recs, err = st.records.eval(vars); err != nil {
			return nil, errs.Wrap(errs.ContractChanged, err, "records")
		}
	} else {
		recs = types.DefaultTypeAdapter.NativeToValue(rv["body"])
	}
	list, ok := recs.(traits.Lister)
	if !ok {
		return nil, errs.New(errs.ContractChanged, "records: the records are a %s, not a list", recs.Type().TypeName())
	}
	p := &page{}
	for it := list.Iterator(); it.HasNext() == types.True; {
		v, err := native(it.Next())
		if err != nil {
			return nil, errs.Wrap(errs.ContractChanged, err, "records")
		}
		m, ok := v.(map[string]any)
		if !ok {
			return nil, errs.New(errs.ContractChanged, "records: a record isn't an object")
		}
		p.records = append(p.records, m)
	}
	vars[varRecords] = list
	if p.next, err = st.pager.nextToken(vars, resp.Header, c.token, len(p.records)); err != nil {
		return nil, errs.Wrap(errs.ContractChanged, err, "paginator")
	}
	return p, nil
}

func orEmpty(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

// responseVar is a response as expressions see it. A body that isn't JSON
// is text, and isJSON is false.
func responseVar(status int, h http.Header, body []byte) (v map[string]any, isJSON bool) {
	headers := make(map[string]any, len(h))
	for k, vs := range h {
		headers[strings.ToLower(k)] = strings.Join(vs, ", ")
	}
	var b any
	isJSON = true
	if len(bytes.TrimSpace(body)) > 0 {
		var err error
		if b, err = decodeJSON(body); err != nil {
			b, isJSON = string(body), false
		}
	}
	return map[string]any{"status": int64(status), "headers": headers, "body": b}, isJSON
}

// decide runs the error rules on a response; ok is whether one matched.
// se is the host's error for a failed response.
func (s *src) decide(st *stream, vars map[string]any, se *host.StatusError) (p *page, ok bool, err error) {
	for _, r := range st.rules {
		if v, err := r.when.eval(vars); err != nil || v != types.True {
			continue
		}
		switch r.action {
		case "ignore":
			return &page{}, true, nil
		case "expired":
			return &page{expired: true}, true, nil
		}
		e := &errs.Error{Code: r.code, Msg: "the response matched an error rule"}
		if se != nil {
			e.Err, e.RetryAfter = se, se.After
			if e.Code == errs.Unspecified {
				e.Code = errs.Code(se.Code)
			}
		}
		if e.Code == errs.Unspecified {
			e.Code = errs.ContractChanged
		}
		if r.message != nil {
			if e.Msg, err = r.message.text(vars, nil); err != nil {
				e.Msg = "the response matched an error rule"
			}
		}
		return nil, true, e
	}
	return nil, false, nil
}

func (s *src) request(ctx context.Context, st *stream, vars map[string]any, token string) (*host.Request, error) {
	var (
		u   *url.URL
		err error
	)
	if token != "" && (st.pager.kind == "next_url" || st.pager.kind == "link") {
		if u, err = s.b.resolve(token); err != nil {
			return nil, err
		}
	} else {
		var path string
		if path, err = st.path.text(vars, url.PathEscape); err != nil {
			return nil, errs.Wrap(errs.ConfigInvalid, err, "request.path")
		}
		raw := s.b.base.String()
		if path != "" {
			raw = strings.TrimSuffix(raw, "/") + "/" + strings.TrimPrefix(path, "/")
		}
		if u, err = url.Parse(raw); err != nil {
			return nil, errs.Wrap(errs.ConfigInvalid, err, "request.path")
		}
		q := u.Query()
		for _, p := range st.params {
			v, err := p.t.value(vars)
			if err != nil {
				return nil, errs.Wrap(errs.ConfigInvalid, err, "request.params")
			}
			vals, err := paramValues(v)
			if err != nil {
				return nil, errs.Wrap(errs.ConfigInvalid, err, "request.params."+p.name)
			}
			for _, x := range vals {
				q.Add(p.name, x)
			}
		}
		st.pager.addParams(q, token)
		u.RawQuery = q.Encode()
	}
	r := &host.Request{Method: st.method, Header: http.Header{}, Auth: s.b.auth, Idempotent: st.idempotent}
	for _, hs := range [][]named{s.b.c.headers, st.headers} {
		for _, h := range hs {
			v, err := h.t.value(vars)
			if err != nil {
				return nil, errs.Wrap(errs.ConfigInvalid, err, "request.headers")
			}
			if v == types.NullValue {
				continue
			}
			x, err := text(v)
			if err != nil {
				return nil, errs.Wrap(errs.ConfigInvalid, err, "request.headers."+h.name)
			}
			r.Header.Set(h.name, x)
		}
	}
	if st.body != nil {
		v, err := st.body.eval(vars)
		if err == nil {
			var n any
			if n, err = native(v); err == nil {
				r.Body, err = marshal(n)
			}
		}
		if err != nil {
			return nil, errs.Wrap(errs.ConfigInvalid, err, "request.body")
		}
		if r.Header.Get("Content-Type") == "" {
			r.Header.Set("Content-Type", "application/json")
		}
	}
	if err := s.signRequest(ctx, r, u, vars); err != nil {
		return nil, err
	}
	r.URL = u.String()
	return r, nil
}

// paramValues is a parameter's values: none for null, one each for a
// list's items.
func paramValues(v ref.Val) ([]string, error) {
	if v == types.NullValue {
		return nil, nil
	}
	l, ok := v.(traits.Lister)
	if !ok {
		s, err := text(v)
		return []string{s}, err
	}
	var out []string
	for it := l.Iterator(); it.HasNext() == types.True; {
		s, err := text(it.Next())
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// resolve resolves a next page's URL against the base URL. It must have the
// base URL's scheme, host and port, which is where credentials go.
func (b *Binding) resolve(token string) (*url.URL, error) {
	u, err := b.base.Parse(token)
	if err != nil {
		return nil, errs.Wrap(errs.ContractChanged, err, "the next page's URL")
	}
	if u.Scheme != b.base.Scheme || !strings.EqualFold(u.Hostname(), b.base.Hostname()) || port(u) != port(b.base) {
		return nil, errs.New(errs.ContractChanged, "the next page's URL leaves %s://%s", b.base.Scheme, b.base.Host)
	}
	u.Fragment = ""
	return u, nil
}

func port(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	if u.Scheme == "https" {
		return "443"
	}
	return "80"
}

func (s *src) signRequest(ctx context.Context, r *host.Request, u *url.URL, vars map[string]any) error {
	sg := s.b.c.sign
	if sg == nil {
		return nil
	}
	switch sg.kind {
	case "hmac":
		headers := map[string]any{}
		for k, vs := range r.Header {
			headers[strings.ToLower(k)] = strings.Join(vs, ", ")
		}
		req := map[string]any{"method": r.Method, "url": u.String(), "path": u.EscapedPath(), "query": u.RawQuery, "headers": headers, "body": string(r.Body)}
		v, err := sg.message.eval(map[string]any{varConfig: vars[varConfig], varNow: vars[varNow], varRequest: req})
		if err != nil {
			return errs.Wrap(errs.ConfigInvalid, err, "http.sign")
		}
		var msg []byte
		switch v := v.(type) {
		case types.String:
			msg = []byte(v)
		case types.Bytes:
			msg = v
		default:
			return errs.New(errs.ConfigInvalid, "http.sign.message: a %s, not a string or bytes", v.Type().TypeName())
		}
		mac, err := s.h.Sign(ctx, sg.key, host.HMAC{Hash: sg.hash, Message: msg})
		if err != nil {
			return err
		}
		enc := hex.EncodeToString(mac)
		if sg.encoding == "base64" {
			enc = base64.StdEncoding.EncodeToString(mac)
		}
		if sg.header != "" {
			r.Header.Set(sg.header, sg.prefix+enc)
			return nil
		}
		q := u.Query()
		q.Set(sg.query, sg.prefix+enc)
		u.RawQuery = q.Encode()
	case "jwt":
		cv := map[string]any{varConfig: vars[varConfig], varNow: vars[varNow]}
		claims, err := evalJSON(sg.claims, cv)
		if err != nil {
			return errs.Wrap(errs.ConfigInvalid, err, "http.sign.claims")
		}
		var jose []byte
		if sg.jose != nil {
			if jose, err = evalJSON(sg.jose, cv); err != nil {
				return errs.Wrap(errs.ConfigInvalid, err, "http.sign.jose_header")
			}
		}
		tok, err := s.h.Sign(ctx, sg.key, host.JWT{Algorithm: sg.alg, Claims: claims, Header: jose})
		if err != nil {
			return err
		}
		r.Header.Set("Authorization", sg.prefix+string(tok))
	}
	return nil
}

func evalJSON(e *expr, vars map[string]any) ([]byte, error) {
	v, err := e.eval(vars)
	if err != nil {
		return nil, err
	}
	n, err := native(v)
	if err != nil {
		return nil, err
	}
	return marshal(n)
}

func (p *pager) addParams(q url.Values, token string) {
	switch p.kind {
	case "cursor":
		if p.param != "" && token != "" {
			q.Set(p.param, token)
		}
	case "offset", "page":
		if token == "" {
			token = strconv.FormatInt(p.start, 10)
		}
		q.Set(p.param, token)
	}
	if p.sizeParam != "" {
		q.Set(p.sizeParam, strconv.FormatInt(p.size, 10))
	}
}

// nextToken is the next page's token, or empty after the last page.
func (p *pager) nextToken(vars map[string]any, h http.Header, token string, n int) (string, error) {
	var next string
	switch p.kind {
	case "":
		return "", nil
	case "cursor", "next_url":
		v, err := p.next.eval(vars)
		if err != nil {
			return "", err
		}
		if v != types.NullValue {
			if next, err = text(v); err != nil {
				return "", fmt.Errorf("next: %w", err)
			}
		}
	case "link":
		next = nextLink(h)
	case "offset", "page":
		if n == 0 || (p.size > 0 && int64(n) < p.size) {
			return "", nil
		}
		cur := p.start
		if token != "" {
			var err error
			if cur, err = strconv.ParseInt(token, 10, 64); err != nil {
				return "", fmt.Errorf("the page token %q isn't a number", token)
			}
		}
		step := int64(1)
		if p.kind == "offset" {
			step = int64(n)
		}
		next = strconv.FormatInt(cur+step, 10)
	}
	if next != "" && p.stop != nil {
		v, err := p.stop.eval(vars)
		if err != nil {
			return "", err
		}
		if v == types.True {
			return "", nil
		}
	}
	return next, nil
}

// nextLink is the URL of a Link header's rel="next" (RFC 8288).
func nextLink(h http.Header) string {
	for _, v := range h.Values("Link") {
		for {
			start := strings.IndexByte(v, '<')
			end := strings.IndexByte(v, '>')
			if start < 0 || end < start {
				break
			}
			target := v[start+1 : end]
			v = v[end+1:]
			params := v
			if i := strings.IndexByte(v, '<'); i >= 0 {
				params = v[:i]
			}
			for _, p := range strings.Split(params, ";") {
				k, val, _ := strings.Cut(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(p), ",")), "=")
				if !strings.EqualFold(strings.TrimSpace(k), "rel") {
					continue
				}
				for _, rel := range strings.Fields(strings.Trim(strings.TrimSpace(val), `"`)) {
					if strings.EqualFold(rel, "next") {
						return target
					}
				}
			}
		}
	}
	return ""
}

// tracker follows a stream's cursor: the floor it resumed from, the floor
// its requests use, and the highest cursor it has seen.
type tracker struct {
	inc   *incremental
	floor json.RawMessage
	since json.RawMessage
	max   json.RawMessage
}

func newTracker(inc *incremental, ss streamState) *tracker {
	t := &tracker{inc: inc, floor: ss.Cursor, since: ss.Cursor, max: ss.Max}
	if ss.Page != "" {
		t.since = ss.Since
	}
	if string(t.since) == "null" {
		t.since = nil
	}
	return t
}

// restart starts the read over without its page token: from the floor,
// which for an ordered stream is the highest cursor seen.
func (t *tracker) restart() { t.since, t.max = t.floor, nil }

func (t *tracker) see(rec map[string]any) error {
	if t.inc == nil {
		return nil
	}
	v, ok := rec[t.inc.field]
	if !ok || v == nil {
		return nil
	}
	raw, err := marshal(v)
	if err != nil {
		return err
	}
	if _, err := t.inc.parse(raw); err != nil {
		return errs.Wrap(errs.ContractChanged, err, "cursor "+t.inc.field)
	}
	if t.max == nil {
		t.max = raw
		return nil
	}
	if c, err := t.inc.compare(raw, t.max); err != nil {
		return err
	} else if c > 0 {
		t.max = raw
	}
	return nil
}

// highest is the larger of the floor and the highest cursor seen.
func (t *tracker) highest() (json.RawMessage, error) {
	switch {
	case t.max == nil:
		return t.floor, nil
	case t.floor == nil:
		return t.max, nil
	}
	c, err := t.inc.compare(t.max, t.floor)
	if err != nil || c > 0 {
		return t.max, err
	}
	return t.floor, nil
}

// pageState is the resume point after a page whose successor is next. An
// ordered stream's floor advances with every page; an unordered one carries
// the highest cursor seen until it completes.
func (t *tracker) pageState(next string) (streamState, error) {
	s := streamState{Page: next}
	if t.inc == nil {
		return s, nil
	}
	s.Cursor, s.Since = t.floor, t.since
	if s.Since == nil {
		s.Since = json.RawMessage("null")
	}
	if !t.inc.ordered {
		s.Max = t.max
		return s, nil
	}
	var err error
	s.Cursor, err = t.highest()
	return s, err
}

// childState adds the cursor to a child stream's place. The floor stays put
// until the stream completes.
func (t *tracker) childState(place streamState) streamState {
	if t.inc != nil {
		place.Cursor, place.Max = t.floor, t.max
	}
	return place
}

// completion is where the next sync starts: nil for the beginning.
func (t *tracker) completion() ([]byte, error) {
	if t.inc == nil {
		return nil, nil
	}
	floor, err := t.highest()
	if err != nil || floor == nil {
		return nil, err
	}
	return marshal(streamState{Cursor: floor})
}

// stateVar is the state requests see: the floor less the lookback.
func (t *tracker) stateVar() (map[string]any, error) {
	if t.inc == nil || t.since == nil {
		return nil, nil
	}
	v, err := t.inc.parse(t.since)
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "state: cursor")
	}
	switch v := v.(type) {
	case time.Time:
		return map[string]any{"cursor": v.Add(-t.inc.lookback)}, nil
	case int64:
		if t.inc.kind == "unix" {
			v -= int64(t.inc.lookback / time.Second)
		}
		return map[string]any{"cursor": v}, nil
	default:
		return map[string]any{"cursor": v}, nil
	}
}

// parse reads a cursor value as its type says: a time for datetime, an
// int64 for unix and integer (from a number or its text), a string for
// string.
func (inc *incremental) parse(raw json.RawMessage) (any, error) {
	var s string
	isString := json.Unmarshal(raw, &s) == nil
	switch inc.kind {
	case "datetime":
		if !isString {
			return nil, fmt.Errorf("%s isn't RFC 3339 text", raw)
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return nil, fmt.Errorf("%s isn't RFC 3339 text", raw)
		}
		return t, nil
	case "unix", "integer":
		if !isString {
			s = string(raw)
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s isn't an integer", raw)
		}
		return n, nil
	default:
		if !isString {
			return nil, fmt.Errorf("%s isn't text", raw)
		}
		return s, nil
	}
}

func (inc *incremental) compare(a, b json.RawMessage) (int, error) {
	x, err := inc.parse(a)
	if err != nil {
		return 0, err
	}
	y, err := inc.parse(b)
	if err != nil {
		return 0, err
	}
	switch x := x.(type) {
	case time.Time:
		return x.Compare(y.(time.Time)), nil
	case int64:
		y := y.(int64)
		switch {
		case x < y:
			return -1, nil
		case x > y:
			return 1, nil
		}
		return 0, nil
	default:
		return strings.Compare(x.(string), y.(string)), nil
	}
}

const (
	flushRecords = 1000
	flushBytes   = 1 << 20
)

// rows sends one stream's records, batched.
type rows struct {
	stream   string
	emit     source.Emitter
	buf      []json.RawMessage
	bufBytes int
}

func (r *rows) add(rec map[string]any) error {
	b, err := marshal(rec)
	if err != nil {
		return errs.Wrap(errs.ContractChanged, err, "a record")
	}
	r.buf = append(r.buf, b)
	r.bufBytes += len(b)
	if len(r.buf) >= flushRecords || r.bufBytes >= flushBytes {
		return r.flush()
	}
	return nil
}

func (r *rows) flush() error {
	if len(r.buf) == 0 {
		return nil
	}
	err := r.emit.Rows(r.stream, r.buf)
	r.buf, r.bufBytes = nil, 0
	return err
}

func (r *rows) resumePoint(s streamState) error {
	if err := r.flush(); err != nil {
		return err
	}
	b, err := marshal(s)
	if err != nil {
		return err
	}
	return r.emit.ResumePoint(r.stream, b)
}

func (r *rows) complete(next []byte) error {
	if err := r.flush(); err != nil {
		return err
	}
	return r.emit.Complete(r.stream, next)
}

// marshal encodes JSON without escaping HTML characters, so text keeps its
// bytes.
func marshal(v any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}
