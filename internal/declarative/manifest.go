// Package declarative runs declarative sources: manifests that describe an
// HTTP API as data, with CEL expressions. Every request goes through host
// services, a resume point follows every page, and records go to the engine
// as raw rows.
package declarative

import (
	"bytes"
	"cmp"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"cel.dev/cel-go/cel"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/sdk/go/host"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

//go:embed manifest.schema.json
var schemaJSON []byte

// Schema is the manifest JSON Schema.
func Schema() []byte { return bytes.Clone(schemaJSON) }

var manifestSchema = sync.OnceValues(func() (*jsonschema.Schema, error) {
	return compileSchema("manifest.schema.json", schemaJSON)
})

func compileSchema(name string, src []byte) (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	if err := c.AddResource(name, doc); err != nil {
		return nil, err
	}
	return c.Compile(name)
}

type manifest struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name    string `json:"name"`
		Version string `json:"version"`
		// spec.Parse reads these.
		Description string          `json:"description"`
		License     string          `json:"license"`
		Repository  string          `json:"repository"`
		Docs        string          `json:"docs"`
		Tags        []string        `json:"tags"`
		Author      string          `json:"author"`
		Logo        string          `json:"logo"`
		Release     json.RawMessage `json:"release"`
	} `json:"metadata"`
	Spec struct {
		Config json.RawMessage `json:"config"`
		// The job size it wants.
		Resources *struct {
			Hint string `json:"hint"`
		} `json:"resources"`
		HTTP  httpSpec `json:"http"`
		Check *struct {
			Stream string `json:"stream"`
		} `json:"check"`
		Streams []streamSpec `json:"streams"`
	} `json:"spec"`
}

type httpSpec struct {
	BaseURL   string            `json:"base_url"`
	Headers   map[string]string `json:"headers"`
	Auth      *authSpec         `json:"auth"`
	Sign      *signSpec         `json:"sign"`
	RateLimit *struct {
		RPS float64 `json:"rps"`
	} `json:"rate_limit"`
	Errors []ruleSpec `json:"errors"`
}

type secretRef struct {
	Secret string `json:"secret"`
}

// valueSpec is a template, or a secret.
type valueSpec struct {
	template string
	secret   string
}

func (v *valueSpec) UnmarshalJSON(b []byte) error {
	if err := json.Unmarshal(b, &v.template); err == nil {
		return nil
	}
	var r secretRef
	err := json.Unmarshal(b, &r)
	v.secret = r.Secret
	return err
}

type authSpec struct {
	Type string `json:"type"`
	// bearer
	Token *secretRef `json:"token"`
	// basic
	Username string     `json:"username"`
	Password *secretRef `json:"password"`
	// api_key
	Key    *secretRef `json:"key"`
	Header string     `json:"header"`
	Query  string     `json:"query"`
	Prefix string     `json:"prefix"`
	// oauth2
	TokenURL     string     `json:"token_url"`
	ClientID     *valueSpec `json:"client_id"`
	ClientSecret *secretRef `json:"client_secret"`
	RefreshToken *secretRef `json:"refresh_token"`
	Scopes       []string   `json:"scopes"`
	AuthStyle    string     `json:"auth_style"`
	// aws_sigv4
	AccessKeyID     *valueSpec `json:"access_key_id"`
	SecretAccessKey *secretRef `json:"secret_access_key"`
	SessionToken    *secretRef `json:"session_token"`
	Region          string     `json:"region"`
	Service         string     `json:"service"`
}

type signSpec struct {
	Type string    `json:"type"`
	Key  secretRef `json:"key"`
	// hmac
	Hash     string  `json:"hash"`
	Message  string  `json:"message"`
	Encoding string  `json:"encoding"`
	Header   string  `json:"header"`
	Query    string  `json:"query"`
	Prefix   *string `json:"prefix"`
	// jwt
	Algorithm  string `json:"algorithm"`
	Claims     string `json:"claims"`
	JOSEHeader string `json:"jose_header"`
}

type streamSpec struct {
	Name       string   `json:"name"`
	PrimaryKey []string `json:"primary_key"`
	Request    struct {
		Method     string            `json:"method"`
		Path       string            `json:"path"`
		Params     map[string]string `json:"params"`
		Headers    map[string]string `json:"headers"`
		Body       string            `json:"body"`
		Idempotent bool              `json:"idempotent"`
	} `json:"request"`
	Records   string `json:"records"`
	Paginator *struct {
		Type      string `json:"type"`
		Stop      string `json:"stop"`
		Next      string `json:"next"`
		Param     string `json:"param"`
		Start     *int64 `json:"start"`
		Size      int64  `json:"size"`
		SizeParam string `json:"size_param"`
	} `json:"paginator"`
	Incremental *struct {
		CursorField string `json:"cursor_field"`
		Type        string `json:"type"`
		Lookback    string `json:"lookback"`
		Ordered     bool   `json:"ordered"`
	} `json:"incremental"`
	Parent *struct {
		Stream string `json:"stream"`
	} `json:"parent"`
	Partitions *struct {
		Values string `json:"values"`
		Range  *struct {
			Start string `json:"start"`
			End   string `json:"end"`
			Step  string `json:"step"`
		} `json:"range"`
	} `json:"partitions"`
	Errors []ruleSpec      `json:"errors"`
	Schema json.RawMessage `json:"schema"`
}

type ruleSpec struct {
	When    string `json:"when"`
	Action  string `json:"action"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Connector is a loaded manifest.
type Connector struct {
	name, version string
	hint          string
	config        *jsonschema.Schema
	// Config properties: true for secrets.
	props    map[string]bool
	defaults map[string]any
	baseURL  *template
	headers  []named
	auth     *auth
	sign     *signer
	rps      float64
	check    *stream
	streams  []*stream
}

// named is a header or a query parameter.
type named struct {
	name string
	t    *template
}

type auth struct {
	kind string
	// The bearer token, the API key or the basic password.
	secret                string
	username              *template
	header, query, prefix string
	tokenURL              *template
	clientID, accessKeyID valueRef
	clientSecret, refresh string
	scopes                []string
	authStyle             string
	secretKey, sessionTok string
	region                *template
	service               string
}

// valueRef is a template, or a secret's name.
type valueRef struct {
	t      *template
	secret string
}

type signer struct {
	kind     string
	key      string
	hash     host.Hash
	message  *expr
	encoding string
	header   string
	query    string
	prefix   string
	alg      host.JWTAlgorithm
	claims   *expr
	jose     *expr
}

type stream struct {
	name       string
	primaryKey []string
	method     string
	path       *template
	params     []named
	headers    []named
	body       *expr
	idempotent bool
	// Nil: response.body.
	records *expr
	pager   pager
	inc     *incremental
	parent  *stream
	// Nil: read whole.
	partitions *partitions
	// The stream's own, then the manifest's.
	rules  []*rule
	schema *arrow.Schema
}

// partitions are a stream's splits: one read of it per value, or per step of a
// range, each with its own place and floor, which requests see as partition.
type partitions struct {
	values *expr
	// A range's partitions are {start, end}, in RFC 3339, from start up to
	// end, by default now.
	start, end *expr
	step       time.Duration
}

type pager struct {
	kind      string
	next      *expr
	stop      *expr
	param     string
	start     int64
	size      int64
	sizeParam string
}

type incremental struct {
	field    string
	kind     string
	lookback time.Duration
	ordered  bool
}

type rule struct {
	when    *expr
	action  string
	code    errs.Code
	message *template
}

// What each place in a manifest sees.
var (
	baseVars     = []string{varConfig}
	requestVars  = []string{varConfig, varNow, varPage, varState}
	responseVars = []string{varConfig, varNow, varPage, varResponse, varState}
	pagerVars    = []string{varConfig, varNow, varPage, varRecords, varResponse, varState}
	httpRuleVars = []string{varConfig, varNow, varResponse}
	messageVars  = []string{varConfig, varNow, varRequest}
	claimVars    = []string{varConfig, varNow}
)

// forStream adds what a stream's expressions see beyond a place's own.
func forStream(vars []string, parent, partitioned bool) []string {
	if parent {
		vars = append(slices.Clone(vars), varParent)
	}
	if partitioned {
		vars = append(slices.Clone(vars), varPartition)
	}
	return vars
}

// Load parses a manifest, in YAML or JSON, and checks it: against the
// manifest JSON Schema, then its expressions' types and references.
func Load(src []byte) (*Connector, error) {
	var doc any
	if err := yaml.Unmarshal(src, &doc); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	j, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(j))
	if err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	sch, err := manifestSchema()
	if err != nil {
		return nil, err
	}
	if err := sch.Validate(inst); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	var m manifest
	dec := json.NewDecoder(bytes.NewReader(j))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	c, err := m.compile()
	if err != nil {
		return nil, fmt.Errorf("manifest %s: %w", m.Metadata.Name, err)
	}
	return c, nil
}

func (c *Connector) Name() string    { return c.name }
func (c *Connector) Version() string { return c.version }

// Hint is the job size it wants; empty if it doesn't say.
func (c *Connector) Hint() string { return c.hint }

// RateLimit is the most requests a second it declares; 0 if it declares
// no limit.
func (c *Connector) RateLimit() float64 { return c.rps }

func (m *manifest) compile() (*Connector, error) {
	c := &Connector{name: m.Metadata.Name, version: m.Metadata.Version, defaults: map[string]any{}}
	if m.Spec.Resources != nil {
		c.hint = m.Spec.Resources.Hint
	}
	if err := c.compileConfig(m.Spec.Config); err != nil {
		return nil, err
	}
	cc := &compiler{props: c.props, envs: map[string]*cel.Env{}}
	var err error
	h := &m.Spec.HTTP
	if c.baseURL, err = cc.template("http.base_url", h.BaseURL, baseVars); err != nil {
		return nil, err
	}
	if c.headers, err = namedTemplates(cc, "http.headers", h.Headers, claimVars); err != nil {
		return nil, err
	}
	if h.Auth != nil {
		if c.auth, err = c.compileAuth(cc, h.Auth); err != nil {
			return nil, err
		}
	}
	if h.Sign != nil {
		if c.sign, err = c.compileSign(cc, h.Sign); err != nil {
			return nil, err
		}
	}
	if h.RateLimit != nil {
		c.rps = h.RateLimit.RPS
	}
	httpRules, err := compileRules(cc, "http.errors", h.Errors, httpRuleVars)
	if err != nil {
		return nil, err
	}

	byName := map[string]*streamSpec{}
	for i := range m.Spec.Streams {
		s := &m.Spec.Streams[i]
		if byName[s.Name] != nil {
			return nil, fmt.Errorf("stream %q is declared twice", s.Name)
		}
		byName[s.Name] = s
	}
	compiled := map[string]*stream{}
	for i := range m.Spec.Streams {
		s := &m.Spec.Streams[i]
		var parent *stream
		if s.Parent != nil {
			ps := byName[s.Parent.Stream]
			switch {
			case ps == nil:
				return nil, fmt.Errorf("stream %s: no parent stream %q", s.Name, s.Parent.Stream)
			case ps.Parent != nil:
				return nil, fmt.Errorf("stream %s: parent %s has a parent of its own", s.Name, ps.Name)
			case len(ps.PrimaryKey) == 0:
				return nil, fmt.Errorf("stream %s: parent %s needs a primary key, to keep its place among parents", s.Name, ps.Name)
			}
			if parent = compiled[ps.Name]; parent == nil {
				if parent, err = c.compileStream(cc, ps, nil, httpRules); err != nil {
					return nil, err
				}
				compiled[ps.Name] = parent
			}
		}
		st := compiled[s.Name]
		if st == nil {
			if st, err = c.compileStream(cc, s, parent, httpRules); err != nil {
				return nil, err
			}
			compiled[s.Name] = st
		}
	}
	for _, s := range m.Spec.Streams {
		c.streams = append(c.streams, compiled[s.Name])
	}
	if m.Spec.Check != nil {
		st := compiled[m.Spec.Check.Stream]
		switch {
		case st == nil:
			return nil, fmt.Errorf("check: no stream %q", m.Spec.Check.Stream)
		case st.parent != nil:
			return nil, fmt.Errorf("check: stream %s has a parent; check a stream without one", st.name)
		}
		c.check = st
	} else {
		for _, st := range c.streams {
			if st.parent == nil {
				c.check = st
				break
			}
		}
	}
	return c, nil
}

func (c *Connector) compileConfig(raw json.RawMessage) error {
	var s struct {
		Properties map[string]struct {
			Type    any             `json:"type"`
			Secret  bool            `json:"x-secret"`
			Default json.RawMessage `json:"default"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	c.props = map[string]bool{}
	for name, p := range s.Properties {
		if p.Secret && p.Type != "string" {
			return fmt.Errorf("config.%s: a secret is a string", name)
		}
		c.props[name] = p.Secret
		if p.Default != nil {
			v, err := decodeJSON(p.Default)
			if err != nil {
				return fmt.Errorf("config.%s: default: %w", name, err)
			}
			c.defaults[name] = v
		}
	}
	var err error
	if c.config, err = compileSchema("config.json", raw); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	return nil
}

// SecretProperties lists the config properties the manifest marks x-secret.
func (c *Connector) SecretProperties() []string {
	var props []string
	for k, secret := range c.props {
		if secret {
			props = append(props, k)
		}
	}
	slices.Sort(props)
	return props
}

// secretName checks that r names a secret.
func (c *Connector) secretName(where string, r *secretRef) (string, error) {
	if r == nil {
		return "", nil
	}
	secret, ok := c.props[r.Secret]
	switch {
	case !ok:
		return "", fmt.Errorf("%s: config has no property %q", where, r.Secret)
	case !secret:
		return "", fmt.Errorf("%s: config.%s isn't marked x-secret", where, r.Secret)
	}
	return r.Secret, nil
}

func (c *Connector) valueRef(cc *compiler, where string, v *valueSpec) (valueRef, error) {
	if v.secret != "" {
		name, err := c.secretName(where, &secretRef{Secret: v.secret})
		return valueRef{secret: name}, err
	}
	t, err := cc.template(where, v.template, baseVars)
	return valueRef{t: t}, err
}

func (c *Connector) compileAuth(cc *compiler, a *authSpec) (*auth, error) {
	out := &auth{kind: a.Type, header: a.Header, query: a.Query, prefix: a.Prefix, scopes: a.Scopes, authStyle: a.AuthStyle, service: a.Service}
	var err error
	switch a.Type {
	case "bearer":
		out.secret, err = c.secretName("http.auth.token", a.Token)
	case "basic":
		if out.secret, err = c.secretName("http.auth.password", a.Password); err == nil {
			out.username, err = cc.template("http.auth.username", a.Username, baseVars)
		}
	case "api_key":
		out.secret, err = c.secretName("http.auth.key", a.Key)
	case "oauth2":
		if out.tokenURL, err = cc.template("http.auth.token_url", a.TokenURL, baseVars); err != nil {
			return nil, err
		}
		if out.clientID, err = c.valueRef(cc, "http.auth.client_id", a.ClientID); err != nil {
			return nil, err
		}
		if out.clientSecret, err = c.secretName("http.auth.client_secret", a.ClientSecret); err != nil {
			return nil, err
		}
		out.refresh, err = c.secretName("http.auth.refresh_token", a.RefreshToken)
	case "aws_sigv4":
		if out.accessKeyID, err = c.valueRef(cc, "http.auth.access_key_id", a.AccessKeyID); err != nil {
			return nil, err
		}
		if out.secretKey, err = c.secretName("http.auth.secret_access_key", a.SecretAccessKey); err != nil {
			return nil, err
		}
		if out.sessionTok, err = c.secretName("http.auth.session_token", a.SessionToken); err != nil {
			return nil, err
		}
		out.region, err = cc.template("http.auth.region", a.Region, baseVars)
	}
	return out, err
}

// sets is the header an auth sets, if any.
func (a *auth) sets() string {
	switch {
	case a == nil:
		return ""
	case a.kind == "api_key":
		return a.header
	}
	return "Authorization"
}

var (
	hashes  = map[string]host.Hash{"sha1": host.SHA1, "sha256": host.SHA256, "sha512": host.SHA512}
	jwtAlgs = map[string]host.JWTAlgorithm{"HS256": host.HS256, "RS256": host.RS256, "ES256": host.ES256}
)

func (c *Connector) compileSign(cc *compiler, s *signSpec) (*signer, error) {
	key, err := c.secretName("http.sign.key", &s.Key)
	if err != nil {
		return nil, err
	}
	out := &signer{kind: s.Type, key: key, header: s.Header, query: s.Query}
	if s.Prefix != nil {
		out.prefix = *s.Prefix
	}
	switch s.Type {
	case "hmac":
		out.hash, out.encoding = hashes[s.Hash], s.Encoding
		if out.encoding == "" {
			out.encoding = "hex"
		}
		if out.message, err = cc.expr("http.sign.message", s.Message, messageVars, cel.StringType, cel.BytesType); err != nil {
			return nil, err
		}
	case "jwt":
		out.alg, out.header = jwtAlgs[s.Algorithm], "Authorization"
		if s.Prefix == nil {
			out.prefix = "Bearer "
		}
		if out.claims, err = cc.expr("http.sign.claims", s.Claims, claimVars, cel.MapType(cel.DynType, cel.DynType)); err != nil {
			return nil, err
		}
		if s.JOSEHeader != "" {
			if out.jose, err = cc.expr("http.sign.jose_header", s.JOSEHeader, claimVars, cel.MapType(cel.DynType, cel.DynType)); err != nil {
				return nil, err
			}
		}
	}
	if out.header != "" && strings.EqualFold(out.header, c.auth.sets()) {
		return nil, fmt.Errorf("http.sign: both auth and sign set the %s header", out.header)
	}
	return out, nil
}

func namedTemplates(cc *compiler, where string, m map[string]string, vars []string) ([]named, error) {
	var out []named
	for _, k := range sortedKeys(m) {
		t, err := cc.template(where+"."+k, m[k], vars)
		if err != nil {
			return nil, err
		}
		out = append(out, named{name: k, t: t})
	}
	return out, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func compileRules(cc *compiler, where string, specs []ruleSpec, vars []string) ([]*rule, error) {
	var out []*rule
	for i, r := range specs {
		w := fmt.Sprintf("%s[%d]", where, i)
		when, err := cc.expr(w+".when", r.When, vars, cel.BoolType)
		if err != nil {
			return nil, err
		}
		ru := &rule{when: when, action: r.Action}
		if r.Action != "fail" && (r.Code != "" || r.Message != "") {
			return nil, fmt.Errorf("%s: code and message are for fail", w)
		}
		if r.Code != "" {
			ru.code = errs.Code(protocolv1.ErrorCode_value["ERROR_CODE_"+r.Code])
		}
		if r.Message != "" {
			if ru.message, err = cc.template(w+".message", r.Message, vars); err != nil {
				return nil, err
			}
		}
		out = append(out, ru)
	}
	return out, nil
}

var cursorKinds = []string{"datetime", "unix", "integer", "string"}

func compilePartitions(cc *compiler, where string, s *streamSpec) (*partitions, error) {
	p := s.Partitions
	if p == nil {
		return nil, nil
	}
	where += ": partitions"
	if s.Parent != nil {
		return nil, fmt.Errorf("%s: a child stream can't have partitions", where)
	}
	out := &partitions{}
	var err error
	if (p.Values == "") == (p.Range == nil) {
		return nil, fmt.Errorf("%s: set either values or range", where)
	}
	if p.Values != "" {
		out.values, err = cc.expr(where+".values", p.Values, claimVars, cel.ListType(cel.DynType))
		return out, err
	}
	if out.step, err = time.ParseDuration(p.Range.Step); err != nil || out.step <= 0 {
		return nil, fmt.Errorf("%s.range.step: %q isn't a positive duration", where, p.Range.Step)
	}
	if out.start, err = cc.expr(where+".range.start", p.Range.Start, claimVars, cel.TimestampType); err != nil {
		return nil, err
	}
	if p.Range.End != "" {
		if out.end, err = cc.expr(where+".range.end", p.Range.End, claimVars, cel.TimestampType); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (c *Connector) compileStream(cc *compiler, s *streamSpec, parent *stream, httpRules []*rule) (*stream, error) {
	where := "stream " + s.Name
	child := parent != nil
	vars := func(base []string) []string { return forStream(base, child, s.Partitions != nil) }
	st := &stream{name: s.Name, primaryKey: s.PrimaryKey, method: s.Request.Method, idempotent: s.Request.Idempotent, parent: parent}
	if st.method == "" {
		st.method = "GET"
	}
	var err error
	if st.partitions, err = compilePartitions(cc, where, s); err != nil {
		return nil, err
	}
	if st.path, err = cc.template(where+": request.path", s.Request.Path, vars(requestVars)); err != nil {
		return nil, err
	}
	if st.params, err = namedTemplates(cc, where+": request.params", s.Request.Params, vars(requestVars)); err != nil {
		return nil, err
	}
	if st.headers, err = namedTemplates(cc, where+": request.headers", s.Request.Headers, vars(requestVars)); err != nil {
		return nil, err
	}
	if s.Request.Body != "" {
		if st.method == "GET" {
			return nil, fmt.Errorf("%s: a GET request has no body", where)
		}
		if st.body, err = cc.expr(where+": request.body", s.Request.Body, vars(requestVars)); err != nil {
			return nil, err
		}
	}
	if s.Records != "" {
		if st.records, err = cc.expr(where+": records", s.Records, vars(responseVars), cel.ListType(cel.DynType)); err != nil {
			return nil, err
		}
	}
	if p := s.Paginator; p != nil {
		st.pager = pager{kind: p.Type, param: p.Param, size: p.Size, sizeParam: p.SizeParam}
		if p.Next != "" {
			if st.pager.next, err = cc.expr(where+": paginator.next", p.Next, vars(pagerVars)); err != nil {
				return nil, err
			}
		}
		if p.Stop != "" {
			if st.pager.stop, err = cc.expr(where+": paginator.stop", p.Stop, vars(pagerVars), cel.BoolType); err != nil {
				return nil, err
			}
		}
		switch p.Type {
		case "offset":
			st.pager.param = cmp.Or(p.Param, "offset")
		case "page":
			st.pager.param, st.pager.start = cmp.Or(p.Param, "page"), 1
		}
		if p.Start != nil {
			st.pager.start = *p.Start
		}
		if p.SizeParam != "" && p.Size == 0 {
			return nil, fmt.Errorf("%s: paginator.size_param needs a size", where)
		}
	}
	if inc := s.Incremental; inc != nil {
		st.inc = &incremental{field: inc.CursorField, kind: inc.Type, ordered: inc.Ordered}
		if inc.Lookback != "" {
			if inc.Type != "datetime" && inc.Type != "unix" {
				return nil, fmt.Errorf("%s: incremental.lookback is for datetime and unix cursors", where)
			}
			if st.inc.lookback, err = time.ParseDuration(inc.Lookback); err != nil {
				return nil, fmt.Errorf("%s: incremental.lookback: %w", where, err)
			}
		}
		if child && inc.Ordered {
			return nil, fmt.Errorf("%s: a child stream's records aren't ordered across parents, so it can't be ordered", where)
		}
		if !slices.Contains(cursorKinds, inc.Type) {
			return nil, fmt.Errorf("%s: incremental.type %q", where, inc.Type)
		}
	}
	rules, err := compileRules(cc, where+": errors", s.Errors, vars(responseVars))
	if err != nil {
		return nil, err
	}
	st.rules = append(rules, httpRules...)
	for _, r := range st.rules {
		if r.action == "expired" && st.pager.kind == "" {
			return nil, fmt.Errorf("%s: an expired rule needs a paginator", where)
		}
	}
	if len(s.Schema) > 0 {
		if st.schema, err = arrowSchema(s.Schema); err != nil {
			return nil, fmt.Errorf("%s: schema: %w", where, err)
		}
	}
	return st, nil
}

// decodeJSON decodes JSON, keeping numbers as json.Number so they keep
// their digits.
func decodeJSON(b []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("more than one JSON value")
	}
	return v, nil
}
