package conformance

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openetl-io/openetl/sdk/go/configschema"
	"github.com/openetl-io/openetl/sdk/go/internal/rpcerr"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

const startTimeout = time.Minute

// common is what both suites share: the connector running with the good
// config, and the checks every connector gets.
type common struct {
	launch                              Launcher
	kind                                protocolv1.ConnectorKind
	config, badCredentials, unreachable []byte
	// The codes a config whose upstream doesn't answer may fail with.
	unreachableCodes []protocolv1.ErrorCode

	mu      sync.Mutex
	proc    Process
	procErr error
	spec    *protocolv1.SpecResponse
	// Pipelines the checks used, for teardown.
	pipelines []string
	// Run when the suite ends.
	closers []func()
}

func (c *common) process(t T) Process {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.proc == nil && c.procErr == nil {
		ctx, cancel := context.WithTimeout(t.Context(), startTimeout)
		defer cancel()
		c.proc, c.procErr = c.launch(ctx, c.config)
		if c.procErr == nil {
			c.spec, c.procErr = protocolv1.NewConnectorServiceClient(c.proc.Conn()).Spec(ctx, &protocolv1.SpecRequest{Host: Host})
			c.procErr = rpcerr.FromStatus(c.procErr)
		}
	}
	if c.procErr != nil {
		t.Fatalf("start the connector: %v", c.procErr)
	}
	logOnFailure(t, c.proc)
	return c.proc
}

// another starts the connector with config for one check.
func (c *common) another(t T, config []byte) Process {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), startTimeout)
	defer cancel()
	p, err := c.launch(ctx, config)
	if err != nil {
		t.Fatalf("start the connector: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	logOnFailure(t, p)
	return p
}

func logOnFailure(t T, p Process) {
	t.Cleanup(func() {
		if out := p.Output(); t.Failed() && out != "" {
			if len(out) > 8<<10 {
				out = "…" + out[len(out)-8<<10:]
			}
			t.Logf("the connector logged:\n%s", out)
		}
	})
}

func (c *common) pipeline() string {
	id := newID()
	c.mu.Lock()
	c.pipelines = append(c.pipelines, id)
	c.mu.Unlock()
	return id
}

func (c *common) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.closers) - 1; i >= 0; i-- {
		c.closers[i]()
	}
	if c.proc != nil {
		if c.spec.GetSetup() || c.kind == protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			cc := protocolv1.NewConnectorServiceClient(c.proc.Conn())
			for _, p := range c.pipelines {
				_, _ = cc.Teardown(ctx, &protocolv1.TeardownRequest{Config: c.proc.Config(), Pipeline: p})
			}
			cancel()
		}
		_ = c.proc.Close()
	}
}

func (c *common) checks() []Check {
	return []Check{
		{"spec", c.checkSpec},
		{"check", c.checkCheck},
		{"errors", c.checkErrors},
		{"setup", c.checkSetup},
	}
}

// Spec: the spec is v1 and of the right kind, its config schema compiles,
// and the suite's configs match it.
func (c *common) checkSpec(t T) {
	c.process(t)
	spec := c.spec
	if v := spec.GetProtocolVersion(); v != "v1" {
		t.Errorf("protocol_version is %q, not v1", v)
	}
	if spec.GetKind() != c.kind {
		t.Fatalf("the connector is a %s, not a %s", spec.GetKind(), c.kind)
	}
	if spec.GetEgress().GetTier() == protocolv1.EgressTier_EGRESS_TIER_UNSPECIFIED {
		t.Errorf("the spec has no egress tier")
	}
	if len(spec.GetConfigSchema()) == 0 {
		flagf(t, "the spec has no config schema, so the platform can't validate configs")
	} else if sch, err := compileSchema(spec.GetConfigSchema()); err != nil {
		t.Errorf("the config schema doesn't compile: %v", err)
	} else {
		for _, p := range configschema.Lint(spec.GetConfigSchema()) {
			t.Errorf("%v", p)
		}
		if form, err := configschema.ReadForm(spec.GetConfigSchema()); err == nil {
			if u := configschema.Undescribed(form); len(u) > 0 {
				flagf(t, "config fields without a description or title, which a form can only show by name: %s", strings.Join(u, ", "))
			}
		}
		for _, cfg := range []struct {
			name string
			b    []byte
		}{{"Config", c.config}, {"BadCredentials", c.badCredentials}, {"Unreachable", c.unreachable}} {
			if cfg.b == nil {
				continue
			}
			if err := validate(sch, cfg.b); err != nil {
				t.Errorf("%s doesn't match the config schema: %v", cfg.name, err)
			}
		}
	}
	if c.kind == protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION {
		checkCapabilities(t, spec.GetDestination())
	}
}

func checkCapabilities(t T, dc *protocolv1.DestinationCapabilities) {
	t.Helper()
	if dc == nil {
		t.Fatalf("a destination's spec has no capabilities")
	}
	if dc.GetSinkClass() == protocolv1.SinkClass_SINK_CLASS_UNSPECIFIED {
		t.Errorf("the capabilities have no sink class")
	}
	if len(dc.GetScopes()) == 0 || slices.Contains(dc.GetScopes(), protocolv1.CommitScope_COMMIT_SCOPE_UNSPECIFIED) {
		t.Errorf("the commit scopes %v name none, or an unspecified one", dc.GetScopes())
	}
	if slices.Contains(dc.GetWriteModes(), protocolv1.WriteMode_WRITE_MODE_UNSPECIFIED) {
		t.Errorf("the write modes %v name an unspecified one", dc.GetWriteModes())
	}
	lo, def, hi := dc.GetMinCommitInterval().AsDuration(), dc.GetDefaultCommitInterval().AsDuration(), dc.GetMaxCommitInterval().AsDuration()
	if def > 0 && (def < lo || hi > 0 && def > hi) {
		t.Errorf("the default commit interval %s isn't between the minimum %s and the maximum %s", def, lo, hi)
	}
}

// Check: a good config passes: none of the connector's items fails, and a
// warning is logged. Here, in errors and in preflight, the answer holds to
// the protocol, as checkItems checks.
func (c *common) checkCheck(t T) {
	p := c.process(t)
	resp, err := protocolv1.NewConnectorServiceClient(p.Conn()).Check(t.Context(), &protocolv1.CheckRequest{Config: p.Config()})
	if err != nil {
		t.Fatalf("Check failed as an RPC, not in its response: %v", rpcerr.FromStatus(err))
	}
	c.passed(t, "Check", resp)
}

// passed reports an answer of what, Check or a preflight, with the good
// config that isn't well formed or doesn't pass.
func (c *common) passed(t T, what string, resp answer) {
	t.Helper()
	const with = "with the good config"
	c.checkAnswer(t, with, c.config, resp)
	items := resp.GetItems()
	// With a failed item, the loop says so; without one, the checks didn't
	// finish.
	_, first := firstFailed(items)
	if e := resp.GetError(); e != nil && first == nil {
		t.Errorf("%s failed %s: %s: %s", what, with, e.GetCode(), e.GetMessage())
	}
	for i, it := range items {
		switch it.GetStatus() {
		case protocolv1.CheckStatus_CHECK_STATUS_FAILED:
			t.Errorf("%s, %s's %s failed with %s: %s", with, what, label(i, it), it.GetError().GetCode(), itemText(it))
		case protocolv1.CheckStatus_CHECK_STATUS_WARNING:
			t.Logf("%s, %s's %s warned: %s", with, what, label(i, it), itemText(it))
		}
	}
}

// preflighted reports a preflight's answer with the good config, as passed
// does. A connector that answers Unimplemented has no preflight, so the
// check is skipped.
func (c *common) preflighted(t T, resp answer, err error) {
	t.Helper()
	if status.Code(err) == codes.Unimplemented {
		t.Skipf("the connector has no preflight: it answers Unimplemented")
	}
	if err != nil {
		t.Fatalf("the preflight failed as an RPC, not in its response: %v", rpcerr.FromStatus(err))
	}
	c.passed(t, "the preflight", resp)
}

// How long errors gives Check with a config whose upstream doesn't answer,
// and how much longer than that it may take to answer.
const (
	answerTime  = 5 * time.Second
	answerSlack = time.Second
)

// Errors: bad credentials are AUTH_INVALID and an upstream that doesn't
// answer is retryable, both from Check and from the connector's work. With
// items, the code is the response's error's, or else the first failed
// item's. With an upstream that doesn't answer, Check given answerTime
// answers in time, rather than running out of it as an RPC.
func (c *common) checkErrors(t T) {
	if c.badCredentials == nil && c.unreachable == nil {
		t.Skipf("the suite has neither BadCredentials nor Unreachable")
	}
	c.process(t)
	cases := []struct {
		name   string
		config []byte
		codes  []protocolv1.ErrorCode
		// Check is given answerTime too.
		timed bool
	}{
		{"bad credentials", c.badCredentials, []protocolv1.ErrorCode{protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID}, false},
		{"an upstream that doesn't answer", c.unreachable, c.unreachableCodes, true},
	}
	for _, tc := range cases {
		if tc.config == nil {
			continue
		}
		p := c.another(t, tc.config)
		with := "with " + tc.name
		resp, err := protocolv1.NewConnectorServiceClient(p.Conn()).Check(t.Context(), &protocolv1.CheckRequest{Config: p.Config()})
		if err != nil {
			t.Errorf("%s, Check failed as an RPC, not in its response: %v", with, rpcerr.FromStatus(err))
		} else {
			c.checkAnswer(t, with, tc.config, resp)
			c.failedWith(t, with, resp, tc.codes)
		}
		if tc.timed {
			c.answersInTime(t, with, p, tc.config)
		}
		if err := c.work(t, p); err == nil {
			t.Errorf("with %s, the connector's work succeeded", tc.name)
		} else if code := codeOf(err); !slices.Contains(tc.codes, code) {
			t.Errorf("with %s, the connector's work failed with %s, not %v: %v", tc.name, code, tc.codes, err)
		}
	}
}

// failedWith reports a Check answer that passed, or failed with a code
// other than codes: the response's error's, or without one, which
// checkAnswer reports, the first failed item's.
func (c *common) failedWith(t T, with string, resp answer, codes []protocolv1.ErrorCode) {
	t.Helper()
	i, first := firstFailed(resp.GetItems())
	switch e := resp.GetError(); {
	case e == nil && first == nil:
		t.Errorf("%s, Check passed", with)
	case e == nil:
		if code := first.GetError().GetCode(); !slices.Contains(codes, code) {
			t.Errorf("%s, %s failed with %s, not %v: %s", with, label(i, first), code, codes, itemText(first))
		}
	case !slices.Contains(codes, e.GetCode()) && first != nil:
		t.Errorf("%s, %s failed with %s, not %v: %s", with, label(i, first), e.GetCode(), codes, e.GetMessage())
	case !slices.Contains(codes, e.GetCode()):
		t.Errorf("%s, Check failed with %s, not %v: %s", with, e.GetCode(), codes, e.GetMessage())
	}
}

// answersInTime reports a Check with config, given answerTime, that runs out
// of it as an RPC, or answers more than answerSlack late, rather than
// answering in time with what it checked.
func (c *common) answersInTime(t T, with string, p Process, config []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), answerTime)
	defer cancel()
	start := time.Now()
	resp, err := protocolv1.NewConnectorServiceClient(p.Conn()).Check(ctx, &protocolv1.CheckRequest{Config: p.Config()})
	took := time.Since(start)
	switch {
	case status.Code(err) == codes.DeadlineExceeded || err != nil && ctx.Err() != nil:
		t.Errorf("%s and %s to answer, Check ran out of time as an RPC: it should stop its checks a little before the RPC's deadline, and answer with what it checked", with, answerTime)
	case err != nil:
		t.Errorf("%s and %s to answer, Check failed as an RPC, not in its response: %v", with, answerTime, rpcerr.FromStatus(err))
	case took > answerTime+answerSlack:
		t.Errorf("%s and %s to answer, Check answered after %s", with, answerTime, took.Round(time.Millisecond))
	default:
		c.checkAnswer(t, with+" and "+answerTime.String()+" to answer", config, resp)
	}
}

// work is a source's read, or a destination's recovery and an epoch's
// writes: what fails at run time when Check would have.
func (c *common) work(t T, p Process) error {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	if c.kind == protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE {
		res, err := read(ctx, p.Conn(), &protocolv1.ReadRequest{Config: p.Config(), Pipeline: c.pipeline(), Streams: c.anyStreams(t)})
		if err != nil {
			return err
		}
		return res.err
	}
	open := &protocolv1.WriteOpen{
		Config: p.Config(), Pipeline: newID(), Namespace: randomName(), Generation: 1,
		Scope: c.spec.GetDestination().GetScopes()[0],
	}
	for _, name := range kitStreams {
		open.Streams = append(open.Streams, &protocolv1.WriteStream{Name: name, Mode: protocolv1.WriteMode_WRITE_MODE_APPEND, PrimaryKey: []string{"id"}})
	}
	w, err := openWriter(ctx, p.Conn(), open)
	if err != nil {
		return err
	}
	defer w.crash()
	if _, err := w.recover(nil, nil); err != nil {
		return err
	}
	if err := w.startReads(kitStreams, time.Now()); err != nil {
		return err
	}
	b := buildBatch(kitStreams[0], records(0, 1, "a"), "insert")
	defer b.Release()
	e := epoch(1, 1)
	if c.spec.GetDestination().GetDelivery() != protocolv1.Delivery_DELIVERY_STAGED {
		if err := w.batch(e, kitStreams[0], b); err != nil {
			return err
		}
		_, err = w.preCommit(e)
		return err
	}
	// A staged destination may reach its upstream only when it delivers, in
	// commit.
	a, err := attachStaging(p)
	if err != nil {
		return err
	}
	defer a.close()
	files, err := a.stage(e, kitStreams[0], 0, 1, []arrow.RecordBatch{b})
	if err != nil {
		return err
	}
	if err := w.stagedFiles(e, kitStreams[0], files); err != nil {
		return err
	}
	staged, err := w.preCommit(e)
	if err != nil {
		return err
	}
	return w.commit(e, staged)
}

// anyStreams are what a read with another config asks for: the good
// config's catalog's streams.
func (c *common) anyStreams(t T) []*protocolv1.ConfiguredStream {
	t.Helper()
	cat, err := protocolv1.NewSourceServiceClient(c.proc.Conn()).Discover(t.Context(), &protocolv1.DiscoverRequest{Config: c.proc.Config()})
	if err != nil {
		t.Fatalf("Discover: %v", rpcerr.FromStatus(err))
	}
	var out []*protocolv1.ConfiguredStream
	for _, s := range cat.GetCatalog().GetStreams() {
		mode := protocolv1.SyncMode_SYNC_MODE_FULL_REFRESH
		if len(s.GetSyncModes()) > 0 {
			mode = s.GetSyncModes()[0]
		}
		out = append(out, &protocolv1.ConfiguredStream{Name: s.GetName(), SyncMode: mode, Cursor: s.GetDefaultCursor()})
	}
	return out
}

// Setup: Setup and Teardown are idempotent.
func (c *common) checkSetup(t T) {
	p := c.process(t)
	if !c.spec.GetSetup() {
		t.Skipf("the connector has no setup")
	}
	cc := protocolv1.NewConnectorServiceClient(p.Conn())
	pipeline := c.pipeline()
	for i := range 2 {
		if _, err := cc.Setup(t.Context(), &protocolv1.SetupRequest{Config: p.Config(), Pipeline: pipeline}); err != nil {
			t.Fatalf("Setup, time %d: %v", i+1, rpcerr.FromStatus(err))
		}
	}
	for i := range 2 {
		if _, err := cc.Teardown(t.Context(), &protocolv1.TeardownRequest{Config: p.Config(), Pipeline: pipeline}); err != nil {
			t.Fatalf("Teardown, time %d: %v", i+1, rpcerr.FromStatus(err))
		}
	}
}

func codeOf(err error) protocolv1.ErrorCode {
	if c, ok := errors.AsType[interface {
		error
		ErrorCode() protocolv1.ErrorCode
	}](err); ok {
		return c.ErrorCode()
	}
	return protocolv1.ErrorCode_ERROR_CODE_INTERNAL
}

func compileSchema(schema []byte) (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schema))
	if err != nil {
		return nil, err
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource("schema.json", doc); err != nil {
		return nil, err
	}
	return c.Compile("schema.json")
}

func validate(sch *jsonschema.Schema, doc []byte) error {
	v, err := jsonschema.UnmarshalJSON(bytes.NewReader(doc))
	if err != nil {
		return err
	}
	return sch.Validate(v)
}
