package conformance

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/santhosh-tekuri/jsonschema/v6"

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

// Check: a good config passes.
func (c *common) checkCheck(t T) {
	p := c.process(t)
	resp, err := protocolv1.NewConnectorServiceClient(p.Conn()).Check(t.Context(), &protocolv1.CheckRequest{Config: p.Config()})
	if err != nil {
		t.Fatalf("Check failed as an RPC, not in its response: %v", rpcerr.FromStatus(err))
	}
	if e := resp.GetError(); e != nil {
		t.Errorf("Check failed with the good config: %s: %s", e.GetCode(), e.GetMessage())
	}
}

// Errors: bad credentials are AUTH_INVALID and an upstream that doesn't
// answer is retryable, both from Check and from the connector's work.
func (c *common) checkErrors(t T) {
	if c.badCredentials == nil && c.unreachable == nil {
		t.Skipf("the suite has neither BadCredentials nor Unreachable")
	}
	c.process(t)
	cases := []struct {
		name   string
		config []byte
		codes  []protocolv1.ErrorCode
	}{
		{"bad credentials", c.badCredentials, []protocolv1.ErrorCode{protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID}},
		{"an upstream that doesn't answer", c.unreachable, c.unreachableCodes},
	}
	for _, tc := range cases {
		if tc.config == nil {
			continue
		}
		p := c.another(t, tc.config)
		resp, err := protocolv1.NewConnectorServiceClient(p.Conn()).Check(t.Context(), &protocolv1.CheckRequest{Config: p.Config()})
		switch {
		case err != nil:
			t.Errorf("with %s, Check failed as an RPC, not in its response: %v", tc.name, rpcerr.FromStatus(err))
		case resp.GetError() == nil:
			t.Errorf("with %s, Check passed", tc.name)
		case !slices.Contains(tc.codes, resp.GetError().GetCode()):
			t.Errorf("with %s, Check failed with %s, not %v: %s", tc.name, resp.GetError().GetCode(), tc.codes, resp.GetError().GetMessage())
		}
		if err := c.work(t, p); err == nil {
			t.Errorf("with %s, the connector's work succeeded", tc.name)
		} else if code := codeOf(err); !slices.Contains(tc.codes, code) {
			t.Errorf("with %s, the connector's work failed with %s, not %v: %v", tc.name, code, tc.codes, err)
		}
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
	files, err := a.stage(e, kitStreams[0], []arrow.RecordBatch{b})
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
