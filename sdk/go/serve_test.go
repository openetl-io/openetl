package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// A configured stream's declared types decode to its Schema; none stays nil,
// and bytes that aren't a schema are an error, not a read without types.
func TestConfiguredStreamSchema(t *testing.T) {
	declared := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 38, Scale: 9}, Nullable: true},
	}, nil)
	b, err := arrowipc.EncodeSchema(declared)
	if err != nil {
		t.Fatal(err)
	}
	cs, err := configuredStream(&protocolv1.ConfiguredStream{Name: "s", Columns: []string{"id", "amount"}, ArrowSchema: b})
	if err != nil || cs.Schema == nil || !cs.Schema.Equal(declared) {
		t.Errorf("decoded %v, %v; want %v", cs.Schema, err, declared)
	}
	if cs, err := configuredStream(&protocolv1.ConfiguredStream{Name: "s"}); err != nil || cs.Schema != nil {
		t.Errorf("without types: %v, %v", cs.Schema, err)
	}
	if _, err := configuredStream(&protocolv1.ConfiguredStream{Name: "s", ArrowSchema: []byte("not arrow")}); err == nil {
		t.Error("bytes that aren't a schema decoded")
	}
}

type preparedDestination struct {
	destination.Destination
	calls *[]string
}

func (d preparedDestination) Setup(_ context.Context, pipeline string) error {
	*d.calls = append(*d.calls, "setup "+pipeline)
	return nil
}

func (d preparedDestination) Teardown(_ context.Context, pipeline string) error {
	*d.calls = append(*d.calls, "teardown "+pipeline)
	return nil
}

func TestSetupAndTeardown(t *testing.T) {
	var calls []string
	b, err := newBase(Connector{
		Setup: true,
		Destination: func([]byte, host.Host) (destination.Destination, error) {
			return preparedDestination{calls: &calls}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	spec, err := b.Spec(t.Context(), &protocolv1.SpecRequest{})
	if err != nil || !spec.GetSetup() {
		t.Fatalf("spec %v, %v", spec, err)
	}
	if _, err := b.Setup(t.Context(), &protocolv1.SetupRequest{Config: []byte("{}"), Pipeline: "p"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Teardown(t.Context(), &protocolv1.TeardownRequest{Config: []byte("{}"), Pipeline: "p"}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(calls, ", ") != "setup p, teardown p" {
		t.Errorf("calls %v", calls)
	}
}

func pass(context.Context, *check.Probe) error { return nil }

// checkedDestination's checks and preflight are what its fields give.
type checkedDestination struct {
	destination.Destination
	checks    func() []check.Check
	preflight func(destination.PreflightRequest) []check.Check
}

func (d checkedDestination) Checks() []check.Check { return d.checks() }

func (d checkedDestination) Preflight(req destination.PreflightRequest) []check.Check {
	return d.preflight(req)
}

func destinationBase(t *testing.T, d checkedDestination) *destinationServer {
	t.Helper()
	b, err := newBase(Connector{Destination: func([]byte, host.Host) (destination.Destination, error) { return d, nil }})
	if err != nil {
		t.Fatal(err)
	}
	return &destinationServer{base: b}
}

func TestCheckItems(t *testing.T) {
	d := destinationBase(t, checkedDestination{checks: func() []check.Check {
		return []check.Check{
			{Name: "connect", Kind: check.Connection, Title: "Reach {address}", Params: check.Params{"address": "db:5432"}, Run: func(_ context.Context, p *check.Probe) error {
				p.Param("version", "17")
				p.Found("PostgreSQL {version}")
				return nil
			}},
			{Name: "write", Kind: check.Permission, Title: "Write to {table}", Field: "role", Params: check.Params{"table": "orders"}, Run: func(context.Context, *check.Probe) error {
				return check.PrivilegeMissing.Wrap(errors.New("pq: denied"), "principal", "etl", "privilege", "INSERT", "object", "orders")
			}},
			{Name: "lag", Title: "Replicate", Needs: []string{"write"}, Run: pass},
		}
	}})
	resp, err := d.Check(t.Context(), &protocolv1.CheckRequest{Config: []byte("{}")})
	if err != nil {
		t.Fatal(err)
	}
	if e := resp.GetError(); e.GetCode() != protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED || e.GetMessage() != "Write to orders: etl has no INSERT on orders: pq: denied" {
		t.Errorf("error %v, want the failed item's", e)
	}
	items := resp.GetItems()
	if len(items) != 3 {
		t.Fatalf("items %v", items)
	}
	if c := items[0]; c.GetStatus() != protocolv1.CheckStatus_CHECK_STATUS_PASSED || c.GetMessage() != "PostgreSQL {version}" || c.GetParams()["version"] != "17" || c.GetError() != nil {
		t.Errorf("passed item %v", c)
	}
	w := items[1]
	if w.GetStatus() != protocolv1.CheckStatus_CHECK_STATUS_FAILED || w.GetFix() != check.PrivilegeMissing.Fix || w.GetField() != "role" || w.GetReason() != "privilege_missing" ||
		w.GetError().GetCode() != protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED || w.GetError().GetMessage() != "etl has no INSERT on orders." || w.GetDetail() != "pq: denied" {
		t.Errorf("failed item %v", w)
	}
	if s := items[2]; s.GetStatus() != protocolv1.CheckStatus_CHECK_STATUS_SKIPPED || s.GetMessage() != "needs write, which failed" {
		t.Errorf("skipped item %v", s)
	}
}

func TestPreflight(t *testing.T) {
	src := &Source[struct{}]{
		Name: "acme",
		Preflight: func(_ *struct{}, _ host.Host, req source.PreflightRequest) []check.Check {
			checks := []check.Check{{Name: "schedule", Title: "Keep up every {interval}", Params: check.Params{"interval": req.Interval.String()}, Advisory: true,
				Run: func(context.Context, *check.Probe) error { return errors.New("the retention is shorter") }}}
			for _, s := range req.Streams {
				checks = append(checks, check.Check{Name: "stream." + s.Name, Title: "Read {stream}", Params: check.Params{"stream": s.Name}, Run: func(_ context.Context, p *check.Probe) error {
					p.Found("by " + s.Cursor)
					return nil
				}})
			}
			return checks
		},
	}
	b, err := newBase(Connector{Source: func(c []byte, h host.Host) (source.Source, error) { return src.Bind(c, h) }})
	if err != nil {
		t.Fatal(err)
	}
	ss := &sourceServer{base: b}
	resp, err := ss.PreflightSource(t.Context(), &protocolv1.PreflightSourceRequest{
		Config: []byte("{}"), Pipeline: "p", Interval: durationpb.New(time.Hour),
		Streams: []*protocolv1.ConfiguredStream{{Name: "orders", Cursor: "seq"}},
	})
	if err != nil || resp.GetError() != nil || len(resp.GetItems()) != 2 {
		t.Fatalf("source preflight %v, %v", resp, err)
	}
	if w, s := resp.GetItems()[0], resp.GetItems()[1]; w.GetStatus() != protocolv1.CheckStatus_CHECK_STATUS_WARNING || w.GetParams()["interval"] != "1h0m0s" ||
		s.GetStatus() != protocolv1.CheckStatus_CHECK_STATUS_PASSED || s.GetMessage() != "by seq" {
		t.Errorf("source preflight's items %v", resp.GetItems())
	}
	// The same source without a preflight, and without checks, answers with
	// no items.
	src.Preflight = nil
	if resp, err := ss.PreflightSource(t.Context(), &protocolv1.PreflightSourceRequest{Config: []byte("{}")}); err != nil || len(resp.GetItems()) != 0 || resp.GetError() != nil {
		t.Errorf("a source without a preflight: %v, %v", resp, err)
	}
	if resp, err := ss.Check(t.Context(), &protocolv1.CheckRequest{Config: []byte("{}")}); err != nil || len(resp.GetItems()) != 0 || resp.GetError() != nil {
		t.Errorf("a source without checks: %v, %v", resp, err)
	}

	ds := destinationBase(t, checkedDestination{preflight: func(req destination.PreflightRequest) []check.Check {
		var checks []check.Check
		for _, s := range req.Streams {
			checks = append(checks, check.Check{Name: "stream." + s.Name, Title: "Write {stream}", Params: check.Params{"stream": s.Name}, Run: func(_ context.Context, p *check.Probe) error {
				if req.Schemas[s.Name] == nil {
					return check.NotFound.Fail("resource", "the schema of "+s.Name)
				}
				p.Found(req.Namespace + "." + s.Name)
				return nil
			}})
		}
		return checks
	}})
	schema, err := arrowipc.EncodeSchema(arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil))
	if err != nil {
		t.Fatal(err)
	}
	dresp, err := ds.PreflightDestination(t.Context(), &protocolv1.PreflightDestinationRequest{
		Config: []byte("{}"), Namespace: "ns",
		Streams: []*protocolv1.WriteStream{{Name: "orders"}, {Name: "users"}},
		Schemas: []*protocolv1.StreamSchema{{Name: "orders", ArrowSchema: schema}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if items := dresp.GetItems(); len(items) != 2 || items[0].GetMessage() != "ns.orders" || items[1].GetStatus() != protocolv1.CheckStatus_CHECK_STATUS_FAILED ||
		dresp.GetError().GetCode() != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID || dresp.GetError().GetMessage() != "Write users: the schema of users doesn't exist." {
		t.Errorf("destination preflight %v", dresp)
	}
}

// A panic listing the checks, or in Check, is an answer's error, not a
// crash.
func TestPanicsAreAnswers(t *testing.T) {
	d := destinationBase(t, checkedDestination{
		checks:    func() []check.Check { panic("listing") },
		preflight: func(destination.PreflightRequest) []check.Check { panic("preflight") },
	})
	resp, err := d.Check(t.Context(), &protocolv1.CheckRequest{Config: []byte("{}")})
	if err != nil || len(resp.GetItems()) != 0 || resp.GetError().GetCode() != protocolv1.ErrorCode_ERROR_CODE_INTERNAL || !strings.Contains(resp.GetError().GetMessage(), "listing") {
		t.Errorf("a panic in Checks: %v, %v", resp, err)
	}
	presp, err := d.PreflightDestination(t.Context(), &protocolv1.PreflightDestinationRequest{Config: []byte("{}")})
	if err != nil || len(presp.GetItems()) != 0 || presp.GetError().GetCode() != protocolv1.ErrorCode_ERROR_CODE_INTERNAL || !strings.Contains(presp.GetError().GetMessage(), "preflight") {
		t.Errorf("a panic in Preflight: %v, %v", presp, err)
	}
	b, err := newBase(Connector{Source: func([]byte, host.Host) (source.Source, error) { return panicky{}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := b.Check(t.Context(), &protocolv1.CheckRequest{Config: []byte("{}")}); err != nil || resp.GetError().GetCode() != protocolv1.ErrorCode_ERROR_CODE_INTERNAL {
		t.Errorf("a panic in Check: %v, %v", resp, err)
	}
}

type panicky struct{ source.Source }

func (panicky) Check(context.Context) error { panic("checking") }

// A check that doesn't return in time leaves the answer time to say so,
// with what ran before it.
func TestCheckAnswersBeforeTheDeadline(t *testing.T) {
	hang := make(chan struct{})
	defer close(hang)
	d := destinationBase(t, checkedDestination{checks: func() []check.Check {
		return []check.Check{
			{Name: "connect", Title: "Connect", Run: pass},
			{Name: "query", Title: "Query", Run: func(context.Context, *check.Probe) error { <-hang; return nil }},
			{Name: "write", Title: "Write", Run: pass},
		}
	}})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	resp, err := d.Check(ctx, &protocolv1.CheckRequest{Config: []byte("{}")})
	if err != nil || ctx.Err() != nil {
		t.Fatalf("Check %v, %v: it didn't answer before the deadline", err, ctx.Err())
	}
	var got []string
	for _, it := range resp.GetItems() {
		got = append(got, it.GetName()+"="+strings.ToLower(strings.TrimPrefix(it.GetStatus().String(), "CHECK_STATUS_")))
	}
	if strings.Join(got, " ") != "connect=passed query=failed write=skipped" || resp.GetItems()[1].GetReason() != "timed_out" || resp.GetError().GetCode() != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
		t.Errorf("items %s, error %v", got, resp.GetError())
	}
}

// secretConn is host services that give each secret's name back as its
// value, with "-value" added.
type secretConn struct{ grpc.ClientConnInterface }

func (secretConn) Invoke(_ context.Context, _ string, args, reply any, _ ...grpc.CallOption) error {
	reply.(*protocolv1.GetSecretResponse).Value = []byte(args.(*protocolv1.GetSecretRequest).GetName() + "-value")
	return nil
}

// The values of the secrets the SDK put in a config are in none of a
// check's or a preflight's strings: not its own config's, while another's
// are no secret of this one.
func TestAnswersHideSecrets(t *testing.T) {
	leaky := func(password, other string) []check.Check {
		return []check.Check{
			{Name: "connect", Title: "Reach {dsn}", Description: "With " + password + " " + other, Params: check.Params{"dsn": "pg://etl:" + password + "@db"}, Run: func(_ context.Context, p *check.Probe) error {
				p.Found("signed in with " + password)
				return nil
			}},
			{Name: "auth", Title: "Sign in", Run: func(context.Context, *check.Probe) error {
				f := check.CredentialsRejected.Wrap(fmt.Errorf("password %q (%s) rejected", password, "p%40ss+1-value"), "password", password)
				f.Message, f.Fix = "{password} was rejected.", "Replace "+password+"."
				return f
			}},
		}
	}
	var password string
	b, err := newBase(Connector{
		ConfigSchema: []byte(`{"type": "object", "properties": {"password": {"type": "string", "x-secret": true}, "other": {"type": "string"}}}`),
		Egress:       Egress{Tier: EgressRaw},
		Destination: func(config []byte, _ host.Host) (destination.Destination, error) {
			var c struct{ Password, Other string }
			if err := json.Unmarshal(config, &c); err != nil {
				return nil, err
			}
			password = c.Password
			return checkedDestination{
				checks:    func() []check.Check { return leaky(c.Password, c.Other) },
				preflight: func(destination.PreflightRequest) []check.Check { return leaky(c.Password, c.Other) },
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	b.host = host.NewClient(secretConn{})
	d := &destinationServer{base: b}
	resp, err := d.Check(t.Context(), &protocolv1.CheckRequest{Config: []byte(`{"password": "p@ss+1", "other": "s3cr3t-value"}`)})
	if err != nil || password != "p@ss+1-value" {
		t.Fatalf("Check %v, the connector got %q", err, password)
	}
	presp, err := d.PreflightDestination(t.Context(), &protocolv1.PreflightDestinationRequest{Config: []byte(`{"password": "p@ss+1"}`)})
	if err != nil {
		t.Fatal(err)
	}
	for _, answer := range []interface {
		GetError() *protocolv1.Error
		GetItems() []*protocolv1.CheckItem
	}{resp, presp} {
		texts := []string{answer.GetError().GetMessage()}
		for _, it := range answer.GetItems() {
			texts = append(texts, it.GetTitle(), it.GetDescription(), it.GetMessage(), it.GetFix(), it.GetDetail(), it.GetError().GetMessage())
			for _, v := range it.GetParams() {
				texts = append(texts, v)
			}
		}
		for _, s := range texts {
			if strings.Contains(s, "p@ss+1-value") || strings.Contains(s, "p%40ss+1-value") || strings.Contains(s, "p%40ss%2B1-value") {
				t.Errorf("an answer shows the secret: %q", s)
			}
		}
		if m := answer.GetError().GetMessage(); m != `Sign in: [redacted] was rejected: password "[redacted]" ([redacted]) rejected` {
			t.Errorf("the answer's error %q", m)
		}
	}
	// "other" isn't a secret, so its value, though it reads like one, shows.
	if d := resp.GetItems()[0].GetDescription(); d != "With [redacted] s3cr3t-value" {
		t.Errorf("the description %q", d)
	}
}

// Serve and the runner convert these by number.
func TestNumbersMatchTheProtocol(t *testing.T) {
	pairs := []struct {
		name      string
		got, want int32
	}{
		{"exact", int32(source.Exact), int32(protocolv1.Replay_REPLAY_EXACT)},
		{"superset", int32(source.Superset), int32(protocolv1.Replay_REPLAY_SUPERSET)},
		{"full refresh", int32(source.FullRefresh), int32(protocolv1.SyncMode_SYNC_MODE_FULL_REFRESH)},
		{"incremental", int32(source.Incremental), int32(protocolv1.SyncMode_SYNC_MODE_INCREMENTAL)},
		{"cdc", int32(source.CDC), int32(protocolv1.SyncMode_SYNC_MODE_CDC)},
		{"stage and apply", int32(destination.StageAndApply), int32(protocolv1.SinkClass_SINK_CLASS_STAGE_AND_APPLY)},
		{"transactional stream", int32(destination.TransactionalStream), int32(protocolv1.SinkClass_SINK_CLASS_TRANSACTIONAL_STREAM)},
		{"deliver and ack", int32(destination.DeliverAndAck), int32(protocolv1.SinkClass_SINK_CLASS_DELIVER_AND_ACK)},
		{"pipeline scope", int32(destination.ScopePipeline), int32(protocolv1.CommitScope_COMMIT_SCOPE_PIPELINE)},
		{"stream scope", int32(destination.ScopeStream), int32(protocolv1.CommitScope_COMMIT_SCOPE_STREAM)},
		{"append", int32(destination.Append), int32(protocolv1.WriteMode_WRITE_MODE_APPEND)},
		{"overwrite", int32(destination.Overwrite), int32(protocolv1.WriteMode_WRITE_MODE_OVERWRITE)},
		{"merge", int32(destination.Merge), int32(protocolv1.WriteMode_WRITE_MODE_MERGE)},
		{"soft delete", int32(destination.SoftDelete), int32(protocolv1.DeleteMode_DELETE_MODE_SOFT)},
		{"hard delete", int32(destination.HardDelete), int32(protocolv1.DeleteMode_DELETE_MODE_HARD)},
		{"applied", int32(destination.Applied), int32(protocolv1.Recovery_RECOVERY_APPLIED)},
		{"redone", int32(destination.Redone), int32(protocolv1.Recovery_RECOVERY_REDONE)},
		{"lost", int32(destination.Lost), int32(protocolv1.Recovery_RECOVERY_LOST)},
		{"passed", int32(check.Passed), int32(protocolv1.CheckStatus_CHECK_STATUS_PASSED)},
		{"warning", int32(check.Warning), int32(protocolv1.CheckStatus_CHECK_STATUS_WARNING)},
		{"failed", int32(check.Failed), int32(protocolv1.CheckStatus_CHECK_STATUS_FAILED)},
		{"skipped", int32(check.Skipped), int32(protocolv1.CheckStatus_CHECK_STATUS_SKIPPED)},
		{"host tier", int32(EgressHost), int32(protocolv1.EgressTier_EGRESS_TIER_HOST)},
		{"proxy tier", int32(EgressProxy), int32(protocolv1.EgressTier_EGRESS_TIER_PROXY)},
		{"raw tier", int32(EgressRaw), int32(protocolv1.EgressTier_EGRESS_TIER_RAW)},
	}
	for _, p := range pairs {
		if p.got != p.want {
			t.Errorf("%s: %d, the protocol's %d", p.name, p.got, p.want)
		}
	}
}
