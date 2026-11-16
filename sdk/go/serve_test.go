package sdk

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

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

type reportingDestination struct{ destination.Destination }

func (reportingDestination) CheckReport(_ context.Context, r *check.Report) {
	r.Pass("connect", "Reach it", "")
	r.Fail("write", "Write", WithCode(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, check.Fix(errors.New("denied"), "grant it")), check.Field("role"))
}

func (reportingDestination) Preflight(_ context.Context, req destination.PreflightRequest, r *check.Report) {
	for _, s := range req.Streams {
		if req.Schemas[s.Name] == nil {
			r.Fail("stream."+s.Name, "Write "+s.Name, errors.New("no schema"))
			continue
		}
		r.Pass("stream."+s.Name, "Write "+s.Name, req.Namespace+"."+s.Name)
	}
}

type preflightedSource struct{ source.Source }

func (preflightedSource) Preflight(_ context.Context, req source.PreflightRequest, r *check.Report) {
	r.Warn("schedule", "Schedule", req.Interval.String())
	for _, s := range req.Streams {
		r.Pass("stream."+s.Name, "Read "+s.Name, s.Cursor)
	}
}

func TestPreflight(t *testing.T) {
	src, err := newBase(Connector{Source: func([]byte, host.Host) (source.Source, error) { return preflightedSource{}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	ss := &sourceServer{base: src}
	resp, err := ss.PreflightSource(t.Context(), &protocolv1.PreflightSourceRequest{
		Config: []byte("{}"), Pipeline: "p", Interval: durationpb.New(time.Hour),
		Streams: []*protocolv1.ConfiguredStream{{Name: "orders", Cursor: "seq"}},
	})
	if err != nil || resp.GetError() != nil || len(resp.GetItems()) != 2 || resp.GetItems()[0].GetMessage() != "1h0m0s" || resp.GetItems()[1].GetMessage() != "seq" {
		t.Errorf("source preflight %v, %v", resp, err)
	}

	dst, err := newBase(Connector{Destination: func([]byte, host.Host) (destination.Destination, error) { return reportingDestination{}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := arrowipc.EncodeSchema(arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil))
	if err != nil {
		t.Fatal(err)
	}
	ds := &destinationServer{base: dst}
	dresp, err := ds.PreflightDestination(t.Context(), &protocolv1.PreflightDestinationRequest{
		Config: []byte("{}"), Namespace: "ns",
		Streams: []*protocolv1.WriteStream{{Name: "orders"}, {Name: "users"}},
		Schemas: []*protocolv1.StreamSchema{{Name: "orders", ArrowSchema: schema}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if items := dresp.GetItems(); len(items) != 2 || items[0].GetMessage() != "ns.orders" || items[1].GetStatus() != protocolv1.CheckStatus_CHECK_STATUS_FAILED || dresp.GetError() == nil {
		t.Errorf("destination preflight %v", dresp)
	}

	plain, err := newBase(Connector{Source: func([]byte, host.Host) (source.Source, error) { return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := (&sourceServer{base: plain}).PreflightSource(t.Context(), &protocolv1.PreflightSourceRequest{Config: []byte("{}")}); err != nil || len(resp.GetItems()) != 0 || resp.GetError() != nil {
		t.Errorf("a source without a preflight: %v, %v", resp, err)
	}
}

func TestCheckReportsItems(t *testing.T) {
	b, err := newBase(Connector{
		Destination: func([]byte, host.Host) (destination.Destination, error) { return reportingDestination{}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := b.Check(t.Context(), &protocolv1.CheckRequest{Config: []byte("{}")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetError().GetCode() != protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Errorf("error %v, want the failed item's", resp.GetError())
	}
	items := resp.GetItems()
	if len(items) != 2 {
		t.Fatalf("items %v", items)
	}
	w := items[1]
	if w.GetStatus() != protocolv1.CheckStatus_CHECK_STATUS_FAILED || w.GetFix() != "grant it" || w.GetField() != "role" || w.GetError().GetCode() != protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Errorf("failed item %v", w)
	}
	if items[0].GetStatus() != protocolv1.CheckStatus_CHECK_STATUS_PASSED || items[0].GetError() != nil {
		t.Errorf("passed item %v", items[0])
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
