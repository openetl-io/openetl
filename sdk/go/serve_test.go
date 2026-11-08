package sdk

import (
	"context"
	"strings"
	"testing"

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
