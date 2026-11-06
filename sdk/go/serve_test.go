package sdk

import (
	"testing"

	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

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
