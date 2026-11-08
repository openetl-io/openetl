package activities

import (
	"context"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/store/db"
)

// ListRetentionProbes claims the pipelines a retention probe asks about, and
// records an operation for each, which lets its runner read the source's
// config and report what it found. A pipeline claimed isn't claimed again
// for in.Before's distance from now, even if this step is retried.
func (a *Activities) ListRetentionProbes(ctx context.Context, in contract.ListRetentionProbesInput) ([]contract.ConnectorOpInput, error) {
	var out []contract.ConnectorOpInput
	err := a.Store.Tx(ctx, func(q *db.Queries) error {
		out = nil
		rows, err := q.ClaimRetentionProbes(ctx, db.ClaimRetentionProbesParams{Before: in.Before, MaxRows: in.Max})
		if err != nil {
			return err
		}
		// The workflow asks one at a time, so the last may start a while
		// after this.
		expire := time.Now().Add(contract.RetentionProbeEvery + contract.OpTimeout)
		for _, r := range rows {
			var c apiv1.ConnectorConfig
			if err := protojson.Unmarshal(r.SourceSpec, &c); err != nil {
				return err
			}
			id := uuid.New()
			src, pipeline := r.SourceID, r.ID
			if err := q.InsertConnectorOp(ctx, db.InsertConnectorOpParams{
				OrgID: r.OrgID, ID: id, DataPlaneID: r.DataPlaneID, SourceID: &src, PipelineID: &pipeline, ExpireTime: expire,
			}); err != nil {
				return err
			}
			out = append(out, contract.ConnectorOpInput{
				OpID: id.String(), OrgID: r.OrgID.String(), WorkspaceID: r.WorkspaceID.String(), DataPlane: r.DataPlaneID,
				Connector: c.GetConnector(), Op: contract.OpRetention, PipelineID: r.ID.String(), SourceID: r.SourceID.String(),
			})
		}
		return nil
	})
	return out, err
}
