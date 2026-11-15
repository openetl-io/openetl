package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/store/db"
)

// How many lines a read of a quarantine file returns unless it says, and at
// most.
const (
	defaultQuarantineLines = 20
	maxQuarantineLines     = 100
)

// How often a read of a quarantine file looks at its work.
var quarantinePoll = 200 * time.Millisecond

// ReadQuarantineFile has a runner of the pipeline's data plane read the
// file's first lines. It sends the work without a workflow, so no history
// keeps the records: it waits for the work itself, and deletes it, outcome
// and all, as soon as it has the outcome, or gives up on it.
func (s *Server) ReadQuarantineFile(ctx context.Context, req *connect.Request[apiv1.ReadQuarantineFileRequest]) (*connect.Response[apiv1.ReadQuarantineFileResponse], error) {
	path := req.Msg.GetPath()
	audit(ctx).set("path", path)
	_, p, err := s.getPipeline(ctx, req.Msg.GetParent())
	if err != nil {
		return nil, err
	}
	limit := int(req.Msg.GetLimit())
	switch {
	case limit < 0:
		return nil, invalid("limit must not be negative")
	case limit == 0:
		limit = defaultQuarantineLines
	case limit > maxQuarantineLines:
		limit = maxQuarantineLines
	}
	if _, err := s.Store.GetQuarantineFile(ctx, db.GetQuarantineFileParams{OrgID: s.Tenant.Org, PipelineID: p.ID, Path: path}); errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("%s lists no quarantine file %q", req.Msg.GetParent(), path)
	} else if err != nil {
		return nil, err
	}
	if s.Dispatch == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("this server doesn't send work to runners"))
	}
	in := contract.ReadQuarantineInput{OrgID: s.Tenant.Org.String(), PipelineID: p.ID.String(), Path: path, Lines: limit}
	ref := contract.WorkRef{OrgID: in.OrgID, ID: "quarantine/" + uuid.NewString()}
	work := contract.WorkInput{
		OrgID: in.OrgID, DataPlane: p.DataPlaneID, ID: ref.ID, Kind: contract.ReadQuarantine, Light: contract.LightLogs, Input: in,
		Heartbeat: contract.QuarantineTimeout, Pickup: contract.QuarantinePickup, Timeout: contract.QuarantineTimeout,
	}
	if err := s.Dispatch.Enqueue(ctx, work, nil); err != nil {
		return nil, err
	}
	defer func() {
		if err := s.Dispatch.Forget(context.WithoutCancel(ctx), ref); err != nil {
			s.log().Warn("a read of a quarantine file wasn't deleted; it expires", "work", ref.ID, "err", err)
		}
	}()
	out, err := s.awaitWork(ctx, ref, contract.QuarantinePickup+contract.QuarantineTimeout+time.Minute)
	if err != nil {
		return nil, err
	}
	switch {
	case out.Code == contract.NoRunner:
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("no runner of data plane %q read the file within %s", p.DataPlaneID, contract.QuarantinePickup))
	case out.Code == errs.ConfigInvalid.String():
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New(out.Message))
	case out.Code != "" || out.Cancelled:
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("reading the file failed: %s", out.Message))
	}
	var res contract.ReadQuarantineResult
	if err := json.Unmarshal(out.Result, &res); err != nil {
		return nil, fmt.Errorf("a read of a quarantine file: %w", err)
	}
	if res.Missing {
		return nil, notFound("quarantine file %q isn't in the data plane's storage any more: quarantine.retention or a lifecycle rule of the store deleted it", path)
	}
	return connect.NewResponse(&apiv1.ReadQuarantineFileResponse{Lines: res.Lines, More: res.More}), nil
}

// awaitWork looks at work until it finishes, for at most wait.
func (s *Server) awaitWork(ctx context.Context, ref contract.WorkRef, wait time.Duration) (contract.WorkOutcome, error) {
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	t := time.NewTicker(quarantinePoll)
	defer t.Stop()
	for {
		st, err := s.Dispatch.Status(ctx, ref)
		if err != nil {
			return contract.WorkOutcome{}, err
		}
		if st.Done {
			return st.Outcome, nil
		}
		select {
		case <-ctx.Done():
			return contract.WorkOutcome{}, connect.NewError(connect.CodeDeadlineExceeded, fmt.Errorf("the read of the file didn't finish within %s", wait))
		case <-t.C:
		}
	}
}
