package api

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/durationpb"

	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/orchestration/dispatch"
	"github.com/unsuitable001/openetl/internal/platform/resources"
	"github.com/unsuitable001/openetl/internal/spec"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// runnerReach is how long a runner's report counts for the plan's checks.
const runnerReach = 10 * time.Minute

// LauncherSubprocess runs a role's binary as a subprocess.
const LauncherSubprocess = dispatch.LauncherSubprocess

func (s *Server) ReportRunner(ctx context.Context, req *connect.Request[dataplanev1.ReportRunnerRequest]) (*connect.Response[dataplanev1.ReportRunnerResponse], error) {
	m := req.Msg
	switch {
	case m.GetRunner() == "" || len(m.GetRunner()) > 255:
		return nil, invalid("runner must name the runner, in at most 255 characters")
	case !spec.ValidPlatform(m.GetPlatform()):
		return nil, invalid("platform %q isn't os/arch", m.GetPlatform())
	case len(m.GetFeatures()) > 100 || len(m.GetLaunchers()) > 10:
		return nil, invalid("too many features or launchers")
	case len(m.GetRunning()) > maxRunnerWork:
		return nil, invalid("a runner can't run more than %d things at once", maxRunnerWork)
	case len(m.GetPool()) > 64:
		return nil, invalid("a pool's name is at most 64 characters")
	}
	dp := callerOf(ctx).dataPlane
	running := make([]dispatch.RunningWork, len(m.GetRunning()))
	for i, w := range m.GetRunning() {
		running[i] = dispatch.RunningWork{ID: w.GetId(), Epoch: int64(min(w.GetEpoch(), math.MaxInt64)), Size: sizeOf(w.GetSize())}
	}
	capacity, reserved := sizeOf(m.GetCapacity()), sizeOf(m.GetReserved())
	stop, err := s.Dispatch.Report(ctx, dispatch.Report{
		Org: s.Tenant.Org, DataPlane: dp, Running: running,
		Runner: db.ReportRunnerParams{
			ID: m.GetRunner(), Release: truncate(m.GetRelease(), 64), Protocol: truncate(m.GetProtocol(), 16),
			Features: nonNil(m.GetFeatures()), Launchers: nonNil(m.GetLaunchers()), Platform: m.GetPlatform(),
			Pool: cmp.Or(m.GetPool(), "default"), CapacityCpu: capacity.CPU, CapacityMemory: capacity.Memory,
			ReservedCpu: reserved.CPU, ReservedMemory: reserved.Memory, Draining: m.GetDraining(),
		},
	})
	if err != nil {
		return nil, err
	}
	if err := s.Store.DeleteStaleRunners(ctx, db.DeleteStaleRunnersParams{OrgID: s.Tenant.Org, Before: time.Now().Add(-24 * time.Hour)}); err != nil {
		s.log().Warn("dropping stale runners failed", "err", err)
	}
	return connect.NewResponse(&dataplanev1.ReportRunnerResponse{Stop: stop}), nil
}

// maxRunnerWork bounds a report, far above what a runner runs.
const maxRunnerWork = 10000

func sizeOf(r *dataplanev1.Resources) resources.Size {
	return resources.Size{CPU: r.GetCpuMillis(), Memory: r.GetMemoryBytes()}
}

// PollWork waits for work for the runner, and hands it over.
func (s *Server) PollWork(ctx context.Context, req *connect.Request[dataplanev1.PollWorkRequest]) (*connect.Response[dataplanev1.PollWorkResponse], error) {
	m := req.Msg
	if m.GetRunner() == "" || len(m.GetRunner()) > 255 {
		return nil, invalid("runner must name the runner, in at most 255 characters")
	}
	work, err := s.Dispatch.Poll(ctx, s.Tenant.Org, callerOf(ctx).dataPlane, m.GetRunner(), int32(min(m.GetFreeOperations(), maxFreeWork)), int32(min(m.GetFreeLogReads(), maxFreeWork)))
	if err != nil {
		return nil, err
	}
	res := &dataplanev1.PollWorkResponse{}
	for _, w := range work {
		out := &dataplanev1.Work{
			Id: w.ID, Kind: w.Kind, Input: w.Input,
			HeartbeatTimeout: durationpb.New(time.Duration(w.HeartbeatMs) * time.Millisecond),
			Timeout:          durationpb.New(time.Duration(w.TimeoutMs) * time.Millisecond),
		}
		if w.Light == "" {
			out.Size = &dataplanev1.Resources{CpuMillis: w.SizeCpu, MemoryBytes: w.SizeMemory}
		}
		if err := json.Unmarshal(w.Trace, &out.Trace); err != nil {
			return nil, err
		}
		res.Work = append(res.Work, out)
	}
	return connect.NewResponse(res), nil
}

// maxFreeWork bounds what a runner can ask for at once.
const maxFreeWork = 64

// ReportResult records what happened to work a runner took.
func (s *Server) ReportResult(ctx context.Context, req *connect.Request[dataplanev1.ReportResultRequest]) (*connect.Response[dataplanev1.ReportResultResponse], error) {
	m := req.Msg
	out := contract.WorkOutcome{
		Result: m.GetResult(), Code: m.GetCode(), Message: m.GetMessage(), NonRetryable: m.GetNonRetryable(),
		RetryAfter: m.GetRetryAfter().AsDuration(), Cancelled: m.GetCancelled(),
	}
	if len(out.Result) > 0 && !json.Valid(out.Result) {
		return nil, invalid("the result isn't JSON")
	}
	accepted, err := s.Dispatch.Result(ctx, s.Tenant.Org, callerOf(ctx).dataPlane, m.GetRunner(), m.GetId(), out, m.GetNoCapacity())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&dataplanev1.ReportResultResponse{Accepted: accepted}), nil
}

// checkRunners checks that a runner of the pipeline's data plane can run its
// source's and destination's connector versions. It's a problem when
// runners reported lately and none can, and a note when none reported.
func (s *Server) checkRunners(ctx context.Context, q *db.Queries, v *pinned) (problems, notes []string, err error) {
	type pinnedRole struct{ role, ref, digest string }
	var roles []pinnedRole
	for _, r := range []pinnedRole{{"source", v.spec.GetSource().GetRef(), v.src.ConnectorDigest}, {"destination", v.spec.GetDestination().GetRef(), v.dst.ConnectorDigest}} {
		if r.digest != "" {
			roles = append(roles, r)
		}
	}
	if len(roles) == 0 {
		return nil, nil, nil
	}
	runners, err := q.RecentRunners(ctx, db.RecentRunnersParams{OrgID: s.Tenant.Org, DataPlaneID: v.dataPlane, Since: time.Now().Add(-runnerReach)})
	if err != nil {
		return nil, nil, err
	}
	if len(runners) == 0 {
		return nil, []string{fmt.Sprintf("no runner of data plane %q has reported in the last ten minutes, so the plan can't check that one can run the source's and destination's connectors", v.dataPlane)}, nil
	}
	for _, r := range roles {
		a, err := q.GetArtifact(ctx, db.GetArtifactParams{OrgID: s.Tenant.Org, Digest: r.digest})
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		ver, err := connectorhost.ParseVersion(a.Content)
		if err != nil {
			return nil, nil, err
		}
		role := ver.Roles[r.role]
		if role == nil {
			continue
		}
		var why []string
		for _, rn := range runners {
			reason := canRun(role.Role, rn)
			if reason == "" {
				why = nil
				break
			}
			why = append(why, reason)
		}
		if len(why) > 0 {
			slices.Sort(why)
			problems = append(problems, fmt.Sprintf("%s %q: %s %s's %s can't run on any runner of data plane %q: %s",
				r.role, r.ref, ver.Name, ver.Version, r.role, v.dataPlane, strings.Join(slices.Compact(why), "; ")))
		}
	}
	return problems, nil, nil
}

// canRun is why a runner can't run a role, or "".
func canRun(r *spec.Role, rn db.Runner) string {
	return dispatch.NeedOf(r).Why(dispatch.RunnerFacts{
		ID: rn.ID, Release: rn.Release, Protocol: rn.Protocol, Platform: rn.Platform, Features: rn.Features, Launchers: rn.Launchers,
	})
}
