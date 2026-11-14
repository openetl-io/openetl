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
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/dispatch"
	"github.com/openetl-io/openetl/internal/orchestration/sizing"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/spec"
	"github.com/openetl-io/openetl/internal/store/db"
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
	case len(m.GetLaunched()) > maxRunnerWork:
		return nil, invalid("a launcher can't have more than %d pods at once", maxRunnerWork)
	case len(m.GetAttempt()) > 255:
		return nil, invalid("attempt is at most 255 characters")
	case m.GetPods() != nil && m.GetAttempt() != "":
		return nil, invalid("a pod runs its attempt, and launches none")
	}
	dp := callerOf(ctx).dataPlane
	running := make([]dispatch.RunningWork, len(m.GetRunning()))
	for i, w := range m.GetRunning() {
		running[i] = dispatch.RunningWork{
			ID: w.GetId(), Epoch: int64(min(w.GetEpoch(), math.MaxInt64)), Size: sizeOf(w.GetSize()),
			Usage: usageOf(w.GetUsage(), w.GetRows(), w.GetBytes(), w.GetLanes()),
		}
	}
	capacity, reserved := sizeOf(m.GetCapacity()), sizeOf(m.GetReserved())
	params := db.ReportRunnerParams{
		ID: m.GetRunner(), Release: truncate(m.GetRelease(), 64), Protocol: truncate(m.GetProtocol(), 16),
		Features: nonNil(m.GetFeatures()), Launchers: nonNil(m.GetLaunchers()), Platform: m.GetPlatform(),
		Pool: cmp.Or(m.GetPool(), "default"), CapacityCpu: capacity.CPU, CapacityMemory: capacity.Memory,
		ReservedCpu: reserved.CPU, ReservedMemory: reserved.Memory, Draining: m.GetDraining(), Attempt: m.GetAttempt(),
	}
	if p := m.GetPods(); p != nil {
		low, high := sizeOf(p.GetMinSize()), sizeOf(p.GetMaxSize())
		params.Launcher, params.PodMinCpu, params.PodMinMemory, params.PodMaxCpu, params.PodMaxMemory = true, low.CPU, low.Memory, high.CPU, high.Memory
		params.MaxPods, params.MaxPodCpu = int32(min(p.GetMaxPods(), math.MaxInt32)), p.GetMaxCpuMillis()
	}
	stop, drain, err := s.Dispatch.Report(ctx, dispatch.Report{Org: s.Tenant.Org, DataPlane: dp, Running: running, Runner: params})
	if err != nil {
		return nil, err
	}
	launched := make([]dispatch.Launch, len(m.GetLaunched()))
	for i, l := range m.GetLaunched() {
		launched[i] = dispatch.Launch{Attempt: l.GetAttempt(), Placement: placementOf(l.GetPlacement())}
	}
	reap, err := s.Dispatch.Reap(ctx, s.Tenant.Org, launched)
	if err != nil {
		return nil, err
	}
	if err := s.Store.DeleteStaleRunners(ctx, db.DeleteStaleRunnersParams{OrgID: s.Tenant.Org, Before: time.Now().Add(-24 * time.Hour)}); err != nil {
		s.log().Warn("dropping stale runners failed", "err", err)
	}
	res := &dataplanev1.ReportRunnerResponse{Stop: stop, Drain: drain}
	for _, l := range reap {
		res.Reap = append(res.Reap, &dataplanev1.Launch{Attempt: l.Attempt, Placement: uint32(max(0, l.Placement))})
	}
	return connect.NewResponse(res), nil
}

// placementOf is a placement's number as the store keeps it; one past the
// store's range matches none.
func placementOf(n uint32) int32 { return int32(min(n, math.MaxInt32)) }

// maxRunnerWork bounds a report, far above what a runner runs.
const maxRunnerWork = 10000

func sizeOf(r *dataplanev1.Resources) resources.Size {
	return resources.Size{CPU: r.GetCpuMillis(), Memory: r.GetMemoryBytes()}
}

func usageOf(u *apiv1.Usage, rows, bytes uint64, lanes *apiv1.LaneUsage) *dispatch.AttemptUsage {
	if u == nil {
		return nil
	}
	return &dispatch.AttemptUsage{
		Usage: resources.Usage{
			CPU: seconds(u.GetCpuSeconds()), Memory: u.GetPeakMemoryBytes(), RunnerCPU: seconds(u.GetRunnerCpuSeconds()),
			RunnerMemory: u.GetRunnerMemoryBytes(), Seconds: seconds(u.GetSeconds()),
		},
		Rows: int64(min(rows, math.MaxInt64)), Bytes: int64(min(bytes, math.MaxInt64)),
		Lanes: resources.Lanes{
			Count: int(min(lanes.GetLanes(), maxLanes)), Limit: int(min(lanes.GetLimit(), maxLanes)),
			Busy: seconds(lanes.GetBusySeconds()), Backpressured: seconds(lanes.GetBackpressuredSeconds()), Idle: seconds(lanes.GetIdleSeconds()),
			Rows: int64(min(lanes.GetRows(), math.MaxInt64)),
		},
	}
}

// maxLanes bounds a report's lanes, far above what an attempt runs.
const maxLanes = 1 << 16

// seconds is 0 for what a runner can't have measured: sizing takes the largest
// figure it hears, and NaN is larger than any.
func seconds(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0
	}
	return v
}

// PollWork waits for work for the runner, and hands it over.
func (s *Server) PollWork(ctx context.Context, req *connect.Request[dataplanev1.PollWorkRequest]) (*connect.Response[dataplanev1.PollWorkResponse], error) {
	m := req.Msg
	if m.GetRunner() == "" || len(m.GetRunner()) > 255 {
		return nil, invalid("runner must name the runner, in at most 255 characters")
	}
	var work []db.Dispatch
	var err error
	if a := m.GetAttempt(); a != nil && m.Worker != nil {
		return s.pollWorker(ctx, a, m.GetWorker())
	}
	if a := m.GetAttempt(); a != nil {
		work, err = s.Dispatch.ClaimAttempt(ctx, s.Tenant.Org, callerOf(ctx).dataPlane, m.GetRunner(), a.GetAttempt(), placementOf(a.GetPlacement()))
	} else {
		work, err = s.Dispatch.Poll(ctx, s.Tenant.Org, callerOf(ctx).dataPlane, m.GetRunner(), int32(min(m.GetFreeOperations(), maxFreeWork)), int32(min(m.GetFreeLogReads(), maxFreeWork)))
	}
	if err != nil {
		return nil, err
	}
	res := &dataplanev1.PollWorkResponse{}
	for _, w := range work {
		out, err := workOut(w)
		if err != nil {
			return nil, err
		}
		res.Work = append(res.Work, out)
	}
	return connect.NewResponse(res), nil
}

func workOut(w db.Dispatch) (*dataplanev1.Work, error) {
	out := &dataplanev1.Work{
		Id: w.ID, Kind: w.Kind, Input: w.Input,
		HeartbeatTimeout: durationpb.New(time.Duration(w.HeartbeatMs) * time.Millisecond),
		Timeout:          durationpb.New(time.Duration(w.TimeoutMs) * time.Millisecond),
		Workers:          uint32(max(0, w.Workers)),
	}
	if w.Light == "" {
		out.Size = &dataplanev1.Resources{CpuMillis: w.SizeCpu, MemoryBytes: w.SizeMemory}
	}
	if w.Workers > 0 {
		out.Coordinator = &dataplanev1.Resources{CpuMillis: w.CoordinatorCpu, MemoryBytes: w.CoordinatorMemory}
	}
	if w.State == "placed" && w.Launched {
		out.Placement = uint32(max(0, w.Placements))
	}
	return out, json.Unmarshal(w.Trace, &out.Trace)
}

// pollWorker answers a worker pod's poll: its attempt while that placement is
// live, with where it joins once the coordinator has said.
func (s *Server) pollWorker(ctx context.Context, a *dataplanev1.Launch, worker uint32) (*connect.Response[dataplanev1.PollWorkResponse], error) {
	if worker >= math.MaxInt32 {
		return nil, invalid("worker %d is out of range", worker)
	}
	w, target, err := s.Dispatch.WorkerAttempt(ctx, s.Tenant.Org, callerOf(ctx).dataPlane, a.GetAttempt(), placementOf(a.GetPlacement()), int32(worker))
	if err != nil || w == nil {
		return connect.NewResponse(&dataplanev1.PollWorkResponse{}), err
	}
	out, err := workOut(*w)
	if err != nil {
		return nil, err
	}
	out.Placement, out.Join = 0, target
	return connect.NewResponse(&dataplanev1.PollWorkResponse{Work: []*dataplanev1.Work{out}}), nil
}

// OpenWorkers records where an attempt's workers join its coordinator.
func (s *Server) OpenWorkers(ctx context.Context, req *connect.Request[dataplanev1.OpenWorkersRequest]) (*connect.Response[dataplanev1.OpenWorkersResponse], error) {
	m := req.Msg
	switch {
	case m.GetRunner() == "" || len(m.GetRunner()) > 255:
		return nil, invalid("runner must name the runner, in at most 255 characters")
	case m.GetAddress() == "" || len(m.GetAddress()) > 1024:
		return nil, invalid("address must be host:port, in at most 1024 characters")
	case m.GetToken() == "" || len(m.GetToken()) > 1024 || len(m.GetCertificate()) > 64<<10:
		return nil, invalid("an attempt's workers need its token, and its certificate is at most 64 KiB")
	case m.GetGeneration() == 0 || m.GetOwner() == "" || len(m.GetOwner()) > 1024:
		return nil, invalid("generation and owner must be what the coordinator claimed the pipeline with")
	}
	target := &dataplanev1.WorkerTarget{Address: m.GetAddress(), Certificate: m.GetCertificate(), Token: m.GetToken(), Generation: m.GetGeneration(), Owner: m.GetOwner()}
	ok, err := s.Dispatch.OpenWorkers(ctx, s.Tenant.Org, callerOf(ctx).dataPlane, m.GetRunner(), m.GetId(), target)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&dataplanev1.OpenWorkersResponse{Accepted: ok}), nil
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
	var accepted bool
	var err error
	if why := m.GetOnePod(); why != "" {
		accepted, err = s.Dispatch.OnePod(ctx, s.Tenant.Org, callerOf(ctx).dataPlane, m.GetRunner(), m.GetId(), truncate(why, 1024))
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(&dataplanev1.ReportResultResponse{Accepted: accepted}), nil
	}
	if n := m.GetPlacement(); n > 0 {
		accepted, err = s.Dispatch.LaunchResult(ctx, s.Tenant.Org, callerOf(ctx).dataPlane, m.GetRunner(), m.GetId(), placementOf(n), out, m.GetNoCapacity())
	} else {
		accepted, err = s.Dispatch.Result(ctx, s.Tenant.Org, callerOf(ctx).dataPlane, m.GetRunner(), m.GetId(), out, m.GetNoCapacity(), usageOf(m.GetUsage(), m.GetRows(), m.GetBytes(), m.GetLanes()))
	}
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&dataplanev1.ReportResultResponse{Accepted: accepted}), nil
}

// planJob is what the pipeline's next attempt would be sized at, on how many
// workers, and checks that a runner or pod launcher of its pool can take the
// size. That's a problem when the pool's runners reported lately and none
// can, and a note when none reported. An attempt on workers needs a pod
// launcher that takes every worker and their coordinator; without a launcher
// in the pool, it runs on one pod.
func (s *Server) planJob(ctx context.Context, q *db.Queries, p *planned, workers int, out *apiv1.PipelinePlan) error {
	sp := sizing.SpecOf(p.v.spec.GetResources())
	rows, err := q.RecentRunners(ctx, db.RecentRunnersParams{OrgID: s.Tenant.Org, DataPlaneID: p.v.dataPlane, Since: time.Now().Add(-runnerReach)})
	if err != nil {
		return err
	}
	var launchers []*dispatch.Pods
	for _, r := range rows {
		if r.Pool == sp.Pool && r.Launcher {
			launchers = append(launchers, &dispatch.Pods{
				Max: resources.Size{CPU: r.PodMaxCpu, Memory: r.PodMaxMemory}, MaxPods: int(r.MaxPods), MaxCPU: r.MaxPodCpu,
			})
		}
	}
	if workers > 0 && len(rows) > 0 && len(launchers) == 0 {
		out.Warnings = append(out.Warnings, &apiv1.PlanWarning{Message: onePodWarning(workers, fmt.Sprintf(
			"no pod launcher of data plane %q and pool %q has reported in the last ten minutes, and an attempt runs on workers only in pods", p.v.dataPlane, sp.Pool))})
		workers = 0
	}
	var capacities []resources.Size
	for _, r := range rows {
		switch {
		case r.Pool != sp.Pool:
		case r.Launcher:
			capacities = append(capacities, dispatch.LargestPod(resources.Size{CPU: r.PodMaxCpu, Memory: r.PodMaxMemory}))
		case r.CapacityCpu > 0 && workers == 0:
			capacities = append(capacities, resources.Size{CPU: r.CapacityCpu, Memory: r.CapacityMemory})
		}
	}
	in := sizing.Input{Spec: sp, Runners: capacities}
	if in.Hint, err = s.planHint(ctx, q, p.v); err != nil {
		return err
	}
	if p.id != uuid.Nil {
		if in.History, err = sizing.History(ctx, q, s.Tenant.Org, p.id); err != nil {
			return err
		}
		res := p.v.spec.GetResources()
		if err := sizing.Rescaled(ctx, q, s.Tenant.Org, p.id, p.version, plan.Continuous(p.v.spec), res.GetFinishWithin().AsDuration(), &in); err != nil {
			return err
		}
	}
	c := sizing.Choose(in)
	out.Job = &apiv1.JobPlan{Size: sizing.Out(c.Size), Reason: c.Reason.API(), Pool: sp.Pool, Workers: uint32(max(workers, 0))}
	if workers > 0 && len(launchers) > 0 && !slices.ContainsFunc(launchers, func(l *dispatch.Pods) bool { return l.AllowsWorkers(workers, c.Size, sizing.Coordinator) }) {
		out.Problems = append(out.Problems, fmt.Sprintf("the job's %d workers of %s and their coordinator, %s, need %d pods with %s between them, more than any pod launcher of data plane %q and pool %q takes",
			workers, c.Size.Name(), sizing.Coordinator.Name(), workers+1, c.Size.Times(workers).Add(sizing.Coordinator), p.v.dataPlane, sp.Pool))
	}
	if len(capacities) == 0 {
		out.Notes = append(out.Notes, fmt.Sprintf("no runner of data plane %q and pool %q has reported in the last ten minutes, so the plan can't check that one can take the job's size", p.v.dataPlane, sp.Pool))
		return nil
	}
	if !sizing.FitsAny(c.Size, capacities) {
		out.Problems = append(out.Problems, fmt.Sprintf("the job's size, %s, is larger than any runner of data plane %q and pool %q can take: %s",
			c.Size.Name(), p.v.dataPlane, sp.Pool, largestCapacities(capacities)))
	}
	if sp.Max != (resources.Size{}) && !sizing.FitsAny(sp.Max, capacities) {
		out.Warnings = append(out.Warnings, &apiv1.PlanWarning{Message: fmt.Sprintf(
			"resources.max, %s, is larger than any runner of pool %q can take, so the job stops growing below it: %s", sp.Max.Name(), sp.Pool, largestCapacities(capacities))})
	}
	return nil
}

// planHint is the largest hint of the version's connectors.
func (s *Server) planHint(ctx context.Context, q *db.Queries, v *pinned) (resources.Size, error) {
	var hints []string
	for _, r := range []struct{ role, digest string }{{"source", v.src.ConnectorDigest}, {"destination", v.dst.ConnectorDigest}} {
		if r.digest == "" {
			continue
		}
		a, err := q.GetArtifact(ctx, db.GetArtifactParams{OrgID: s.Tenant.Org, Digest: r.digest})
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return resources.Size{}, err
		}
		ver, err := connectorhost.ParseVersion(a.Content)
		if err != nil {
			return resources.Size{}, err
		}
		if role := ver.Roles[r.role]; role != nil {
			hints = append(hints, role.Resources.Hint)
		}
	}
	return sizing.LargestHint(hints...), nil
}

func largestCapacities(capacities []resources.Size) string {
	largest := slices.MaxFunc(capacities, func(a, b resources.Size) int { return cmp.Compare(a.Weight(), b.Weight()) })
	return "the largest has " + largest.String()
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
