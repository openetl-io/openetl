package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/spec"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// runnerReach is how long a runner's report counts for the plan's checks.
// Runners report every minute.
const runnerReach = 10 * time.Minute

// LauncherSubprocess runs a role's binary as a subprocess.
const LauncherSubprocess = "subprocess"

func (s *Server) ReportRunner(ctx context.Context, req *connect.Request[dataplanev1.ReportRunnerRequest]) (*connect.Response[dataplanev1.ReportRunnerResponse], error) {
	m := req.Msg
	switch {
	case m.GetRunner() == "" || len(m.GetRunner()) > 255:
		return nil, invalid("runner must name the runner, in at most 255 characters")
	case !spec.ValidPlatform(m.GetPlatform()):
		return nil, invalid("platform %q isn't os/arch", m.GetPlatform())
	case len(m.GetFeatures()) > 100 || len(m.GetLaunchers()) > 10:
		return nil, invalid("too many features or launchers")
	}
	dp := callerOf(ctx).dataPlane
	err := s.Store.ReportRunner(ctx, db.ReportRunnerParams{
		OrgID: s.Tenant.Org, DataPlaneID: dp, ID: m.GetRunner(), Release: truncate(m.GetRelease(), 64), Protocol: truncate(m.GetProtocol(), 16),
		Features: nonNil(m.GetFeatures()), Launchers: nonNil(m.GetLaunchers()), Platform: m.GetPlatform(),
	})
	if err != nil {
		return nil, err
	}
	if err := s.Store.DeleteStaleRunners(ctx, db.DeleteStaleRunnersParams{OrgID: s.Tenant.Org, Before: time.Now().Add(-24 * time.Hour)}); err != nil {
		s.log().Warn("dropping stale runners failed", "err", err)
	}
	return connect.NewResponse(&dataplanev1.ReportRunnerResponse{}), nil
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
	if p := r.Requires.Platform; p != "" && rn.Release != "dev" && (!spec.ValidVersion(rn.Release) || spec.CompareVersions(rn.Release, p) < 0) {
		return fmt.Sprintf("it needs openetl %s or later, and runner %s is %s", p, rn.ID, rn.Release)
	}
	if r.Manifest != nil {
		return ""
	}
	if p := r.Spec.GetProtocolVersion(); p != rn.Protocol {
		return fmt.Sprintf("it speaks protocol %s, and runner %s speaks %s", p, rn.ID, rn.Protocol)
	}
	for _, f := range r.Spec.GetRequiredFeatures() {
		if !slices.Contains(rn.Features, f) {
			return fmt.Sprintf("it needs the feature %s, which runner %s hasn't", f, rn.ID)
		}
	}
	if _, ok := r.Binaries[rn.Platform]; ok && slices.Contains(rn.Launchers, LauncherSubprocess) {
		return ""
	}
	containers := slices.ContainsFunc(rn.Launchers, func(l string) bool { return l != LauncherSubprocess })
	if r.Image != nil && containers && slices.Contains(r.Image.Platforms, rn.Platform) {
		return ""
	}
	switch {
	case r.Image != nil && len(r.Binaries) == 0 && !containers:
		return fmt.Sprintf("it has only an image, and runner %s has no container launcher", rn.ID)
	case r.Image != nil && containers:
		return fmt.Sprintf("neither its binaries nor its image are for %s, runner %s's platform", rn.Platform, rn.ID)
	}
	return fmt.Sprintf("it has no binary for %s, runner %s's platform", rn.Platform, rn.ID)
}
