package scaling

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/sizing"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
)

// Options are the loop's timings; zero means production's.
type Options struct {
	// How often it samples and decides. Zero: 30 s.
	Every time.Duration
	Windows
}

// Loop samples the lanes of the running stream jobs, and rescales those
// whose lanes call for it. Any number of them can run: a rescale locks its
// pipeline's row and checks the gap again under it.
type Loop struct {
	Store   *store.Store
	Options Options
	Logger  *slog.Logger
}

func (l *Loop) log() *slog.Logger {
	if l.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return l.Logger
}

func (l *Loop) every() time.Duration { return cmp.Or(l.Options.Every, 30*time.Second) }

// Run samples and decides every l.Options.Every until ctx ends.
func (l *Loop) Run(ctx context.Context) error {
	t := time.NewTicker(l.every())
	defer t.Stop()
	var warned time.Time
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		orgs, err := l.Store.ListOrgs(ctx)
		if err == nil {
			for _, org := range orgs {
				err = errors.Join(err, l.Tick(ctx, org))
			}
		}
		if err != nil && ctx.Err() == nil && time.Since(warned) >= time.Minute {
			l.log().Warn("the job loop failed; trying again", "err", err)
			warned = time.Now()
		}
	}
}

// Tick samples the org's running stream jobs, and decides about each.
func (l *Loop) Tick(ctx context.Context, org uuid.UUID) error {
	every := l.every()
	if _, err := l.Store.SampleLanes(ctx, db.SampleLanesParams{OrgID: org, Kind: contract.RunStreamJob, EverySeconds: 0.8 * every.Seconds()}); err != nil {
		return err
	}
	keep := l.Options.shrink() + 2*every
	if err := l.Store.PruneLaneSamples(ctx, db.PruneLaneSamplesParams{OrgID: org, Before: time.Now().Add(-keep)}); err != nil {
		return err
	}
	jobs, err := l.Store.ScalingCandidates(ctx, db.ScalingCandidatesParams{OrgID: org, Kind: contract.RunStreamJob})
	if err != nil {
		return err
	}
	for _, j := range jobs {
		err = errors.Join(err, l.consider(ctx, org, j))
	}
	return err
}

// consider decides about one running job, and acts on it.
func (l *Loop) consider(ctx context.Context, org uuid.UUID, j db.ScalingCandidatesRow) error {
	rows, err := l.Store.AttemptLaneSamples(ctx, db.AttemptLaneSamplesParams{OrgID: org, AttemptID: j.ID})
	if err != nil || len(rows) == 0 {
		return err
	}
	in := Input{Now: time.Now(), Windows: l.Options.Windows}
	for _, r := range rows {
		in.Samples = append(in.Samples, Sample{
			Time: r.SampleTime, Lanes: int(r.Lanes), Limit: int(r.LaneLimit),
			Busy: r.BusySeconds, Backpressured: r.BackpressuredSeconds, Idle: r.IdleSeconds, Rows: r.LaneRows,
		})
	}
	since := in.Samples[len(in.Samples)-1].Time.Add(-l.Options.grow() - 2*l.every())
	progress, err := l.Store.RecentProgress(ctx, db.RecentProgressParams{OrgID: org, PipelineID: j.PipelineID, Since: since})
	if err != nil {
		return err
	}
	in.Lag = lagOf(progress)
	v, err := l.Store.GetPipelineVersion(ctx, db.GetPipelineVersionParams{OrgID: org, PipelineID: j.PipelineID, Version: j.Version})
	if err != nil {
		return err
	}
	var spec apiv1.PipelineSpec
	if err := protojson.Unmarshal(v.Spec, &spec); err != nil {
		return err
	}
	sp := sizing.SpecOf(spec.GetResources())
	in.Min, in.Max = sp.Min, sp.Max
	in.Fixed = spec.GetResources().GetSize() != nil || (j.ScaleCpu > 0 && j.ScaleVersion == j.Version)
	if j.RescaleTime != nil {
		in.LastRescale = *j.RescaleTime
	}
	d := Decide(in)
	if d.Lanes == 0 && d.Note == j.Note {
		return nil
	}
	return l.Store.Tx(ctx, func(q *db.Queries) error {
		scale, err := q.LockJobScale(ctx, db.LockJobScaleParams{OrgID: org, PipelineID: j.PipelineID})
		if err != nil {
			return err
		}
		if err := q.SetScaleNote(ctx, db.SetScaleNoteParams{OrgID: org, PipelineID: j.PipelineID, Note: d.Note}); err != nil {
			return err
		}
		// Another loop may have rescaled it since it read the row.
		if d.Lanes == 0 || (scale.RescaleTime != nil && time.Since(*scale.RescaleTime) < l.Options.gap()) {
			return nil
		}
		if err := q.SetScaleLanes(ctx, db.SetScaleLanesParams{OrgID: org, PipelineID: j.PipelineID, Lanes: lanes32(d.Lanes)}); err != nil {
			return err
		}
		id := j.ID
		if _, err := q.RescaleRunning(ctx, db.RescaleRunningParams{OrgID: org, PipelineID: j.PipelineID, ID: &id}); err != nil {
			return err
		}
		from := int(j.AttemptLanes)
		l.log().Info("rescaling a stream job", "pipeline", j.PipelineID, "attempt", j.ID, "from", from, "to", d.Lanes, "busy", d.Busy, "backpressured", d.Backpressured)
		names, err := q.GetPipelineNames(ctx, db.GetPipelineNamesParams{OrgID: org, ID: j.PipelineID})
		if err != nil {
			return err
		}
		_, err = events.Record(ctx, q, org, events.Event{
			Workspace: names.WorkspaceID, Type: events.PipelineRescaled, Pipeline: names.Name,
			Data: events.Rescale{
				Pipeline: "workspaces/" + names.Workspace + "/pipelines/" + names.Name, Version: j.Version,
				By: "autoscaler", Lanes: d.Lanes, FromLanes: from,
			},
		})
		return err
	})
}

func lanes32(n int) int32 {
	if n > 0 && n <= math.MaxInt32 {
		return int32(n)
	}
	return 0
}

// progress is a checkpoint's progress, as checkpoint_mirror keeps it.
type progress struct {
	LagMs      *int64  `json:"lag_ms,omitempty"`
	LagRecords *uint64 `json:"lag_records,omitempty"`
	LagBytes   *uint64 `json:"lag_bytes,omitempty"`
}

// lagOf is the worst lag of each checkpoint, in time if its source gives it,
// then records, then bytes: the first unit every checkpoint has.
func lagOf(rows []db.RecentProgressRow) []Lag {
	type point struct {
		at   time.Time
		lags [3]float64
		has  [3]bool
	}
	var pts []point
	for _, r := range rows {
		var ps []progress
		if json.Unmarshal(r.Progress, &ps) != nil {
			continue
		}
		p := point{at: r.CommitTime}
		for _, s := range ps {
			for i, v := range []*float64{ms(s.LagMs), u(s.LagRecords), u(s.LagBytes)} {
				if v != nil {
					p.lags[i], p.has[i] = max(p.lags[i], *v), true
				}
			}
		}
		pts = append(pts, p)
	}
	for unit := range 3 {
		every := len(pts) > 0
		for _, p := range pts {
			every = every && p.has[unit]
		}
		if !every {
			continue
		}
		out := make([]Lag, len(pts))
		for i, p := range pts {
			out[i] = Lag{Time: p.at, Value: p.lags[unit]}
		}
		return out
	}
	return nil
}

func ms(v *int64) *float64 {
	if v == nil {
		return nil
	}
	f := float64(*v)
	return &f
}

func u(v *uint64) *float64 {
	if v == nil {
		return nil
	}
	f := float64(*v)
	return &f
}
