// Package sizing works out the size of a pipeline's next attempt.
package sizing

import (
	"cmp"
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/store/db"
)

// Reason is what set a size.
type Reason string

const (
	FromSpec    Reason = "spec"
	FromHint    Reason = "hint"
	FromDefault Reason = "default"
	FromHistory Reason = "history"
	FromOOM     Reason = "out_of_memory"
	FromLargest Reason = "largest_runner"
	FromRescale Reason = "rescale"
	// The job loop's lanes, for a continuous pipeline.
	FromAutoscaler   Reason = "autoscaler"
	FromFinishWithin Reason = "finish_within"
)

// historyLength is how many of a job's latest attempts size it.
const historyLength = 10

// API is the reason as the API says it.
func (r Reason) API() apiv1.JobSizeReason {
	return apiv1.JobSizeReason(apiv1.JobSizeReason_value["JOB_SIZE_REASON_"+strings.ToUpper(string(r))])
}

// Spec is what a pipeline's spec says of its jobs' size.
type Spec struct {
	// Zero: auto.
	Size resources.Size
	Min  resources.Size
	// Zero: auto goes up to xl, and a set size stays as it is.
	Max  resources.Size
	Pool string
}

// DefaultPool is the pool of a pipeline that names none.
const DefaultPool = "default"

// SpecOf reads a pipeline's spec.resources, which spec.Parse has checked.
func SpecOf(r *apiv1.JobResources) Spec {
	return Spec{Size: Of(r.GetSize()), Min: Of(r.GetMin()), Max: Of(r.GetMax()), Pool: cmp.Or(r.GetPool(), DefaultPool)}
}

// Of is the size a JobSize says: its preset's, or its amounts.
func Of(s *apiv1.JobSize) resources.Size {
	if p, ok := resources.Preset(s.GetPreset()); ok {
		return p
	}
	return resources.Size{CPU: s.GetCpuMillis(), Memory: s.GetMemoryBytes()}
}

// Out is a size as the API says it.
func Out(s resources.Size) *apiv1.JobSize {
	out := &apiv1.JobSize{CpuMillis: s.CPU, MemoryBytes: s.Memory}
	if _, ok := resources.Preset(s.Name()); ok {
		out.Preset = s.Name()
	}
	return out
}

// Attempt is what an earlier attempt of the job says about its size.
type Attempt struct {
	Size resources.Size
	// CPU seconds and peak resident bytes, the runner's share included.
	CPU     float64
	Memory  int64
	Seconds float64
	// The code it failed with; empty if none.
	Code string
}

// Input is what a job's size comes from.
type Input struct {
	Spec Spec
	// The largest hint among its connectors'; zero if none has one.
	Hint resources.Size
	// Its latest attempts, newest first.
	History []Attempt
	// The capacities of the runners of its pool; empty if none is reporting.
	Runners []resources.Size
	// RescalePipeline's size, which holds as the spec's would; zero if none.
	Rescale resources.Size
	// The lanes an automatic size gives CPU for, and what chose them; 0 if
	// nothing did.
	Lanes       int
	LanesReason Reason
}

// Choice is a job's size, and what set it.
type Choice struct {
	Size   resources.Size
	Reason Reason
}

// Choose sizes a job's next attempt.
func Choose(in Input) Choice {
	sp, fixedBy := in.Spec, FromSpec
	if in.Rescale != (resources.Size{}) {
		sp.Size, fixedBy = in.Rescale, FromRescale
	}
	fixed := sp.Size != resources.Size{}
	var c Choice
	switch peak, measured := peak(in.History); {
	case fixed:
		c = Choice{sp.Size, fixedBy}
	case in.Lanes > 0:
		// A lane a vCPU; memory as its peaks say.
		c = Choice{resources.Size{CPU: int64(in.Lanes) * 1000, Memory: peak.Memory}.Smallest(), in.LanesReason}
	case measured:
		c = Choice{peak.Smallest(), FromHistory}
	case in.Hint != resources.Size{}:
		c = Choice{in.Hint, FromHint}
	default:
		c = Choice{resources.Default, FromDefault}
	}
	// What was killed for memory needs more than what it ran at, whatever its
	// peak says: it may have died before showing it. It holds until 10 attempts
	// have passed without another.
	for _, a := range in.History {
		if up := a.Size.Up(); a.Code == errs.ResourceExhausted.String() && !up.Fits(c.Size) {
			c = Choice{c.Size.Above(up), FromOOM}
		}
	}
	floor := sp.Min
	if fixed {
		floor = sp.Size
	}
	if !floor.Fits(c.Size) {
		c = Choice{c.Size.Above(floor), fixedBy}
	}
	limit := sp.Max
	switch {
	case limit != resources.Size{}:
	case fixed:
		limit = sp.Size
	default:
		limit = resources.XL.Above(sp.Min)
	}
	if !c.Size.Fits(limit) {
		c = Choice{c.Size.Below(limit), FromSpec}
	}
	if fits := largest(in.Runners, c.Size); fits != c.Size && floor.Fits(fits) {
		c = Choice{fits, FromLargest}
	}
	return c
}

// largest is the largest size at or below s that a runner can take: s itself
// if one can, or if there are no runners or none takes any preset.
func largest(runners []resources.Size, s resources.Size) resources.Size {
	if len(runners) == 0 || FitsAny(s, runners) {
		return s
	}
	for _, p := range slices.Backward(resources.Presets) {
		if p.Fits(s) && FitsAny(p, runners) {
			return p
		}
	}
	return s
}

// FitsAny reports whether one of the runners can take s.
func FitsAny(s resources.Size, runners []resources.Size) bool {
	return slices.ContainsFunc(runners, s.Fits)
}

// headroom is how far above its recent peak a job is sized.
const headroom = 1.25

// shortAttempt is how long an attempt must run, in seconds, for its CPU to say
// anything: starting up takes most of a shorter one's.
const shortAttempt = 60

// peak is the 95th percentile of the attempts' use, with headroom; false if
// none measured anything.
func peak(history []Attempt) (resources.Size, bool) {
	var cpu, mem []float64
	for _, a := range history {
		if a.Memory > 0 {
			mem = append(mem, float64(a.Memory))
		}
		if a.Seconds >= shortAttempt {
			cpu = append(cpu, a.CPU/a.Seconds)
		}
	}
	if len(mem) == 0 && len(cpu) == 0 {
		return resources.Size{}, false
	}
	return resources.Size{
		CPU:    int64(percentile95(cpu) * headroom * 1000),
		Memory: int64(percentile95(mem) * headroom),
	}, true
}

func percentile95(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	slices.Sort(v)
	at := 0.95 * float64(len(v)-1)
	i := int(at)
	if i+1 == len(v) {
		return v[i]
	}
	return v[i] + (at-float64(i))*(v[i+1]-v[i])
}

// SyncAttempt is what an attempt of a pipeline's last sync moved.
type SyncAttempt struct {
	// What its source emitted.
	Bytes   int64
	Seconds float64
	// Its lanes, and the most it could use: 0 if more can always be used.
	Lanes, Limit int
}

// FinishWithin is how many lanes a scheduled job needs to finish within d:
// the last sync's bytes, at the rate its lanes moved them, up to the most its
// last attempt could use. 0 if the last sync doesn't say.
func FinishWithin(d time.Duration, last []SyncAttempt) int {
	if d <= 0 || len(last) == 0 {
		return 0
	}
	var bytes, laneBytes int64
	var laneSeconds float64
	for _, a := range last {
		bytes += a.Bytes
		if a.Lanes > 0 && a.Seconds > 0 {
			laneBytes += a.Bytes
			laneSeconds += a.Seconds * float64(a.Lanes)
		}
	}
	if laneBytes == 0 || laneSeconds == 0 {
		return 0
	}
	perLane := float64(laneBytes) / laneSeconds
	lanes := max(1, int(math.Ceil(float64(bytes)/(perLane*d.Seconds()))))
	if limit := last[len(last)-1].Limit; limit > 0 {
		lanes = min(lanes, limit)
	}
	return lanes
}

// LargestHint is the largest of hints, by weight: a preset's name for each
// connector that gives one. Zero if none does.
func LargestHint(hints ...string) resources.Size {
	var out resources.Size
	for _, h := range hints {
		if p, ok := resources.Preset(h); ok && p.Weight() > out.Weight() {
			out = p
		}
	}
	return out
}

// Rescaled adds to in what rescaled the pipeline's jobs besides its spec:
// RescalePipeline's size for this version, the job loop's lanes for a
// continuous pipeline, and finishWithin's for a scheduled one.
func Rescaled(ctx context.Context, q *db.Queries, org, pipeline uuid.UUID, version string, continuous bool, finishWithin time.Duration, in *Input) error {
	scale, err := q.GetJobScale(ctx, db.GetJobScaleParams{OrgID: org, PipelineID: pipeline})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return err
	default:
		if scale.SizeVersion == version {
			in.Rescale = resources.Size{CPU: scale.SizeCpu, Memory: scale.SizeMemory}
		}
		if continuous && scale.Lanes > 0 {
			in.Lanes, in.LanesReason = int(scale.Lanes), FromAutoscaler
		}
	}
	if continuous || finishWithin <= 0 {
		return nil
	}
	last, err := LastSync(ctx, q, org, pipeline)
	if err != nil {
		return err
	}
	if n := FinishWithin(finishWithin, last); n > 0 {
		in.Lanes, in.LanesReason = n, FromFinishWithin
	}
	return nil
}

// LastSync reads the attempts of the pipeline's latest scheduled sync that
// succeeded.
func LastSync(ctx context.Context, q *db.Queries, org, pipeline uuid.UUID) ([]SyncAttempt, error) {
	rows, err := q.LastSyncAttempts(ctx, db.LastSyncAttemptsParams{OrgID: org, PipelineID: pipeline})
	if err != nil {
		return nil, err
	}
	out := make([]SyncAttempt, len(rows))
	for i, r := range rows {
		out[i] = SyncAttempt{Bytes: r.Bytes, Seconds: r.Seconds, Lanes: int(r.Lanes), Limit: int(r.LaneLimit)}
	}
	return out, nil
}

// History reads the pipeline's latest attempts.
func History(ctx context.Context, q *db.Queries, org, pipeline uuid.UUID) ([]Attempt, error) {
	rows, err := q.RecentAttempts(ctx, db.RecentAttemptsParams{OrgID: org, PipelineID: pipeline, MaxRows: historyLength})
	if err != nil {
		return nil, err
	}
	out := make([]Attempt, len(rows))
	for i, r := range rows {
		out[i] = Attempt{
			Size: resources.Size{CPU: r.SizeCpu, Memory: r.SizeMemory},
			CPU:  r.CpuSeconds + r.RunnerCpuSeconds, Memory: r.PeakMemory + r.RunnerMemory,
			Seconds: r.Seconds, Code: r.Code,
		}
	}
	return out, nil
}
