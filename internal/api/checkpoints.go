package api

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/heal"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// A checkpoint's create_time partitions the mirror, so one far from now
// would make a partition nothing else uses.
const maxClockSkew = 24 * time.Hour

type mirroredStream struct {
	Name            string `json:"name"`
	Rows            uint64 `json:"rows"`
	QuarantinedRows uint64 `json:"quarantined_rows"`
	CollapsedRows   uint64 `json:"collapsed_rows"`
	// The rows of the sync so far; nil in checkpoints from before them.
	SyncRows     *uint64 `json:"sync_rows,omitempty"`
	Forced       bool    `json:"forced"`
	Complete     bool    `json:"complete"`
	SchemaDigest string  `json:"schema_digest"`
	HeldCells    uint64  `json:"held_cells,omitempty"`
	// The columns it held at the checkpoint; discarded ones count as
	// nothing.
	HeldColumns []string `json:"held_columns,omitempty"`
	// Where the epoch got less than planned.
	Level        apiv1.DeliveryLevel `json:"level,omitempty"`
	PlannedLevel apiv1.DeliveryLevel `json:"planned_level,omitempty"`
	// How the source reads it; unspecified in checkpoints from before.
	Read   apiv1.SyncMode `json:"read,omitempty"`
	Paused bool           `json:"paused,omitempty"`
}

// heldColumns are the columns st holds, whole or some of their cells.
func heldColumns(st *dataplanev1.CheckpointStream) []string {
	var out []string
	for _, h := range st.GetHolds() {
		if !h.GetDiscarded() && !slices.Contains(out, h.GetColumn()) {
			out = append(out, h.GetColumn())
		}
	}
	slices.Sort(out)
	return out
}

// mirroredProgress is a read's progress as checkpoint_mirror.progress keeps
// it.
type mirroredProgress struct {
	Stream             string  `json:"stream"`
	LagBytes           *uint64 `json:"lag_bytes,omitempty"`
	LagRecords         *uint64 `json:"lag_records,omitempty"`
	LagMs              *int64  `json:"lag_ms,omitempty"`
	RetentionLeftBytes *uint64 `json:"retention_left_bytes,omitempty"`
	RetentionLeftMs    *int64  `json:"retention_left_ms,omitempty"`
}

// bigints converts a request's uint64s for bigint columns. err is
// InvalidArgument for the first that's larger than a bigint holds, which
// converts to 0.
type bigints struct{ err error }

func (b *bigints) of(field string, n uint64) int64 {
	v, ok := ints.Int64(n)
	if !ok && b.err == nil {
		b.err = invalid("%s %d is more than %d", field, n, int64(math.MaxInt64))
	}
	return v
}

func millis(d *durationpb.Duration) *int64 {
	if d == nil {
		return nil
	}
	ms := d.AsDuration().Milliseconds()
	return &ms
}

// ReportCheckpoint adds a committed checkpoint to the pipeline's mirror,
// unless it's there.
func (s *Server) ReportCheckpoint(ctx context.Context, req *connect.Request[dataplanev1.ReportCheckpointRequest]) (*connect.Response[dataplanev1.ReportCheckpointResponse], error) {
	m := req.Msg
	id, err := uuid.Parse(m.GetPipelineId())
	if err != nil {
		return nil, invalid("pipeline_id: %v", err)
	}
	if err := s.pipelineOnDataPlane(ctx, id); err != nil {
		return nil, err
	}
	if m.GetRunId() == "" {
		return nil, invalid("run_id is empty")
	}
	if !m.GetCreateTime().IsValid() {
		return nil, invalid("create_time is missing or invalid")
	}
	created := m.GetCreateTime().AsTime()
	if d := time.Since(created); d > maxClockSkew || d < -maxClockSkew {
		return nil, invalid("create_time %s is more than %s from the control plane's clock", created.Format(time.RFC3339), maxClockSkew)
	}
	streams := make([]mirroredStream, 0, len(m.GetStreams()))
	for _, st := range m.GetStreams() {
		streams = append(streams, mirroredStream{
			Name: st.GetName(), Rows: st.GetRows(), QuarantinedRows: st.GetQuarantinedRows(), CollapsedRows: st.GetCollapsedRows(), SyncRows: st.SyncRows,
			Forced: st.GetForced(), Complete: st.GetComplete(), SchemaDigest: st.GetSchemaDigest(),
			HeldCells: st.GetHeldCells(), HeldColumns: heldColumns(st), Level: st.GetLevel(), PlannedLevel: st.GetPlannedLevel(),
			Read: st.GetRead(), Paused: st.GetPaused(),
		})
	}
	slices.SortFunc(streams, func(a, b mirroredStream) int { return strings.Compare(a.Name, b.Name) })
	raw, err := json.Marshal(streams)
	if err != nil {
		return nil, err
	}
	var progress []byte
	if len(m.GetProgress()) > 0 {
		ps := make([]mirroredProgress, len(m.GetProgress()))
		for i, p := range m.GetProgress() {
			ps[i] = mirroredProgress{
				Stream: p.GetStream(), LagBytes: p.LagBytes, LagRecords: p.LagRecords, LagMs: millis(p.GetLag()),
				RetentionLeftBytes: p.RetentionLeftBytes, RetentionLeftMs: millis(p.GetRetentionLeft()),
			}
		}
		if progress, err = json.Marshal(ps); err != nil {
			return nil, err
		}
	}
	var usage []byte
	if u := m.GetUsage(); u != nil {
		if usage, err = protojson.Marshal(u); err != nil {
			return nil, err
		}
	}
	var b bigints
	arg := db.InsertMirroredCheckpointParams{
		OrgID: s.Tenant.Org, PipelineID: id, RunID: m.GetRunId(), CommitTime: created, Streams: raw, Progress: progress, Usage: usage,
		Epoch: b.of("epoch", m.GetEpoch()), Generation: b.of("generation", m.GetGeneration()),
	}
	if p := m.GetPrevious(); p != nil {
		e, g := b.of("previous.epoch", p.GetEpoch()), b.of("previous.generation", p.GetGeneration())
		arg.PreviousEpoch, arg.PreviousGeneration = &e, &g
	}
	holds, err := s.holdRows(id, m, &b)
	if err != nil {
		return nil, err
	}
	if b.err != nil {
		return nil, b.err
	}
	added, err := s.Store.MirrorCheckpoint(ctx, arg)
	if store.IsForeignKeyViolation(err) {
		return nil, notFound("pipeline %s doesn't exist", id)
	}
	if err != nil {
		return nil, err
	}
	if err := s.recordHolds(ctx, id, arg.Generation, arg.Epoch, holds); err != nil {
		return nil, err
	}
	if err := s.takeResets(ctx, id, m.GetLastReset()); err != nil {
		return nil, err
	}
	// The log has a record that reads, so a restart from an earlier one
	// isn't needed.
	if err := s.Store.TakeRewinds(ctx, db.TakeRewindsParams{OrgID: s.Tenant.Org, PipelineID: id}); err != nil {
		return nil, err
	}
	if err := s.afterCheckpoint(ctx, id, m); err != nil {
		return nil, err
	}
	return connect.NewResponse(&dataplanev1.ReportCheckpointResponse{Added: added}), nil
}

// afterCheckpoint is what a running sync's commit tells heal. A continuous
// run's commit resolves the incidents progress resolves, and ends a crash
// loop's count; the read's lag and retention, the streams' holds and their
// quarantined rows open or resolve theirs.
func (s *Server) afterCheckpoint(ctx context.Context, pipeline uuid.UUID, m *dataplanev1.ReportCheckpointRequest) error {
	sync, err := s.Store.GetSyncByRun(ctx, db.GetSyncByRunParams{OrgID: s.Tenant.Org, PipelineID: pipeline, RunID: m.GetRunId()})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil || sync.Status != "running" {
		return err
	}
	if sync.Continuous {
		err := s.Store.Tx(ctx, func(q *db.Queries) error {
			if _, err := q.SyncCommitted(ctx, db.SyncCommittedParams{OrgID: s.Tenant.Org, PipelineID: pipeline, RunID: m.GetRunId()}); err != nil {
				return err
			}
			_, err := heal.Resolve(ctx, q, s.Tenant.Org, pipeline, heal.ResolvedByProgress, nil, fmt.Sprintf("run %s committed epoch %d", sync.ID, m.GetEpoch()))
			return err
		})
		if err != nil {
			return err
		}
		if err := s.checkLag(ctx, pipeline, sync, m); err != nil {
			return err
		}
	}
	if err := s.checkRetention(ctx, pipeline, sync, m); err != nil {
		return err
	}
	if err := s.checkHolds(ctx, pipeline, sync, m); err != nil {
		return err
	}
	return s.checkQuarantine(ctx, pipeline, sync, m)
}

// defaultLaggingAfter is how far behind its source a continuous pipeline's
// read gets before it's lagging, unless the server says otherwise.
const defaultLaggingAfter = 5 * time.Minute

// checkLag records pipeline.lagging when a continuous pipeline's running
// read reports it's further behind its source than the server's
// LaggingAfter, and pipeline.recovered once it's back within half of it.
// The pipeline's lagging_since holds the state, so each crossing is recorded
// once, however often a checkpoint is reported.
func (s *Server) checkLag(ctx context.Context, pipeline uuid.UUID, sync db.Sync, m *dataplanev1.ReportCheckpointRequest) error {
	var worst *protocolv1.Progress
	for _, p := range m.GetProgress() {
		if p.GetLag() != nil && (worst == nil || p.GetLag().AsDuration() > worst.GetLag().AsDuration()) {
			worst = p
		}
	}
	if worst == nil {
		return nil
	}
	threshold, lag := cmp.Or(s.LaggingAfter, defaultLaggingAfter), worst.GetLag().AsDuration()
	if lag > threshold/2 && lag <= threshold {
		return nil
	}
	return s.Store.Tx(ctx, func(q *db.Queries) error {
		var since time.Time
		typ := events.PipelineLagging
		if lag > threshold {
			n, err := q.StartLagging(ctx, db.StartLaggingParams{OrgID: s.Tenant.Org, ID: pipeline})
			if err != nil || n == 0 {
				return err
			}
			since = time.Now()
			_, err = heal.Observe(ctx, q, s.Tenant.Org, heal.Observation{
				Pipeline: pipeline, Version: sync.Version, Class: heal.Lagging, Severity: heal.Warning,
				Message: fmt.Sprintf("The read is %s behind its source, past the server's threshold of %s.", lag.Round(time.Second), threshold),
				Details: map[string]string{
					"lag_seconds": strconv.FormatFloat(lag.Seconds(), 'f', 0, 64), "threshold_seconds": strconv.FormatFloat(threshold.Seconds(), 'f', 0, 64),
					"stream": worst.GetStream(),
				},
				Sync: &sync.ID, Key: sync.ID.String() + "/" + since.Format(time.RFC3339Nano),
			})
			if err != nil {
				return err
			}
		} else {
			was, err := q.StopLagging(ctx, db.StopLaggingParams{OrgID: s.Tenant.Org, ID: pipeline})
			if err != nil || len(was) == 0 {
				return err
			}
			typ, since = events.PipelineRecovered, was[0]
			resolution := fmt.Sprintf("the read is %s behind its source, within half the threshold", lag.Round(time.Second))
			if _, err := heal.Resolve(ctx, q, s.Tenant.Org, pipeline, []string{string(heal.Lagging)}, nil, resolution); err != nil {
				return err
			}
		}
		names, err := q.GetPipelineNames(ctx, db.GetPipelineNamesParams{OrgID: s.Tenant.Org, ID: pipeline})
		if err != nil {
			return err
		}
		name := resourceName(names.Workspace, "pipelines", names.Name)
		_, err = events.Record(ctx, q, s.Tenant.Org, events.Event{
			Workspace: names.WorkspaceID, Type: typ, Pipeline: names.Name,
			Data: events.Lag{
				Pipeline: name, Sync: name + "/syncs/" + sync.ID.String(), LagSeconds: lag.Seconds(),
				ThresholdSeconds: threshold.Seconds(), Since: since, Stream: worst.GetStream(),
			},
		})
		return err
	})
}

// A read is at risk from the share of what its source keeps for it that
// it has used, and safe again under the last.
const (
	retentionAtRisk   = 0.5
	retentionCritical = 0.8
	retentionSafe     = 0.4
)

// checkRetention opens retention_at_risk when a read has used half of what
// its source keeps for it, by bytes or time, turns it critical at 80%, and
// resolves it under 40%.
func (s *Server) checkRetention(ctx context.Context, pipeline uuid.UUID, sync db.Sync, m *dataplanev1.ReportCheckpointRequest) error {
	return s.observeRetention(ctx, pipeline, retentionAt{version: sync.Version, sync: &sync.ID, key: sync.ID.String()}, m.GetProgress())
}

// retentionAt is where a read's retention was seen: a sync's report, or a
// probe while the pipeline wasn't running.
type retentionAt struct {
	version string
	// Nil for a probe.
	sync *uuid.UUID
	key  string
	// Added to the message.
	what string
}

func (s *Server) observeRetention(ctx context.Context, pipeline uuid.UUID, seen retentionAt, progress []*protocolv1.Progress) error {
	used, at := -1.0, (*protocolv1.Progress)(nil)
	for _, p := range progress {
		if u := retentionUsed(p); u > used {
			used, at = u, p
		}
	}
	if at == nil || (used >= retentionSafe && used < retentionAtRisk) {
		return nil
	}
	return s.Store.Tx(ctx, func(q *db.Queries) error {
		if used < retentionSafe {
			_, err := heal.Resolve(ctx, q, s.Tenant.Org, pipeline, []string{string(heal.RetentionAtRisk)}, nil,
				fmt.Sprintf("the read has used %.0f%% of what its source keeps for it%s", used*100, seen.what))
			return err
		}
		o := heal.Observation{
			Pipeline: pipeline, Version: seen.version, Class: heal.RetentionAtRisk, Severity: heal.Warning, Stream: at.GetStream(),
			Details: map[string]string{"used": fmt.Sprintf("%.0f%%", used*100)},
			Sync:    seen.sync, Key: seen.key,
		}
		if used >= retentionCritical {
			o.Severity = heal.Critical
		}
		var have []string
		if at.LagBytes != nil && at.RetentionLeftBytes != nil {
			o.Details["lag_bytes"], o.Details["retention_left_bytes"] = strconv.FormatUint(at.GetLagBytes(), 10), strconv.FormatUint(at.GetRetentionLeftBytes(), 10)
			have = append(have, fmt.Sprintf("%d bytes behind with %d left", at.GetLagBytes(), at.GetRetentionLeftBytes()))
		}
		if at.GetLag() != nil && at.GetRetentionLeft() != nil {
			lag, left := at.GetLag().AsDuration(), at.GetRetentionLeft().AsDuration()
			o.Details["lag_seconds"], o.Details["retention_left_seconds"] = strconv.FormatFloat(lag.Seconds(), 'f', 0, 64), strconv.FormatFloat(left.Seconds(), 'f', 0, 64)
			have = append(have, fmt.Sprintf("%s behind with %s left", lag.Round(time.Second), left.Round(time.Second)))
		}
		o.Message = fmt.Sprintf("The read has used %.0f%% of what its source keeps for it%s: %s.", used*100, seen.what, strings.Join(have, "; "))
		_, err := heal.Observe(ctx, q, s.Tenant.Org, o)
		return err
	})
}

// retentionUsed is the share of what its source keeps for it that a read
// has used, by bytes or by time, whichever is more; -1 if p doesn't say.
func retentionUsed(p *protocolv1.Progress) float64 {
	used := -1.0
	if p.LagBytes != nil && p.RetentionLeftBytes != nil && p.GetLagBytes()+p.GetRetentionLeftBytes() > 0 {
		used = float64(p.GetLagBytes()) / float64(p.GetLagBytes()+p.GetRetentionLeftBytes())
	}
	if p.GetLag() != nil && p.GetRetentionLeft() != nil {
		lag, left := p.GetLag().AsDuration(), p.GetRetentionLeft().AsDuration()
		if lag+left > 0 {
			used = max(used, lag.Seconds()/(lag+left).Seconds())
		}
	}
	return used
}

// A stream quarantining more than quarantineShare of its rows, and at least
// quarantineMinRows of them, in the sync's last quarantineWindow of commits
// opens a quarantine incident; under half the share, with at least
// quarantineMinRows rows, resolves it.
const (
	quarantineShare   = 0.01
	quarantineMinRows = 100
	quarantineWindow  = time.Hour
)

func (s *Server) checkQuarantine(ctx context.Context, pipeline uuid.UUID, sync db.Sync, m *dataplanev1.ReportCheckpointRequest) error {
	check := map[string]bool{}
	reported := map[string]*dataplanev1.CheckpointStream{}
	read := 0
	for _, st := range m.GetStreams() {
		reported[st.GetName()] = st
		if !st.GetPaused() {
			read++
		}
		if st.GetQuarantinedRows() > 0 {
			check[st.GetName()] = true
		}
	}
	open, err := s.Store.OpenIncidents(ctx, db.OpenIncidentsParams{OrgID: s.Tenant.Org, PipelineID: pipeline, Class: string(heal.Quarantine)})
	if err != nil {
		return err
	}
	for _, o := range open {
		check[o.Stream] = true
	}
	if len(check) == 0 {
		return nil
	}
	totals, err := s.Store.SyncStreams(ctx, db.SyncStreamsParams{
		OrgID: s.Tenant.Org, PipelineID: pipeline, RunIds: []string{m.GetRunId()}, Since: m.GetCreateTime().AsTime().Add(-quarantineWindow),
	})
	if err != nil {
		return err
	}
	var restart, stop bool
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		for _, t := range totals {
			if !check[t.Name] || t.Rows < quarantineMinRows {
				continue
			}
			share := float64(t.QuarantinedRows) / float64(t.Rows)
			switch {
			case t.QuarantinedRows >= quarantineMinRows && share > quarantineShare:
				st := reported[t.Name]
				others := read
				if st != nil && !st.GetPaused() {
					others--
				}
				o := heal.Observation{
					Pipeline: pipeline, Version: sync.Version, Stream: t.Name, Class: heal.Quarantine, Severity: heal.Warning,
					Message: fmt.Sprintf("%d of the stream's %d rows in the last hour were quarantined.", t.QuarantinedRows, t.Rows),
					Details: map[string]string{
						"share": fmt.Sprintf("%.1f%%", share*100), "quarantined_rows": strconv.FormatInt(t.QuarantinedRows, 10),
						"rows": strconv.FormatInt(t.Rows, 10),
					},
					Sync: &sync.ID, Key: sync.ID.String(), Read: st.GetRead(), OtherStreams: max(others, 0),
				}
				if f := st.GetQuarantineFile(); f != "" {
					o.Details["quarantine_file"] = f
				}
				seen, err := heal.Observe(ctx, q, s.Tenant.Org, o)
				if err != nil {
					return err
				}
				restart = restart || seen.StreamPaused && seen.Continuous
				stop = stop || seen.Paused
			case share < quarantineShare/2:
				stream := t.Name
				_, err := heal.Resolve(ctx, q, s.Tenant.Org, pipeline, []string{string(heal.Quarantine)}, &stream,
					fmt.Sprintf("%.2f%% of the stream's rows in the last hour were quarantined", share*100))
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// The sync carries on: a stream paused, or the pipeline, takes effect
	// from the next sync, or a continuous run's next job.
	if (restart || stop && sync.Continuous) && s.Schedules != nil {
		if err := s.Schedules.Refresh(ctx, s.Tenant.Org.String(), pipeline.String()); err != nil {
			s.log().Warn("restarting the run without the paused stream failed; it stops at its next restart", "pipeline", pipeline, "err", err)
		}
	}
	if stop && !sync.Continuous && s.Schedules != nil {
		if err := s.Schedules.SetPaused(ctx, pipeline.String(), true); err != nil {
			s.log().Warn("pausing the schedule failed; the pipeline is paused, so its syncs do nothing", "pipeline", pipeline, "err", err)
		}
	}
	return nil
}

func (s *Server) ListCheckpoints(ctx context.Context, req *connect.Request[apiv1.ListCheckpointsRequest]) (*connect.Response[apiv1.ListCheckpointsResponse], error) {
	_, p, err := s.getPipeline(ctx, req.Msg.GetParent())
	if err != nil {
		return nil, err
	}
	after, n, err := page(req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	params := db.ListCheckpointsParams{OrgID: s.Tenant.Org, PipelineID: p.ID, MaxRows: n}
	if after != "" {
		e, g, ok := strings.Cut(after, ".")
		epoch, err1 := strconv.ParseInt(e, 10, 64)
		gen, err2 := strconv.ParseInt(g, 10, 64)
		if !ok || err1 != nil || err2 != nil {
			return nil, invalid("bad page_token")
		}
		params.BeforeEpoch, params.BeforeGeneration = &epoch, gen
	}
	rows, err := s.Store.ListCheckpoints(ctx, params)
	if err != nil {
		return nil, err
	}
	versions, err := s.schemaVersions(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListCheckpointsResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		last := rows[len(rows)-1]
		res.NextPageToken = nextToken(fmt.Sprintf("%d.%d", last.Epoch, last.Generation))
	}
	for _, r := range rows {
		var streams []mirroredStream
		if err := json.Unmarshal(r.Streams, &streams); err != nil {
			return nil, err
		}
		c := &apiv1.Checkpoint{
			Epoch: ints.Uint64(r.Epoch), Generation: ints.Uint64(r.Generation),
			CommitTime: timestamppb.New(r.CommitTime),
		}
		if r.PreviousEpoch != nil && r.PreviousGeneration != nil {
			c.PreviousEpoch, c.PreviousGeneration = ints.Uint64(*r.PreviousEpoch), ints.Uint64(*r.PreviousGeneration)
		}
		if r.SyncID != nil {
			c.Sync = req.Msg.GetParent() + "/syncs/" + r.SyncID.String()
		}
		if len(r.Usage) > 0 {
			c.Usage = &apiv1.Usage{}
			if err := protojson.Unmarshal(r.Usage, c.Usage); err != nil {
				return nil, err
			}
		}
		for _, st := range streams {
			c.Streams = append(c.Streams, &apiv1.CheckpointStream{
				Name: st.Name, Rows: st.Rows, QuarantinedRows: st.QuarantinedRows, CollapsedRows: st.CollapsedRows,
				Forced: st.Forced, Complete: st.Complete, SchemaVersion: versions[st.Name+"\x00"+st.SchemaDigest],
				HeldCells: st.HeldCells, HeldColumns: st.HeldColumns, Level: st.Level, PlannedLevel: st.PlannedLevel,
			})
		}
		res.Checkpoints = append(res.Checkpoints, c)
	}
	return connect.NewResponse(res), nil
}

// schemaVersions maps each stream's schema digests to their versions, keyed
// by stream and digest, NUL between.
func (s *Server) schemaVersions(ctx context.Context, pipeline uuid.UUID) (map[string]int32, error) {
	rows, err := s.Store.StreamSchemaVersions(ctx, db.StreamSchemaVersionsParams{OrgID: s.Tenant.Org, PipelineID: pipeline})
	if err != nil {
		return nil, err
	}
	out := make(map[string]int32, len(rows))
	for _, r := range rows {
		out[r.Stream+"\x00"+r.Digest] = r.Version
	}
	return out, nil
}
