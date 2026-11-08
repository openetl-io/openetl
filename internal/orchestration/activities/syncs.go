// Package activities holds the control plane's steps, which run on
// orchestrators.
package activities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"

	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/heal"
	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/dispatch"
	"github.com/openetl-io/openetl/internal/pkgcatalog"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/secrets"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
)

type Activities struct {
	Store *store.Store
	// Sends workflows' work to data planes.
	Dispatch *dispatch.Dispatcher
	// Pauses the schedules of pipelines a playbook paused, and changes those a
	// playbook slowed down. Nil: only the pipeline is paused, which stops its
	// syncs all the same, and the schedule changes at the pipeline's next
	// apply.
	Schedules interface {
		SetPaused(ctx context.Context, pipelineID string, paused bool) error
		Upsert(ctx context.Context, orgID, pipelineID string, sched *apiv1.Schedule, paused bool) error
	}
	// How RefreshCatalog reaches registries and GitHub. Nil: it lists
	// nothing.
	Catalog *CatalogOptions
	Logger  *slog.Logger
}

// CatalogOptions are how RefreshCatalog lists connected sources.
type CatalogOptions struct {
	// Opens sources' secrets; nil: a source with a secret fails to list.
	Secrets *secrets.Keyring
	// A netguard client.
	HTTP      *http.Client
	PlainHTTP []string
	GitHubAPI string
	Official  []string
}

// Steps is every step, for a backend to run.
func (a *Activities) Steps() []orchestration.Step {
	return []orchestration.Step{
		orchestration.NewStep(contract.StartSync, a.StartSync),
		orchestration.NewAction(contract.FinishSync, a.FinishSync),
		orchestration.NewStep(contract.GetStreamState, a.GetStreamState),
		orchestration.NewStep(contract.StartRun, a.StartRun),
		orchestration.NewAction(contract.EnqueueWork, a.EnqueueWork),
		orchestration.NewStep(contract.GetWork, a.GetWork),
		orchestration.NewAction(contract.CancelWork, a.CancelWork),
		orchestration.NewStep(contract.DropMirrorPartitions, a.DropMirrorPartitions),
		orchestration.NewStep(contract.AuditPartitions, a.AuditPartitions),
		orchestration.NewStep(contract.RefreshCatalog, a.RefreshCatalog),
		orchestration.NewStep(contract.ListRetentionProbes, a.ListRetentionProbes),
	}
}

func (a *Activities) log() *slog.Logger {
	if a.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return a.Logger
}

// abandonedSync is what a sync whose workflow was lost records.
const abandonedSync = "its workflow was lost, and a later sync took over"

// StartSync records a running sync of the pipeline's current version, unless
// the pipeline is paused. Retries of one workflow run record it once. It stops
// any other sync of the pipeline still running, whose workflow was lost; the
// checkpoint log fences what that one left running anyway.
func (a *Activities) StartSync(ctx context.Context, in contract.SyncInput) (contract.StartSyncResult, error) {
	var res contract.StartSyncResult
	org, pipeline, err := ids(in.OrgID, in.PipelineID)
	if err != nil {
		return res, err
	}
	p, err := a.Store.GetPipelineByID(ctx, db.GetPipelineByIDParams{OrgID: org, ID: pipeline})
	if errors.Is(err, pgx.ErrNoRows) {
		return res, errs.New(errs.ConfigInvalid, "pipeline %s no longer exists", in.PipelineID)
	}
	if err != nil {
		return res, err
	}
	if p.Paused {
		return contract.StartSyncResult{Paused: true}, nil
	}
	v, err := a.Store.GetSyncSpec(ctx, db.GetSyncSpecParams{OrgID: org, PipelineID: pipeline, Version: p.Version})
	if err != nil {
		return res, err
	}
	var spec apiv1.PipelineSpec
	if err := protojson.Unmarshal(v.PipelineSpec, &spec); err != nil {
		return res, err
	}
	// A schedule a mode change left behind mustn't fence the stream job.
	if spec.GetMode() == apiv1.PipelineMode_PIPELINE_MODE_CONTINUOUS {
		return contract.StartSyncResult{Continuous: true}, nil
	}
	connectors, err := connectorNames(v)
	if err != nil {
		return res, err
	}
	steps, err := postSync(v.Version, v.Dbt, v.SparkClusters, &spec)
	if err != nil {
		return res, err
	}
	run := orchestration.StepInfoOf(ctx)
	// A UUIDv7 holds when the sync started, which data planes prune its log
	// by.
	sync, err := uuid.NewV7()
	if err != nil {
		return res, err
	}
	var id uuid.UUID
	manual := false
	var abandoned []string
	err = a.Store.Tx(ctx, func(q *db.Queries) error {
		left, err := q.StopAbandonedSyncs(ctx, db.StopAbandonedSyncsParams{OrgID: org, PipelineID: pipeline, RunID: run.RunID, Error: strPtr(abandonedSync)})
		if err != nil {
			return err
		}
		for _, l := range left {
			abandoned = append(abandoned, l.ID.String())
			if err := recordSync(ctx, q, events.SyncStopped, l, ""); err != nil {
				return err
			}
		}
		m, err := q.TakeManualSync(ctx, db.TakeManualSyncParams{OrgID: org, ID: pipeline})
		if err == nil {
			manual = m
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		row, err := q.StartSync(ctx, db.StartSyncParams{
			OrgID:      org,
			ID:         sync,
			PipelineID: pipeline,
			Version:    p.Version,
			WorkflowID: run.WorkflowID,
			RunID:      run.RunID,
		})
		if err != nil {
			return err
		}
		id = row.ID
		return recordSync(ctx, q, events.SyncStarted, row, "")
	})
	if err != nil {
		return res, err
	}
	a.cancelWorkOf(ctx, org, abandoned)
	return contract.StartSyncResult{
		SyncID: id.String(), Version: p.Version, DataPlane: p.DataPlaneID,
		WorkspaceID: p.WorkspaceID.String(), Connectors: connectors, PostSync: steps, Manual: manual,
		Pool: spec.GetResources().GetPool(),
	}, nil
}

func strPtr(s string) *string { return &s }

// cancelWorkOf stops what abandoned syncs left waiting or running. If it can't,
// the checkpoint log fences their attempts, and their other work runs out.
func (a *Activities) cancelWorkOf(ctx context.Context, org uuid.UUID, syncs []string) {
	if len(syncs) == 0 {
		return
	}
	ids, err := a.Store.UnfinishedWorkOfSyncs(ctx, db.UnfinishedWorkOfSyncsParams{OrgID: org, SyncIds: syncs})
	if err != nil {
		a.log().Warn("finding what abandoned syncs left running failed", "syncs", syncs, "err", err)
		return
	}
	for _, id := range ids {
		if err := a.Dispatch.Cancel(ctx, contract.WorkRef{OrgID: org.String(), ID: id}); err != nil {
			a.log().Warn("stopping what an abandoned sync left running failed", "work", id, "err", err)
		}
	}
}

// postSync is the pipeline version's post-sync steps, with the bundles and
// Spark clusters' versions it pins.
func postSync(version string, dbt, spark []byte, spec *apiv1.PipelineSpec) ([]contract.PostSyncStep, error) {
	var bundles, clusters map[string]string
	if err := json.Unmarshal(dbt, &bundles); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(spark, &clusters); err != nil {
		return nil, err
	}
	var steps []contract.PostSyncStep
	for _, s := range spec.GetPostSync() {
		if sp := s.GetSpark(); sp != nil {
			cv, ok := clusters[sp.GetCluster()]
			if !ok {
				return nil, errs.New(errs.ConfigInvalid, "pipeline version %s pins no version of Spark cluster %q", version, sp.GetCluster())
			}
			c := sp.GetCode()
			steps = append(steps, contract.PostSyncStep{Spark: &contract.SparkStep{
				Cluster: sp.GetCluster(), ClusterVersion: cv, Jar: c.GetJar(), Python: c.GetPython(),
				SHA256: c.GetSha256(), MainClass: c.GetMainClass(), Args: sp.GetArgs(), Conf: sp.GetConf(),
			}})
			continue
		}
		d := s.GetDbt()
		digest, ok := bundles[d.GetProject()]
		if !ok {
			return nil, errs.New(errs.ConfigInvalid, "pipeline version %s pins no bundle of dbt project %q", version, d.GetProject())
		}
		command := d.GetCommand()
		if command == apiv1.DbtCommand_DBT_COMMAND_UNSPECIFIED {
			command = apiv1.DbtCommand_DBT_COMMAND_BUILD
		}
		steps = append(steps, contract.PostSyncStep{Dbt: &contract.DbtStep{
			Project: d.GetProject(), Digest: digest,
			Command: strings.ToLower(strings.TrimPrefix(command.String(), "DBT_COMMAND_")),
			Select:  d.GetSelect(), Exclude: d.GetExclude(), Schema: d.GetSchema(),
		}})
	}
	return steps, nil
}

// connectorNames names the pipeline version's source and destination
// connectors.
func connectorNames(v db.GetSyncSpecRow) ([]string, error) {
	var names []string
	for _, spec := range [][]byte{v.SourceSpec, v.DestinationSpec} {
		var c apiv1.ConnectorConfig
		if err := protojson.Unmarshal(spec, &c); err != nil {
			return nil, err
		}
		names = append(names, c.GetConnector())
	}
	return names, nil
}

func (a *Activities) FinishSync(ctx context.Context, in contract.FinishSyncInput) error {
	org, id, err := ids(in.OrgID, in.SyncID)
	if err != nil {
		return err
	}
	params := db.FinishSyncParams{OrgID: org, ID: id, Status: "succeeded"}
	switch {
	case in.Stopped:
		params.Status = "stopped"
	case in.Error != "":
		params.Status, params.Error = "failed", &in.Error
	}
	if in.Result != nil {
		stats := struct {
			*contract.AttemptResult
			PostSync []contract.PostSyncResult `json:",omitempty"`
		}{in.Result, in.PostSync}
		if params.Stats, err = json.Marshal(stats); err != nil {
			return err
		}
	}
	var seen heal.Seen
	var pipeline uuid.UUID
	err = a.Store.Tx(ctx, func(q *db.Queries) error {
		row, err := q.FinishSync(ctx, params)
		// Already ended, by whatever took over from a workflow thought lost, or
		// gone with its pipeline.
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		pipeline = row.PipelineID
		typ := map[string]string{"succeeded": events.SyncSucceeded, "stopped": events.SyncStopped, "failed": events.SyncFailed}[params.Status]
		if err := recordSync(ctx, q, typ, row, in.Code); err != nil {
			return err
		}
		if in.Code == errs.SchemaDrift.String() {
			if err := recordSync(ctx, q, events.SchemaBlocked, row, in.Code); err != nil {
				return err
			}
		}
		switch params.Status {
		case "failed":
			class := heal.ClassOf(in.Code)
			o := heal.Observation{
				Pipeline: row.PipelineID, Version: row.Version, Stream: heal.StreamOf(class, in.Error), Class: class,
				Code: in.Code, Message: in.Error, Sync: &row.ID, Key: row.ID.String(),
			}
			if row.EndTime != nil {
				o.Ran = row.EndTime.Sub(row.StartTime)
			}
			seen, err = heal.Observe(ctx, q, org, o)
		case "succeeded":
			_, err = heal.Resolve(ctx, q, org, row.PipelineID, heal.ResolvedByProgress, nil, "sync "+row.ID.String()+" succeeded")
		}
		// A pipeline deleted meanwhile has nothing to heal.
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	})
	if err == nil && seen.Paused && !seen.Continuous && a.Schedules != nil {
		if perr := a.Schedules.SetPaused(ctx, pipeline.String(), true); perr != nil {
			a.log().Warn("pausing the schedule failed; the pipeline is paused, so its syncs do nothing", "pipeline", pipeline, "err", perr)
		}
	}
	if err == nil && seen.Schedule != nil && a.Schedules != nil {
		// An update keeps the schedule's pause state; paused applies only if
		// it has to be made again.
		paused := false
		if p, perr := a.Store.GetPipelineByID(ctx, db.GetPipelineByIDParams{OrgID: org, ID: pipeline}); perr == nil {
			paused = p.Paused
		}
		if perr := a.Schedules.Upsert(ctx, org.String(), pipeline.String(), seen.Schedule, paused); perr != nil {
			a.log().Warn("changing the schedule failed; it changes at the pipeline's next apply", "pipeline", pipeline, "err", perr)
		}
	}
	return err
}

// recordSync records a sync's event, once however often it's retried. A
// pipeline deleted meanwhile gets none.
func recordSync(ctx context.Context, q *db.Queries, typ string, s db.Sync, code string) error {
	names, err := q.GetPipelineNames(ctx, db.GetPipelineNamesParams{OrgID: s.OrgID, ID: s.PipelineID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	pipeline := "workspaces/" + names.Workspace + "/pipelines/" + names.Name
	data := events.Sync{
		Pipeline: pipeline, Sync: pipeline + "/syncs/" + s.ID.String(), Version: s.Version, Continuous: s.Continuous,
		StartTime: s.StartTime, EndTime: s.EndTime, Code: code,
	}
	if s.Error != nil {
		data.Error = *s.Error
	}
	_, err = events.Record(ctx, q, s.OrgID, events.Event{
		Workspace: names.WorkspaceID, Type: typ, Pipeline: names.Name, Data: data, Key: typ + "/" + s.ID.String(),
	})
	return err
}

// GetStreamState reads the pipeline for its supervisor, with the run it has
// running, if any. A pipeline that no longer exists isn't continuous.
func (a *Activities) GetStreamState(ctx context.Context, in contract.StreamInput) (contract.StreamState, error) {
	var st contract.StreamState
	org, pipeline, err := ids(in.OrgID, in.PipelineID)
	if err != nil {
		return st, err
	}
	p, err := a.Store.GetPipelineByID(ctx, db.GetPipelineByIDParams{OrgID: org, ID: pipeline})
	if errors.Is(err, pgx.ErrNoRows) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	v, err := a.Store.GetSyncSpec(ctx, db.GetSyncSpecParams{OrgID: org, PipelineID: pipeline, Version: p.Version})
	if err != nil {
		return st, err
	}
	var spec apiv1.PipelineSpec
	if err := protojson.Unmarshal(v.PipelineSpec, &spec); err != nil {
		return st, err
	}
	connectors, err := connectorNames(v)
	if err != nil {
		return st, err
	}
	releases, err := a.Store.NewestHoldRelease(ctx, db.NewestHoldReleaseParams{OrgID: org, PipelineID: pipeline})
	if err != nil {
		return st, err
	}
	requested, err := a.Store.NewestCheckpointRequest(ctx, db.NewestCheckpointRequestParams{OrgID: org, PipelineID: pipeline})
	if err != nil {
		return st, err
	}
	releases = withRequests(releases, requested)
	left, err := a.leftRun(ctx, org, pipeline)
	if err != nil {
		return st, err
	}
	paused, err := a.Store.ListPausedStreams(ctx, db.ListPausedStreamsParams{OrgID: org, Pipelines: []uuid.UUID{pipeline}})
	if err != nil {
		return st, err
	}
	streams := make([]string, len(paused))
	for i, r := range paused {
		streams[i] = r.Stream
	}
	return contract.StreamState{
		Continuous: spec.GetMode() == apiv1.PipelineMode_PIPELINE_MODE_CONTINUOUS,
		Paused:     p.Paused, Version: p.Version, DataPlane: p.DataPlaneID,
		WorkspaceID: p.WorkspaceID.String(), Connectors: connectors, Releases: releases, PausedStreams: strings.Join(streams, ","),
		Pool: spec.GetResources().GetPool(), Left: left,
	}, nil
}

// leftRun is the pipeline's run that's running, and its latest job, or nil.
func (a *Activities) leftRun(ctx context.Context, org, pipeline uuid.UUID) (*contract.LeftRun, error) {
	run, err := a.Store.RunningRun(ctx, db.RunningRunParams{OrgID: org, PipelineID: pipeline})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	left := &contract.LeftRun{SyncID: run.ID.String(), RunKey: run.RunID, Version: run.Version, DataPlane: run.DataPlaneID}
	job, err := a.Store.LatestAttempt(ctx, db.LatestAttemptParams{OrgID: org, SyncID: run.ID})
	if errors.Is(err, pgx.ErrNoRows) {
		return left, nil
	}
	if err != nil {
		return nil, err
	}
	left.Job, left.JobStart, left.Earlier = job.ID, job.CreateTime, job.Attempt-1
	left.Releases, err = a.Store.NewestHoldReleaseAt(ctx, db.NewestHoldReleaseAtParams{OrgID: org, PipelineID: pipeline, At: job.CreateTime})
	if err != nil {
		return nil, err
	}
	requested, err := a.Store.NewestCheckpointRequestAt(ctx, db.NewestCheckpointRequestAtParams{OrgID: org, PipelineID: pipeline, At: job.CreateTime})
	if err != nil {
		return nil, err
	}
	left.Releases = withRequests(left.Releases, requested)
	return left, nil
}

// withRequests adds the newest checkpoint request, a reset or a rewind, to
// the newest hold release, so either restarts a running run. Without one it
// is the release alone, as it was before requests.
func withRequests(releases, requested string) string {
	if requested == "" {
		return releases
	}
	return releases + "/" + requested
}

// StartRun records a running run of a continuous pipeline's version. Retries
// of one step record it once.
func (a *Activities) StartRun(ctx context.Context, in contract.StartRunInput) (contract.StartRunResult, error) {
	var res contract.StartRunResult
	org, pipeline, err := ids(in.OrgID, in.PipelineID)
	if err != nil {
		return res, err
	}
	info := orchestration.StepInfoOf(ctx)
	// The workflow run holds several runs.
	res.RunKey = info.RunID + "/" + info.StepID
	sync, err := uuid.NewV7()
	if err != nil {
		return res, err
	}
	err = a.Store.Tx(ctx, func(q *db.Queries) error {
		row, err := q.StartSync(ctx, db.StartSyncParams{
			OrgID:      org,
			ID:         sync,
			PipelineID: pipeline,
			Version:    in.Version,
			WorkflowID: info.WorkflowID,
			RunID:      res.RunKey,
			Continuous: true,
		})
		if err != nil {
			return err
		}
		res.SyncID = row.ID.String()
		return recordSync(ctx, q, events.SyncStarted, row, "")
	})
	if store.IsForeignKeyViolation(err) {
		return res, errs.New(errs.ConfigInvalid, "pipeline %s no longer exists", in.PipelineID)
	}
	return res, err
}

func (a *Activities) DropMirrorPartitions(ctx context.Context, in contract.DropMirrorPartitionsInput) ([]string, error) {
	return a.Store.DropMirrorPartitions(ctx, in.Before)
}

// RefreshCatalog lists every org's connected sources of connectors into the
// catalog. A source that fails keeps its last listing, and its error.
func (a *Activities) RefreshCatalog(ctx context.Context, _ struct{}) ([]contract.ListedSource, error) {
	if a.Catalog == nil {
		return nil, nil
	}
	orgs, err := a.Store.ListOrgs(ctx)
	if err != nil {
		return nil, err
	}
	var out []contract.ListedSource
	for _, org := range orgs {
		c := &pkgcatalog.Catalog{
			Queries: a.Store.Queries, Org: org, Secrets: a.Catalog.Secrets, HTTP: a.Catalog.HTTP, PlainHTTP: a.Catalog.PlainHTTP,
			GitHubAPI: a.Catalog.GitHubAPI, Official: a.Catalog.Official, Logger: a.log().With("org", org),
		}
		listed, err := c.Refresh(ctx)
		if err != nil {
			return out, err
		}
		for _, l := range listed {
			out = append(out, contract.ListedSource{Org: org.String(), Source: l.Source, Packages: l.Packages, Error: l.Error})
		}
	}
	return out, nil
}

// AuditPartitions creates the audit log's next month's partition, and drops
// the months that end by in.Before.
func (a *Activities) AuditPartitions(ctx context.Context, in contract.AuditPartitionsInput) ([]string, error) {
	if err := a.Store.EnsureAuditPartitions(ctx, in.Now); err != nil {
		return nil, err
	}
	if in.Before.IsZero() {
		return nil, nil
	}
	return a.Store.DropAuditPartitions(ctx, in.Before)
}

func ids(a, b string) (uuid.UUID, uuid.UUID, error) {
	x, err := uuid.Parse(a)
	if err != nil {
		return x, x, errs.Wrap(errs.ConfigInvalid, err, fmt.Sprintf("id %q", a))
	}
	y, err := uuid.Parse(b)
	if err != nil {
		return x, y, errs.Wrap(errs.ConfigInvalid, err, fmt.Sprintf("id %q", b))
	}
	return x, y, nil
}
