//go:build integration

package heal_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/heal"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/store/storetest"
)

type fixture struct {
	s    *store.Store
	conn *pgx.Conn
	org  uuid.UUID
	// By heal mode: "", HEAL_MODE_SUGGEST, HEAL_MODE_OFF.
	pipelines map[string]uuid.UUID
}

// newFixture makes a pipeline for each heal mode.
func newFixture(t *testing.T) *fixture {
	ctx := t.Context()
	url := storetest.NewDatabase(t)
	s, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	f := &fixture{s: s, conn: conn, org: uuid.New(), pipelines: map[string]uuid.UUID{}}
	ws, src, dst := uuid.New(), uuid.New(), uuid.New()
	args := pgx.NamedArgs{"org": f.org, "ws": ws, "src": src, "dst": dst, "dp": "dp-" + f.org.String()}
	stmts := []string{
		`INSERT INTO orgs (id, name) VALUES (@org, 'o')`,
		`INSERT INTO workspaces (org_id, id, name) VALUES (@org, @ws, 'default')`,
		`INSERT INTO data_planes (org_id, id) VALUES (@org, @dp)`,
		`INSERT INTO sources (org_id, workspace_id, id, name, spec, version, revision, connector_digest, secrets)
			VALUES (@org, @ws, @src, 'src', '{"connector": "postgres"}', 's1', 1, 'sha256:s', '{src-dsn}')`,
		`INSERT INTO destinations (org_id, workspace_id, id, name, spec, version, revision, connector_digest, secrets)
			VALUES (@org, @ws, @dst, 'dst', '{"connector": "postgres"}', 'd1', 1, 'sha256:d', '{}')`,
		`INSERT INTO source_versions (org_id, source_id, version, spec, connector_digest, secrets, provenance)
			VALUES (@org, @src, 's1', '{"connector": "postgres"}', 'sha256:s', '{src-dsn}', 'test')`,
		`INSERT INTO destination_versions (org_id, destination_id, version, spec, connector_digest, secrets, provenance)
			VALUES (@org, @dst, 'd1', '{"connector": "postgres"}', 'sha256:d', '{}', 'test')`,
	}
	for i, mode := range []string{"", "HEAL_MODE_SUGGEST", "HEAL_MODE_OFF"} {
		id := uuid.New()
		f.pipelines[mode] = id
		spec := `{}`
		if mode != "" {
			spec = `{"heal": {"mode": "` + mode + `"}}`
		}
		p, sp := fmt.Sprintf("p%d", i), fmt.Sprintf("spec%d", i)
		args[p], args[sp] = id, spec
		stmts = append(stmts,
			fmt.Sprintf(`INSERT INTO pipelines (org_id, workspace_id, id, name, source_id, destination_id, data_plane_id, version, revision)
				VALUES (@org, @ws, @%s, '%s', @src, @dst, @dp, 'v1', 1)`, p, p),
			fmt.Sprintf(`INSERT INTO pipeline_versions (org_id, pipeline_id, version, spec, source_id, destination_id, data_plane_id,
				source_version, destination_version, dbt, provenance)
				VALUES (@org, @%s, 'v1', @%s, @src, @dst, @dp, 's1', 'd1', '{}', 'test')`, p, sp),
		)
	}
	for _, q := range stmts {
		if _, err := conn.Exec(ctx, q, args); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	return f
}

func (f *fixture) observe(t *testing.T, o heal.Observation) heal.Seen {
	t.Helper()
	var seen heal.Seen
	err := f.s.Tx(t.Context(), func(q *db.Queries) error {
		var err error
		seen, err = heal.Observe(t.Context(), q, f.org, o)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return seen
}

// events returns the types of the events recorded since the last call.
func (f *fixture) events(t *testing.T) []string {
	t.Helper()
	rows, err := f.conn.Query(t.Context(), `UPDATE outbox SET dispatch_time = now() WHERE org_id = $1 AND dispatch_time IS NULL
		RETURNING type, data`, f.org)
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for rows.Next() {
		var typ string
		var data []byte
		if err := rows.Scan(&typ, &data); err != nil {
			t.Fatal(err)
		}
		if typ == events.PipelinePaused {
			var p events.Pipeline
			if err := json.Unmarshal(data, &p); err != nil || p.Incident == "" {
				t.Errorf("pipeline.paused names no incident: %s", data)
			}
		}
		types = append(types, typ)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(types)
	return types
}

func (f *fixture) paused(t *testing.T, mode string) bool {
	t.Helper()
	var paused bool
	if err := f.conn.QueryRow(t.Context(), `SELECT paused FROM pipelines WHERE id = $1`, f.pipelines[mode]).Scan(&paused); err != nil {
		t.Fatal(err)
	}
	return paused
}

func actions(t *testing.T, r db.Incident) []heal.Action {
	t.Helper()
	var as []heal.Action
	if err := json.Unmarshal(r.Actions, &as); err != nil {
		t.Fatal(err)
	}
	return as
}

// An incident opens once, counts each new occurrence, pauses its pipeline
// once, and resolves when progress says so; the next one opens anew.
func TestObserveAndResolve(t *testing.T) {
	f := newFixture(t)
	p := f.pipelines[""]
	s1, s2 := uuid.New(), uuid.New()
	auth := func(sync uuid.UUID) heal.Observation {
		return heal.Observation{Pipeline: p, Class: heal.Auth, Code: "AUTH_INVALID", Message: "AUTH_INVALID: password authentication failed", Sync: &sync, Key: sync.String()}
	}

	seen := f.observe(t, auth(s1))
	if !seen.Opened || !seen.Paused || seen.Continuous || !f.paused(t, "") {
		t.Fatalf("first sighting: %+v, paused %v", seen, f.paused(t, ""))
	}
	if as := actions(t, seen.Incident); len(as) != 1 || as[0].Kind != "pause" || as[0].State != "applied" {
		t.Errorf("actions %+v", as)
	}
	if got := f.events(t); !slices.Equal(got, []string{events.IncidentOpened, events.PipelinePaused}) {
		t.Errorf("events %v", got)
	}

	// An activity retried: the same occurrence.
	again := f.observe(t, auth(s1))
	if again.Opened || again.Paused || again.Incident.ID != seen.Incident.ID || again.Incident.Occurrences != 1 {
		t.Errorf("the same sync again: %+v", again)
	}
	next := f.observe(t, auth(s2))
	if next.Opened || next.Paused || next.Incident.Occurrences != 2 || *next.Incident.SyncID != s2 {
		t.Errorf("the next sync: %+v", next)
	}
	if as := actions(t, next.Incident); len(as) != 1 {
		t.Errorf("pausing a paused pipeline added an action: %+v", as)
	}
	if got := f.events(t); len(got) != 0 {
		t.Errorf("repeats recorded %v", got)
	}

	// A warning that turns critical is escalated once.
	r := uuid.New()
	risk := heal.Observation{Pipeline: p, Class: heal.RetentionAtRisk, Severity: heal.Warning, Message: "55%", Details: map[string]string{"used": "55%"}, Key: r.String()}
	f.observe(t, risk)
	risk.Severity, risk.Details["used"] = heal.Critical, "85%"
	f.observe(t, risk)
	f.observe(t, risk)
	if got := f.events(t); !slices.Equal(got, []string{events.IncidentEscalated, events.IncidentOpened}) {
		t.Errorf("events %v", got)
	}

	var resolved []db.Incident
	err := f.s.Tx(t.Context(), func(q *db.Queries) error {
		var err error
		resolved, err = heal.Resolve(t.Context(), q, f.org, p, heal.ResolvedByProgress, nil, "sync x succeeded")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resolved) != 1 || resolved[0].ID != seen.Incident.ID || resolved[0].Status != "resolved" || *resolved[0].Resolution != "sync x succeeded" {
		t.Errorf("resolved %+v", resolved)
	}
	if got := f.events(t); !slices.Equal(got, []string{events.IncidentResolved}) {
		t.Errorf("events %v", got)
	}
	if reopened := f.observe(t, auth(uuid.New())); !reopened.Opened || reopened.Incident.ID == seen.Incident.ID {
		t.Errorf("after resolving, another failure: %+v", reopened)
	}
}

// Heal mode suggest proposes the pause, and off does nothing.
func TestHealModes(t *testing.T) {
	f := newFixture(t)
	for mode, want := range map[string]string{"HEAL_MODE_SUGGEST": "proposed", "HEAL_MODE_OFF": ""} {
		sync := uuid.New()
		seen := f.observe(t, heal.Observation{
			Pipeline: f.pipelines[mode], Stream: "items", Class: heal.SchemaBlocked, Code: "SCHEMA_DRIFT", Message: "m", Sync: &sync, Key: sync.String(),
		})
		if seen.Paused || f.paused(t, mode) {
			t.Errorf("%s paused the pipeline", mode)
		}
		as := actions(t, seen.Incident)
		switch {
		case want == "" && len(as) != 0:
			t.Errorf("%s: actions %+v", mode, as)
		case want != "" && (len(as) != 1 || as[0].State != want):
			t.Errorf("%s: actions %+v", mode, as)
		}
	}
}

// Sightings at the same time open one incident.
func TestConcurrentSightings(t *testing.T) {
	f := newFixture(t)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			sync := uuid.New()
			err := f.s.Tx(context.Background(), func(q *db.Queries) error {
				_, err := heal.Observe(context.Background(), q, f.org, heal.Observation{
					Pipeline: f.pipelines["HEAL_MODE_OFF"], Class: heal.SyncFailed, Code: "INTERNAL", Message: "boom", Sync: &sync, Key: sync.String(),
				})
				return err
			})
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	var n, occurrences int
	if err := f.conn.QueryRow(t.Context(), `SELECT count(*), sum(occurrences) FROM incidents WHERE org_id = $1`, f.org).Scan(&n, &occurrences); err != nil {
		t.Fatal(err)
	}
	if n != 1 || occurrences != 8 {
		t.Errorf("%d incidents with %d occurrences, want 1 with 8", n, occurrences)
	}
}
