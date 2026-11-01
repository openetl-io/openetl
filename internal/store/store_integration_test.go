//go:build integration

package store_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/unsuitable001/openetl/internal/auth"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/db"
	"github.com/unsuitable001/openetl/internal/store/storetest"
)

func TestMigrateAndBootstrapAreIdempotent(t *testing.T) {
	ctx := t.Context()
	s, err := store.Open(ctx, storetest.NewDatabase(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for range 2 {
		if err := s.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var key string
	var orgs []uuid.UUID
	then := func(q *db.Queries, org uuid.UUID) error {
		orgs = append(orgs, org)
		if key != "" {
			return nil
		}
		var err error
		key, err = store.EnsureOwner(ctx, q, org, "openetld bootstrap")
		return err
	}
	a, err := s.Bootstrap(ctx, "default", then)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Bootstrap(ctx, "default", then)
	if err != nil {
		t.Fatal(err)
	}
	if a != b || len(orgs) != 2 || orgs[1] != a.Org {
		t.Errorf("second bootstrap gave %+v, first %+v; then ran for %v", b, a, orgs)
	}

	k, err := s.AuthenticateAPIKey(ctx, auth.Hash(key))
	if err != nil {
		t.Fatal(err)
	}
	if k.OrgID != a.Org || k.Principal != store.BootstrapPrincipal || !slices.Equal(k.Bindings, []string{"owner"}) {
		t.Errorf("the owner key authenticates as %+v", k)
	}
	// A second key, as openetld bootstrap-key makes, after the principal was
	// disabled.
	if err := s.SetPrincipalDisabled(ctx, db.SetPrincipalDisabledParams{OrgID: a.Org, ID: k.PrincipalID, Disabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuthenticateAPIKey(ctx, auth.Hash(key)); !errors.Is(err, pgx.ErrNoRows) {
		t.Errorf("a disabled principal's key: %v", err)
	}
	var again string
	if err := s.Tx(ctx, func(q *db.Queries) (err error) {
		again, err = store.EnsureOwner(ctx, q, a.Org, "openetld bootstrap-key")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, kk := range []string{key, again} {
		if _, err := s.AuthenticateAPIKey(ctx, auth.Hash(kk)); err != nil {
			t.Errorf("after bootstrap-key: %v", err)
		}
	}
	owners, err := s.LockOrgOwners(ctx, a.Org)
	if err != nil || len(owners) != 1 {
		t.Errorf("owners %v, %v", owners, err)
	}
	entries, err := s.ListAuditEntries(ctx, db.ListAuditEntriesParams{OrgID: a.Org, Actor: store.Openetld, MaxRows: 10})
	if err != nil {
		t.Fatal(err)
	}
	var methods []string
	for _, e := range entries {
		if len(e.Resources) != 2 || e.Resources[0] != "principals/admin" || e.Code == nil || *e.Code != "ok" {
			t.Errorf("entry %+v", e)
		}
		methods = append(methods, e.Method)
	}
	if want := []string{"openetld bootstrap-key", "openetld bootstrap"}; !slices.Equal(methods, want) {
		t.Errorf("audited %v, want %v", methods, want)
	}
}

// An entry is completed once, and then never changes or goes.
func TestAuditLogIsAppendOnly(t *testing.T) {
	ctx := t.Context()
	url := storetest.NewDatabase(t)
	s, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	org := uuid.New()
	id := uuid.Must(uuid.NewV7())
	if err := s.InsertAuditEntry(ctx, db.InsertAuditEntryParams{
		OrgID: org, ID: id, Time: time.Now(), Actor: "principals/a", Method: "m", Resources: []string{"r"}, Details: []byte("{}"),
	}); err != nil {
		t.Fatal(err)
	}
	ok, now := "ok", time.Now()
	complete := db.CompleteAuditEntryParams{OrgID: org, ID: id, Resources: []string{"r", "s"}, Details: []byte(`{"k": 1}`), Code: &ok, EndTime: &now}
	for i, want := range []int64{1, 0} {
		if n, err := s.CompleteAuditEntry(ctx, complete); err != nil || n != want {
			t.Errorf("completion %d: %d rows, %v", i+1, n, err)
		}
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, q := range []string{
		"UPDATE audit_log SET actor = 'principals/b'",
		"UPDATE audit_log SET code = NULL",
		"UPDATE audit_log SET details = '{}'",
		"DELETE FROM audit_log",
	} {
		if _, err := conn.Exec(ctx, q); err == nil {
			t.Errorf("%s: no error", q)
		}
	}
	got, err := s.ListAuditEntries(ctx, db.ListAuditEntriesParams{OrgID: org, Resource: "s", MaxRows: 10})
	if err != nil || len(got) != 1 || *got[0].Code != "ok" || got[0].Actor != "principals/a" || string(got[0].Details) != `{"k": 1}` {
		t.Errorf("entries %+v, %v", got, err)
	}
}

// A day's partition goes once the whole day is before the cutoff.
func TestDropMirrorPartitions(t *testing.T) {
	ctx := t.Context()
	url := storetest.NewDatabase(t)
	s, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, day := range []string{"2026-09-01", "2026-09-30", "2026-10-01", "2026-10-31"} {
		start, _ := time.Parse(time.DateOnly, day)
		if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE checkpoint_mirror_%s PARTITION OF checkpoint_mirror FOR VALUES FROM ('%s') TO ('%s')`,
			start.Format("20060102"), start.Format(time.RFC3339), start.AddDate(0, 0, 1).Format(time.RFC3339))); err != nil {
			t.Fatal(err)
		}
	}
	dropped, err := s.DropMirrorPartitions(ctx, time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"checkpoint_mirror_20260901", "checkpoint_mirror_20260930"}; !slices.Equal(dropped, want) {
		t.Fatalf("dropped %v, want %v", dropped, want)
	}
	var left []string
	rows, err := conn.Query(ctx, `SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'checkpoint_mirror'::regclass ORDER BY 1`)
	if err == nil {
		left, err = pgx.CollectRows(rows, pgx.RowTo[string])
	}
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"checkpoint_mirror_20261001", "checkpoint_mirror_20261031"}; !slices.Equal(left, want) {
		t.Fatalf("partitions left %v, want %v", left, want)
	}
}

// Migration 11 pins what pipeline versions from before it ran: their
// sources' and destinations' current configs, and their dbt projects'
// current bundles. Versions whose source is gone go.
func TestPinnedVersionsMigration(t *testing.T) {
	ctx := t.Context()
	url := storetest.NewDatabase(t)
	s, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.MigrateTo(ctx, 10); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	org, ws, src, gone, dst, pipeline := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	args := pgx.NamedArgs{"org": org, "ws": ws, "src": src, "gone": gone, "dst": dst, "pipeline": pipeline, "dp": "dp-" + org.String()}
	for _, q := range []string{
		`INSERT INTO orgs (id, name) VALUES (@org, 'o')`,
		`INSERT INTO workspaces (org_id, id, name) VALUES (@org, @ws, 'default')`,
		`INSERT INTO data_planes (org_id, id) VALUES (@org, @dp)`,
		`INSERT INTO sources (org_id, workspace_id, id, name, spec, version, revision, connector_digest, secrets)
			VALUES (@org, @ws, @src, 'src', '{"connector": "faker"}', 's1', 1, '', '{}')`,
		`INSERT INTO sources (org_id, workspace_id, id, name, spec, version, revision) VALUES (@org, @ws, @gone, 'gone', '{}', 'g1', 1)`,
		`INSERT INTO destinations (org_id, workspace_id, id, name, spec, version, revision, connector_digest, secrets)
			VALUES (@org, @ws, @dst, 'dst', '{"connector": "postgres"}', 'd1', 1, 'sha256:c', '{dsn}')`,
		`INSERT INTO artifacts (org_id, digest, media_type, content) VALUES (@org, 'sha256:b', 'x', '')`,
		`INSERT INTO dbt_projects (org_id, workspace_id, id, name, digest, revision) VALUES (@org, @ws, gen_random_uuid(), 'proj', 'sha256:b', 1)`,
		`INSERT INTO pipelines (org_id, workspace_id, id, name, source_id, destination_id, data_plane_id, version, revision)
			VALUES (@org, @ws, @pipeline, 'p', @src, @dst, @dp, 'v2', 2)`,
		`INSERT INTO pipeline_versions (org_id, pipeline_id, version, spec, source_id, destination_id, data_plane_id)
			VALUES (@org, @pipeline, 'v1', '{}', @gone, @dst, @dp)`,
		`INSERT INTO pipeline_versions (org_id, pipeline_id, version, spec, source_id, destination_id, data_plane_id)
			VALUES (@org, @pipeline, 'v2', '{"postSync": [{"dbt": {"project": "proj"}}, {"dbt": {"project": "proj", "command": "DBT_COMMAND_TEST"}}]}', @src, @dst, @dp)`,
		`DELETE FROM sources WHERE id = @gone`,
	} {
		if _, err := conn.Exec(ctx, q, args); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	v, err := s.GetSyncSpec(ctx, db.GetSyncSpecParams{OrgID: org, PipelineID: pipeline, Version: "v2"})
	if err != nil {
		t.Fatal(err)
	}
	if string(v.SourceSpec) != `{"connector": "faker"}` || v.DestinationConnector != "sha256:c" || !slices.Equal(v.DestinationSecrets, []string{"dsn"}) {
		t.Errorf("v2 pins %+v", v)
	}
	if string(v.Dbt) != `{"proj": "sha256:b"}` {
		t.Errorf("v2 pins dbt %s", v.Dbt)
	}
	if _, err := s.GetPipelineVersion(ctx, db.GetPipelineVersionParams{OrgID: org, PipelineID: pipeline, Version: "v1"}); !errors.Is(err, pgx.ErrNoRows) {
		t.Errorf("v1, whose source is gone: %v", err)
	}
}
