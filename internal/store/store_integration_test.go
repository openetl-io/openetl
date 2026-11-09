//go:build integration

package store_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/openetl-io/openetl/internal/auth"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/store/storetest"
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
		WHERE i.inhparent = 'checkpoint_mirror'::regclass AND c.relname <= 'checkpoint_mirror_20261031' ORDER BY 1`)
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

// A drop stuck behind an open reader gives up, because every query on the
// table would queue behind it, and works once the reader has gone.
func TestDropGivesUpBehindAnOpenReader(t *testing.T) {
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
	cutoff := time.Date(2020, 3, 1, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		table, partition, to string
		drop                 func(context.Context, time.Time) ([]string, error)
	}{
		{"checkpoint_mirror", "checkpoint_mirror_20200101", "2020-01-02", s.DropMirrorPartitions},
		{"audit_log", "audit_log_202001", "2020-02-01", s.DropAuditPartitions},
	} {
		t.Run(c.table, func(t *testing.T) {
			if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s PARTITION OF %s FOR VALUES FROM ('2020-01-01') TO ('%s')`, c.partition, c.table, c.to)); err != nil {
				t.Fatal(err)
			}
			reader, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.Rollback(context.Background()) }()
			if _, err := reader.Exec(ctx, "SELECT 1 FROM "+c.table+" LIMIT 1"); err != nil {
				t.Fatal(err)
			}
			waiting, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			dropped, err := c.drop(waiting, cutoff)
			if pe, ok := errors.AsType[*pgconn.PgError](err); !ok || pe.Code != "55P03" || len(dropped) != 0 {
				t.Fatalf("behind a reader: dropped %v, %v; want a lock timeout", dropped, err)
			}
			if err := reader.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			if dropped, err := c.drop(ctx, cutoff); err != nil || !slices.Equal(dropped, []string{c.partition}) {
				t.Fatalf("after the reader: dropped %v, %v", dropped, err)
			}
		})
	}
}

// Migrate makes the partitions for the days checkpoints are about to land in,
// so reports don't have to.
func TestMigrateCreatesTheMirrorsDays(t *testing.T) {
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
	today := time.Now().UTC().Truncate(24 * time.Hour)
	for _, day := range []time.Time{today, today.AddDate(0, 0, 1)} {
		name := "checkpoint_mirror_" + day.Format("20060102")
		var exists bool
		if err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil || !exists {
			t.Errorf("partition %s exists: %v (%v)", name, exists, err)
		}
	}
}

// Making a partition behind an open reader gives up, as dropping one does.
func TestCreatingAPartitionGivesUpBehindAnOpenReader(t *testing.T) {
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
	far := time.Date(2040, 6, 15, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		table  string
		ensure func(context.Context, time.Time) error
	}{
		{"checkpoint_mirror", s.EnsureMirrorPartitions},
		{"audit_log", s.EnsureAuditPartitions},
	} {
		t.Run(c.table, func(t *testing.T) {
			reader, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.Rollback(context.Background()) }()
			if _, err := reader.Exec(ctx, "SELECT 1 FROM "+c.table+" LIMIT 1"); err != nil {
				t.Fatal(err)
			}
			waiting, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			err = c.ensure(waiting, far)
			if pe, ok := errors.AsType[*pgconn.PgError](err); !ok || pe.Code != "55P03" {
				t.Fatalf("behind a reader: %v; want a lock timeout", err)
			}
			if err := reader.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			if err := c.ensure(ctx, far); err != nil {
				t.Fatalf("after the reader: %v", err)
			}
		})
	}
}

// A month's partition of the audit log is created when an entry first needs
// it, and goes once the whole month is before the cutoff.
func TestAuditLogPartitions(t *testing.T) {
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
	entry := func(at time.Time) db.InsertAuditEntryParams {
		return db.InsertAuditEntryParams{OrgID: org, ID: uuid.Must(uuid.NewV7()), Time: at, Actor: "principals/a", Method: "m", Resources: []string{"r"}, Details: []byte("{}")}
	}
	for _, at := range []time.Time{time.Date(2025, 1, 10, 0, 0, 0, 0, time.FixedZone("x", -5*3600)), time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 30, 23, 59, 0, 0, time.UTC), time.Now()} {
		if err := s.WriteAuditEntry(ctx, entry(at)); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := s.ListAuditEntries(ctx, db.ListAuditEntriesParams{OrgID: org, MaxRows: 10}); err != nil || len(got) != 4 {
		t.Fatalf("%d entries, %v", len(got), err)
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	partitions := func() []string {
		rows, err := conn.Query(ctx, `SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
			WHERE i.inhparent = 'audit_log'::regclass ORDER BY 1`)
		if err != nil {
			t.Fatal(err)
		}
		names, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		return names
	}
	month := func(t time.Time) string { return "audit_log_" + t.UTC().Format("200601") }
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)
	if got := partitions(); got[0] != "audit_log_202501" || got[1] != "audit_log_202608" || got[2] != "audit_log_202609" || got[len(got)-1] != month(next) {
		t.Fatalf("partitions %v", got)
	}
	if _, err := conn.Exec(ctx, "DELETE FROM audit_log"); err == nil {
		t.Error("an entry was deleted from a partition")
	}

	dropped, err := s.DropAuditPartitions(ctx, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"audit_log_202501", "audit_log_202608", "audit_log_202609"}; !slices.Equal(dropped, want) {
		t.Fatalf("dropped %v, want %v", dropped, want)
	}
	if got := partitions(); got[0] != month(now) || got[len(got)-1] != month(next) {
		t.Errorf("partitions left %v", got)
	}
	if got, err := s.ListAuditEntries(ctx, db.ListAuditEntriesParams{OrgID: org, MaxRows: 10}); err != nil || len(got) != 1 {
		t.Errorf("%d entries left, %v", len(got), err)
	}
}

// A secret holds a sealed value or a reference, and only a sealed value needs
// the org's data key.
func TestASecretHoldsAValueOrAReference(t *testing.T) {
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
	org, keyless, ws, ws2 := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	args := pgx.NamedArgs{"org": org, "keyless": keyless, "ws": ws, "ws2": ws2}
	exec := func(q string) error {
		_, err := conn.Exec(ctx, q, args)
		return err
	}
	for _, q := range []string{
		`INSERT INTO orgs (id, name) VALUES (@org, 'o'), (@keyless, 'k')`,
		`INSERT INTO workspaces (org_id, id, name) VALUES (@org, @ws, 'default'), (@keyless, @ws2, 'default')`,
		`INSERT INTO org_keys (org_id, wrapped_key) VALUES (@org, 'k')`,
		`INSERT INTO secrets (org_id, workspace_id, id, name, ciphertext, revision) VALUES (@org, @ws, gen_random_uuid(), 'sealed', 'c', 1)`,
	} {
		if err := exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	r, err := s.GetSecret(ctx, db.GetSecretParams{OrgID: org, WorkspaceID: ws, Name: "sealed"})
	if err != nil || string(r.Ciphertext) != "c" || r.Ref != nil {
		t.Errorf("the sealed secret: %+v, %v", r, err)
	}
	ref := "aws-sm://prod/db"
	if err := s.InsertSecret(ctx, db.InsertSecretParams{OrgID: keyless, WorkspaceID: ws2, ID: uuid.New(), Name: "ref", Ref: &ref}); err != nil {
		t.Errorf("a reference in an org without a data key: %v", err)
	}
	for _, q := range []string{
		`INSERT INTO secrets (org_id, workspace_id, id, name, revision) VALUES (@org, @ws, gen_random_uuid(), 'neither', 1)`,
		`INSERT INTO secrets (org_id, workspace_id, id, name, ciphertext, ref, revision) VALUES (@org, @ws, gen_random_uuid(), 'both', 'c', 'aws-sm://x', 1)`,
	} {
		if err := exec(q); err == nil || !strings.Contains(err.Error(), "secrets_value_or_ref") {
			t.Errorf("%s: %v", q, err)
		}
	}
}

// A connector version is one package version, with an artifact for each of
// its roles.
func TestAConnectorVersionHasAnArtifactPerRole(t *testing.T) {
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
	org := uuid.New()
	for _, q := range []string{
		`INSERT INTO orgs (id, name) VALUES (@org, 'o')`,
		`INSERT INTO artifacts (org_id, digest, media_type, content) VALUES (@org, 'sha256:s', 'x', ''), (@org, 'sha256:d', 'x', ''), (@org, 'sha256:k', 'x', '')`,
		`INSERT INTO connector_versions (org_id, name, version, provenance, author)
			VALUES (@org, 'openetl/postgres', '0.1.0', 'cli:s', 'Early'),
			       (@org, 'openetl/kafka', '0.2.0', 'cli:k', '')`,
		`INSERT INTO connector_roles (org_id, name, version, role, digest)
			VALUES (@org, 'openetl/postgres', '0.1.0', 'source', 'sha256:s'),
			       (@org, 'openetl/postgres', '0.1.0', 'destination', 'sha256:d'),
			       (@org, 'openetl/kafka', '0.2.0', 'source', 'sha256:k')`,
	} {
		if _, err := conn.Exec(ctx, q, pgx.NamedArgs{"org": org}); err != nil {
			t.Fatal(err)
		}
	}
	pg, err := s.GetConnectorVersion(ctx, db.GetConnectorVersionParams{OrgID: org, Name: "openetl/postgres", Version: "0.1.0"})
	if err != nil || pg.Author != "Early" || pg.Provenance != "cli:s" {
		t.Fatalf("postgres 0.1.0: %+v, %v", pg, err)
	}
	roles, err := s.ListVersionRoles(ctx, db.ListVersionRolesParams{OrgID: org, Name: "openetl/postgres", Version: "0.1.0"})
	if err != nil || len(roles) != 2 {
		t.Fatalf("roles %+v, %v", roles, err)
	}
	for _, r := range roles {
		if r.Digest != map[string]string{"source": "sha256:s", "destination": "sha256:d"}[r.Role] {
			t.Errorf("role %+v", r)
		}
	}
	if got, err := s.ListConnectorRoles(ctx, db.ListConnectorRolesParams{OrgID: org, Name: "openetl/kafka", Role: "source"}); err != nil || len(got) != 1 || got[0].Digest != "sha256:k" {
		t.Errorf("kafka's source: %+v, %v", got, err)
	}
	if got, err := s.ConnectorVersionsByDigest(ctx, db.ConnectorVersionsByDigestParams{OrgID: org, Digests: []string{"sha256:d"}}); err != nil || len(got) != 1 || got[0].Role != "destination" || got[0].Name != "openetl/postgres" {
		t.Errorf("by digest: %+v, %v", got, err)
	}
	if err := s.InsertConnectorRole(ctx, db.InsertConnectorRoleParams{OrgID: org, Name: "openetl/none", Version: "1.0.0", Role: "source", Digest: "sha256:s"}); err == nil {
		t.Error("a role of a version that doesn't exist")
	}
}

// What a supervisor compares to see a hold released or a reset or rewind asked
// for changes with each one, though the server that made it has a slower clock
// and gave it an ID older than the newest.
func TestReleasesAndRequestsAreSeenWhateverTheirIDs(t *testing.T) {
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
	tenant, err := s.Bootstrap(ctx, "default", func(*db.Queries, uuid.UUID) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	// A pipeline's source and destination don't matter here.
	if _, err := conn.Exec(ctx, "SET session_replication_role = replica"); err != nil {
		t.Fatal(err)
	}
	org, pipeline := tenant.Org, uuid.New()
	if _, err := conn.Exec(ctx, `INSERT INTO pipelines (org_id, workspace_id, id, name, source_id, destination_id, data_plane_id, version, revision)
		VALUES ($1, $2, $3, 'p', $2, $2, 'default', 'v1', 1)`, org, uuid.New(), pipeline); err != nil {
		t.Fatal(err)
	}
	newer, older := uuid.Must(uuid.NewV7()), uuid.MustParse("00000000-0000-7000-8000-000000000001")
	soon, before := time.Now().Add(time.Hour), time.Now().Add(-time.Hour)

	releases := func() string {
		t.Helper()
		v, err := s.NewestHoldRelease(ctx, db.NewestHoldReleaseParams{OrgID: org, PipelineID: pipeline})
		if err != nil {
			t.Fatal(err)
		}
		at, err := s.NewestHoldReleaseAt(ctx, db.NewestHoldReleaseAtParams{OrgID: org, PipelineID: pipeline, At: soon})
		if err != nil || at != v {
			t.Fatalf("at %q, %v; now %q", at, err, v)
		}
		return v
	}
	release := func(id uuid.UUID, stream string) {
		t.Helper()
		if _, err := s.InsertHoldRelease(ctx, db.InsertHoldReleaseParams{OrgID: org, ID: id, PipelineID: pipeline, Stream: stream, ColumnName: "c", Principal: "p"}); err != nil {
			t.Fatal(err)
		}
	}
	if got := releases(); got != "" {
		t.Errorf("no releases: %q", got)
	}
	release(newer, "a")
	first := releases()
	release(older, "b")
	if got := releases(); got == first || got == "" {
		t.Errorf("a release with an older ID left %q as it was", got)
	}
	if got, err := s.NewestHoldReleaseAt(ctx, db.NewestHoldReleaseAtParams{OrgID: org, PipelineID: pipeline, At: before}); err != nil || got != "" {
		t.Errorf("before any: %q, %v", got, err)
	}

	requests := func() string {
		t.Helper()
		v, err := s.NewestCheckpointRequest(ctx, db.NewestCheckpointRequestParams{OrgID: org, PipelineID: pipeline})
		if err != nil {
			t.Fatal(err)
		}
		at, err := s.NewestCheckpointRequestAt(ctx, db.NewestCheckpointRequestAtParams{OrgID: org, PipelineID: pipeline, At: soon})
		if err != nil || at != v {
			t.Fatalf("at %q, %v; now %q", at, err, v)
		}
		return v
	}
	request := func(id uuid.UUID) {
		t.Helper()
		if _, err := s.InsertCheckpointRequest(ctx, db.InsertCheckpointRequestParams{OrgID: org, ID: id, PipelineID: pipeline, Kind: "reset", Streams: []string{}, Principal: "p"}); err != nil {
			t.Fatal(err)
		}
	}
	if got := requests(); got != "" {
		t.Errorf("no requests: %q", got)
	}
	request(newer)
	first = requests()
	request(older)
	if got := requests(); got == first || got == "" {
		t.Errorf("a request with an older ID left %q as it was", got)
	}
	if got, err := s.NewestCheckpointRequestAt(ctx, db.NewestCheckpointRequestAtParams{OrgID: org, PipelineID: pipeline, At: before}); err != nil || got != "" {
		t.Errorf("before any: %q, %v", got, err)
	}
}
