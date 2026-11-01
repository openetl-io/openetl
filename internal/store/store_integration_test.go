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
