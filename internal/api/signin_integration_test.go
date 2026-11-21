//go:build integration

package api_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/api"
	"github.com/openetl-io/openetl/internal/auth"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/store/storetest"
)

// A sign-in that read the password before a reset or a lock committed starts
// no session: a reset has already deleted the sessions, and a lock refuses
// the right password too.
func TestSignInRacingAChangeToItsPassword(t *testing.T) {
	const email, password = "ann@example.com", "an old long password"
	for name, change := range map[string]func(ctx context.Context, q *db.Queries, org, id uuid.UUID) error{
		"reset": func(ctx context.Context, q *db.Queries, org, id uuid.UUID) error {
			if err := q.SetPassword(ctx, db.SetPasswordParams{OrgID: org, PrincipalID: id, Hash: auth.HashPassword("a new long password")}); err != nil {
				return err
			}
			_, err := q.DeleteSessions(ctx, db.DeleteSessionsParams{OrgID: org, PrincipalID: id})
			return err
		},
		"lock": func(ctx context.Context, q *db.Queries, org, id uuid.UUID) error {
			for range 10 {
				err := q.RecordSignInFailure(ctx, db.RecordSignInFailureParams{OrgID: org, PrincipalID: id, MaxFailures: 10, LockSeconds: 900})
				if err != nil {
					return err
				}
			}
			return nil
		},
	} {
		t.Run(name, func(t *testing.T) {
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
			tenant, err := s.Bootstrap(ctx, "dp1", nil)
			if err != nil {
				t.Fatal(err)
			}
			id := uuid.New()
			if err := s.CreatePrincipal(ctx, db.CreatePrincipalParams{OrgID: tenant.Org, ID: id, Name: "ann", Type: "user", DisplayName: "Ann", Email: email}); err != nil {
				t.Fatal(err)
			}
			if err := s.SetPassword(ctx, db.SetPasswordParams{OrgID: tenant.Org, PrincipalID: id, Hash: auth.HashPassword(password)}); err != nil {
				t.Fatal(err)
			}
			conn, err := pgx.Connect(ctx, url)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close(context.Background()) })
			watch, err := pgx.Connect(ctx, url)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = watch.Close(context.Background()) })
			// The change, made and not yet committed.
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(context.Background()) }()
			if err := change(ctx, db.New(tx), tenant.Org, id); err != nil {
				t.Fatal(err)
			}

			srv := &api.Server{Store: s, Tenant: tenant, Logger: slog.New(slog.DiscardHandler)}
			done := make(chan error, 1)
			go func() {
				_, err := srv.CreateSession(ctx, connect.NewRequest(&apiv1.CreateSessionRequest{Email: email, Password: password}))
				done <- err
			}()
			// The sign-in reads the committed password, then waits on the change.
			for waiting := false; !waiting; {
				select {
				case err := <-done:
					t.Fatalf("the sign-in ended before the change committed: %v", err)
				case <-time.After(20 * time.Millisecond):
				}
				err := watch.QueryRow(ctx, `SELECT count(*) > 0 FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-done; connect.CodeOf(err) != connect.CodeUnauthenticated {
				t.Fatalf("sign-in: %v, want it refused", err)
			}
			var sessions int
			if err := watch.QueryRow(ctx, `SELECT count(*) FROM sessions WHERE principal_id = $1`, id).Scan(&sessions); err != nil {
				t.Fatal(err)
			}
			if sessions != 0 {
				t.Errorf("%d sessions, want none", sessions)
			}
		})
	}
}
