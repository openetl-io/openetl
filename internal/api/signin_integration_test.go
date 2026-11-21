//go:build integration

package api

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/auth"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/store/storetest"
)

// A sign-in or a password change that checked the password before a reset or
// a lock committed does nothing once it has: a reset has already deleted the
// sessions, and a lock refuses the right password too.
func TestPasswordChecksRacingAChange(t *testing.T) {
	const email, old, reset = "ann@example.com", "an old long password", "a reset long password"
	changes := map[string]func(ctx context.Context, q *db.Queries, org, id uuid.UUID) error{
		"reset": func(ctx context.Context, q *db.Queries, org, id uuid.UUID) error {
			if err := q.SetPassword(ctx, db.SetPasswordParams{OrgID: org, PrincipalID: id, Hash: auth.HashPassword(reset)}); err != nil {
				return err
			}
			_, err := q.DeleteSessions(ctx, db.DeleteSessionsParams{OrgID: org, PrincipalID: id})
			return err
		},
		"lock": func(ctx context.Context, q *db.Queries, org, id uuid.UUID) error {
			for range maxSignInFailures {
				err := q.RecordSignInFailure(ctx, db.RecordSignInFailureParams{OrgID: org, PrincipalID: id, MaxFailures: maxSignInFailures, LockSeconds: 900})
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
	checks := map[string]struct {
		call func(ctx context.Context, s *Server, id uuid.UUID) error
		want connect.Code
	}{
		"sign-in": {
			func(ctx context.Context, s *Server, _ uuid.UUID) error {
				_, err := s.CreateSession(ctx, connect.NewRequest(&apiv1.CreateSessionRequest{Email: email, Password: old}))
				return err
			},
			connect.CodeUnauthenticated,
		},
		"change": {
			func(ctx context.Context, s *Server, id uuid.UUID) error {
				ctx = context.WithValue(ctx, callerKey{}, &caller{org: s.Tenant.Org, principal: principalName("ann"), principalID: id, session: uuid.New()})
				_, err := s.ChangePassword(ctx, connect.NewRequest(&apiv1.ChangePasswordRequest{CurrentPassword: old, NewPassword: "a changed long password"}))
				return err
			},
			connect.CodeFailedPrecondition,
		},
	}
	for change, apply := range changes {
		for check, c := range checks {
			t.Run(check+" "+change, func(t *testing.T) {
				ctx := t.Context()
				url := storetest.NewDatabase(t)
				st, err := store.Open(ctx, url)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(st.Close)
				if err := st.Migrate(ctx); err != nil {
					t.Fatal(err)
				}
				tenant, err := st.Bootstrap(ctx, "dp1", nil)
				if err != nil {
					t.Fatal(err)
				}
				id := uuid.New()
				if err := st.CreatePrincipal(ctx, db.CreatePrincipalParams{OrgID: tenant.Org, ID: id, Name: "ann", Type: "user", DisplayName: "Ann", Email: email}); err != nil {
					t.Fatal(err)
				}
				if err := st.SetPassword(ctx, db.SetPasswordParams{OrgID: tenant.Org, PrincipalID: id, Hash: auth.HashPassword(old)}); err != nil {
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
				if err := apply(ctx, db.New(tx), tenant.Org, id); err != nil {
					t.Fatal(err)
				}

				s := &Server{Store: st, Tenant: tenant, Logger: slog.New(slog.DiscardHandler)}
				done := make(chan error, 1)
				go func() { done <- c.call(ctx, s, id) }()
				// The check reads the committed password, then waits on the change.
				for waiting := false; !waiting; {
					select {
					case err := <-done:
						t.Fatalf("it ended before the change committed: %v", err)
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
				if err := <-done; connect.CodeOf(err) != c.want {
					t.Fatalf("%v, want %v", err, c.want)
				}
				var sessions int
				var hash string
				err = watch.QueryRow(ctx, `SELECT (SELECT count(*) FROM sessions WHERE principal_id = $1), (SELECT hash FROM passwords WHERE principal_id = $1)`, id).Scan(&sessions, &hash)
				if err != nil {
					t.Fatal(err)
				}
				if sessions != 0 {
					t.Errorf("%d sessions, want none", sessions)
				}
				want := map[string]string{"reset": reset, "lock": old}[change]
				if ok, _ := auth.CheckPassword(hash, want); !ok {
					t.Errorf("the password isn't %q", want)
				}
			})
		}
	}
}
