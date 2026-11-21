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
	const old, reset = "an old long password", "a reset long password"
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
				_, err := s.CreateSession(ctx, connect.NewRequest(&apiv1.CreateSessionRequest{Email: annEmail, Password: old}))
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
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				st, tenant, id, url := annWithPassword(ctx, t, old)
				conn, watch := dial(ctx, t, url), dial(ctx, t, url)
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
				waitForLock(ctx, t, watch, done)
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

// A user deleted while their sign-in starts a session waits for the session,
// and then deletes it, rather than deadlocking with it: the delete locks the
// principal and then cascades to the password, so the sign-in takes them in
// that order too.
func TestSignInRacingADelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	st, tenant, id, url := annWithPassword(ctx, t, "an old long password")
	watch := dial(ctx, t, url)
	s := &Server{Store: st, Tenant: tenant}
	done := make(chan error, 1)
	err := st.Tx(ctx, func(q *db.Queries) error {
		cur, err := q.GetPassword(ctx, db.GetPasswordParams{OrgID: tenant.Org, PrincipalID: id})
		if err != nil {
			return err
		}
		if changed, err := s.passwordChanged(ctx, q, id, cur.Hash); err != nil || changed {
			t.Fatalf("changed %v, %v", changed, err)
		}
		go func() {
			_, err := st.DeletePrincipal(ctx, db.DeletePrincipalParams{OrgID: tenant.Org, ID: id})
			done <- err
		}()
		waitForLock(ctx, t, watch, done)
		return q.InsertSession(ctx, db.InsertSessionParams{
			OrgID: tenant.Org, ID: uuid.New(), PrincipalID: id, Hash: auth.NewSession().Hash, ExpireTime: time.Now().Add(time.Hour),
		})
	})
	if err != nil {
		t.Fatalf("the sign-in: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("the delete: %v", err)
	}
	var sessions int
	if err := watch.QueryRow(ctx, `SELECT count(*) FROM sessions WHERE principal_id = $1`, id).Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if sessions != 0 {
		t.Errorf("%d sessions, want none", sessions)
	}
}

const annEmail = "ann@example.com"

// annWithPassword makes a database with one user, ann, whose password is pw.
func annWithPassword(ctx context.Context, t *testing.T, pw string) (*store.Store, store.Tenant, uuid.UUID, string) {
	t.Helper()
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
	if err := st.CreatePrincipal(ctx, db.CreatePrincipalParams{OrgID: tenant.Org, ID: id, Name: "ann", Type: "user", DisplayName: "Ann", Email: annEmail}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetPassword(ctx, db.SetPasswordParams{OrgID: tenant.Org, PrincipalID: id, Hash: auth.HashPassword(pw)}); err != nil {
		t.Fatal(err)
	}
	return st, tenant, id, url
}

func dial(ctx context.Context, t *testing.T, url string) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

// waitForLock returns once a query in the database waits on a lock, and
// fails if done is sent first.
func waitForLock(ctx context.Context, t *testing.T, watch *pgx.Conn, done <-chan error) {
	t.Helper()
	for waiting := false; !waiting; {
		select {
		case err := <-done:
			t.Fatalf("it ended without waiting: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		err := watch.QueryRow(ctx, `SELECT count(*) > 0 FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
	}
}
