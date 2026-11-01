package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/unsuitable001/openetl/internal/auth"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// CreateAPIKey adds a key for a principal. A nil expiry never expires.
func CreateAPIKey(ctx context.Context, q *db.Queries, org, principal uuid.UUID, expire *time.Time) (auth.Credential, uuid.UUID, error) {
	c, id := auth.NewKey(), uuid.New()
	err := q.InsertAPIKey(ctx, db.InsertAPIKeyParams{OrgID: org, ID: id, PrincipalID: principal, Hash: c.Hash, Prefix: c.Shown, ExpireTime: expire})
	return c, id, err
}

func CreateRunnerToken(ctx context.Context, q *db.Queries, org uuid.UUID, dataPlane string) (auth.Credential, uuid.UUID, error) {
	c, id := auth.NewRunnerToken(), uuid.New()
	err := q.InsertRunnerToken(ctx, db.InsertRunnerTokenParams{OrgID: org, ID: id, DataPlaneID: dataPlane, Hash: c.Hash, Prefix: c.Shown})
	return c, id, err
}

// BootstrapPrincipal is the service account made with the org.
const BootstrapPrincipal = "admin"

// EnsureOwner makes the bootstrap principal an enabled owner of the org,
// creating it if it's gone, and gives it a new key.
func EnsureOwner(ctx context.Context, q *db.Queries, org uuid.UUID) (string, error) {
	p, err := q.GetPrincipal(ctx, db.GetPrincipalParams{OrgID: org, Name: BootstrapPrincipal})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		p = db.Principal{ID: uuid.New()}
		err = q.CreatePrincipal(ctx, db.CreatePrincipalParams{
			OrgID: org, ID: p.ID, Name: BootstrapPrincipal, Type: "service_account", DisplayName: "Bootstrap owner",
		})
	case err == nil && p.Disabled:
		err = q.SetPrincipalDisabled(ctx, db.SetPrincipalDisabledParams{OrgID: org, ID: p.ID, Disabled: false})
	}
	if err != nil {
		return "", err
	}
	if _, err := q.InsertRoleBinding(ctx, db.InsertRoleBindingParams{OrgID: org, PrincipalID: p.ID, Role: auth.Owner.String()}); err != nil {
		return "", err
	}
	c, _, err := CreateAPIKey(ctx, q, org, p.ID, nil)
	return c.Secret, err
}
