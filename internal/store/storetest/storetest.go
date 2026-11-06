// Package storetest creates throwaway metadata databases on the compose
// Postgres (make deps-up) or OPENETL_TEST_POSTGRES_DSN.
package storetest

import (
	"cmp"
	"context"
	"crypto/rand"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func DSN() string {
	return cmp.Or(os.Getenv("OPENETL_TEST_POSTGRES_DSN"), "postgres://openetl:openetl@localhost:5432/openetl")
}

// NewDatabase creates an empty database, dropped when the test ends, and
// returns its URL.
func NewDatabase(t testing.TB) string {
	t.Helper()
	u, drop, err := Create(t.Context())
	if err != nil {
		t.Fatalf("storetest: %v", err)
	}
	t.Cleanup(drop)
	return u
}

// Create creates an empty database and returns its URL, and a function that
// drops it.
func Create(ctx context.Context) (string, func(), error) {
	admin, err := pgx.Connect(ctx, DSN())
	if err != nil {
		return "", nil, err
	}
	name := "openetl_test_" + strings.ToLower(rand.Text())[:12]
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		_ = admin.Close(ctx)
		return "", nil, err
	}
	drop := func() {
		ctx := context.Background()
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		_ = admin.Close(ctx)
	}
	u, err := url.Parse(DSN())
	if err != nil {
		drop()
		return "", nil, err
	}
	u.Path = "/" + name
	return u.String(), drop, nil
}
