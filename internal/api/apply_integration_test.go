//go:build integration

package api_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/api"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/store/storetest"
)

type applyEnv struct {
	tenant  store.Tenant
	url     string
	conn    *pgx.Conn
	applier apiv1connect.ApplyServiceClient
}

func newApplyEnv(t *testing.T) *applyEnv {
	t.Helper()
	return newApplyEnvWith(t, connectorhost.Env{})
}

// newApplyEnvWith is newApplyEnv with a server that knows connectors.
func newApplyEnvWith(t *testing.T, connectors connectorhost.Env) *applyEnv {
	t.Helper()
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
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	srv := &api.Server{Store: s, Tenant: tenant, Schedules: &refreshes{}, DefaultDataPlane: "dp1", Authless: true, Logger: slog.New(slog.DiscardHandler), Connectors: connectors}
	h, err := srv.Handler()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return &applyEnv{tenant: tenant, url: url, conn: conn, applier: apiv1connect.NewApplyServiceClient(http.DefaultClient, ts.URL)}
}

func fakerSource(name string, seed int) *apiv1.Document {
	return &apiv1.Document{Content: fmt.Appendf(nil, `
apiVersion: openetl.io/v1alpha1
kind: Source
metadata: { name: %s }
spec: { connector: faker, config: { seed: %d, count: 10 } }
`, name, seed)}
}

// An apply waits for the one before it, so two that name the same resources
// in different orders can't deadlock.
func TestAppliesRunOneAtATime(t *testing.T) {
	ctx := t.Context()
	e := newApplyEnv(t)
	other, err := e.conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Rollback(context.Background()) }()
	if err := db.New(other).LockApply(ctx, e.tenant.Org); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := e.applier.Apply(ctx, connect.NewRequest(&apiv1.ApplyRequest{Documents: []*apiv1.Document{fakerSource("gen", 1)}}))
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("the apply finished during another: %v", err)
	case <-time.After(time.Second):
	}
	if err := other.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the apply is still waiting after the other ended")
	}
}

// Applies naming the same sources in opposite orders all land.
func TestOverlappingAppliesDontDeadlock(t *testing.T) {
	ctx := t.Context()
	e := newApplyEnv(t)
	apply := func(seed int, names ...string) error {
		var docs []*apiv1.Document
		for _, n := range names {
			docs = append(docs, fakerSource(n, seed))
		}
		_, err := e.applier.Apply(ctx, connect.NewRequest(&apiv1.ApplyRequest{Documents: docs}))
		return err
	}
	if err := apply(1, "a", "b", "c", "d"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Go(func() {
			for i := range 5 {
				names := []string{"a", "b", "c", "d"}
				if g%2 == 1 {
					names = []string{"d", "c", "b", "a"}
				}
				if err := apply(2+g*100+i, names...); err != nil {
					t.Errorf("apply %d of goroutine %d: %v", i, g, err)
				}
			}
		})
	}
	wg.Wait()
}

// A source or destination keeps its aws role, which a built-in connector,
// running inside openetld, can't take.
func TestApplyAWSAccess(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, connectorhost.ExecutablePath("destination", "acme/lake"))
	if err := os.MkdirAll(filepath.Dir(exe), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(exe, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	e := newApplyEnvWith(t, connectorhost.Env{Connectors: dir})
	doc := func(kind, connector string) *apiv1.Document {
		return &apiv1.Document{Content: fmt.Appendf(nil, `
apiVersion: openetl.io/v1alpha1
kind: %s
metadata: { name: lake }
spec:
  connector: %s
  config: {}
  aws: { role: 'arn:aws:iam::222222222222:role/openetl-lake', externalId: lake-7f3a }
`, kind, connector)}
	}
	if _, err := e.applier.Apply(t.Context(), connect.NewRequest(&apiv1.ApplyRequest{Documents: []*apiv1.Document{doc("Source", "faker")}})); err == nil ||
		!strings.Contains(err.Error(), "aws applies only to connectors that run as their own process, and faker is built into openetld") {
		t.Errorf("a built-in source with an aws role: %v", err)
	}
	if _, err := e.applier.Apply(t.Context(), connect.NewRequest(&apiv1.ApplyRequest{Documents: []*apiv1.Document{doc("Destination", "acme/lake")}})); err != nil {
		t.Fatal(err)
	}
	var spec string
	if err := e.conn.QueryRow(t.Context(), `SELECT spec::text FROM destination_versions`).Scan(&spec); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(spec, `"role": "arn:aws:iam::222222222222:role/openetl-lake"`) || !strings.Contains(spec, `"externalId": "lake-7f3a"`) {
		t.Errorf("the destination's stored version: %s", spec)
	}
}
