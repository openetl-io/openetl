//go:build integration

package ops_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/ops"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/store/storetest"
)

type fakeOps struct {
	mu    sync.Mutex
	err   error
	calls []contract.ConnectorOpInput
}

func (f *fakeOps) Run(_ context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, in)
	return contract.ConnectorOpResult{}, f.err
}

func (f *fakeOps) set(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeOps) got() []contract.ConnectorOpInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]contract.ConnectorOpInput(nil), f.calls...)
}

type fixture struct {
	st     *store.Store
	conn   *pgx.Conn
	tenant store.Tenant
	ops    *fakeOps
	id     uuid.UUID
}

func setup(t *testing.T) *fixture {
	t.Helper()
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
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	f := &fixture{st: st, conn: conn, tenant: tenant, ops: &fakeOps{}, id: uuid.New()}
	src := uuid.New()
	err = st.InsertTeardown(ctx, db.InsertTeardownParams{
		OrgID: tenant.Org, ID: f.id, WorkspaceID: tenant.Workspace, DataPlaneID: "dp1", PipelineID: uuid.New(), Pipeline: "shop",
		Op: contract.OpTeardown, Connector: "postgres", SourceID: &src, Name: "src", Spec: []byte(`{"connector": "postgres"}`), Version: "v1", Secrets: []string{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fixture) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	td := &ops.Teardowns{Store: f.st, Ops: f.ops, Logger: slog.New(slog.DiscardHandler), Every: 10 * time.Millisecond}
	go td.Run(ctx)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("never: %s", what)
}

func (f *fixture) row(t *testing.T) (tries int32, lastError *string, wait time.Duration, ok bool) {
	t.Helper()
	var secs float64
	err := f.conn.QueryRow(t.Context(), `SELECT tries, last_error, extract(epoch FROM next_time - now())::float8 FROM teardowns WHERE id = $1`, f.id).Scan(&tries, &lastError, &secs)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, 0, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return tries, lastError, time.Duration(secs * float64(time.Second)), true
}

// A failed teardown waits and runs again under the same operation, with a
// workflow of its own for each try, until it succeeds.
func TestTeardownRetriesUntilItSucceeds(t *testing.T) {
	f := setup(t)
	f.ops.set(errors.New("the source can't be reached"))
	f.start(t)
	eventually(t, "the first try recorded", func() bool {
		tries, last, _, ok := f.row(t)
		return ok && tries == 1 && last != nil
	})
	if tries, last, wait, _ := f.row(t); !strings.Contains(*last, "can't be reached") || wait < 30*time.Second {
		t.Fatalf("after a failure: tries %d, last %q, next in %v", tries, *last, wait)
	}
	// Due again, and this time it works.
	f.ops.set(nil)
	if _, err := f.conn.Exec(t.Context(), `UPDATE teardowns SET next_time = now() WHERE id = $1`, f.id); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the teardown removed", func() bool { _, _, _, ok := f.row(t); return !ok })
	calls := f.ops.got()
	if len(calls) != 2 || calls[0].OpID != f.id.String() || calls[1].OpID != f.id.String() || calls[0].Try != 1 || calls[1].Try != 2 {
		t.Fatalf("calls %+v, want two tries of one operation", calls)
	}
	if calls[0].SourceID == "" || calls[0].Op != contract.OpTeardown || calls[0].DataPlane != "dp1" {
		t.Errorf("first call: %+v", calls[0])
	}
}

// After days of tries a teardown stops, and says what's left behind.
func TestATeardownIsGivenUpOn(t *testing.T) {
	f := setup(t)
	if _, err := f.conn.Exec(t.Context(), `UPDATE teardowns SET create_time = now() - interval '8 days' WHERE id = $1`, f.id); err != nil {
		t.Fatal(err)
	}
	f.ops.set(errors.New("the source is gone"))
	f.start(t)
	eventually(t, "the teardown given up on", func() bool { _, _, _, ok := f.row(t); return !ok })
	var typ, pipeline string
	var data []byte
	err := f.conn.QueryRow(t.Context(), `SELECT type, pipeline, data FROM outbox WHERE org_id = $1 AND type = $2`, f.tenant.Org, events.PipelineTeardownFailed).Scan(&typ, &pipeline, &data)
	if err != nil {
		t.Fatalf("the failure's event: %v", err)
	}
	if pipeline != "shop" || !strings.Contains(string(data), "the source is gone") || !strings.Contains(string(data), `"connector": "src"`) {
		t.Errorf("event %s %s", pipeline, data)
	}
}
