//go:build integration

package webhooks_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/events"
	"github.com/unsuitable001/openetl/internal/netguard"
	"github.com/unsuitable001/openetl/internal/secrets"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/db"
	"github.com/unsuitable001/openetl/internal/store/storetest"
	"github.com/unsuitable001/openetl/internal/webhooks"
)

type fixture struct {
	st     *store.Store
	tenant store.Tenant
	keys   *secrets.Keyring
	// The signing key in the secret "hook-key".
	key []byte
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := t.Context()
	st, err := store.Open(ctx, storetest.NewDatabase(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	f := &fixture{st: st}
	if f.tenant, err = st.Bootstrap(ctx, "default", nil); err != nil {
		t.Fatal(err)
	}
	if f.keys, err = secrets.Open(ctx, "base64key://"+base64.URLEncoding.EncodeToString(random(32)), st, []uuid.UUID{f.tenant.Org}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.keys.Close() })
	f.key = random(32)
	f.setSecret(t, "hook-key", "whsec_"+base64.StdEncoding.EncodeToString(f.key))
	return f
}

func random(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func (f *fixture) setSecret(t *testing.T, name, value string) {
	t.Helper()
	id := uuid.New()
	sealed, err := f.keys.Seal(t.Context(), f.tenant.Org, secrets.AAD(f.tenant.Org, f.tenant.Workspace, id), []byte(value))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.st.InsertSecret(t.Context(), db.InsertSecretParams{OrgID: f.tenant.Org, WorkspaceID: f.tenant.Workspace, ID: id, Name: name, Ciphertext: sealed}); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) webhook(t *testing.T, name string, spec *apiv1.WebhookSpec) uuid.UUID {
	t.Helper()
	raw, err := protojson.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	if err := f.st.InsertWebhook(t.Context(), db.InsertWebhookParams{
		OrgID: f.tenant.Org, WorkspaceID: f.tenant.Workspace, ID: id, Name: name, Spec: raw, Version: name, Secrets: spec.GetSecrets(),
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

// record records an event of each type, for pipeline, in one transaction.
func (f *fixture) record(t *testing.T, pipeline string, types ...string) []uuid.UUID {
	t.Helper()
	var ids []uuid.UUID
	err := f.st.Tx(t.Context(), func(q *db.Queries) error {
		for _, typ := range types {
			id, err := events.Record(t.Context(), q, f.tenant.Org, events.Event{
				Workspace: f.tenant.Workspace, Type: typ, Pipeline: pipeline, Data: events.Pipeline{Pipeline: "workspaces/default/pipelines/" + pipeline},
			})
			if err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func (f *fixture) notifier() *webhooks.Notifier {
	return &webhooks.Notifier{
		Store: f.st, Org: f.tenant.Org, Secrets: f.keys,
		Client:  webhooks.Client(netguard.Policy{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}}),
		Retries: []time.Duration{0, 0, 0},
	}
}

// work runs n's rounds until nothing is left to do now.
func work(t *testing.T, n *webhooks.Notifier) {
	t.Helper()
	for range 20 {
		d, err := n.Dispatch(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		a, err := n.Deliver(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if d+a == 0 {
			return
		}
	}
}

type delivery struct {
	id      string
	payload events.Payload
	err     error
}

// receiver answers with status (200 when it runs out), and records each
// request with whether key signed it.
type receiver struct {
	*httptest.Server
	mu       sync.Mutex
	got      []delivery
	statuses []int
}

func newReceiver(t *testing.T, key []byte, statuses ...int) *receiver {
	r := &receiver{statuses: statuses}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		d := delivery{id: req.Header.Get("webhook-id"), err: webhooks.Verify(key, req.Header, body, time.Now(), time.Minute)}
		if err := json.Unmarshal(body, &d.payload); err != nil && d.err == nil {
			d.err = err
		}
		r.mu.Lock()
		r.got = append(r.got, d)
		status := http.StatusOK
		if len(r.statuses) > 0 {
			status, r.statuses = r.statuses[0], r.statuses[1:]
		}
		r.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte("thanks"))
	}))
	t.Cleanup(r.Close)
	return r
}

func (r *receiver) deliveries() []delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]delivery(nil), r.got...)
}

func (f *fixture) deliveries(t *testing.T, webhook uuid.UUID) []db.ListWebhookDeliveriesRow {
	t.Helper()
	rows, err := f.st.ListWebhookDeliveries(t.Context(), db.ListWebhookDeliveriesParams{OrgID: f.tenant.Org, WebhookID: webhook, MaxRows: 100})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestOutboxIsTransactional(t *testing.T) {
	f := newFixture(t)
	r := newReceiver(t, f.key)
	f.webhook(t, "all", &apiv1.WebhookSpec{Url: r.URL, Events: []string{"*"}, Secrets: []string{"hook-key"}})
	rolledBack := errors.New("rolled back")
	err := f.st.Tx(t.Context(), func(q *db.Queries) error {
		if _, err := events.Record(t.Context(), q, f.tenant.Org, events.Event{Workspace: f.tenant.Workspace, Type: events.SyncFailed, Data: struct{}{}}); err != nil {
			return err
		}
		return rolledBack
	})
	if !errors.Is(err, rolledBack) {
		t.Fatal(err)
	}
	want := f.record(t, "shop", events.SyncStarted)
	work(t, f.notifier())
	got := r.deliveries()
	if len(got) != 1 || got[0].id != want[0].String() || got[0].payload.Type != events.SyncStarted || got[0].err != nil {
		t.Fatalf("deliveries %+v, want only the committed event's", got)
	}
}

func TestFiltersAndSignatures(t *testing.T) {
	f := newFixture(t)
	f.setSecret(t, "old-key", "whsec_"+base64.StdEncoding.EncodeToString(random(32)))
	syncs := newReceiver(t, f.key)
	shop := newReceiver(t, f.key)
	f.webhook(t, "syncs", &apiv1.WebhookSpec{Url: syncs.URL, Events: []string{"sync.*"}, Secrets: []string{"old-key", "hook-key"}})
	f.webhook(t, "shop", &apiv1.WebhookSpec{Url: shop.URL, Events: []string{"*"}, Pipelines: []string{"shop"}, Secrets: []string{"hook-key"}})
	f.record(t, "shop", events.SyncStarted, events.SchemaChanged)
	f.record(t, "orders", events.SyncFailed, events.PipelineLagging)
	work(t, f.notifier())

	types := func(ds []delivery) map[string]bool {
		out := map[string]bool{}
		for _, d := range ds {
			if d.err != nil {
				t.Errorf("delivery %s: %v", d.id, d.err)
			}
			out[d.payload.Type] = true
		}
		return out
	}
	if got := types(syncs.deliveries()); len(got) != 2 || !got[events.SyncStarted] || !got[events.SyncFailed] {
		t.Errorf("sync.* got %v", got)
	}
	if got := types(shop.deliveries()); len(got) != 2 || !got[events.SyncStarted] || !got[events.SchemaChanged] {
		t.Errorf("shop's events got %v", got)
	}
}

func TestNotifiersShareTheWork(t *testing.T) {
	f := newFixture(t)
	r := newReceiver(t, f.key)
	f.webhook(t, "all", &apiv1.WebhookSpec{Url: r.URL, Events: []string{"*"}, Secrets: []string{"hook-key"}})
	var want []uuid.UUID
	for range 10 {
		want = append(want, f.record(t, "shop", events.SyncStarted, events.SyncSucceeded, events.PipelineUpdated, events.SchemaChanged, events.PipelinePaused)...)
	}
	g, ctx := errgroup.WithContext(t.Context())
	for range 4 {
		n := f.notifier()
		g.Go(func() error {
			for range 40 {
				if _, err := n.Dispatch(ctx); err != nil {
					return err
				}
				if _, err := n.Deliver(ctx); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, d := range r.deliveries() {
		seen[d.id]++
	}
	for _, id := range want {
		if seen[id.String()] != 1 {
			t.Errorf("event %s delivered %d times", id, seen[id.String()])
		}
	}
	if len(seen) != len(want) {
		t.Errorf("%d events delivered, want %d", len(seen), len(want))
	}
}

func TestRetries(t *testing.T) {
	f := newFixture(t)
	r := newReceiver(t, f.key, http.StatusServiceUnavailable, http.StatusTooManyRequests)
	hook := f.webhook(t, "flaky", &apiv1.WebhookSpec{Url: r.URL, Events: []string{"*"}, Secrets: []string{"hook-key"}})
	f.record(t, "shop", events.SyncFailed)
	n := f.notifier()
	work(t, n)
	ds := f.deliveries(t, hook)
	if len(ds) != 1 || ds[0].State != "succeeded" || ds[0].Attempts != 3 || *ds[0].ResponseStatus != 200 || *ds[0].ResponseBody != "thanks" || ds[0].Error != nil {
		t.Fatalf("delivery %+v", ds)
	}
	if got := r.deliveries(); len(got) != 3 || got[0].id != got[2].id {
		t.Fatalf("attempts %+v, each with the event's ID", got)
	}
	w, err := f.st.GetWebhook(t.Context(), db.GetWebhookParams{OrgID: f.tenant.Org, WorkspaceID: f.tenant.Workspace, Name: "flaky"})
	if err != nil || w.FailingSince != nil {
		t.Fatalf("after a success, failing since %v (%v)", w.FailingSince, err)
	}

	// Out of retries.
	down := newReceiver(t, f.key, 500, 500, 500, 500, 500)
	hook = f.webhook(t, "down", &apiv1.WebhookSpec{Url: down.URL, Events: []string{"*"}, Secrets: []string{"hook-key"}})
	f.record(t, "shop", events.SyncFailed)
	work(t, n)
	ds = f.deliveries(t, hook)
	if len(ds) != 1 || ds[0].State != "failed" || ds[0].Attempts != 4 || *ds[0].ResponseStatus != 500 {
		t.Fatalf("delivery %+v, want failed after 4 attempts", ds)
	}
}

func TestRefusals(t *testing.T) {
	f := newFixture(t)
	bad := newReceiver(t, f.key, http.StatusBadRequest)
	gone := newReceiver(t, f.key, http.StatusGone)
	moved := newReceiver(t, f.key, http.StatusFound)
	blocked := f.webhook(t, "blocked", &apiv1.WebhookSpec{Url: "http://10.0.0.1:9/hook", Events: []string{"*"}, Secrets: []string{"hook-key"}})
	badHook := f.webhook(t, "bad", &apiv1.WebhookSpec{Url: bad.URL, Events: []string{"*"}, Secrets: []string{"hook-key"}})
	goneHook := f.webhook(t, "gone", &apiv1.WebhookSpec{Url: gone.URL, Events: []string{"*"}, Secrets: []string{"hook-key"}})
	movedHook := f.webhook(t, "moved", &apiv1.WebhookSpec{Url: moved.URL, Events: []string{"*"}, Secrets: []string{"hook-key"}})
	f.record(t, "shop", events.SyncFailed)
	work(t, f.notifier())
	for _, h := range []uuid.UUID{badHook, goneHook, movedHook, blocked} {
		if ds := f.deliveries(t, h); len(ds) != 1 || ds[0].State != "failed" || ds[0].Attempts != 1 {
			t.Errorf("delivery %+v, want failed at once", ds)
		}
	}
	w, err := f.st.GetWebhook(t.Context(), db.GetWebhookParams{OrgID: f.tenant.Org, WorkspaceID: f.tenant.Workspace, Name: "gone"})
	if err != nil || !w.Disabled || w.DisabledReason == nil {
		t.Fatalf("after 410 Gone: %+v (%v)", w, err)
	}
	f.record(t, "shop", events.SyncFailed)
	work(t, f.notifier())
	if n := len(gone.deliveries()); n != 1 {
		t.Errorf("a disabled webhook got %d deliveries", n)
	}
	if n := len(bad.deliveries()); n != 2 {
		t.Errorf("400 isn't a reason to disable a webhook, but it got %d deliveries", n)
	}
}

func TestFailingWebhooksAreDisabled(t *testing.T) {
	f := newFixture(t)
	r := newReceiver(t, f.key, 500, 500)
	hook := f.webhook(t, "down", &apiv1.WebhookSpec{Url: r.URL, Events: []string{"*"}, Secrets: []string{"hook-key"}})
	n := f.notifier()
	n.Retries = []time.Duration{time.Hour}
	n.DisableAfter = time.Millisecond
	f.record(t, "shop", events.SyncFailed)
	work(t, n)
	time.Sleep(5 * time.Millisecond)
	f.record(t, "shop", events.SyncFailed)
	work(t, n)
	w, err := f.st.GetWebhook(t.Context(), db.GetWebhookParams{OrgID: f.tenant.Org, WorkspaceID: f.tenant.Workspace, Name: "down"})
	if err != nil || !w.Disabled || w.FailingSince == nil {
		t.Fatalf("after failing for longer than DisableAfter: %+v (%v)", w, err)
	}
	for _, d := range f.deliveries(t, hook) {
		if d.State != "failed" {
			t.Errorf("a disabled webhook's delivery is %s", d.State)
		}
	}

	if err := f.st.EnableWebhook(t.Context(), db.EnableWebhookParams{OrgID: f.tenant.Org, ID: hook}); err != nil {
		t.Fatal(err)
	}
	want := f.record(t, "shop", events.SyncSucceeded)
	work(t, n)
	got := r.deliveries()
	if last := got[len(got)-1]; last.id != want[0].String() {
		t.Errorf("after enabling, the last delivery was %s, want %s", last.id, want[0])
	}
}

func TestMissingSecretsDontDisable(t *testing.T) {
	f := newFixture(t)
	r := newReceiver(t, f.key)
	hook := f.webhook(t, "nokey", &apiv1.WebhookSpec{Url: r.URL, Events: []string{"*"}, Secrets: []string{"missing"}})
	n := f.notifier()
	n.DisableAfter = time.Millisecond
	f.record(t, "shop", events.SyncFailed)
	work(t, n)
	ds := f.deliveries(t, hook)
	if len(ds) != 1 || ds[0].State != "failed" || ds[0].Error == nil || len(r.deliveries()) != 0 {
		t.Fatalf("delivery %+v; the endpoint got %d", ds, len(r.deliveries()))
	}
	w, err := f.st.GetWebhook(t.Context(), db.GetWebhookParams{OrgID: f.tenant.Org, WorkspaceID: f.tenant.Workspace, Name: "nokey"})
	if err != nil || w.Disabled || w.FailingSince != nil {
		t.Fatalf("a missing secret counted against the endpoint: %+v (%v)", w, err)
	}
}

func TestPrune(t *testing.T) {
	f := newFixture(t)
	r := newReceiver(t, f.key)
	hook := f.webhook(t, "all", &apiv1.WebhookSpec{Url: r.URL, Events: []string{"*"}, Secrets: []string{"hook-key"}})
	f.record(t, "shop", events.SyncFailed)
	n := f.notifier()
	work(t, n)
	n.Retention = time.Nanosecond
	if err := n.Prune(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ds := f.deliveries(t, hook); len(ds) != 0 {
		t.Errorf("%d deliveries left after pruning", len(ds))
	}
}

func TestRunDelivers(t *testing.T) {
	f := newFixture(t)
	r := newReceiver(t, f.key)
	f.webhook(t, "all", &apiv1.WebhookSpec{Url: r.URL, Events: []string{"*"}, Secrets: []string{"hook-key"}})
	n := f.notifier()
	n.Poll = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()
	f.record(t, "shop", events.SyncStarted)
	deadline := time.Now().Add(10 * time.Second)
	for len(r.deliveries()) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(r.deliveries()) != 1 {
		t.Fatalf("%d deliveries", len(r.deliveries()))
	}
}
