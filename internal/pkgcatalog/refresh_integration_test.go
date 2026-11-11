//go:build integration

package pkgcatalog_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/openetl-io/openetl/internal/oci"
	"github.com/openetl-io/openetl/internal/pkgcatalog"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/store/storetest"
)

// A listing that runs out of time replaces the source's last error, though
// the time that ended it has ended Refresh's context too; one its caller gave
// up on leaves it alone.
func TestRefreshSavesAFailureAfterItsDeadline(t *testing.T) {
	ctx := t.Context()
	hung := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	t.Cleanup(hung.Close)
	st, err := store.Open(ctx, storetest.NewDatabase(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	tenant, err := st.Bootstrap(ctx, "dp1", func(q *db.Queries, org uuid.UUID) error {
		return pkgcatalog.ConnectOfficial(ctx, q, org, oci.Scheme+strings.TrimPrefix(hung.URL, "http://")+"/acme/connectors")
	})
	if err != nil {
		t.Fatal(err)
	}
	c := &pkgcatalog.Catalog{Queries: st.Queries, Org: tenant.Org, HTTP: hung.Client(), PlainHTTP: []string{strings.TrimPrefix(hung.URL, "http://")}}
	lastError := func() string {
		sources, err := st.ListConnectorSources(ctx, tenant.Org)
		if err != nil || len(sources) != 1 {
			t.Fatalf("sources %v, %v", sources, err)
		}
		return sources[0].Error
	}

	gaveUp, cancel := context.WithCancel(ctx)
	defer cancel()
	time.AfterFunc(200*time.Millisecond, cancel)
	if _, err := c.Refresh(gaveUp); err != nil {
		t.Fatal(err)
	}
	if e := lastError(); e != "" {
		t.Errorf("a listing its caller gave up on saved %q", e)
	}

	limited, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	defer stop()
	if _, err := c.Refresh(limited); err != nil {
		t.Fatal(err)
	}
	if e := lastError(); !strings.Contains(e, context.DeadlineExceeded.Error()) {
		t.Errorf("after the deadline the error is %q", e)
	}
}
