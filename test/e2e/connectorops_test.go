//go:build e2e

package e2e

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
)

func sourceDoc(name, spec string) string {
	return fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, spec)
}

func destinationDoc(name, spec string) string {
	return fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q}, "spec": %s}`, name, spec)
}

// wrongPassword is the e2e database URL with another password.
func wrongPassword(t *testing.T) string {
	t.Helper()
	u, err := url.Parse(env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(u.User.Username(), "not-the-password")
	return u.String()
}

// Check and Discover run on a runner and report what they found.
func TestCheckAndDiscover(t *testing.T) {
	startRunner(t)
	name := unique("ops")
	sch := pgx.Identifier{schema(name)}.Sanitize()
	pgExec(t,
		"CREATE SCHEMA "+sch,
		"CREATE TABLE "+sch+".items (id bigint PRIMARY KEY, name text, price numeric(10, 2))",
		"CREATE TABLE "+sch+".events (id bigint, kind text)",
	)
	pg := func(dsnSecret, schemaName string) string {
		return fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": %q}, "schema": %q}}`, dsnSecret, schemaName)
	}
	apply(t,
		sourceDoc(name, pg("e2e-dsn", schema(name))),
		sourceDoc(name+"-auth", pg(setSecret(t, wrongPassword(t)), schema(name))),
		sourceDoc(name+"-missing", pg("e2e-dsn", "no_such_schema")),
		destinationDoc(name, pg("e2e-dsn", schema(name))),
	)
	sources := apiv1connect.NewSourceServiceClient(env.http, env.apiURL)
	check := func(source string) *apiv1.CheckResult {
		t.Helper()
		r, err := sources.CheckSource(t.Context(), connect.NewRequest(&apiv1.CheckSourceRequest{Name: "workspaces/default/sources/" + source}))
		if err != nil {
			t.Fatal(err)
		}
		return r.Msg.GetResult()
	}
	if r := check(name); !r.GetOk() || r.GetVersion() == "" {
		t.Errorf("check: %v", r)
	}
	if r := check(name + "-auth"); r.GetOk() || r.GetCode() != "AUTH_INVALID" {
		t.Errorf("check with a wrong password: %v", r)
	}
	if r := check(name + "-missing"); r.GetOk() || r.GetCode() != "CONFIG_INVALID" || !strings.Contains(r.GetMessage(), "no_such_schema") {
		t.Errorf("check of a missing schema: %v", r)
	}
	d, err := apiv1connect.NewDestinationServiceClient(env.http, env.apiURL).CheckDestination(t.Context(),
		connect.NewRequest(&apiv1.CheckDestinationRequest{Name: "workspaces/default/destinations/" + name}))
	if err != nil {
		t.Fatal(err)
	}
	if !d.Msg.GetResult().GetOk() {
		t.Errorf("destination check: %v", d.Msg.GetResult())
	}
	_, err = sources.CheckSource(t.Context(), connect.NewRequest(&apiv1.CheckSourceRequest{Name: "workspaces/default/sources/" + name, DataPlane: "elsewhere"}))
	wantCode(t, err, connect.CodeInvalidArgument, "check on a data plane that doesn't exist")

	discover := func() *apiv1.Catalog {
		t.Helper()
		r, err := sources.DiscoverSource(t.Context(), connect.NewRequest(&apiv1.DiscoverSourceRequest{Name: "workspaces/default/sources/" + name}))
		if err != nil {
			t.Fatal(err)
		}
		return r.Msg.GetCatalog()
	}
	cat := discover()
	var got []string
	for _, s := range cat.GetStreams() {
		var cols []string
		for _, c := range s.GetColumns() {
			cols = append(cols, fmt.Sprintf("%s %s %v", c.GetName(), c.GetType(), c.GetNullable()))
		}
		got = append(got, fmt.Sprintf("%s %s [%s] %s", s.GetName(), s.GetReplay(), strings.Join(s.GetPrimaryKey(), ","), strings.Join(cols, "; ")))
	}
	slices.Sort(got)
	// items' id is its primary key, so it's NOT NULL.
	want := []string{
		"events REPLAY_EXACT [] id int64 true; kind utf8 true",
		"items REPLAY_EXACT [id] id int64 false; name utf8 true; price decimal(10, 2) true",
	}
	if !slices.Equal(got, want) {
		t.Errorf("catalog:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if cat.GetVersion() == "" || cat.GetDiscoverTime() == nil {
		t.Errorf("catalog version %q, time %v", cat.GetVersion(), cat.GetDiscoverTime())
	}
	discover()
	n, err := query[int](t, `SELECT count(*) FROM source_catalogs c JOIN sources s ON s.id = c.source_id WHERE s.name = $1`, name)
	if err != nil || n != 1 {
		t.Errorf("%d catalogs stored (%v), want 1", n, err)
	}
}
