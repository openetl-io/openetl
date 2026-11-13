package engineclient

import (
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/sqlplan"
)

func planEngine(t *testing.T) Options {
	t.Helper()
	bin := os.Getenv("OPENETL_ENGINE")
	if bin == "" {
		t.Skip("set OPENETL_ENGINE to an openetl-engine binary; make test does")
	}
	return Options{Binary: bin}
}

var customers = sqlplan.Stream{Name: "customers", Key: []string{"id"}, Changelog: true, Columns: []sqlplan.Column{
	{Name: "id", Type: "int64"}, {Name: "email", Type: "utf8"}, {Name: "created_at", Type: "timestamp[us, tz=UTC]"},
}}

// The engine plans each statement: its output, class, key and schema.
func TestPlan(t *testing.T) {
	opts := planEngine(t)
	a, err := Plan(t.Context(), opts, sqlplan.Request{
		SQL:     "INSERT INTO customers\nSELECT id, lower(email) AS email FROM customers;\nINSERT INTO contacts SELECT id AS customer_id, email AS contact_email FROM customers",
		Streams: []sqlplan.Stream{customers},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Errors) != 0 || len(a.Statements) != 2 {
		t.Fatalf("%+v", a)
	}
	first := a.Statements[0]
	if first.Output != "customers" || !first.Replaces || first.Class != "row_wise" || first.Tier != 1 || first.At != (sqlplan.Position{Line: 1, Column: 1}) ||
		!slices.Equal(first.Key, []string{"id"}) || first.Exchange != nil || first.Operator != "" ||
		!slices.Equal(first.Columns, []sqlplan.Field{{Name: "id", Type: "int64", Nullable: true}, {Name: "email", Type: "utf8", Nullable: true}}) {
		t.Errorf("first: %+v", first)
	}
	r, err := ipc.NewReader(strings.NewReader(string(a.Statements[1].ArrowSchema)))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	if s := r.Schema(); s.NumFields() != 2 || s.Field(0).Name != "customer_id" || s.Field(1).Name != "contact_email" {
		t.Errorf("second's schema: %v", s)
	}
}

// SQL with errors is an answer; a request the engine can't plan fails as
// CONFIG_INVALID.
func TestPlanErrors(t *testing.T) {
	opts := planEngine(t)
	a, err := Plan(t.Context(), opts, sqlplan.Request{
		SQL:     "INSERT INTO hourly\nSELECT count(*) AS n FROM customers\nGROUP BY tumble(created_at, INTERVAL '1 hour')",
		Streams: []sqlplan.Stream{customers},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Statements) != 0 || len(a.Errors) != 1 || a.Errors[0].Kind != "later_tier" || a.Errors[0].Tier != 2 || a.Errors[0].At == nil || a.Errors[0].At.Line != 3 {
		t.Errorf("%+v", a)
	}
	_, err = Plan(t.Context(), opts, sqlplan.Request{SQL: "INSERT INTO x SELECT * FROM s", Streams: []sqlplan.Stream{{Name: "s", Columns: []sqlplan.Column{{Name: "a", Type: "string"}}}}})
	var e *errs.Error
	if !errors.As(err, &e) || e.Code != errs.ConfigInvalid || !strings.Contains(err.Error(), "stream s column a") {
		t.Errorf("an unknown type: %v", err)
	}
	if _, err := Plan(t.Context(), Options{Binary: "/nonexistent/openetl-engine"}, sqlplan.Request{}); err == nil {
		t.Error("no engine")
	}
}
