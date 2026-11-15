package engineclient

import (
	"errors"
	"maps"
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
		!slices.Equal(first.Key, []string{"id"}) || first.Need != nil || first.Operator != "" || !maps.Equal(first.Passes, map[string]string{"id": "id"}) ||
		!slices.Equal(first.Columns, []sqlplan.Field{{Name: "id", Type: "int64", Nullable: true}, {Name: "email", Type: "utf8", Nullable: true}}) {
		t.Errorf("first: %+v", first)
	}
	if second := a.Statements[1]; !maps.Equal(second.Passes, map[string]string{"id": "customer_id", "email": "contact_email"}) {
		t.Errorf("second passes %v", second.Passes)
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

// A group window needs its group keys and keeps state, under an operator's
// ID; its output's key is window_start and the keys, which it passes.
func TestPlanGroupWindow(t *testing.T) {
	opts := planEngine(t)
	orders := sqlplan.Stream{Name: "orders", EventTime: "at", Columns: []sqlplan.Column{
		{Name: "region", Type: "utf8"}, {Name: "amount", Type: "decimal(12, 2)"}, {Name: "at", Type: "timestamp[us, tz=UTC]"},
	}}
	a, err := Plan(t.Context(), opts, sqlplan.Request{
		SQL:     "INSERT INTO minutes\nSELECT window_start, region AS area, sum(amount) AS revenue FROM orders\nGROUP BY tumble(at, INTERVAL '1 minute'), region",
		Streams: []sqlplan.Stream{orders},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Errors) != 0 || len(a.Statements) != 1 {
		t.Fatalf("%+v", a)
	}
	st := a.Statements[0]
	if st.Class != "group_window" || st.Tier != 2 || !slices.Equal(st.Key, []string{"window_start", "area"}) || !slices.Equal(st.Need, []string{"region"}) ||
		!maps.Equal(st.Passes, map[string]string{"region": "area"}) || !strings.HasPrefix(st.Operator, "window-") {
		t.Errorf("%+v", st)
	}
	orders.EventTime = ""
	a, err = Plan(t.Context(), opts, sqlplan.Request{SQL: "INSERT INTO m SELECT window_start, count(*) AS n FROM orders GROUP BY tumble(at, INTERVAL '1 minute')", Streams: []sqlplan.Stream{orders}})
	if err != nil || len(a.Errors) != 1 || !strings.Contains(a.Errors[0].Message, "orders has no event time") {
		t.Errorf("without event time: %+v, %v", a, err)
	}
}

// SQL with errors is an answer; a request the engine can't plan fails as
// CONFIG_INVALID.
func TestPlanErrors(t *testing.T) {
	opts := planEngine(t)
	a, err := Plan(t.Context(), opts, sqlplan.Request{
		SQL:     "INSERT INTO ranked\nSELECT id,\nrank() OVER (ORDER BY created_at) AS r FROM customers",
		Streams: []sqlplan.Stream{customers},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Statements) != 0 || len(a.Errors) != 1 || a.Errors[0].Kind != "later_tier" || a.Errors[0].Tier != 3 || a.Errors[0].At == nil || a.Errors[0].At.Line != 3 {
		t.Errorf("%+v", a)
	}
	keyless := sqlplan.Stream{Name: "events", Changelog: true, Columns: []sqlplan.Column{{Name: "id", Type: "int64"}, {Name: "kind", Type: "utf8"}}}
	a, err = Plan(t.Context(), opts, sqlplan.Request{SQL: "INSERT INTO clicks\nSELECT id, kind FROM events\nWHERE kind = 'click'", Streams: []sqlplan.Stream{keyless}})
	if err != nil || len(a.Statements) != 0 || len(a.Errors) != 1 || a.Errors[0].Kind != "changelog_key" || a.Errors[0].At == nil ||
		*a.Errors[0].At != (sqlplan.Position{Line: 3, Column: 7}) || !strings.Contains(a.Errors[0].Message, "scheduled pipeline") {
		t.Errorf("a WHERE over a changelog without a key: %+v, %v", a, err)
	}
	a, err = Plan(t.Context(), opts, sqlplan.Request{SQL: "INSERT INTO x SELECT 1 AS one FROM missing"})
	if err != nil || len(a.Errors) != 1 || a.Errors[0].Kind != "name" {
		t.Errorf("no streams: %+v, %v", a, err)
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
