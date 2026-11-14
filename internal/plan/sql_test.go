package plan

import (
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/sqlplan"
)

var sqlCatalog = &apiv1.Catalog{Streams: []*apiv1.CatalogStream{
	{Name: "customers", PrimaryKey: []string{"id"}, Replay: apiv1.Replay_REPLAY_EXACT, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_CDC},
		Columns: []*apiv1.CatalogColumn{{Name: "id", Type: "int64"}, {Name: "email", Type: "utf8", Nullable: true}, {Name: "created_at", Type: "timestamp[us, tz=UTC]"}}},
	{Name: "orders", PrimaryKey: []string{"id"}, Replay: apiv1.Replay_REPLAY_EXACT, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_CDC},
		Columns: []*apiv1.CatalogColumn{{Name: "id", Type: "int64"}, {Name: "amount", Type: "decimal(12, 2)"}}},
	{Name: "events", CursorField: "at", Replay: apiv1.Replay_REPLAY_SUPERSET, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_INCREMENTAL}},
}}

const twoStatements = "INSERT INTO customers\nSELECT id, lower(email) AS email, created_at FROM customers;\n\nINSERT INTO contacts SELECT id AS customer_id, email AS contact_email FROM customers"

func sqlSpec(sql string, passthrough apiv1.Passthrough, tables map[string]string, streams ...*apiv1.PipelineStream) *apiv1.PipelineSpec {
	return &apiv1.PipelineSpec{
		Delivery:    &apiv1.Delivery{Require: apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE},
		Source:      &apiv1.PipelineSource{Streams: streams},
		Destination: &apiv1.PipelineDestination{Tables: tables},
		Transform:   &apiv1.Transform{Engine: &apiv1.Transform_Sql{Sql: &apiv1.SqlTransform{Sql: sql, Passthrough: passthrough}}},
	}
}

// What the engine answers for twoStatements.
func twoAnswers() *sqlplan.Answer {
	return &sqlplan.Answer{Statements: []sqlplan.Statement{
		{At: sqlplan.Position{Line: 1, Column: 1}, Output: "customers", Replaces: true, Class: "row_wise", Tier: 1, Inputs: []string{"customers"},
			Columns: []sqlplan.Field{{Name: "id", Type: "int64", Nullable: true}, {Name: "email", Type: "utf8", Nullable: true}, {Name: "created_at", Type: "timestamp[us, tz=UTC]", Nullable: true}},
			Key:     []string{"id"}, ArrowSchema: []byte("schema")},
		{At: sqlplan.Position{Line: 4, Column: 1}, Output: "contacts", Class: "row_wise", Tier: 1, Inputs: []string{"customers"},
			Columns: []sqlplan.Field{{Name: "customer_id", Type: "int64", Nullable: true}, {Name: "contact_email", Type: "utf8", Nullable: true}}, Key: []string{"customer_id"}},
	}}
}

// What the engine answers for statements that drop customers' key, and that
// replace customers with a projection of orders.
func otherAnswers() *sqlplan.Answer {
	return &sqlplan.Answer{Statements: []sqlplan.Statement{
		{At: sqlplan.Position{Line: 1, Column: 1}, Output: "shifted", Class: "row_wise", Tier: 1, Inputs: []string{"customers"},
			Columns: []sqlplan.Field{{Name: "id", Type: "int64", Nullable: true}}},
		{At: sqlplan.Position{Line: 2, Column: 1}, Output: "customers", Replaces: true, Class: "row_wise", Tier: 1, Inputs: []string{"orders"},
			Columns: []sqlplan.Field{{Name: "id", Type: "int64", Nullable: true}}, Key: []string{"id"}},
	}}
}

// A request carries the streams the SQL may name, with their columns from
// the catalog or else from the pipeline's schema history, and names those
// whose schema neither has.
func TestSQLRequest(t *testing.T) {
	spec := sqlSpec(twoStatements+";\nINSERT INTO e SELECT * FROM events", 0, nil,
		&apiv1.PipelineStream{Name: "customers", Timestamps: apiv1.TimestampUnit_TIMESTAMP_UNIT_NANOSECOND}, &apiv1.PipelineStream{Name: "events"}, &apiv1.PipelineStream{Name: "orders"})
	req, unknown := SQLRequest(spec, sqlCatalog, nil)
	if req.SQL != spec.GetTransform().GetSql().GetSql() || len(req.Streams) != 1 || !slices.Equal(unknown, []string{"events"}) {
		t.Fatalf("request %+v, unknown %v", req, unknown)
	}
	c := req.Streams[0]
	if c.Name != "customers" || !c.Changelog || !c.Nanoseconds || !slices.Equal(c.Key, []string{"id"}) ||
		!slices.Equal(c.Columns, []sqlplan.Column{{Name: "id", Type: "int64"}, {Name: "email", Type: "utf8"}, {Name: "created_at", Type: "timestamp[us, tz=UTC]"}}) {
		t.Errorf("customers: %+v", c)
	}
	history := map[string][]sqlplan.Column{"events": {{Name: "at", Type: "utf8"}}}
	req, unknown = SQLRequest(spec, sqlCatalog, history)
	if len(unknown) != 0 || len(req.Streams) != 2 || req.Streams[1].Name != "events" || req.Streams[1].Changelog || !slices.Equal(req.Streams[1].Columns, history["events"]) {
		t.Errorf("request %+v, unknown %v", req, unknown)
	}
	// Every stream, when source.streams lists none.
	req, _ = SQLRequest(sqlSpec("INSERT INTO x SELECT * FROM orders", 0, nil), sqlCatalog, nil)
	if len(req.Streams) != 1 || req.Streams[0].Name != "orders" {
		t.Errorf("request %+v", req)
	}
}

// The destination gets the statements' outputs and the streams no statement
// writes; an output that replaces a stream takes its place, its table and
// its require.
func TestPipelineSQL(t *testing.T) {
	tables := map[string]string{"customers": "dim_customers", "contacts": "contact_list"}
	spec := sqlSpec(twoStatements, 0, tables, &apiv1.PipelineStream{Name: "customers"}, &apiv1.PipelineStream{Name: "orders"})
	plans, problems, _ := PipelineSQL(spec, sqlCatalog, warehouse, twoAnswers())
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	var got []string
	for _, p := range plans {
		got = append(got, p.GetName()+" "+p.GetWrite().String()+" "+p.GetTable())
	}
	want := []string{"orders WRITE_MODE_MERGE ", "customers WRITE_MODE_MERGE dim_customers", "contacts WRITE_MODE_MERGE contact_list"}
	if !slices.Equal(got, want) {
		t.Errorf("plans %q", got)
	}
	if r := plans[1].GetReason(); !strings.Contains(r, "transform's output over exact inputs") {
		t.Errorf("reason %q", r)
	}

	tp := Transform(spec, sqlCatalog, twoAnswers())
	if !slices.Equal(tp.GetPassedThrough(), []string{"orders"}) || !slices.Equal(tp.GetWithheld(), []string{"customers"}) ||
		len(tp.GetStatements()) != 2 || tp.GetStatements()[0].GetLine() != 1 || tp.GetStatements()[1].GetLine() != 4 ||
		!slices.Equal(tp.GetStatements()[0].GetPrimaryKey(), []string{"id"}) || tp.GetStatements()[0].GetClass() != "row_wise" {
		t.Errorf("transform %v", tp)
	}

	// With passthrough none, only what statements write reaches the
	// destination, and destination.tables can name only that.
	none := sqlSpec(twoStatements, apiv1.Passthrough_PASSTHROUGH_NONE, map[string]string{"orders": "fct_orders"},
		&apiv1.PipelineStream{Name: "customers"}, &apiv1.PipelineStream{Name: "orders"})
	plans, problems, _ = PipelineSQL(none, sqlCatalog, warehouse, twoAnswers())
	if len(plans) != 2 || len(problems) != 1 || !strings.Contains(problems[0].Error(), `names stream "orders", which is neither an output of transform.sql nor a stream that passes through`) {
		t.Errorf("plans %v, problems %v", plans, problems)
	}
	tp = Transform(none, sqlCatalog, twoAnswers())
	if len(tp.GetPassedThrough()) != 0 || !slices.Equal(tp.GetWithheld(), []string{"customers", "orders"}) {
		t.Errorf("transform %v", tp)
	}

	// A replacing output takes its stream's require, whether or not its
	// statement reads that stream; an output without a key can't be merged.
	strict := sqlSpec("INSERT INTO shifted SELECT id + 1 AS id FROM customers;\nINSERT INTO customers SELECT id FROM orders", 0, nil,
		&apiv1.PipelineStream{Name: "customers", Require: apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE}, &apiv1.PipelineStream{Name: "orders"})
	strict.Destination.Write = apiv1.WriteMode_WRITE_MODE_MERGE
	superset := &apiv1.Catalog{Streams: []*apiv1.CatalogStream{proto.CloneOf(sqlCatalog.GetStreams()[0]), proto.CloneOf(sqlCatalog.GetStreams()[1])}}
	superset.Streams[1].Replay = apiv1.Replay_REPLAY_SUPERSET
	_, problems, _ = PipelineSQL(strict, superset, warehouse, otherAnswers())
	var msgs []string
	for _, p := range problems {
		msgs = append(msgs, p.Error())
	}
	all := strings.Join(msgs, "\n")
	if len(msgs) != 2 || !strings.Contains(all, `destination.write: merge needs a primary key, and stream "shifted" has none`) ||
		!strings.Contains(all, `source.streams[customers].require: stream "customers" gets effectively_once`) {
		t.Errorf("problems %q", msgs)
	}
}

// Without an answer, the streams are checked as inputs, and none is planned.
func TestPipelineSQLUnplanned(t *testing.T) {
	spec := sqlSpec(twoStatements, 0, map[string]string{"contacts": "contact_list"},
		&apiv1.PipelineStream{Name: "customers", EventTime: "created_at"}, &apiv1.PipelineStream{Name: "events", EventTime: "at"})
	plans, problems, _ := PipelineSQL(spec, sqlCatalog, warehouse, nil)
	if len(plans) != 0 || len(problems) != 0 {
		t.Errorf("plans %v, problems %v", plans, problems)
	}
	_, problems, _ = PipelineSQL(sqlSpec(twoStatements, 0, nil, &apiv1.PipelineStream{Name: "customers", EventTime: "placed_at"}), sqlCatalog, warehouse, nil)
	if len(problems) != 1 || !strings.Contains(problems[0].Error(), `eventTime names column "placed_at"`) {
		t.Errorf("problems = %v", problems)
	}
}

func TestSQLPlanKeepsOutputs(t *testing.T) {
	p := SQLPlan(twoAnswers())
	if len(p.GetOutputs()) != 2 || p.GetOutputs()[0].GetName() != "customers" || string(p.GetOutputs()[0].GetArrowSchema()) != "schema" ||
		!slices.Equal(p.GetOutputs()[0].GetPrimaryKey(), []string{"id"}) || p.GetOutputs()[1].GetColumns()[0].GetName() != "customer_id" || len(p.GetOperators()) != 0 {
		t.Errorf("%v", p)
	}
}
