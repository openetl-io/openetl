package activities

import (
	"bytes"
	"os"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/engineclient"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// Every type a source can declare reaches the engine's plan through the
// catalog, as an apply's plan has it, and through the job, as a sync's
// start has it, and both plan the same: the catalog names each type as the
// engine reads it.
func TestThePlanAndTheRunTypeAStreamAlike(t *testing.T) {
	bin := os.Getenv("OPENETL_ENGINE")
	if _, err := os.Stat(bin); bin == "" || err != nil {
		t.Skip("OPENETL_ENGINE names no engine binary")
	}
	js, err := extensions.NewJSONType(arrow.BinaryTypes.String)
	if err != nil {
		t.Fatal(err)
	}
	uuid := extensions.NewUUIDType()
	declared := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "i16", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "j", Type: js, Nullable: true},
		{Name: "raw", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 12, Scale: 2}, Nullable: true},
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "local", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "day", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "clock", Type: arrow.FixedWidthTypes.Time64us, Nullable: true},
		{Name: "u", Type: uuid, Nullable: true},
		{Name: "span", Type: arrow.FixedWidthTypes.MonthDayNanoInterval, Nullable: true},
		{Name: "ns", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
		{Name: "docs", Type: arrow.ListOf(js), Nullable: true},
		{Name: "ids", Type: arrow.ListOfNonNullable(uuid), Nullable: true},
	}, nil)
	// As a connector's Discover gives it, through the protocol.
	raw, err := arrowipc.EncodeSchema(declared)
	if err != nil {
		t.Fatal(err)
	}
	discovered, err := arrowipc.DecodeSchema(raw)
	if err != nil {
		t.Fatal(err)
	}
	const sql = "INSERT INTO t SELECT * FROM s"
	spec := &apiv1.PipelineSpec{Transform: &apiv1.Transform{Engine: &apiv1.Transform_Sql{Sql: &apiv1.SqlTransform{Sql: sql}}}}
	cat := catalogProto(source.Catalog{Streams: []source.CatalogStream{{Name: "s", Schema: discovered, SyncModes: []source.SyncMode{source.FullRefresh}}}})
	opts := engineclient.Options{Binary: bin}
	byCatalog, unknown := plan.SQLRequest(spec, cat, nil)
	if len(unknown) > 0 || len(byCatalog.Streams) != 1 {
		t.Fatalf("the catalog's request %+v, without the columns of %v", byCatalog, unknown)
	}
	planned, err := engineclient.Plan(t.Context(), opts, byCatalog)
	if err != nil || len(planned.Errors) > 0 {
		t.Fatalf("the plan of the catalog's columns %v: %v, %v", byCatalog.Streams[0].Columns, err, planned.Errors)
	}
	byJob, _, err := sqlRequest(sql, []runner.Stream{{Name: "s", Schema: discovered}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	run, err := engineclient.Plan(t.Context(), opts, byJob)
	if err != nil || len(run.Errors) > 0 {
		t.Fatalf("the plan of the job's schema: %v, %v", err, run.Errors)
	}
	got, want := planned.Statements[0], run.Statements[0]
	if !slices.Equal(got.Columns, want.Columns) || !bytes.Equal(got.ArrowSchema, want.ArrowSchema) {
		t.Errorf("by the catalog, the output's columns are %v; by the job, %v", got.Columns, want.Columns)
	}
	if len(want.Columns) != declared.NumFields() {
		t.Errorf("the output has %d columns, of %d", len(want.Columns), declared.NumFields())
	}
}
