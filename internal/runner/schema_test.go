package runner

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

type applier struct {
	destination.Writer
	calls [][]string
}

func (a *applier) ApplySchema(_ context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	var names []string
	for _, s := range streams {
		names = append(names, s.Name)
	}
	a.calls = append(a.calls, names)
	return destination.SchemaApplied{}, nil
}

func encode(t *testing.T, s *arrow.Schema) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(s))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestApplySchemasOnlyWhenTheyChange(t *testing.T) {
	x := arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	md := arrow.NewMetadata([]string{"openetl.table", "openetl.source_names"}, []string{"a", "{}"})
	reordered := arrow.NewMetadata([]string{"openetl.source_names", "openetl.table"}, []string{"{}", "a"})
	a := &applier{}
	applied := map[string]*arrow.Schema{}
	steps := []map[string][]byte{
		{"a": encode(t, arrow.NewSchema([]arrow.Field{x}, &md)), "b": nil},
		{"a": encode(t, arrow.NewSchema([]arrow.Field{x}, &reordered))},
		{"a": encode(t, arrow.NewSchema([]arrow.Field{x, {Name: "y", Type: arrow.BinaryTypes.String}}, &md))},
	}
	for _, s := range steps {
		if err := applySchemas(t.Context(), a, applied, s); err != nil {
			t.Fatal(err)
		}
	}
	if len(a.calls) != 2 || a.calls[0][0] != "a" || a.calls[1][0] != "a" || len(a.calls[0]) != 1 {
		t.Errorf("ApplySchema calls %v, want [[a] [a]]", a.calls)
	}
}

func TestOverridesApplyToDeclaredDecimals(t *testing.T) {
	declared := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 38, Scale: 9}, Nullable: true},
	}, nil)
	s := Stream{Name: "s", Schema: declared, Overrides: map[string]arrow.DataType{"amount": arrow.BinaryTypes.String}}
	if err := s.checkOverrides(); err != nil {
		t.Fatal(err)
	}
	got := s.Declared()
	if !arrow.TypeEqual(got.Field(1).Type, arrow.BinaryTypes.String) || !got.Field(1).Nullable || !arrow.TypeEqual(got.Field(0).Type, arrow.PrimitiveTypes.Int64) {
		t.Errorf("declared %v", got)
	}
	if !arrow.TypeEqual(declared.Field(1).Type, &arrow.Decimal128Type{Precision: 38, Scale: 9}) {
		t.Error("Declared changed the source's schema")
	}

	for name, o := range map[string]map[string]arrow.DataType{
		"not a decimal": {"id": arrow.BinaryTypes.String},
		"missing":       {"other": arrow.PrimitiveTypes.Float64},
		"unsupported":   {"amount": arrow.PrimitiveTypes.Int64},
	} {
		if err := (Stream{Name: "s", Schema: declared, Overrides: o}).checkOverrides(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := (Stream{Name: "s", Overrides: map[string]arrow.DataType{"amount": arrow.BinaryTypes.String}}).checkOverrides(); err == nil {
		t.Error("an override on an undeclared stream was accepted")
	}
}

type capsDest struct {
	destination.Destination
	caps destination.Capabilities
}

func (d capsDest) Capabilities() destination.Capabilities { return d.caps }

func TestNanosecondsNeedADestinationThatKeepsThem(t *testing.T) {
	for _, keeps := range []bool{false, true} {
		j := Job{
			Owner:       "a",
			Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "o", "p"),
			Source:      struct{ source.Reader }{},
			Dataflow:    Passthrough{},
			Streams:     []Stream{{Name: "s", Mode: destination.Append, NanosecondTimestamps: true}},
			Destination: capsDest{caps: destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopeStream}, NanosecondTimestamps: keeps}},
			Settings:    delivery.Settings{Require: delivery.AtLeastOnce, Scope: destination.ScopeStream},
		}
		err := j.validate()
		if keeps != (err == nil) || (err != nil && errs.CodeOf(err) != errs.ConfigInvalid) {
			t.Errorf("destination keeps nanoseconds: %v; validate: %v", keeps, err)
		}
	}
}
