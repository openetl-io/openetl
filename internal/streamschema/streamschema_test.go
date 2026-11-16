package streamschema

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
)

// Columns name each type as `openetl-engine plan` reads it: an extension by
// its name, whether its Go type or the field's metadata carries it, in a
// list's item too.
func TestColumnsNameTypesAsThePlanReadsThem(t *testing.T) {
	js, err := extensions.NewJSONType(arrow.BinaryTypes.String)
	if err != nil {
		t.Fatal(err)
	}
	named := func(name string) arrow.Metadata {
		return arrow.NewMetadata([]string{"ARROW:extension:name"}, []string{name})
	}
	for _, c := range []struct {
		f    arrow.Field
		want string
	}{
		{arrow.Field{Type: arrow.PrimitiveTypes.Int64}, "int64"},
		{arrow.Field{Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}}, "timestamp[us, tz=UTC]"},
		{arrow.Field{Type: &arrow.Decimal128Type{Precision: 12, Scale: 2}}, "decimal(12, 2)"},
		{arrow.Field{Type: js}, "extension<arrow.json>"},
		{arrow.Field{Type: extensions.NewUUIDType()}, "extension<arrow.uuid>"},
		{arrow.Field{Type: arrow.BinaryTypes.String, Metadata: named("arrow.json")}, "extension<arrow.json>"},
		{arrow.Field{Type: arrow.ListOf(js)}, "list<item: extension<arrow.json>, nullable>"},
		{arrow.Field{Type: arrow.ListOfNonNullable(extensions.NewUUIDType())}, "list<item: extension<arrow.uuid>>"},
		{arrow.Field{Type: arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: named("arrow.json")})},
			"list<item: extension<arrow.json>, nullable>"},
		{arrow.Field{Type: arrow.LargeListOf(arrow.PrimitiveTypes.Int64)}, "large_list<item: int64, nullable>"},
		{arrow.Field{Type: arrow.BinaryTypes.StringView}, "utf8_view"},
	} {
		c.f.Name = "c"
		if got := Columns(arrow.NewSchema([]arrow.Field{c.f}, nil))[0].Type; got != c.want {
			t.Errorf("%s is named %q, want %q", c.f.Type, got, c.want)
		}
	}
}
