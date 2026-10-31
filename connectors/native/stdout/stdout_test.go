package stdout

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/unsuitable001/openetl/sdk/go/destination"
)

func TestWritesJSONLines(t *testing.T) {
	var out bytes.Buffer
	d, err := NewWriter([]byte("{}"), &out)
	if err != nil {
		t.Fatal(err)
	}
	w, err := d.Open(t.Context(), destination.OpenConfig{})
	if err != nil {
		t.Fatal(err)
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "name", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[{"id": 1, "name": "a"}, {"id": 2, "name": null}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	e := destination.Epoch{Generation: 1, N: 1}
	if err := w.Write(t.Context(), e, "users", rec); err != nil {
		t.Fatal(err)
	}
	if _, err := w.PreCommit(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	want := `{"stream":"users","record":{"id":1,"name":"a"}}` + "\n" + `{"stream":"users","record":{"id":2,"name":null}}` + "\n"
	if out.String() != want {
		t.Errorf("output:\n%s\nwant:\n%s", out.String(), want)
	}
	if _, err := NewWriter([]byte(`{"pretty": true}`), &out); err == nil {
		t.Error("config was accepted")
	}
}
