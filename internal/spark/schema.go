package spark

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// SchemaJSON is an engine's schema, an Arrow IPC stream holding only it, as
// Spark's JSON schema of the types Spark 3.5 reads from the engine's
// Parquet. A column Spark can't read, such as a time of day, fails it.
func SchemaJSON(b []byte) (json.RawMessage, error) {
	r, err := ipc.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer r.Release()
	t, err := structType(r.Schema().Fields())
	if err != nil {
		return nil, err
	}
	return json.Marshal(t)
}

type sparkField struct {
	Name     string         `json:"name"`
	Type     any            `json:"type"`
	Nullable bool           `json:"nullable"`
	Metadata map[string]any `json:"metadata"`
}

func structType(fields []arrow.Field) (map[string]any, error) {
	out := make([]sparkField, 0, len(fields))
	for _, f := range fields {
		t, err := sparkType(f.Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", f.Name, err)
		}
		out = append(out, sparkField{Name: f.Name, Type: t, Nullable: f.Nullable, Metadata: map[string]any{}})
	}
	return map[string]any{"type": "struct", "fields": out}, nil
}

func sparkType(t arrow.DataType) (any, error) {
	switch t := t.(type) {
	case arrow.ExtensionType:
		return sparkType(t.StorageType())
	case *arrow.DictionaryType:
		return sparkType(t.ValueType)
	case *arrow.Decimal128Type:
		return decimal(t.Precision, t.Scale)
	case *arrow.Decimal256Type:
		return decimal(t.Precision, t.Scale)
	case *arrow.TimestampType:
		if t.TimeZone == "" {
			return "timestamp_ntz", nil
		}
		return "timestamp", nil
	case *arrow.ListType:
		return arrayType(t.ElemField())
	case *arrow.LargeListType:
		return arrayType(t.ElemField())
	case *arrow.FixedSizeListType:
		return arrayType(t.ElemField())
	case *arrow.StructType:
		return structType(t.Fields())
	case *arrow.MapType:
		k, err := sparkType(t.KeyType())
		if err != nil {
			return nil, err
		}
		v, err := sparkType(t.ItemType())
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "map", "keyType": k, "valueType": v, "valueContainsNull": t.ItemField().Nullable}, nil
	}
	if s, ok := simple[t.ID()]; ok {
		return s, nil
	}
	switch t.ID() {
	case arrow.UINT64:
		return "decimal(20,0)", nil
	case arrow.TIME32, arrow.TIME64:
		return nil, fmt.Errorf("it's a time of day (%s), which Spark 3.5 can't read", t)
	}
	return nil, fmt.Errorf("it's %s, which Spark 3.5 can't read", t)
}

var simple = map[arrow.Type]string{
	arrow.NULL:              "void",
	arrow.BOOL:              "boolean",
	arrow.INT8:              "byte",
	arrow.INT16:             "short",
	arrow.INT32:             "integer",
	arrow.INT64:             "long",
	arrow.UINT8:             "short",
	arrow.UINT16:            "integer",
	arrow.UINT32:            "long",
	arrow.FLOAT32:           "float",
	arrow.FLOAT64:           "double",
	arrow.STRING:            "string",
	arrow.LARGE_STRING:      "string",
	arrow.STRING_VIEW:       "string",
	arrow.BINARY:            "binary",
	arrow.LARGE_BINARY:      "binary",
	arrow.BINARY_VIEW:       "binary",
	arrow.FIXED_SIZE_BINARY: "binary",
	arrow.DATE32:            "date",
	arrow.DATE64:            "date",
}

func decimal(p, s int32) (any, error) {
	if p > 38 {
		return nil, fmt.Errorf("it's a decimal of %d digits, and Spark keeps at most 38", p)
	}
	return fmt.Sprintf("decimal(%d,%d)", p, s), nil
}

func arrayType(elem arrow.Field) (any, error) {
	e, err := sparkType(elem.Type)
	if err != nil {
		return nil, err
	}
	return map[string]any{"type": "array", "elementType": e, "containsNull": elem.Nullable}, nil
}
