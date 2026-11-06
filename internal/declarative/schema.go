package declarative

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
)

// arrowSchema declares the types of the properties a stream's JSON Schema
// lists. Every field is nullable, since APIs leave fields out.
func arrowSchema(raw json.RawMessage) (*arrow.Schema, error) {
	var s struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	if len(s.Properties) == 0 {
		return nil, nil
	}
	var fields []arrow.Field
	for _, name := range sortedKeys(s.Properties) {
		t, err := arrowType(s.Properties[name])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		fields = append(fields, arrow.Field{Name: name, Type: t, Nullable: true})
	}
	return arrow.NewSchema(fields, nil), nil
}

func arrowType(raw json.RawMessage) (arrow.DataType, error) {
	var p struct {
		Type            any    `json:"type"`
		Format          string `json:"format"`
		ContentEncoding string `json:"contentEncoding"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	// ["string", "null"] is a nullable string.
	var kinds []string
	switch t := p.Type.(type) {
	case string:
		kinds = []string{t}
	case []any:
		for _, k := range t {
			if s, ok := k.(string); ok && s != "null" {
				kinds = append(kinds, s)
			}
		}
	}
	if len(kinds) != 1 {
		return nil, fmt.Errorf("give one type besides null, not %v", p.Type)
	}
	switch kinds[0] {
	case "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	case "integer":
		return arrow.PrimitiveTypes.Int64, nil
	case "number":
		return arrow.PrimitiveTypes.Float64, nil
	case "object", "array":
		return extensions.NewJSONType(arrow.BinaryTypes.String)
	case "string":
		switch {
		case p.ContentEncoding == "base64":
			return arrow.BinaryTypes.Binary, nil
		case p.Format == "date-time":
			return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
		case p.Format == "date":
			return arrow.FixedWidthTypes.Date32, nil
		case p.Format == "time":
			return arrow.FixedWidthTypes.Time64us, nil
		case p.Format == "uuid":
			return extensions.NewUUIDType(), nil
		case p.Format == "" || slices.Contains(textFormats, p.Format):
			return arrow.BinaryTypes.String, nil
		}
		return nil, fmt.Errorf("format %q has no logical type; leave it out for a string", p.Format)
	}
	return nil, fmt.Errorf("type %q has no logical type", kinds[0])
}

// Formats that stay strings.
var textFormats = []string{"email", "hostname", "ipv4", "ipv6", "uri", "uri-reference", "iri"}
