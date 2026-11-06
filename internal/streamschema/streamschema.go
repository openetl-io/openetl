// Package streamschema describes the stream schemas the engine reports: an
// Arrow IPC stream holding only the schema.
package streamschema

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// Schema metadata the engine writes.
const (
	tableKey       = "openetl.table"
	sourceNamesKey = "openetl.source_names"
)

type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	// Where the destination name differs.
	Source string `json:"source,omitempty"`
}

type Description struct {
	Table   string
	Columns []Column
	// Of the table and columns, so the same schema always has the same
	// digest and a schema history records it once.
	Digest string
}

func Describe(raw []byte) (Description, error) {
	r, err := ipc.NewReader(bytes.NewReader(raw))
	if err != nil {
		return Description{}, err
	}
	defer r.Release()
	s := r.Schema()
	md := s.Metadata()
	var d Description
	d.Table, _ = md.GetValue(tableKey)
	var sources map[string]string
	if v, ok := md.GetValue(sourceNamesKey); ok {
		if err := json.Unmarshal([]byte(v), &sources); err != nil {
			return Description{}, err
		}
	}
	d.Columns = Columns(s)
	for i := range d.Columns {
		d.Columns[i].Source = sources[d.Columns[i].Name]
	}
	cols, err := json.Marshal(d.Columns)
	if err != nil {
		return Description{}, err
	}
	sum := sha256.Sum256(append([]byte(d.Table+"\x00"), cols...))
	d.Digest = hex.EncodeToString(sum[:])
	return d, nil
}

// Columns describes a schema's fields.
func Columns(s *arrow.Schema) []Column {
	cols := make([]Column, 0, s.NumFields())
	for _, f := range s.Fields() {
		cols = append(cols, Column{Name: f.Name, Type: typeName(f), Nullable: f.Nullable})
	}
	return cols
}

// typeName is the field's Arrow type, with its extension where the field
// metadata names one.
func typeName(f arrow.Field) string {
	if _, ok := f.Type.(arrow.ExtensionType); ok {
		return f.Type.String()
	}
	if ext, ok := f.Metadata.GetValue("ARROW:extension:name"); ok {
		return "extension<" + ext + ">"
	}
	return f.Type.String()
}
