package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// dataOf is what each type's data is, as the callers of Record pass it.
var dataOf = map[string]any{
	SyncStarted: Sync{}, SyncSucceeded: Sync{}, SyncFailed: Sync{}, SyncStopped: Sync{}, SchemaBlocked: Sync{},
	PipelineCreated: Pipeline{}, PipelineUpdated: Pipeline{}, PipelineDeleted: Pipeline{}, PipelineTeardownFailed: Pipeline{},
	PipelinePaused: Pipeline{}, PipelineResumed: Pipeline{}, PipelineOverridden: Pipeline{}, PipelineReset: Pipeline{},
	StreamPaused: Pipeline{}, StreamResumed: Pipeline{},
	PipelineLagging: Lag{}, PipelineRecovered: Lag{},
	PipelineRescaled: Rescale{},
	SchemaChanged:    Schema{},
	IncidentOpened:   Incident{}, IncidentEscalated: Incident{}, IncidentResolved: Incident{},
	WebhookTest: Test{},
}

type doc map[string]any

func parseSchema(t *testing.T) doc {
	t.Helper()
	var d doc
	if err := json.Unmarshal(PayloadSchema(), &d); err != nil {
		t.Fatal(err)
	}
	return d
}

func (d doc) def(ref string) doc {
	defs, _ := d["$defs"].(map[string]any)
	def, _ := defs[strings.TrimPrefix(ref, "#/$defs/")].(map[string]any)
	return def
}

// branches returns the def each event type's data is, by type.
func (d doc) branches() map[string]string {
	out := map[string]string{}
	entries, _ := d["allOf"].([]any)
	for _, e := range entries {
		e, _ := e.(map[string]any)
		cond, _ := e["if"].(map[string]any)
		condProps, _ := cond["properties"].(map[string]any)
		typ, _ := condProps["type"].(map[string]any)
		name, _ := typ["const"].(string)
		then, _ := e["then"].(map[string]any)
		thenProps, _ := then["properties"].(map[string]any)
		data, _ := thenProps["data"].(map[string]any)
		out[name], _ = data["$ref"].(string)
	}
	return out
}

// coverage lists the event types the schema and Types don't agree on.
func (d doc) coverage() []string {
	var out []string
	branches := d.branches()
	props, _ := d["properties"].(map[string]any)
	typ, _ := props["type"].(map[string]any)
	enum, _ := typ["enum"].([]any)
	var listed []string
	for _, e := range enum {
		s, _ := e.(string)
		listed = append(listed, s)
	}
	for _, name := range Types {
		if branches[name] == "" {
			out = append(out, name+" has no data in the schema")
		}
		if !slices.Contains(listed, name) {
			out = append(out, name+" isn't in type's enum")
		}
		if dataOf[name] == nil {
			out = append(out, name+" isn't in dataOf")
		}
	}
	for _, name := range slices.Sorted(maps.Keys(branches)) {
		if !slices.Contains(Types, name) {
			out = append(out, name+" has data in the schema but isn't in Types")
		}
	}
	for _, name := range listed {
		if !slices.Contains(Types, name) {
			out = append(out, name+" is in type's enum but isn't in Types")
		}
	}
	return out
}

// drift lists how def differs from the struct st. A field is required unless
// it's omitempty.
func (d doc) drift(path string, def doc, st reflect.Type) []string {
	var out []string
	props, _ := def["properties"].(map[string]any)
	list, _ := def["required"].([]any)
	var required []string
	for _, r := range list {
		s, _ := r.(string)
		required = append(required, s)
	}
	seen := map[string]bool{}
	for i := range st.NumField() {
		f := st.Field(i)
		tag := strings.Split(f.Tag.Get("json"), ",")
		name := tag[0]
		if !f.IsExported() || name == "-" {
			continue
		}
		if name == "" {
			name = f.Name
		}
		seen[name] = true
		p, ok := props[name].(map[string]any)
		if !ok {
			out = append(out, fmt.Sprintf("%s.%s isn't in the schema", path, name))
			continue
		}
		omit := slices.Contains(tag[1:], "omitempty")
		switch req := slices.Contains(required, name); {
		case req && omit:
			out = append(out, fmt.Sprintf("%s.%s is required but omitempty", path, name))
		case !req && !omit:
			out = append(out, fmt.Sprintf("%s.%s is optional but always sent", path, name))
		}
		out = append(out, d.typeDrift(path+"."+name, p, f.Type)...)
	}
	for _, name := range slices.Sorted(maps.Keys(props)) {
		if !seen[name] {
			out = append(out, fmt.Sprintf("%s.%s isn't in the struct", path, name))
		}
	}
	return out
}

func (d doc) typeDrift(path string, p doc, t reflect.Type) []string {
	if t.Kind() == reflect.Pointer {
		return d.typeDrift(path, p, t.Elem())
	}
	if ref, ok := p["$ref"].(string); ok {
		def := d.def(ref)
		if def == nil || t.Kind() != reflect.Struct {
			return []string{fmt.Sprintf("%s is %s, which doesn't describe a %s", path, ref, t)}
		}
		return d.drift(path, def, t)
	}
	want := ""
	switch {
	case t == reflect.TypeFor[time.Time]():
		want = "string"
	case t == reflect.TypeFor[json.RawMessage]():
		want = "object"
	default:
		switch t.Kind() {
		case reflect.String:
			want = "string"
		case reflect.Bool:
			want = "boolean"
		case reflect.Int, reflect.Int32, reflect.Int64:
			want = "integer"
		case reflect.Float64:
			want = "number"
		case reflect.Slice:
			want = "array"
		case reflect.Map:
			want = "object"
		default:
			return []string{fmt.Sprintf("%s: this test doesn't know %s", path, t)}
		}
	}
	if p["type"] != want {
		return []string{fmt.Sprintf("%s is %v in the schema, and %s in the struct", path, p["type"], t)}
	}
	if t == reflect.TypeFor[json.RawMessage]() {
		return nil
	}
	switch t.Kind() {
	case reflect.Slice:
		items, _ := p["items"].(map[string]any)
		return d.typeDrift(path+"[]", items, t.Elem())
	case reflect.Map:
		values, _ := p["additionalProperties"].(map[string]any)
		return d.typeDrift(path+"{}", values, t.Elem())
	}
	return nil
}

func TestSchemaCoversEveryType(t *testing.T) {
	for _, problem := range parseSchema(t).coverage() {
		t.Error(problem)
	}
}

func TestSchemaMatchesTheStructs(t *testing.T) {
	d := parseSchema(t)
	for _, problem := range d.drift("payload", d, reflect.TypeFor[Payload]()) {
		t.Error(problem)
	}
	branches := d.branches()
	for _, typ := range Types {
		def := d.def(branches[typ])
		if def == nil || dataOf[typ] == nil {
			continue
		}
		for _, problem := range d.drift(typ, def, reflect.TypeOf(dataOf[typ])) {
			t.Error(problem)
		}
	}
}

func TestDriftIsFound(t *testing.T) {
	type data struct {
		A string `json:"a"`
		B int32  `json:"b,omitempty"`
	}
	str := map[string]any{"type": "string"}
	num := map[string]any{"type": "integer"}
	for _, tc := range []struct {
		name string
		def  doc
		want []string
	}{
		{"matches", doc{"properties": map[string]any{"a": str, "b": num}, "required": []any{"a"}}, nil},
		{"a field the schema lacks", doc{"properties": map[string]any{"a": str}, "required": []any{"a"}}, []string{"x.b isn't in the schema"}},
		{"a field the struct lacks", doc{"properties": map[string]any{"a": str, "b": num, "c": str}, "required": []any{"a"}}, []string{"x.c isn't in the struct"}},
		{"always sent but optional", doc{"properties": map[string]any{"a": str, "b": num}}, []string{"x.a is optional but always sent"}},
		{"omitempty but required", doc{"properties": map[string]any{"a": str, "b": num}, "required": []any{"a", "b"}}, []string{"x.b is required but omitempty"}},
		{"another type", doc{"properties": map[string]any{"a": num, "b": num}, "required": []any{"a"}}, []string{"x.a is integer in the schema, and string in the struct"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := (doc{}).drift("x", tc.def, reflect.TypeFor[data]()); !slices.Equal(got, tc.want) {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
	t.Run("a type with no data", func(t *testing.T) {
		d := parseSchema(t)
		entries, _ := d["allOf"].([]any)
		d["allOf"] = entries[:len(entries)-1]
		if got := d.coverage(); !slices.Contains(got, Types[len(Types)-1]+" has no data in the schema") {
			t.Errorf("got %q", got)
		}
	})
}

// Values for the fields the schema limits to a list.
var listed = map[string]string{"by": "api", "severity": "warning"}

// sample is a value of t: with every field set, or with only those JSON
// always carries.
func sample(t reflect.Type, full bool) reflect.Value {
	v := reflect.New(t).Elem()
	if t == reflect.TypeFor[time.Time]() {
		v.Set(reflect.ValueOf(time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)))
		return v
	}
	switch t.Kind() {
	case reflect.String:
		v.SetString("x")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int32, reflect.Int64:
		v.SetInt(1)
	case reflect.Float64:
		v.SetFloat(1.5)
	case reflect.Pointer:
		v.Set(sample(t.Elem(), full).Addr())
	case reflect.Slice:
		v.Set(reflect.MakeSlice(t, 1, 1))
		v.Index(0).Set(sample(t.Elem(), full))
	case reflect.Map:
		v.Set(reflect.MakeMap(t))
		v.SetMapIndex(reflect.ValueOf("k"), sample(t.Elem(), full))
	case reflect.Struct:
		for i := range t.NumField() {
			f := t.Field(i)
			if !full && strings.Contains(f.Tag.Get("json"), "omitempty") {
				continue
			}
			v.Field(i).Set(sample(f.Type, full))
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			if s, ok := listed[name]; ok {
				v.Field(i).SetString(s)
			}
		}
	}
	return v
}

func marshalled(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	return inst
}

func TestPayloadsAreWhatTheSchemaSays(t *testing.T) {
	parsed, err := jsonschema.UnmarshalJSON(bytes.NewReader(PayloadSchema()))
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource("event.schema.json", parsed); err != nil {
		t.Fatal(err)
	}
	sch, err := c.Compile("event.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	for _, typ := range Types {
		st := reflect.TypeOf(dataOf[typ])
		for name, full := range map[string]bool{"every field": true, "only the required fields": false} {
			data, err := json.Marshal(sample(st, full).Interface())
			if err != nil {
				t.Fatal(err)
			}
			if err := sch.Validate(marshalled(t, Payload{Type: typ, Timestamp: now, Data: data})); err != nil {
				t.Errorf("%s with %s: %v", typ, name, err)
			}
		}
		if sch.Validate(marshalled(t, Payload{Type: typ, Timestamp: now, Data: json.RawMessage(`{}`)})) == nil {
			t.Errorf("%s passed with empty data", typ)
		}
	}
	if sch.Validate(marshalled(t, Payload{Type: "sync.unknown", Timestamp: now, Data: json.RawMessage(`{}`)})) == nil {
		t.Error("an unknown type passed")
	}
}
