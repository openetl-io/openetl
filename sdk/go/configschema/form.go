package configschema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// Form is what a form needs from a config schema: its groups, and its
// fields in the order the schema declares them.
type Form struct {
	// From the root's x-groups, in their order. A field whose group isn't
	// set goes before every group.
	Groups []Group
	Fields []Field
}

// Group is a section of a form, declared in the root's x-groups.
type Group struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

// Field is a property of the config, or of an object in it.
type Field struct {
	// The property's name.
	Name string
	// Dotted from the config's root, such as consumer.name.
	Path string
	// The JSON Schema type, such as string or object; empty when it doesn't
	// say.
	Type        string
	Title       string
	Description string
	Required    bool
	// Its value is a secret's name, never the secret.
	Secret bool
	// x-advanced: a form shows it folded away.
	Advanced   bool
	Deprecated bool
	// x-group's id; a nested field's is its parent's unless it sets one.
	Group string
	// JSON; nil when there's none.
	Default json.RawMessage
	// The values it takes, from enum, or from oneOf or anyOf branches that
	// each have a const.
	Options []Option
	// x-when: it applies only when every condition holds, so a form hides
	// it otherwise.
	When []Condition
	// x-format, or else format: date-time, duration, uri and so on.
	Format             string
	Minimum, Maximum   *float64
	Pattern            string
	Examples           []json.RawMessage
	MinItems, MaxItems *int
	// An array's items' type.
	Items string
	// An object's fields.
	Fields []Field

	// Format came from x-format.
	xFormat bool
}

// Option is a value an enum field takes.
type Option struct {
	// JSON.
	Value       json.RawMessage
	Title       string
	Description string
}

// Condition holds when the field at Field, a dotted path from the config's
// root, has one of Values (JSON). A field that isn't set has its default.
type Condition struct {
	Field  string
	Values []json.RawMessage
}

// Formats are the x-format values a form understands, besides JSON
// Schema's own format values.
var Formats = []string{
	// Go's duration syntax: 500ms, 1m30s.
	"duration",
	// RFC 3339: 2026-11-15T08:00:00Z.
	"date-time",
	"uri",
	"email",
	"hostname",
	// A regular expression, in Go's RE2 syntax.
	"regex",
	// Shown in a text area.
	"multiline",
}

// ReadForm reads a config schema's form. A property whose schema is false,
// which no value matches, isn't a field.
func ReadForm(schema []byte) (Form, error) {
	if len(bytes.TrimSpace(schema)) == 0 {
		return Form{}, nil
	}
	var root node
	if err := json.Unmarshal(schema, &root); err != nil {
		return Form{}, fmt.Errorf("config schema: %w", err)
	}
	return Form{Groups: root.Groups, Fields: root.fields("", "")}, nil
}

// node is a schema as far as forms go.
type node struct {
	Type        any               `json:"type"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	Default     json.RawMessage   `json:"default"`
	Const       json.RawMessage   `json:"const"`
	Enum        []json.RawMessage `json:"enum"`
	OneOf       []node            `json:"oneOf"`
	AnyOf       []node            `json:"anyOf"`
	Examples    []json.RawMessage `json:"examples"`
	Minimum     *float64          `json:"minimum"`
	Maximum     *float64          `json:"maximum"`
	MinItems    *int              `json:"minItems"`
	MaxItems    *int              `json:"maxItems"`
	Pattern     string            `json:"pattern"`
	Format      string            `json:"format"`
	Deprecated  bool              `json:"deprecated"`
	Items       *node             `json:"items"`
	Properties  properties        `json:"properties"`
	Required    []string          `json:"required"`

	Secret   bool                       `json:"x-secret"`
	Advanced bool                       `json:"x-advanced"`
	Group    string                     `json:"x-group"`
	XFormat  string                     `json:"x-format"`
	When     map[string]json.RawMessage `json:"x-when"`
	Groups   []Group                    `json:"x-groups"`

	// The schema is false: no value matches it.
	never bool
}

// UnmarshalJSON reads a schema, or the booleans true and false, which
// draft 2020-12 allows in a schema's place.
func (n *node) UnmarshalJSON(b []byte) error {
	switch string(bytes.TrimSpace(b)) {
	case "true":
		*n = node{}
		return nil
	case "false":
		*n = node{never: true}
		return nil
	}
	type plain node
	return json.Unmarshal(b, (*plain)(n))
}

// properties keeps an object's properties in the order the schema has them.
type properties struct {
	names []string
	nodes map[string]node
}

func (p *properties) UnmarshalJSON(b []byte) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return errors.New("properties isn't an object")
	}
	p.nodes = map[string]node{}
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return err
		}
		name, _ := t.(string)
		var n node
		if err := dec.Decode(&n); err != nil {
			return fmt.Errorf("property %s: %w", name, err)
		}
		if _, dup := p.nodes[name]; !dup {
			p.names = append(p.names, name)
		}
		p.nodes[name] = n
	}
	_, err := dec.Token()
	return err
}

func (n *node) typ() string {
	switch t := n.Type.(type) {
	case string:
		return t
	case []any:
		for _, v := range t {
			if v, ok := v.(string); ok && v != "null" {
				return v
			}
		}
	}
	if n.Properties.nodes != nil {
		return "object"
	}
	return ""
}

func (n *node) options() []Option {
	var out []Option
	for _, v := range n.Enum {
		out = append(out, Option{Value: v})
	}
	for _, branches := range [][]node{n.OneOf, n.AnyOf} {
		// A branch without a const takes values that no option names.
		if slices.ContainsFunc(branches, func(b node) bool { return b.Const == nil }) {
			continue
		}
		for _, b := range branches {
			out = append(out, Option{Value: b.Const, Title: b.Title, Description: b.Description})
		}
	}
	return out
}

func (n *node) fields(prefix, group string) []Field {
	var out []Field
	for _, name := range n.Properties.names {
		p := n.Properties.nodes[name]
		if p.never {
			continue
		}
		f := Field{
			Name: name, Path: prefix + name, Type: p.typ(), Title: p.Title, Description: p.Description,
			Required: slices.Contains(n.Required, name), Secret: p.Secret, Advanced: p.Advanced, Deprecated: p.Deprecated,
			Group: cmp(p.Group, group), Default: p.Default, Options: p.options(), Format: cmp(p.XFormat, p.Format),
			Minimum: p.Minimum, Maximum: p.Maximum, Pattern: p.Pattern, Examples: p.Examples, MinItems: p.MinItems, MaxItems: p.MaxItems,
			xFormat: p.XFormat != "",
		}
		for _, k := range slices.Sorted(maps.Keys(p.When)) {
			c := Condition{Field: k, Values: []json.RawMessage{p.When[k]}}
			var many []json.RawMessage
			if bytes.HasPrefix(bytes.TrimSpace(p.When[k]), []byte("[")) && json.Unmarshal(p.When[k], &many) == nil {
				c.Values = many
			}
			f.When = append(f.When, c)
		}
		if p.Items != nil {
			f.Items = p.Items.typ()
			if len(f.Options) == 0 {
				f.Options = p.Items.options()
			}
		}
		if f.Type == "object" {
			f.Fields = p.fields(f.Path+".", f.Group)
		}
		out = append(out, f)
	}
	return out
}

func cmp(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// Lint finds what would break a form in a config schema: an x-group the
// root's x-groups don't declare, an x-when on a field that doesn't exist or
// on a value it can't take, a default that isn't one of a field's options,
// and an x-format a form doesn't know. Each problem is an error naming the
// field's path.
func Lint(schema []byte) []error {
	form, err := ReadForm(schema)
	if err != nil {
		return []error{err}
	}
	var problems []error
	add := func(path, format string, args ...any) {
		problems = append(problems, fmt.Errorf("config schema: %s: %s", path, fmt.Sprintf(format, args...)))
	}
	groups := map[string]bool{}
	for _, g := range form.Groups {
		if g.ID == "" {
			add("x-groups", "a group has no id")
		} else if groups[g.ID] {
			add("x-groups", "%s is declared twice", g.ID)
		}
		groups[g.ID] = true
	}
	all := map[string]Field{}
	var index func([]Field)
	index = func(fs []Field) {
		for _, f := range fs {
			all[f.Path] = f
			index(f.Fields)
		}
	}
	index(form.Fields)
	for _, path := range slices.Sorted(maps.Keys(all)) {
		f := all[path]
		if f.Group != "" && !groups[f.Group] {
			add(path, "x-group %s isn't in the root's x-groups", f.Group)
		}
		if f.xFormat && !slices.Contains(Formats, f.Format) {
			add(path, "x-format %s isn't one a form knows", f.Format)
		}
		if f.Default != nil && len(f.Options) > 0 && f.Type != "array" && !hasValue(f.Options, f.Default) {
			add(path, "the default %s isn't one of its values", f.Default)
		}
		for _, c := range f.When {
			on, ok := all[c.Field]
			switch {
			case !ok:
				add(path, "x-when names %s, which isn't a field", c.Field)
			case c.Field == path:
				add(path, "x-when names the field itself")
			case len(on.Options) > 0:
				for _, v := range c.Values {
					if !hasValue(on.Options, v) {
						add(path, "x-when wants %s to be %s, which isn't one of its values", c.Field, v)
					}
				}
			}
		}
	}
	return problems
}

func hasValue(opts []Option, v json.RawMessage) bool {
	return slices.ContainsFunc(opts, func(o Option) bool { return sameJSON(o.Value, v) })
}

func sameJSON(a, b json.RawMessage) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	xb, _ := json.Marshal(x)
	yb, _ := json.Marshal(y)
	return bytes.Equal(xb, yb)
}

// Undescribed lists the fields without a description, which a form can only
// show by name.
func Undescribed(form Form) []string {
	var out []string
	var walk func([]Field)
	walk = func(fs []Field) {
		for _, f := range fs {
			if f.Description == "" && f.Title == "" {
				out = append(out, f.Path)
			}
			walk(f.Fields)
		}
	}
	walk(form.Fields)
	return out
}
