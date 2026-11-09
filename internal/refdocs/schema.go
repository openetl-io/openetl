package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// A schema page lists every field of a JSON Schema: a table for each
// object, by its path, with its rules and its variants. A $def used in one
// place is written out where it's used; one that two or more sibling fields
// are (such as each role of a package) gets one section for all of them,
// with a placeholder in its path; any other used more than once is a type of
// its own, under Types. The renderer refuses a keyword it doesn't know, so a
// schema can't gain a rule that the page leaves out.

type section struct {
	level    int
	heading  string
	intro    []string
	rows     []row
	rules    []string
	outro    []string
	variants []*section
	children []*section
	anchor   string
}

type row struct {
	name, typ, desc string
	required        bool
}

type schemaDoc struct {
	root  *object
	defs  *object
	uses  map[string]int
	links []*section
	// Sections of defs that sibling fields share, and of defs that are
	// types of their own, by def.
	merged map[string]*section
	named  map[string]*section
	order  []string
}

// Keywords a schema node may have; the root may also have $schema, $id,
// title and $defs.
var keywords = []string{
	"$ref", "description", "type", "properties", "required", "additionalProperties", "unevaluatedProperties",
	"allOf", "anyOf", "oneOf", "not", "if", "then", "else", "dependentRequired", "minProperties", "propertyNames",
	"items", "minItems", "maxItems", "uniqueItems", "enum", "const", "pattern", "minLength", "maxLength",
	"minimum", "maximum", "exclusiveMinimum",
}

func newSchemaDoc(b []byte) (*schemaDoc, error) {
	root, err := decode(b)
	if err != nil {
		return nil, err
	}
	d := &schemaDoc{root: root, defs: root.obj("$defs"), uses: map[string]int{}, merged: map[string]*section{}, named: map[string]*section{}}
	if d.defs == nil {
		d.defs = &object{vals: map[string]any{}}
	}
	d.count(root)
	return d, nil
}

func (d *schemaDoc) count(v any) {
	switch v := v.(type) {
	case *object:
		if ref := v.str("$ref"); ref != "" {
			d.uses[strings.TrimPrefix(ref, "#/$defs/")]++
		}
		for _, k := range v.keys {
			d.count(v.vals[k])
		}
	case []any:
		for _, e := range v {
			d.count(e)
		}
	}
}

// render writes the page's body from its first heading: the top level's
// fields, the sections under it, and the types.
func (d *schemaDoc) render() (string, error) {
	for _, k := range d.root.keys {
		if !slices.Contains(keywords, k) && !slices.Contains([]string{"$schema", "$id", "title", "$defs"}, k) {
			return "", fmt.Errorf("the top level has %s, which refdocs doesn't render", k)
		}
	}
	top := &section{level: 2, heading: "Top-level fields"}
	body := &object{vals: d.root.vals}
	for _, k := range d.root.keys {
		if slices.Contains(keywords, k) && k != "description" {
			body.keys = append(body.keys, k)
		}
	}
	if err := d.fill(top, top, "", body, false); err != nil {
		return "", err
	}
	var order []*section
	var walk func(*section)
	walk = func(s *section) {
		order = append(order, s)
		for _, v := range s.variants {
			walk(v)
		}
		for _, c := range s.children {
			walk(c)
		}
	}
	walk(top)
	var types *section
	if len(d.order) > 0 {
		types = &section{level: 2, heading: "Types", intro: []string{"Fields above refer to these types by name."}}
		order = append(order, types)
		for _, name := range d.order {
			walk(d.named[name])
		}
	}
	seen := map[string]int{}
	for _, s := range order {
		id := slug(s.heading)
		if n := seen[id]; n > 0 {
			seen[id]++
			id = fmt.Sprintf("%s-%d", id, n)
		} else {
			seen[id] = 1
		}
		s.anchor = id
	}
	var b strings.Builder
	for _, s := range order {
		d.emit(&b, s)
	}
	return d.resolve(b.String()), nil
}

func (d *schemaDoc) emit(b *strings.Builder, s *section) {
	fmt.Fprintf(b, "%s %s\n\n", strings.Repeat("#", s.level), s.heading)
	for _, p := range s.intro {
		b.WriteString(p + "\n\n")
	}
	if len(s.rows) > 0 {
		b.WriteString("| Field | Type | Required | Description |\n|---|---|---|---|\n")
		for _, r := range s.rows {
			req := "No"
			if r.required {
				req = "Yes"
			}
			fmt.Fprintf(b, "| `%s` | %s | %s | %s |\n", r.name, cell(r.typ), req, cell(r.desc))
		}
		b.WriteString("\n")
	}
	for _, r := range s.rules {
		b.WriteString("- " + r + "\n")
	}
	if len(s.rules) > 0 {
		b.WriteString("\n")
	}
	for _, p := range s.outro {
		b.WriteString(p + "\n\n")
	}
}

// link is a placeholder for a link to s, which resolve fills in once every
// section has its anchor.
func (d *schemaDoc) link(text string, s *section) string {
	d.links = append(d.links, s)
	return fmt.Sprintf("[%s](\x00%d\x00)", text, len(d.links)-1)
}

var placeholder = regexp.MustCompile("\x00([0-9]+)\x00")

func (d *schemaDoc) resolve(s string) string {
	return placeholder.ReplaceAllStringFunc(s, func(m string) string {
		i, _ := strconv.Atoi(strings.Trim(m, "\x00"))
		return "#" + d.links[i].anchor
	})
}

func check(s *object, where string) error {
	for _, k := range s.keys {
		if !slices.Contains(keywords, k) {
			return fmt.Errorf("%s has %s, which refdocs doesn't render", where, k)
		}
	}
	return nil
}

// fill gives sec the fields, rules and variants of the object s at path.
// Sections for objects under it go to owner's children.
func (d *schemaDoc) fill(sec, owner *section, path string, s *object, variant bool) error {
	if err := check(s, at(path)); err != nil {
		return err
	}
	props := s.obj("properties")
	if props == nil {
		props = &object{vals: map[string]any{}}
	}
	required := s.strings("required")
	// Defs that every use of is a field here share a section.
	groups := map[string][]string{}
	for _, name := range props.keys {
		if b, _ := d.base(props.obj(name)); b != "" && d.uses[b] >= 2 && d.defs.obj(b).has("properties") {
			groups[b] = append(groups[b], name)
		}
	}
	for _, name := range props.keys {
		p := props.obj(name)
		if constraint(p) {
			rules, err := d.sentences(p, name+".")
			if err != nil {
				return err
			}
			sec.rules = append(sec.rules, rules...)
			continue
		}
		fpath := join(path, name)
		if b, _ := d.base(p); b != "" && len(groups[b]) == d.uses[b] && len(groups[b]) >= 2 && d.merged[b] == nil && d.named[b] == nil {
			mpath := join(path, strings.ToUpper(b))
			m := &section{level: owner.level, heading: code(mpath)}
			m.intro = append(m.intro, fmt.Sprintf("%s is %s.", code(strings.ToUpper(b)), list(codes("", groups[b]), "or")))
			if desc := text(d.defs.obj(b).str("description")); desc != "" {
				m.intro = append(m.intro, desc)
			}
			d.merged[b] = m
			owner.children = append(owner.children, m)
			if err := d.fill(m, m, mpath, d.defs.obj(b), false); err != nil {
				return err
			}
		}
		typ, err := d.typeOf(p, owner, fpath)
		if err != nil {
			return err
		}
		desc := d.describe(p)
		parts := []string{}
		if desc != "" {
			parts = append(parts, desc)
		}
		facts, err := d.facts(p)
		if err != nil {
			return fmt.Errorf("%s: %w", at(fpath), err)
		}
		parts = append(parts, facts...)
		_, wrappers := d.base(p)
		for _, w := range wrappers {
			rules, err := d.wrapperRules(w)
			if err != nil {
				return fmt.Errorf("%s: %w", at(fpath), err)
			}
			parts = append(parts, rules...)
		}
		if len(parts) == 0 {
			return fmt.Errorf("%s has no description", at(fpath))
		}
		sec.rows = append(sec.rows, row{name: name, typ: typ, desc: strings.Join(parts, " "), required: slices.Contains(required, name)})
	}
	for _, name := range required {
		if !props.has(name) {
			return fmt.Errorf("%s requires %s, which it doesn't define", at(path), name)
		}
	}
	// What constrains the object as a whole.
	rest := &object{vals: map[string]any{}}
	for _, k := range s.keys {
		switch k {
		case "anyOf", "oneOf", "not", "dependentRequired", "minProperties":
			rest.keys = append(rest.keys, k)
			rest.vals[k] = s.vals[k]
		}
	}
	rules, err := d.sentences(rest, "")
	if err != nil {
		return fmt.Errorf("%s: %w", at(path), err)
	}
	sec.rules = append(sec.rules, rules...)
	conds := []*object{}
	if s.has("if") {
		conds = append(conds, s)
	}
	for _, e := range s.list("allOf") {
		e, ok := e.(*object)
		if !ok {
			return fmt.Errorf("%s: an allOf entry isn't an object", at(path))
		}
		switch {
		case e.has("if"):
			conds = append(conds, e)
		case constraint(e):
			rules, err := d.sentences(e, "")
			if err != nil {
				return fmt.Errorf("%s: %w", at(path), err)
			}
			sec.rules = append(sec.rules, rules...)
		default:
			return fmt.Errorf("%s: an allOf entry neither constrains nor is a variant", at(path))
		}
	}
	for _, c := range conds {
		if err := d.condition(sec, owner, path, c); err != nil {
			return err
		}
	}
	if !variant && !closed(s) && !s.has("additionalProperties") {
		sec.outro = append(sec.outro, "Other fields are allowed.")
	}
	return nil
}

// condition adds an if/then/else: a variant when it tests a field for a
// value, and rules otherwise.
func (d *schemaDoc) condition(sec, owner *section, path string, c *object) error {
	cond, then, els := c.obj("if"), c.obj("then"), c.obj("else")
	if then == nil {
		return fmt.Errorf("%s: an if without a then", at(path))
	}
	if field, value, ok := discriminator(cond); ok && els == nil {
		heading := fmt.Sprintf("When %s is %s", code(field), code(value))
		if path != "" {
			heading = fmt.Sprintf("%s when %s is %s", code(path), code(field), code(value))
		}
		v := &section{level: sec.level + 1, heading: heading}
		if desc := text(then.str("description")); desc != "" {
			v.intro = append(v.intro, desc)
		}
		sec.variants = append(sec.variants, v)
		if props := then.obj("properties"); props != nil {
			for _, name := range props.keys {
				// Written out, it would get a section named after the field,
				// which every variant's would share.
				if b := refName(props.obj(name)); b != "" {
					d.uses[b] = max(d.uses[b], 2)
				}
			}
		}
		return d.fill(v, owner, path, then, true)
	}
	has, err := hasPhrase(cond, "")
	if err != nil {
		return fmt.Errorf("%s: if: %w", at(path), err)
	}
	parts := []string{}
	if desc := text(then.str("description")); desc != "" {
		parts = append(parts, desc)
	}
	rules, err := d.sentences(then, "")
	if err != nil {
		return fmt.Errorf("%s: then: %w", at(path), err)
	}
	sec.rules = append(sec.rules, fmt.Sprintf("If it has %s: %s", has, strings.Join(append(parts, rules...), " ")))
	if els != nil {
		rules, err := d.sentences(els, "")
		if err != nil {
			return fmt.Errorf("%s: else: %w", at(path), err)
		}
		sec.rules = append(sec.rules, "Otherwise: "+strings.Join(rules, " "))
	}
	return nil
}

// discriminator reports whether an if tests one field for one value.
func discriminator(cond *object) (string, string, bool) {
	if cond == nil || len(cond.keys) != 1 || !cond.has("properties") {
		return "", "", false
	}
	props := cond.obj("properties")
	if len(props.keys) != 1 {
		return "", "", false
	}
	p := props.obj(props.keys[0])
	if p == nil || len(p.keys) != 1 || !p.has("const") {
		return "", "", false
	}
	v, ok := p.vals["const"].(string)
	return props.keys[0], v, ok
}

// base is the def that p is, or wraps in an allOf, through defs used once;
// and the wrappers, whose other rules apply to p alone. It's empty when p
// is written out where it's used.
func (d *schemaDoc) base(p *object) (string, []*object) {
	var wrappers []*object
	for p != nil {
		switch {
		case p.has("$ref"):
			name := refName(p)
			if d.uses[name] >= 2 {
				return name, wrappers
			}
			p = d.defs.obj(name)
		case p.has("allOf"):
			first, ok := p.list("allOf")[0].(*object)
			if !ok || !first.has("$ref") {
				return "", nil
			}
			wrappers = append(wrappers, p)
			p = first
		default:
			return "", nil
		}
	}
	return "", nil
}

func (d *schemaDoc) wrapperRules(w *object) ([]string, error) {
	var out []string
	for _, k := range w.keys {
		switch k {
		case "allOf":
			for _, e := range w.list("allOf")[1:] {
				e, ok := e.(*object)
				if !ok || !constraint(e) {
					return nil, errors.New("an allOf entry after the first doesn't only constrain")
				}
				rules, err := d.sentences(e, "")
				if err != nil {
					return nil, err
				}
				out = append(out, rules...)
			}
		case "description", "$ref":
		default:
			rules, err := d.sentences(&object{keys: []string{k}, vals: map[string]any{k: w.vals[k]}}, "")
			if err != nil {
				return nil, err
			}
			out = append(out, rules...)
		}
	}
	return out, nil
}

// typeOf is a field's type, with a link to its section, adding sections
// for objects under it to owner.
func (d *schemaDoc) typeOf(p *object, owner *section, path string) (string, error) {
	if err := check(p, at(path)); err != nil {
		return "", err
	}
	if b, _ := d.base(p); b != "" {
		if m := d.merged[b]; m != nil {
			return d.link("object", m), nil
		}
		t, err := d.namedType(b)
		if err != nil {
			return "", err
		}
		return d.link(code(b), t), nil
	}
	if p.has("$ref") {
		// Written out here, with the field's description as well as the
		// def's.
		def := d.defs.obj(refName(p))
		q := &object{keys: slices.Clone(def.keys), vals: maps.Clone(def.vals)}
		if !q.has("description") {
			q.keys = append(q.keys, "description")
		}
		q.vals["description"] = d.rawDescription(p)
		return d.typeOf(q, owner, path)
	}
	for _, k := range []string{"anyOf", "oneOf"} {
		if alternatives(p, k) {
			var types []string
			for _, a := range p.list(k) {
				a, ok := a.(*object)
				if !ok {
					return "", fmt.Errorf("%s: an %s entry isn't an object", at(path), k)
				}
				t, err := d.typeOf(a, owner, path)
				if err != nil {
					return "", err
				}
				if !slices.Contains(types, t) {
					types = append(types, t)
				}
			}
			return strings.Join(types, " or "), nil
		}
	}
	if p.has("const") {
		return jsonType(p.vals["const"]), nil
	}
	if p.has("enum") {
		var types []string
		for _, v := range p.list("enum") {
			if t := jsonType(v); !slices.Contains(types, t) {
				types = append(types, t)
			}
		}
		return strings.Join(types, " or "), nil
	}
	switch t := p.str("type"); {
	case t == "array":
		items := p.obj("items")
		if items == nil {
			return "array", nil
		}
		it, err := d.typeOf(items, owner, path+"[]")
		if err != nil {
			return "", err
		}
		return "array of " + it, nil
	case t == "object" || p.has("properties"):
		if v := p.obj("additionalProperties"); v != nil {
			key, ph := "names", "NAME"
			if n := p.obj("propertyNames"); n != nil && n.has("$ref") {
				t, err := d.typeOf(n, owner, path)
				if err != nil {
					return "", err
				}
				key, ph = t, strings.ToUpper(refName(n))
			}
			vt, err := d.typeOf(v, owner, join(path, ph))
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("map of %s to %s", key, vt), nil
		}
		if !p.has("properties") && !p.has("allOf") {
			return "object", nil
		}
		s := &section{level: owner.level, heading: code(path)}
		if desc := d.describe(p); desc != "" {
			s.intro = append(s.intro, desc)
		}
		owner.children = append(owner.children, s)
		if err := d.fill(s, s, path, p, false); err != nil {
			return "", err
		}
		return d.link("object", s), nil
	case t == "string" || t == "integer" || t == "number" || t == "boolean":
		return t, nil
	}
	return "", fmt.Errorf("%s has no type refdocs can name", at(path))
}

// namedType is the section of a def that is a type of its own, made the
// first time a field refers to it.
func (d *schemaDoc) namedType(name string) (*section, error) {
	if s := d.named[name]; s != nil {
		return s, nil
	}
	s := &section{level: 3, heading: code(name)}
	d.named[name] = s
	d.order = append(d.order, name)
	def := d.defs.obj(name)
	if err := d.fillType(s, name, def); err != nil {
		return nil, fmt.Errorf("type %s: %w", name, err)
	}
	// The def's description leads its first paragraph.
	if desc := text(def.str("description")); desc != "" {
		if len(s.intro) == 0 {
			s.intro = []string{desc}
		} else {
			s.intro[0] = desc + " " + s.intro[0]
		}
	}
	return s, nil
}

func (d *schemaDoc) fillType(s *section, name string, def *object) error {
	if err := check(def, name); err != nil {
		return err
	}
	switch {
	case def.str("type") == "array" && def.obj("items") != nil && def.obj("items").has("properties"):
		lead := "A list of objects with these fields."
		for _, ph := range phrases(def) {
			lead += " " + sentence(ph)
		}
		s.intro = append(s.intro, lead)
		return d.fill(s, s, name+"[]", def.obj("items"), false)
	case def.has("properties"):
		return d.fill(s, s, name, def, false)
	}
	t, err := d.typeOf(def, s, name)
	if err != nil {
		return err
	}
	facts, err := d.facts(def)
	if err != nil {
		return err
	}
	lead := "A " + t + "."
	if alternatives(def, "anyOf") || alternatives(def, "oneOf") {
		alts := strings.Split(t, " or ")
		lead = "One of " + list(alts, "or") + "."
		if len(alts) == 2 {
			lead = "Either " + alts[0] + " or " + alts[1] + "."
		}
		if len(facts) > 0 {
			lead = ""
		}
	}
	if lead != "" {
		facts = append([]string{lead}, facts...)
	}
	s.intro = append(s.intro, strings.Join(facts, " "))
	return nil
}

// describe is a field's description: its own, or that of the def it's
// written out from.
func (d *schemaDoc) describe(p *object) string { return text(d.rawDescription(p)) }

func (d *schemaDoc) rawDescription(p *object) string {
	own := strings.TrimSpace(p.str("description"))
	if b, _ := d.base(p); b != "" || !p.has("$ref") {
		return own
	}
	def := d.rawDescription(d.defs.obj(refName(p)))
	switch {
	case own == "":
		return def
	case def == "":
		return own
	}
	return own + " " + def
}

// facts are sentences on the values a field takes that its type doesn't
// say.
func (d *schemaDoc) facts(p *object) ([]string, error) {
	if b, _ := d.base(p); b != "" {
		return nil, nil
	}
	if p.has("$ref") {
		return d.facts(d.defs.obj(refName(p)))
	}
	for _, k := range []string{"anyOf", "oneOf"} {
		if !alternatives(p, k) {
			continue
		}
		var alts []string
		inline := false
		for _, a := range p.list(k) {
			a, _ := a.(*object)
			phrase, own, err := d.alternative(a)
			if err != nil {
				return nil, err
			}
			inline = inline || own
			alts = append(alts, phrase)
		}
		if !inline {
			return nil, nil
		}
		if len(alts) == 2 {
			return []string{fmt.Sprintf("Either %s, or %s.", alts[0], alts[1])}, nil
		}
		return []string{"One of " + list(alts, "or") + "."}, nil
	}
	var out []string
	for _, ph := range phrases(p) {
		out = append(out, sentence(ph))
	}
	if items := p.obj("items"); items != nil {
		if b, _ := d.base(items); b == "" {
			if items.has("$ref") {
				items = d.defs.obj(refName(items))
			}
			for _, ph := range phrases(items) {
				out = append(out, "Each item "+ph+".")
			}
		}
	}
	if n := p.obj("propertyNames"); n != nil && !n.has("$ref") {
		for _, ph := range phrases(n) {
			out = append(out, "Each name "+ph+".")
		}
	}
	return out, nil
}

// alternative describes one of a field's alternatives, and whether it's
// written out here rather than named.
func (d *schemaDoc) alternative(a *object) (string, bool, error) {
	if a == nil {
		return "", false, errors.New("an alternative isn't an object")
	}
	if b, _ := d.base(a); b != "" {
		t, err := d.namedType(b)
		if err != nil {
			return "", false, err
		}
		return d.link(code(b), t), false, nil
	}
	if a.has("$ref") {
		return d.alternative(d.defs.obj(refName(a)))
	}
	t := a.str("type")
	if t == "" {
		return "", false, errors.New("an alternative has no type")
	}
	ph := phrases(a)
	if len(ph) == 0 {
		return "a " + t, true, nil
	}
	return fmt.Sprintf("a %s that %s", t, list(ph, "and")), true, nil
}

// phrases are what a value's own keywords say about it, each to follow
// "it" or "each item".
func phrases(p *object) []string {
	var out []string
	num := func(k string) string {
		v, _ := p.vals[k].(json.Number)
		return v.String()
	}
	if p.has("const") {
		out = append(out, "is always "+literal(p.vals["const"]))
	}
	if p.has("enum") {
		var vs []string
		for _, v := range p.list("enum") {
			vs = append(vs, literal(v))
		}
		out = append(out, "is one of "+list(vs, "or"))
	}
	if p.has("pattern") {
		out = append(out, "matches "+code(p.str("pattern")))
	}
	switch minL, maxL := num("minLength"), num("maxLength"); {
	case minL != "" && maxL != "":
		out = append(out, fmt.Sprintf("has %s to %s characters", minL, maxL))
	case minL == "1":
		out = append(out, "isn't empty")
	case minL != "":
		out = append(out, fmt.Sprintf("has at least %s characters", minL))
	case maxL != "":
		out = append(out, fmt.Sprintf("has at most %s characters", maxL))
	}
	switch minV, maxV, gt := num("minimum"), num("maximum"), num("exclusiveMinimum"); {
	case minV != "" && maxV != "":
		out = append(out, fmt.Sprintf("is from %s to %s", minV, maxV))
	case minV != "":
		out = append(out, "is at least "+minV)
	case gt != "":
		out = append(out, "is more than "+gt)
	case maxV != "":
		out = append(out, "is at most "+maxV)
	}
	switch minI, maxI := num("minItems"), num("maxItems"); {
	case minI == "1" && maxI != "":
		out = append(out, fmt.Sprintf("has 1 to %s items", maxI))
	case minI == "1":
		out = append(out, "has at least one item")
	case minI != "":
		out = append(out, fmt.Sprintf("has at least %s items", minI))
	case maxI != "":
		out = append(out, fmt.Sprintf("has at most %s items", maxI))
	}
	if p.vals["uniqueItems"] == true {
		out = append(out, "has no item twice")
	}
	if n := num("minProperties"); n != "" && !p.has("properties") {
		if n == "1" {
			out = append(out, "has at least one entry")
		} else {
			out = append(out, fmt.Sprintf("has at least %s entries", n))
		}
	}
	return out
}

// constraint reports whether s only constrains fields defined elsewhere.
func constraint(s *object) bool {
	if s == nil || len(s.keys) == 0 {
		return false
	}
	constrains := false
	for _, k := range s.keys {
		switch k {
		case "description":
		case "required", "minProperties":
			constrains = true
		case "not":
			if !constraint(s.obj("not")) {
				return false
			}
			constrains = true
		case "anyOf", "oneOf":
			for _, a := range s.list(k) {
				a, ok := a.(*object)
				if !ok || !constraint(a) {
					return false
				}
			}
			constrains = true
		case "properties":
			props := s.obj("properties")
			for _, k := range props.keys {
				if !constraint(props.obj(k)) {
					return false
				}
			}
			constrains = true
		default:
			return false
		}
	}
	return constrains
}

// sentences are the rules a constraint sets, with field paths under
// prefix.
func (d *schemaDoc) sentences(s *object, prefix string) ([]string, error) {
	var out []string
	for _, k := range s.keys {
		switch k {
		case "description", "type", "properties", "additionalProperties", "unevaluatedProperties", "propertyNames":
			if k != "properties" || !constraint(s) {
				continue
			}
			props := s.obj("properties")
			for _, name := range props.keys {
				more, err := d.sentences(props.obj(name), prefix+name+".")
				if err != nil {
					return nil, err
				}
				out = append(out, more...)
			}
		case "required":
			out = append(out, "Needs "+list(codes(prefix, s.strings("required")), "and")+".")
		case "not":
			fields, err := negation(s.obj("not"), prefix)
			if err != nil {
				return nil, err
			}
			out = append(out, "Can't have "+list(fields, "or")+".")
		case "anyOf", "oneOf":
			var alts []string
			plain := true
			for _, a := range s.list(k) {
				a, _ := a.(*object)
				phrase, err := hasPhrase(a, prefix)
				if err != nil {
					return nil, err
				}
				plain = plain && !strings.Contains(phrase, " ")
				alts = append(alts, phrase)
			}
			lead := "Has at least one of "
			if k == "oneOf" {
				lead = "Has exactly one of "
			}
			if plain {
				out = append(out, lead+list(alts, "or")+".")
			} else {
				out = append(out, lead+"these: "+strings.Join(alts[:len(alts)-1], "; ")+"; or "+alts[len(alts)-1]+".")
			}
		case "minProperties":
			n, _ := s.vals[k].(json.Number)
			switch {
			case prefix != "":
				out = append(out, fmt.Sprintf("%s has at least %s.", code(strings.TrimSuffix(prefix, ".")), count(n.String(), "entry", "entries")))
			case n.String() == "1":
				out = append(out, "Has at least one of its fields.")
			default:
				out = append(out, fmt.Sprintf("Has at least %s of its fields.", n))
			}
		case "dependentRequired":
			deps := s.obj(k)
			for _, field := range deps.keys {
				out = append(out, fmt.Sprintf("%s needs %s.", code(prefix+field), list(codes(prefix, deps.strings(field)), "and")))
			}
		default:
			return nil, fmt.Errorf("a rule has %s, which refdocs doesn't render", k)
		}
	}
	return out, nil
}

// hasPhrase says what an object has when it matches a, such as "`go`" or
// "`binaries` with at least one entry".
func hasPhrase(a *object, prefix string) (string, error) {
	if a == nil {
		return "", errors.New("an alternative isn't an object")
	}
	var parts []string
	quals := map[string]string{}
	if props := a.obj("properties"); props != nil {
		for _, name := range props.keys {
			p := props.obj(name)
			for _, k := range p.keys {
				switch k {
				case "minProperties":
					n, _ := p.vals[k].(json.Number)
					quals[name] = " with at least " + count(n.String(), "entry", "entries")
				case "required":
					nested, err := hasPhrase(p, prefix+name+".")
					if err != nil {
						return "", err
					}
					parts = append(parts, nested)
				default:
					return "", fmt.Errorf("a condition on %s has %s, which refdocs doesn't render", name, k)
				}
			}
		}
	}
	for _, k := range a.keys {
		if k != "required" && k != "properties" {
			return "", fmt.Errorf("a condition has %s, which refdocs doesn't render", k)
		}
	}
	var fields []string
	for _, f := range a.strings("required") {
		fields = append(fields, code(prefix+f)+quals[f])
	}
	fields = append(fields, parts...)
	if len(fields) == 0 {
		return "", errors.New("a condition requires nothing")
	}
	return list(fields, "and"), nil
}

// negation lists the fields an object can't have, from a not.
func negation(n *object, prefix string) ([]string, error) {
	switch {
	case n == nil:
		return nil, errors.New("a not isn't an object")
	case len(n.keys) == 1 && len(n.strings("required")) == 1:
		return codes(prefix, n.strings("required")), nil
	case len(n.keys) == 1 && n.has("anyOf"):
		var out []string
		for _, a := range n.list("anyOf") {
			a, _ := a.(*object)
			more, err := negation(a, prefix)
			if err != nil {
				return nil, err
			}
			out = append(out, more...)
		}
		return out, nil
	case len(n.keys) == 1 && n.has("properties"):
		props := n.obj("properties")
		var out []string
		for _, name := range props.keys {
			p := props.obj(name)
			if len(p.keys) != 1 || len(p.strings("required")) != 1 {
				return nil, fmt.Errorf("a not on %s says more than one field", name)
			}
			out = append(out, codes(prefix+name+".", p.strings("required"))...)
		}
		return out, nil
	}
	return nil, errors.New("a not says more than which fields an object can't have")
}

func closed(s *object) bool {
	return s.vals["additionalProperties"] == false || s.vals["unevaluatedProperties"] == false
}

func refName(p *object) string { return strings.TrimPrefix(p.str("$ref"), "#/$defs/") }

func jsonType(v any) string {
	switch v.(type) {
	case string:
		return "string"
	case bool:
		return "boolean"
	case json.Number:
		return "number"
	}
	return "null"
}

func literal(v any) string {
	switch v := v.(type) {
	case string:
		return code(v)
	case json.Number:
		return code(v.String())
	case bool:
		return code(strconv.FormatBool(v))
	}
	return code("null")
}

func join(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}

func at(path string) string {
	if path == "" {
		return "the top level"
	}
	return path
}

func codes(prefix string, names []string) []string {
	var out []string
	for _, n := range names {
		out = append(out, code(prefix+n))
	}
	return out
}

// count is "one entry" or "3 entries".
func count(n, one, many string) string {
	if n == "1" {
		return "one " + one
	}
	return n + " " + many
}

// alternatives reports whether p's anyOf or oneOf are the types it can
// take, rather than rules on which of its fields it has.
func alternatives(p *object, k string) bool {
	if !p.has(k) {
		return false
	}
	for _, a := range p.list(k) {
		if a, ok := a.(*object); !ok || !constraint(a) {
			return true
		}
	}
	return false
}

// sentence makes a phrase a sentence of its own.
func sentence(phrase string) string {
	return strings.ToUpper(phrase[:1]) + phrase[1:] + "."
}
