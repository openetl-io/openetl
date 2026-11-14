package spec

import (
	"fmt"
	"math/big"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// CheckIntegers refuses content, a YAML or JSON document for v, if a value
// of one of v's integer fields isn't a whole number in the field's range,
// naming the field and the value: yaml.v3 would truncate 1.9 to 1. A field's
// min and max tags narrow its type's range. A whole number written with a
// fraction or an exponent, such as 2.0 or 1e1, is whole, as JSON Schema and
// protojson count it. A null is left to the decoder, which reads it as
// unset, and so is what doesn't fit v's shape.
func CheckIntegers(content []byte, v any) error {
	var n yaml.Node
	if err := yaml.Unmarshal(content, &n); err != nil {
		return err
	}
	c := integerCheck{seen: map[nodeAs]bool{}}
	return c.check(&n, reflect.TypeOf(v), "", bounds{})
}

// nodeAs is a node read as a type; an alias's node is checked once per type.
type nodeAs struct {
	n *yaml.Node
	t reflect.Type
}

type integerCheck struct{ seen map[nodeAs]bool }

// bounds are a field's min and max tags; nil: its type's.
type bounds struct{ lo, hi *big.Int }

var (
	nodeType        = reflect.TypeFor[yaml.Node]()
	durationType    = reflect.TypeFor[time.Duration]()
	unmarshalerType = reflect.TypeFor[yaml.Unmarshaler]()
)

func (c *integerCheck) check(n *yaml.Node, t reflect.Type, path string, b bounds) error {
	for n.Kind == yaml.DocumentNode && len(n.Content) == 1 {
		n = n.Content[0]
	}
	if n.Kind == yaml.AliasNode && n.Alias != nil {
		n = n.Alias
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nodeType || t == durationType || reflect.PointerTo(t).Implements(unmarshalerType) {
		return nil
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return integer(n, t, path, b)
	case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array:
	default:
		return nil
	}
	if c.seen[nodeAs{n, t}] {
		return nil
	}
	c.seen[nodeAs{n, t}] = true
	switch t.Kind() {
	case reflect.Slice, reflect.Array:
		if n.Kind != yaml.SequenceNode {
			return nil
		}
		for i, e := range n.Content {
			if err := c.check(e, t.Elem(), fmt.Sprintf("%s[%d]", path, i), bounds{}); err != nil {
				return err
			}
		}
		return nil
	case reflect.Map:
		for _, e := range entries(n) {
			if err := c.check(e.v, t.Elem(), join(path, e.k.Value), bounds{}); err != nil {
				return err
			}
		}
		return nil
	}
	fields := map[string]reflect.StructField{}
	structFields(t, fields)
	for _, e := range entries(n) {
		f, ok := fields[e.k.Value]
		if !ok {
			continue
		}
		fb, err := tagBounds(f)
		if err != nil {
			return err
		}
		if err := c.check(e.v, f.Type, join(path, e.k.Value), fb); err != nil {
			return err
		}
	}
	return nil
}

type entry struct{ k, v *yaml.Node }

// entries are the keys and values yaml.v3 decodes of n, a mapping: its own,
// then those of the mappings its merge key names that no earlier one set,
// as a merge list's earlier mappings win over its later ones. Only n's last
// merge key counts, as in yaml.v3.
func entries(n *yaml.Node) []entry {
	var out []entry
	mergeEntries(n, nil, map[*yaml.Node]bool{}, &out)
	return out
}

// mergeEntries adds n's entries that set doesn't hold to out; a nil set
// takes every key of n. A mapping already seen adds nothing new.
func mergeEntries(n *yaml.Node, set map[string]bool, seen map[*yaml.Node]bool, out *[]entry) {
	if n.Kind == yaml.AliasNode && n.Alias != nil {
		n = n.Alias
	}
	if n.Kind != yaml.MappingNode || seen[n] {
		return
	}
	seen[n] = true
	var merge *yaml.Node
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		if k.Kind == yaml.ScalarNode && k.Value == "<<" && k.ShortTag() == "!!merge" {
			merge = v
			continue
		}
		if set != nil {
			if set[k.Value] {
				continue
			}
			set[k.Value] = true
		}
		*out = append(*out, entry{k, v})
	}
	if merge == nil {
		return
	}
	if set == nil {
		set = map[string]bool{}
		for i := 0; i < len(n.Content); i += 2 {
			set[n.Content[i].Value] = true
		}
	}
	if merge.Kind == yaml.AliasNode && merge.Alias != nil {
		merge = merge.Alias
	}
	if merge.Kind != yaml.SequenceNode {
		mergeEntries(merge, set, seen, out)
		return
	}
	for _, m := range merge.Content {
		mergeEntries(m, set, seen, out)
	}
}

// structFields are the fields yaml.v3 reads of t, by key, inline ones
// included.
func structFields(t reflect.Type, out map[string]reflect.StructField) {
	for i := range t.NumField() {
		f := t.Field(i)
		tag := f.Tag.Get("yaml")
		name, opts, _ := strings.Cut(tag, ",")
		switch {
		case name == "-":
			continue
		case slices.Contains(strings.Split(opts, ","), "inline"):
			if ft := deref(f.Type); ft.Kind() == reflect.Struct {
				structFields(ft, out)
			}
			continue
		case !f.IsExported():
			continue
		case name == "":
			name = strings.ToLower(f.Name)
		}
		out[name] = f
	}
}

func tagBounds(f reflect.StructField) (bounds, error) {
	var b bounds
	for _, t := range []struct {
		tag string
		to  **big.Int
	}{{"min", &b.lo}, {"max", &b.hi}} {
		if s, ok := f.Tag.Lookup(t.tag); ok {
			v, ok := new(big.Int).SetString(s, 10)
			if !ok {
				return b, fmt.Errorf("field %s: tag %s %q isn't an integer", f.Name, t.tag, s)
			}
			*t.to = v
		}
	}
	return b, nil
}

func integer(n *yaml.Node, t reflect.Type, path string, b bounds) error {
	lo, hi := typeRange(t)
	if b.lo != nil {
		lo = b.lo
	}
	if b.hi != nil {
		hi = b.hi
	}
	refuse := func() error {
		if n.Kind != yaml.ScalarNode {
			return fmt.Errorf("%s: isn't a whole number from %s to %s", path, lo, hi)
		}
		return fmt.Errorf("%s: %q isn't a whole number from %s to %s", path, n.Value, lo, hi)
	}
	if n.Kind != yaml.ScalarNode {
		return refuse()
	}
	switch n.ShortTag() {
	case "!!null":
		return nil
	case "!!int", "!!float":
	default:
		return refuse()
	}
	x, ok := number(n.Value)
	if !ok {
		return refuse()
	}
	if x.Cmp(lo) < 0 || x.Cmp(hi) > 0 {
		return refuse()
	}
	return nil
}

// yamlFloat is the text yaml.v3 reads as a float, once it drops underscores.
var yamlFloat = regexp.MustCompile(`^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$`)

// number is s, the text of a YAML integer or float, as an integer, decided
// from its digits rather than from a float64, which would round
// 1.0000000000000001 to 1. ok is false if it has a fraction, or isn't a
// number. A number with more digits than any field holds comes back as
// ±10^40, out of every range.
func number(s string) (x *big.Int, ok bool) {
	s = strings.ReplaceAll(s, "_", "")
	if i, err := strconv.ParseInt(s, 0, 64); err == nil {
		return big.NewInt(i), true
	}
	if u, err := strconv.ParseUint(s, 0, 64); err == nil {
		return new(big.Int).SetUint64(u), true
	}
	if !yamlFloat.MatchString(s) {
		return nil, false
	}
	neg := s[0] == '-'
	s = strings.TrimLeft(s, "+-")
	mantissa, exp, _ := strings.Cut(strings.ToLower(s), "e")
	whole, frac, _ := strings.Cut(mantissa, ".")
	e := 0
	if exp != "" {
		var err error
		if e, err = strconv.Atoi(exp); err != nil {
			// Beyond an int: far beyond any field, or far below 1.
			e = 1 << 30
			if exp[0] == '-' {
				e = -e
			}
		}
	}
	digits := strings.TrimLeft(whole+frac, "0")
	if digits == "" {
		return big.NewInt(0), true
	}
	scale := e - len(frac)
	trimmed := strings.TrimRight(digits, "0")
	scale += len(digits) - len(trimmed)
	switch {
	case scale < 0:
		return nil, false
	case len(trimmed)+scale > 40:
		x = new(big.Int).Exp(big.NewInt(10), big.NewInt(40), nil)
	default:
		x, _ = new(big.Int).SetString(trimmed+strings.Repeat("0", scale), 10)
	}
	if neg {
		x.Neg(x)
	}
	return x, true
}

func typeRange(t reflect.Type) (lo, hi *big.Int) {
	bits := uint(t.Bits())
	one := big.NewInt(1)
	switch t.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return big.NewInt(0), new(big.Int).Sub(new(big.Int).Lsh(one, bits), one)
	}
	hi = new(big.Int).Sub(new(big.Int).Lsh(one, bits-1), one)
	return new(big.Int).Neg(new(big.Int).Add(hi, one)), hi
}

func join(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}
