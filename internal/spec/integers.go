package spec

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"slices"
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
		if n.Kind != yaml.MappingNode {
			return nil
		}
		for i := 0; i+1 < len(n.Content); i += 2 {
			if err := c.check(n.Content[i+1], t.Elem(), join(path, n.Content[i].Value), bounds{}); err != nil {
				return err
			}
		}
		return nil
	}
	if n.Kind != yaml.MappingNode {
		return nil
	}
	fields := map[string]reflect.StructField{}
	structFields(t, fields)
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		if k.ShortTag() == "!!merge" {
			if err := c.merge(v, t, path); err != nil {
				return err
			}
			continue
		}
		f, ok := fields[k.Value]
		if !ok {
			continue
		}
		fb, err := tagBounds(f)
		if err != nil {
			return err
		}
		if err := c.check(v, f.Type, join(path, k.Value), fb); err != nil {
			return err
		}
	}
	return nil
}

// merge checks a merge key's value, a mapping or a list of them, as part of
// the mapping that holds it.
func (c *integerCheck) merge(v *yaml.Node, t reflect.Type, path string) error {
	if v.Kind == yaml.AliasNode && v.Alias != nil {
		v = v.Alias
	}
	if v.Kind != yaml.SequenceNode {
		return c.check(v, t, path, bounds{})
	}
	for _, e := range v.Content {
		if err := c.check(e, t, path, bounds{}); err != nil {
			return err
		}
	}
	return nil
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
	var v any
	if err := n.Decode(&v); err != nil {
		return refuse()
	}
	var x *big.Int
	switch v := v.(type) {
	case int:
		x = big.NewInt(int64(v))
	case int64:
		x = big.NewInt(v)
	case uint64:
		x = new(big.Int).SetUint64(v)
	case float64:
		if math.IsInf(v, 0) || math.IsNaN(v) || v != math.Trunc(v) {
			return refuse()
		}
		x, _ = big.NewFloat(v).Int(nil)
	default:
		return refuse()
	}
	if x.Cmp(lo) < 0 || x.Cmp(hi) > 0 {
		return refuse()
	}
	return nil
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
