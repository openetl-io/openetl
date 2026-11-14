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
		for _, e := range entries(n, t.Key()) {
			if err := c.check(e.v, t.Elem(), join(path, fmt.Sprint(e.key)), bounds{}); err != nil {
				return err
			}
		}
		return nil
	}
	fields := map[string]reflect.StructField{}
	structFields(t, fields)
	for _, e := range entries(n, stringType) {
		name, _ := e.key.(string)
		f, ok := fields[name]
		if !ok {
			continue
		}
		fb, err := tagBounds(f)
		if err != nil {
			return err
		}
		if err := c.check(e.v, f.Type, join(path, name), fb); err != nil {
			return err
		}
	}
	return nil
}

type entry struct {
	key any
	v   *yaml.Node
}

var stringType = reflect.TypeFor[string]()

// entries are the keys and values yaml.v3 decodes of n, a mapping, with
// each key decoded as a keyType, as yaml.v3 decodes it, through an alias or
// a tag: n's own, then those of the mappings its merge key names that no
// earlier one set, as a merge list's earlier mappings win over its later
// ones. Only n's last merge key counts. A key that isn't a scalar, which
// yaml.v3 refuses, is left out.
func entries(n *yaml.Node, keyType reflect.Type) []entry {
	var out []entry
	mergeEntries(n, keyType, nil, map[*yaml.Node]bool{}, &out)
	return out
}

// mergeEntries adds n's entries whose keys set doesn't hold to out; a nil
// set takes every key of n. A mapping already seen adds nothing new.
func mergeEntries(n *yaml.Node, keyType reflect.Type, set map[string]bool, seen map[*yaml.Node]bool, out *[]entry) {
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
		if isMerge(k) {
			merge = v
			continue
		}
		key, ok := decodeKey(k, keyType)
		if !ok {
			continue
		}
		if set != nil {
			if set[keyID(key)] {
				continue
			}
			set[keyID(key)] = true
		}
		*out = append(*out, entry{key, v})
	}
	if merge == nil {
		return
	}
	if set == nil {
		// As yaml.v3 does, n's keys decoded as any.
		set = map[string]bool{}
		for i := 0; i < len(n.Content); i += 2 {
			if key, ok := decodeKey(n.Content[i], nil); ok {
				set[keyID(key)] = true
			}
		}
	}
	if merge.Kind == yaml.AliasNode && merge.Alias != nil {
		merge = merge.Alias
	}
	if merge.Kind != yaml.SequenceNode {
		mergeEntries(merge, keyType, set, seen, out)
		return
	}
	for _, m := range merge.Content {
		mergeEntries(m, keyType, set, seen, out)
	}
}

func isMerge(k *yaml.Node) bool {
	return k.Kind == yaml.ScalarNode && k.Value == "<<" && (k.Tag == "" || k.Tag == "!" || k.Tag == "!!merge")
}

// decodeKey is k decoded as a keyType, or as any if keyType is nil; false
// if k isn't a scalar or an alias of one, or doesn't decode.
func decodeKey(k *yaml.Node, keyType reflect.Type) (any, bool) {
	if k.Kind == yaml.AliasNode && k.Alias != nil && k.Alias.Kind == yaml.ScalarNode {
		k = k.Alias
	}
	if k.Kind != yaml.ScalarNode {
		return nil, false
	}
	if keyType == nil {
		var v any
		return v, k.Decode(&v) == nil
	}
	v := reflect.New(keyType)
	if k.Decode(v.Interface()) != nil {
		return nil, false
	}
	return v.Elem().Interface(), true
}

// keyID is equal for two keys yaml.v3's merge counts as the same.
func keyID(key any) string { return fmt.Sprintf("%T\x00%v", key, key) }

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
	tag := n.ShortTag()
	switch tag {
	case "!!null":
		return nil
	case "!!int", "!!float":
	default:
		return refuse()
	}
	x, ok := number(n.Value, tag)
	if !ok {
		return refuse()
	}
	if x.Cmp(lo) < 0 || x.Cmp(hi) > 0 {
		return refuse()
	}
	return nil
}

// yamlFloat is a decimal float's text, without underscores, as yaml.v3
// matches it.
var yamlFloat = regexp.MustCompile(`^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$`)

// number is s, the text of a scalar tagged tag, !!int or !!float, as an
// integer, decided from its digits rather than from a float64, which would
// round 1.0000000000000001 to 1. ok is false if it has a fraction, or if
// yaml.v3 doesn't read it as a number of that tag. A number with more digits
// than any field holds comes back as ±10^40 or beyond, out of every range.
//
// It follows yaml.v3's resolve: text that starts with a dot is a float if
// strconv.ParseFloat takes it, underscores and all. Other text that starts
// with a digit or a sign, without its underscores, is an int64, a uint64, a
// float that ParseFloat takes, or a 0b or 0o integer. A uint64 can't be a
// !!float, nor a float an !!int. Anything else is a string.
func number(s, tag string) (*big.Int, bool) {
	if s == "" || !strings.ContainsRune("0123456789+-.", rune(s[0])) {
		return nil, false
	}
	plain := strings.ReplaceAll(s, "_", "")
	if s[0] == '.' {
		if _, err := strconv.ParseFloat(s, 64); err != nil || tag != "!!float" {
			return nil, false
		}
		return decimal(plain)
	}
	if i, err := strconv.ParseInt(plain, 0, 64); err == nil {
		return big.NewInt(i), true
	}
	if u, err := strconv.ParseUint(plain, 0, 64); err == nil {
		return new(big.Int).SetUint64(u), tag == "!!int"
	}
	if yamlFloat.MatchString(plain) {
		if _, err := strconv.ParseFloat(plain, 64); err == nil {
			if tag != "!!float" {
				return nil, false
			}
			return decimal(plain)
		}
	}
	for _, b := range []struct {
		prefix string
		base   int
	}{{"0b", 2}, {"0o", 8}} {
		switch {
		case strings.HasPrefix(plain, b.prefix):
			if i, err := strconv.ParseInt(plain[2:], b.base, 64); err == nil {
				return big.NewInt(i), true
			}
			if u, err := strconv.ParseUint(plain[2:], b.base, 64); err == nil {
				return new(big.Int).SetUint64(u), tag == "!!int"
			}
		case strings.HasPrefix(plain, "-"+b.prefix):
			if i, err := strconv.ParseInt("-"+plain[3:], b.base, 64); err == nil {
				return big.NewInt(i), true
			}
		}
	}
	return nil, false
}

// decimal is s, a decimal float's text without underscores, as an integer;
// false if it has a fraction or isn't such a text.
func decimal(s string) (x *big.Int, ok bool) {
	if !yamlFloat.MatchString(s) {
		return nil, false
	}
	neg := s[0] == '-'
	mantissa, exp, _ := strings.Cut(strings.ToLower(strings.TrimLeft(s, "+-")), "e")
	whole, frac, _ := strings.Cut(mantissa, ".")
	// An exponent beyond ±(len(s)+64) leaves a nonzero number above 10^64
	// or below 1 either way, so it's held there before any arithmetic.
	limit := len(s) + 64
	e := 0
	if exp != "" {
		expNeg := exp[0] == '-'
		digits := strings.TrimLeft(strings.TrimLeft(exp, "+-"), "0")
		switch v, err := strconv.Atoi(digits); {
		case digits == "":
		case len(digits) > 9 || err != nil || v > limit:
			e = limit
		default:
			e = v
		}
		if expNeg {
			e = -e
		}
	}
	digits := strings.TrimLeft(whole+frac, "0")
	if digits == "" {
		return big.NewInt(0), true
	}
	trimmed := strings.TrimRight(digits, "0")
	scale := e - len(frac) + len(digits) - len(trimmed)
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
