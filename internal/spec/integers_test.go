package spec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"
)

// integerFields are the integer fields of the documents Parse reads, each in
// a document with the value at VALUE, and its range.
var integerFields = []struct {
	path, doc string
	lo, hi    string
	get       func(*Document) uint64
}{
	{
		path: "resources.workers", lo: "1", hi: "64",
		doc: "apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\nspec:\n  schedule: {every: 1h}\n  source: {ref: s}\n  destination: {ref: d}\n  resources: {workers: VALUE}\n",
		get: func(d *Document) uint64 { return uint64(d.Pipeline.GetResources().GetWorkers()) },
	},
	{
		path: "transform.flink.parallelism", lo: "0", hi: "2147483647",
		doc: "apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\nspec:\n  mode: continuous\n  source: {ref: s, streams: [{name: payments}]}\n  destination: {ref: d}\n" +
			"  transform:\n    flink: {cluster: fl, code: {jar: 's3://j/a.jar', sha256: " + strings.Repeat("cd", 32) + "}, parallelism: VALUE}\n",
		get: func(d *Document) uint64 { return uint64(d.Pipeline.GetTransform().GetFlink().GetParallelism()) },
	},
	{
		path: "roles.source.state.version", lo: "1", hi: "4294967295",
		doc: only("    source:\n      requires: {protocol: v1}\n      runtime: {binaries: {linux/amd64: {url: https://example.com/x}}}\n      state: {version: VALUE, reads: 1}\n      egress: {tier: raw}\n      config: {}\n"),
		get: func(d *Document) uint64 { return uint64(d.Descriptor.Roles[RoleSource].State.Version) },
	},
	{
		path: "roles.source.state.reads", lo: "1", hi: "4294967295",
		doc: only("    source:\n      requires: {protocol: v1}\n      runtime: {binaries: {linux/amd64: {url: https://example.com/x}}}\n      state: {version: 4294967295, reads: VALUE}\n      egress: {tier: raw}\n      config: {}\n"),
		get: func(d *Document) uint64 { return uint64(d.Descriptor.Roles[RoleSource].State.Reads) },
	},
	{
		path: "roles.destination.capabilities.maxIdentifierLength", lo: "0", hi: "4294967295",
		doc: only("    destination:\n      requires: {protocol: v1}\n      runtime: {binaries: {linux/amd64: {url: https://example.com/x}}}\n      capabilities: {class: deliver_and_ack, maxIdentifierLength: VALUE}\n      egress: {tier: raw}\n      config: {}\n"),
		get: func(d *Document) uint64 {
			return uint64(d.Descriptor.Roles[RoleDestination].Spec.GetDestination().GetMaxIdentifierLength())
		},
	},
	{
		path: "roles.destination.capabilities.maxWriterLanes", lo: "0", hi: "4294967295",
		doc: only("    destination:\n      requires: {protocol: v1}\n      runtime: {binaries: {linux/amd64: {url: https://example.com/x}}}\n      capabilities: {class: deliver_and_ack, maxWriterLanes: VALUE}\n      egress: {tier: raw}\n      config: {}\n"),
		get: func(d *Document) uint64 {
			return uint64(d.Descriptor.Roles[RoleDestination].Spec.GetDestination().GetMaxWriterLanes())
		},
	},
}

// An integer field takes a whole number in its range, 1.0 and 1e1 included,
// and refuses a fraction, a number below or above its range and what isn't
// a number, naming the field and the value, where yaml.v3 alone would
// truncate 1.9 to 1.
func TestIntegerFields(t *testing.T) {
	for _, f := range integerFields {
		refused := func(value string) {
			t.Helper()
			_, err := Parse([]byte(strings.Replace(f.doc, "VALUE", value, 1)))
			want := f.path + ": " + strconv.Quote(strings.Trim(value, `"`)) + " isn't a whole number from " + f.lo + " to " + f.hi
			if strings.HasPrefix(value, "[") {
				want = f.path + ": isn't a whole number from " + f.lo + " to " + f.hi
			}
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("%s: %s: %v, want %q", f.path, value, err, want)
			}
		}
		taken := func(value string, want uint64) {
			t.Helper()
			doc, err := Parse([]byte(strings.Replace(f.doc, "VALUE", value, 1)))
			if err != nil {
				t.Errorf("%s: %s: %v", f.path, value, err)
				return
			}
			if got := f.get(doc); got != want {
				t.Errorf("%s: %s read as %d, want %d", f.path, value, got, want)
			}
		}
		taken("1.0", 1)
		taken("1e1", 10)
		taken("0x10", 16)
		hi, err := strconv.ParseUint(f.hi, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		taken(f.hi, hi)
		taken(f.hi+".0", hi)
		for _, v := range []string{"1.9", "0.5", "-1.5", "1e-1", ".inf", ".nan", "-1", "two", `"2"`, "true", "[2]", "99999999999999999999", "1" + f.hi} {
			refused(v)
		}
		refused(strconv.FormatUint(hi+1, 10))
		if f.lo != "0" {
			refused("0")
		}
	}
}

// Every integer field of a document Parse reads is in integerFields.
func TestIntegerFieldsListed(t *testing.T) {
	var found []string
	for _, ty := range []reflect.Type{
		reflect.TypeFor[pipelineYAML](), reflect.TypeFor[connectorYAML](), reflect.TypeFor[connectorSourceYAML](),
		reflect.TypeFor[dbtProjectYAML](), reflect.TypeFor[webhookYAML](), reflect.TypeFor[sparkClusterYAML](),
		reflect.TypeFor[flinkClusterYAML](), reflect.TypeFor[envelope](),
	} {
		integerPaths(ty, "", &found)
	}
	// A role's fields, and a v1alpha1 descriptor's, are tried in either role.
	var role []string
	integerPaths(reflect.TypeFor[roleYAML](), "", &role)
	integerPaths(reflect.TypeFor[v1alpha1DescriptorYAML](), "", &role)
	var listed []string
	for _, f := range integerFields {
		listed = append(listed, strings.TrimPrefix(strings.TrimPrefix(f.path, "roles.source."), "roles.destination."))
	}
	for _, p := range append(found, role...) {
		if !slices.Contains(listed, p) {
			t.Errorf("%s is an integer field that TestIntegerFields doesn't try", p)
		}
	}
}

// integerPaths lists t's integer fields, as checkIntegers names them.
func integerPaths(t reflect.Type, prefix string, out *[]string) {
	fields := map[string]reflect.StructField{}
	structFields(t, fields)
	for name, f := range fields {
		switch ft := deref(f.Type); {
		case ft == durationType:
		case ft.Kind() >= reflect.Int && ft.Kind() <= reflect.Uint64:
			*out = append(*out, prefix+name)
		case object(ft):
			integerPaths(ft, prefix+name+".", out)
		case (ft.Kind() == reflect.Slice || ft.Kind() == reflect.Map) && object(deref(ft.Elem())):
			integerPaths(deref(ft.Elem()), prefix+name+".", out)
		}
	}
}

// The published descriptor schema takes and refuses the same integers as
// Parse.
func TestDescriptorSchemaIntegers(t *testing.T) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(DescriptorSchema()))
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource("connector.schema.json", doc); err != nil {
		t.Fatal(err)
	}
	sch, err := c.Compile("connector.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range integerFields {
		if !strings.HasPrefix(f.path, "roles.") {
			continue
		}
		for _, v := range []string{"1.0", "1e1", "1.9", "-1", "4294967296"} {
			y := strings.Replace(f.doc, "VALUE", v, 1)
			var a any
			if err := yaml.Unmarshal([]byte(y), &a); err != nil {
				t.Fatal(err)
			}
			b, err := json.Marshal(a)
			if err != nil {
				t.Fatal(err)
			}
			inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
			if err != nil {
				t.Fatal(err)
			}
			_, parseErr := Parse([]byte(y))
			if schemaErr := sch.Validate(inst); (schemaErr == nil) != (parseErr == nil) {
				t.Errorf("%s: %s: the schema says %v, and Parse %v", f.path, v, schemaErr, parseErr)
			}
		}
	}
}

// A value reached through an alias or a merge key is checked; a mapping
// that merges itself doesn't loop, and a null is unset.
func TestIntegerAliases(t *testing.T) {
	header := "apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\nspec:\n  schedule: {every: 1h}\n  source: {ref: s}\n  destination: {ref: d}\n"
	for name, doc := range map[string]string{
		"alias": header + "  heal: {mode: &w 1.9}\n  resources: {workers: *w}\n",
		"merge": header + "  resources: {<<: [{pool: a}, {workers: 1.9}]}\n",
	} {
		if _, err := Parse([]byte(doc)); err == nil || !strings.Contains(err.Error(), `resources.workers: "1.9" isn't`) {
			t.Errorf("%s: %v, want workers 1.9 refused", name, err)
		}
	}
	done := make(chan error, 1)
	go func() {
		_, err := Parse([]byte(header + "  resources: &r {workers: 2, <<: *r}\n"))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Error("a mapping that merges itself parsed")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a mapping that merges itself loops")
	}
	doc, err := Parse([]byte(header + "  resources: {workers: null}\n"))
	if err != nil || doc.Pipeline.GetResources().GetWorkers() != 0 {
		t.Errorf("workers null: %v, %v", doc, err)
	}
}

// A merge key in a map, such as a descriptor's roles, is checked too.
func TestIntegerMapMerge(t *testing.T) {
	doc := only("    <<: {source: {requires: {protocol: v1}, runtime: {binaries: {linux/amd64: {url: https://example.com/x}}}, state: {version: 1.9, reads: 1}, egress: {tier: raw}, config: {}}}\n")
	if _, err := Parse([]byte(doc)); err == nil || !strings.Contains(err.Error(), `roles.source.state.version: "1.9" isn't`) {
		t.Errorf("a merged role's version 1.9: %v, want it refused", err)
	}
}

// Only the values yaml.v3 decodes are checked: a key of the mapping itself
// wins over a merged one, and an earlier mapping of a merge list over a
// later one, so a value they hide isn't refused.
func TestIntegerMergePrecedence(t *testing.T) {
	header := "apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\nspec:\n  schedule: {every: 1h}\n  source: {ref: s}\n  destination: {ref: d}\n"
	for resources, want := range map[string]uint32{
		"{<<: {workers: 65}, workers: 4}":                  4,
		"{workers: 4, <<: {workers: 1.9}}":                 4,
		"{<<: [{workers: 4}, {workers: 65}]}":              4,
		"{<<: [{pool: a}, {<<: {workers: 3}}]}":            3,
		"{<<: {workers: 5, <<: {workers: 1.9}}}":           5,
		"{<<: [{workers: 6, <<: {pool: 1.9}}, {pool: b}]}": 6,
	} {
		doc, err := Parse([]byte(header + "  resources: " + resources + "\n"))
		if err != nil {
			t.Errorf("%s: %v", resources, err)
			continue
		}
		if got := doc.Pipeline.GetResources().GetWorkers(); got != want {
			t.Errorf("%s: workers %d, want %d", resources, got, want)
		}
	}
	for _, resources := range []string{"{<<: [{workers: 65}, {workers: 4}]}", "{<<: [{pool: a}, {workers: 1.9}], pool: b}"} {
		if _, err := Parse([]byte(header + "  resources: " + resources + "\n")); err == nil || !strings.Contains(err.Error(), "resources.workers: ") {
			t.Errorf("%s: %v, want workers refused", resources, err)
		}
	}
}

// Whether a number is whole, and in range, is decided from its text, so a
// fraction float64 rounds away is still refused.
func TestIntegerText(t *testing.T) {
	header := "apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\nspec:\n  schedule: {every: 1h}\n  source: {ref: s}\n  destination: {ref: d}\n"
	for v, want := range map[string]uint32{"1.0": 1, "1e1": 10, "0x10": 16, "10.0e-1": 1, "+2.": 2, "0.064e3": 64, "1_0": 10, "!!float 3": 3, "64.000000000000000000000": 64} {
		doc, err := Parse([]byte(header + "  resources: {workers: " + v + "}\n"))
		if err != nil {
			t.Errorf("%s: %v", v, err)
			continue
		}
		if got := doc.Pipeline.GetResources().GetWorkers(); got != want {
			t.Errorf("%s: workers %d, want %d", v, got, want)
		}
	}
	for _, v := range []string{"1.0000000000000001", "64.00000000000001", "4.0000000000000000001", "1e-400", "0.1e1000000000000000000", "1e1000000000000000000", "64.1e0", "-0.5e1"} {
		if _, err := Parse([]byte(header + "  resources: {workers: " + v + "}\n")); err == nil || !strings.Contains(err.Error(), `resources.workers: "`) {
			t.Errorf("%s: %v, want it refused", v, err)
		}
	}
	for _, v := range []string{"0e1000000000000000000", "0.0"} {
		if _, err := Parse([]byte(header + "  transform: {flink: {cluster: fl, code: {jar: 's3://j/a.jar', sha256: " + strings.Repeat("cd", 32) + "}, parallelism: " + v + "}}\n")); err != nil && strings.Contains(err.Error(), "parallelism") {
			t.Errorf("parallelism %s: %v, want 0 taken", v, err)
		}
	}
}

// A key is read as yaml.v3 reads it, through an alias or as !!binary, so
// the value of a key that names an integer field is checked.
func TestIntegerKeys(t *testing.T) {
	header := "apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\nspec:\n  schedule: {every: 1h}\n  destination: {ref: d}\n"
	for name, spec := range map[string]string{
		"alias":           "  source: {ref: &k workers}\n  resources: {*k : 1.9}\n",
		"binary":          "  source: {ref: s}\n  resources: {!!binary d29ya2Vycw==: 1.9}\n",
		"merged alias":    "  source: {ref: &k workers}\n  resources: {<<: {*k : 1.9}}\n",
		"non-specific <<": "  source: {ref: s}\n  resources: {! <<: {workers: 1.9}}\n",
	} {
		if _, err := Parse([]byte(header + spec)); err == nil || !strings.Contains(err.Error(), `resources.workers: "1.9" isn't a whole number`) {
			t.Errorf("%s: %v, want 1.9 refused", name, err)
		}
	}
	// A key of the mapping wins over a merged one under the identity
	// yaml.v3 gives each.
	doc, err := Parse([]byte(header + "  source: {ref: &k workers}\n  resources: {*k : 4, <<: {workers: 65}}\n"))
	if err != nil || doc.Pipeline.GetResources().GetWorkers() != 4 {
		t.Errorf("an alias key over a merged one: %v, %v", doc, err)
	}
}

// Extreme numbers neither panic nor run long, and are refused or read as
// their digits say.
func TestIntegerExtremes(t *testing.T) {
	header := "apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\nspec:\n  schedule: {every: 1h}\n  source: {ref: s}\n  destination: {ref: d}\n"
	long := strings.Repeat("9", 100000)
	for _, v := range []string{
		"!!float 1.1e-9223372036854775808", "!!float 1.1e9223372036854775807", "!!float 1e-9223372036854775809",
		"!!float 1e99999999999999999999999", "1.1e-9223372036854775808", "0.1e9223372036854775807",
		"1." + long, long, "-" + long, "1e" + long, "1e-" + long, "+1_0.5", "!!int 2.0", "! 1.9", "!!str 4", "!!float ._5e1",
	} {
		done := make(chan error, 1)
		go func() {
			_, err := Parse([]byte(header + "  resources: {workers: " + v + "}\n"))
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "resources.workers: ") {
				t.Errorf("%.40s: %v, want it refused", v, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%.40s runs long", v)
		}
	}
	for v, want := range map[string]string{
		"0e-9223372036854775808": "0", "-0.000e9223372036854775807": "0", "0." + strings.Repeat("0", 1000) + "e9223372036854775807": "0",
		"0." + strings.Repeat("0", 1000) + "4e1004": "4000", "4" + strings.Repeat("0", 1000) + "e-1000": "4",
		"-12_3.4_5e2": "-12345", "0x_1F": "31", "-0b101": "-5", "0o17": "15", ".5_0e1": "5", "0o+5": "5", "0b-101": "-5", "-0o-1": "", "1" + strings.Repeat("0", 40): "10000000000000000000000000000000000000000",
		// yaml.v3 reads these as strings: ParseFloat refuses them.
		"1e9223372036854775807": "", "1e400": "", ".1e400": "", "._5e1": "",
	} {
		x, ok := number(v, "!!float")
		if ok != (want != "") || ok && x.String() != want {
			t.Errorf("number(%.40s) = %v, %v, want %q", v, x, ok, want)
		}
	}
}

// FuzzNumber checks number against yaml.v3, which decides whether the text
// is a number, and big.Rat, which gives its value; it never panics.
func FuzzNumber(f *testing.F) {
	for _, s := range []string{"1", "1.0", "1e1", "1.9", "-1", "0x10", "1_0", ".5", "5.", "+.5e1", "1.1e-9223372036854775808",
		"1e9223372036854775807", "-1e-9223372036854775808", "0.1e2", "-0e-5", "123456789012345678901234567890", "1.0000000000000001",
		"9e40", "1e-40", ".5_0e1", "._5e1", "0b101", "0o17", "010", "09", "1e400", "10000000000000000000", "_00", "0o+0", "0b-101", "-0o17", "0b1_0", "0o777777777777777777777", "1" + strings.Repeat("0", 101) + "e-101"} {
		f.Add(s)
	}
	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(40), nil)
	f.Fuzz(func(t *testing.T, s string) {
		if s == "" || len(s) > 1000 || strings.Trim(s, "0123456789abcdefABCDEFxXoO_.+-") != "" {
			return
		}
		var y struct {
			V any `yaml:"v"`
		}
		xi, ok := number(s, "!!int")
		if err := yaml.Unmarshal([]byte("v: !!int "+s+"\n"), &y); (err == nil) != ok || ok && fmt.Sprint(y.V) != xi.String() {
			t.Errorf("number(%q) as an !!int = %v, %v, and yaml.v3 reads %#v, %v", s, xi, ok, y.V, err)
		}
		y.V = nil
		x, ok := number(s, "!!float")
		if err := yaml.Unmarshal([]byte("v: !!float "+s+"\n"), &y); err != nil {
			if ok {
				t.Errorf("number(%q) = %v, and yaml.v3 refuses it: %v", s, x, err)
			}
			return
		}
		fv, isFloat := y.V.(float64)
		if !isFloat {
			t.Fatalf("yaml.v3 reads !!float %q as %#v", s, y.V)
		}
		plain := strings.ReplaceAll(s, "_", "")
		_, intErr := strconv.ParseInt(plain, 0, 64)
		if intErr == nil || !yamlFloat.MatchString(plain) {
			// An integer's text: yaml.v3's float is exact below 2^53.
			if !ok || math.Abs(fv) < 1<<53 && x.Cmp(big.NewInt(int64(fv))) != 0 {
				t.Errorf("number(%q) = %v, %v, want %v", s, x, ok, fv)
			}
			return
		}
		// big.Rat builds 10^e, cheap up to a few thousand. Beyond the
		// text's length plus 64, a nonzero value is above 10^64 or below 1
		// whatever its digits, so only that is checked.
		if _, exp, _ := strings.Cut(strings.ToLower(plain), "e"); exp != "" {
			if e, err := strconv.Atoi(exp); err != nil || e > len(plain)+64 || e < -len(plain)-64 {
				if ok && x.Sign() != 0 && new(big.Int).Abs(x).Cmp(limit) < 0 {
					t.Errorf("number(%q) = %v, want 0, a fraction or at least 10^40", s, x)
				}
				return
			}
		}
		r, rok := new(big.Rat).SetString(plain)
		if !rok {
			t.Fatalf("big.Rat can't read %q", plain)
		}
		switch {
		case !r.IsInt():
			if ok {
				t.Errorf("number(%q) = %v, want a fraction", s, x)
			}
		case !ok:
			t.Errorf("number(%q) refused, want %v", s, r.Num())
		case new(big.Int).Abs(r.Num()).Cmp(limit) < 0:
			if x.Cmp(r.Num()) != 0 {
				t.Errorf("number(%q) = %v, want %v", s, x, r.Num())
			}
		case new(big.Int).Abs(x).Cmp(limit) < 0 || x.Sign() != r.Num().Sign():
			t.Errorf("number(%q) = %v, want ±10^40 or beyond", s, x)
		}
	})
}

// Underscores in a float: the check takes what yaml.v3 decodes, into a
// uint32 and into any, and refuses what it doesn't, as for a float that
// starts with a dot, which strconv.ParseFloat reads underscores and all.
func TestIntegerUnderscores(t *testing.T) {
	type doc struct {
		V uint32 `yaml:"v"`
	}
	for text, want := range map[string]struct {
		decodes bool
		v       uint32
		asAny   any
	}{
		".5_0e1":         {true, 5, 5.0},
		"0.5_0e1":        {true, 5, 5.0},
		"5_0":            {true, 50, 50},
		"!!float .5_0e1": {true, 5, nil},
		"._5e1":          {false, 0, "._5e1"},
		".5__0e1":        {false, 0, ".5__0e1"},
		"!!float ._5e1":  {false, 0, nil},
	} {
		content := []byte("v: " + text + "\n")
		var d doc
		err := yaml.Unmarshal(content, &d)
		if (err == nil) != want.decodes || d.V != want.v {
			t.Errorf("%s: yaml.v3 decodes it into a uint32 as %d, %v", text, d.V, err)
		}
		var a map[string]any
		if err := yaml.Unmarshal(content, &a); want.asAny != nil && (err != nil || a["v"] != want.asAny) {
			t.Errorf("%s: yaml.v3 decodes it into any as %#v, %v", text, a["v"], err)
		}
		if err := CheckIntegers(content, &d); (err == nil) != want.decodes {
			t.Errorf("%s: the check says %v, and yaml.v3 decodes it: %v", text, err, want.decodes)
		}
	}
}
