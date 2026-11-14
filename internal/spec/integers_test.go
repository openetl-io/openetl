package spec

import (
	"bytes"
	"encoding/json"
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
