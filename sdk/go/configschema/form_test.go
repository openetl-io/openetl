package configschema

import (
	"encoding/json"
	"strings"
	"testing"
)

const formSchema = `{
  "type": "object",
  "x-groups": [{"id": "connection", "title": "Connection"}, {"id": "reading", "title": "Reading"}],
  "required": ["region"],
  "properties": {
    "region": {"type": "string", "description": "The region.", "x-group": "connection", "examples": ["eu-west-1"]},
    "secretAccessKey": {"type": "string", "x-secret": true, "description": "The key.", "x-group": "connection"},
    "startPosition": {
      "type": "string", "default": "earliest", "x-group": "reading", "description": "Where to start.",
      "oneOf": [
        {"const": "earliest", "title": "Earliest", "description": "The oldest record."},
        {"const": "timestamp", "title": "At a time"}
      ]
    },
    "startTimestamp": {"type": "string", "format": "date-time", "x-group": "reading", "x-when": {"startPosition": "timestamp"}, "description": "When."},
    "consumer": {
      "type": "object", "x-group": "reading", "description": "How shards are read.",
      "properties": {
        "type": {"type": "string", "enum": ["polling", "efo"], "default": "polling", "description": "Polling or fan-out."},
        "interval": {"type": "string", "x-format": "duration", "x-advanced": true, "x-when": {"consumer.type": ["polling"]}, "minimum": 0, "description": "Between polls."}
      }
    },
    "aNameOnly": {"type": ["null", "integer"], "maximum": 10}
  }
}`

func TestReadForm(t *testing.T) {
	f, err := ReadForm([]byte(formSchema))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Groups) != 2 || f.Groups[1].ID != "reading" {
		t.Errorf("groups: %+v", f.Groups)
	}
	var names []string
	for _, x := range f.Fields {
		names = append(names, x.Name)
	}
	if got := strings.Join(names, ","); got != "region,secretAccessKey,startPosition,startTimestamp,consumer,aNameOnly" {
		t.Errorf("fields in %s, want the schema's order", got)
	}
	region, secret, start, ts, consumer, last := f.Fields[0], f.Fields[1], f.Fields[2], f.Fields[3], f.Fields[4], f.Fields[5]
	if !region.Required || region.Group != "connection" || string(region.Examples[0]) != `"eu-west-1"` {
		t.Errorf("region: %+v", region)
	}
	if !secret.Secret {
		t.Errorf("secretAccessKey isn't a secret")
	}
	if len(start.Options) != 2 || start.Options[1].Title != "At a time" || string(start.Default) != `"earliest"` {
		t.Errorf("startPosition: %+v", start)
	}
	if ts.Format != "date-time" || len(ts.When) != 1 || ts.When[0].Field != "startPosition" || string(ts.When[0].Values[0]) != `"timestamp"` {
		t.Errorf("startTimestamp: %+v", ts)
	}
	if consumer.Type != "object" || len(consumer.Fields) != 2 {
		t.Fatalf("consumer: %+v", consumer)
	}
	iv := consumer.Fields[1]
	if iv.Path != "consumer.interval" || iv.Group != "reading" || !iv.Advanced || iv.Format != "duration" || iv.When[0].Field != "consumer.type" {
		t.Errorf("consumer.interval: %+v", iv)
	}
	if last.Type != "integer" || last.Maximum == nil || *last.Maximum != 10 {
		t.Errorf("aNameOnly: %+v", last)
	}
	if u := Undescribed(f); len(u) != 1 || u[0] != "aNameOnly" {
		t.Errorf("Undescribed: %v", u)
	}
	if p := Lint([]byte(formSchema)); len(p) != 0 {
		t.Errorf("Lint: %v", p)
	}
}

func TestLint(t *testing.T) {
	var s map[string]any
	if err := json.Unmarshal([]byte(formSchema), &s); err != nil {
		t.Fatal(err)
	}
	props := s["properties"].(map[string]any)
	props["region"].(map[string]any)["x-group"] = "nowhere"
	props["startTimestamp"].(map[string]any)["x-when"] = map[string]any{"startPosition": "latest", "missing": true}
	props["startPosition"].(map[string]any)["default"] = "never"
	props["secretAccessKey"].(map[string]any)["x-format"] = "colour"
	b, _ := json.Marshal(s)
	var got []string
	for _, p := range Lint(b) {
		got = append(got, p.Error())
	}
	for _, want := range []string{
		"region: x-group nowhere",
		`startTimestamp: x-when wants startPosition to be "latest"`,
		"startTimestamp: x-when names missing",
		`startPosition: the default "never"`,
		"secretAccessKey: x-format colour",
	} {
		found := false
		for _, g := range got {
			found = found || strings.Contains(g, want)
		}
		if !found {
			t.Errorf("no problem %q in %q", want, got)
		}
	}
}

// Valid schemas that a form reads without a problem: a oneOf whose branches
// aren't all consts, boolean subschemas, and an x-when on null.
func TestLintValidSchemas(t *testing.T) {
	for name, schema := range map[string]string{
		"a oneOf of a const and a type": `{"properties": {
			"size": {"default": 100, "oneOf": [{"const": "auto"}, {"type": "integer", "minimum": 1}]},
			"batch": {"type": "integer", "x-when": {"size": 5}}}}`,
		"boolean subschemas": `{"properties": {"extra": false, "any": true, "tags": {"type": "array", "items": false}}}`,
		"an x-when on null":  `{"properties": {"mode": {"type": ["string", "null"], "enum": ["a", null]}, "b": {"type": "string", "x-when": {"mode": null}}}}`,
	} {
		if p := Lint([]byte(schema)); len(p) != 0 {
			t.Errorf("%s: %v", name, p)
		}
	}
	f, err := ReadForm([]byte(`{"properties": {
		"size": {"default": 100, "oneOf": [{"const": "auto"}, {"type": "integer"}]},
		"extra": false, "any": true, "tags": {"type": "array", "items": false},
		"b": {"x-when": {"size": null, "any": [null, 1]}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, x := range f.Fields {
		names = append(names, x.Name)
	}
	if got := strings.Join(names, ","); got != "size,any,tags,b" {
		t.Errorf("fields %s, want every property but the one that can't be set", got)
	}
	if size := f.Fields[0]; len(size.Options) != 0 {
		t.Errorf("size takes any integer, but has the options %v", size.Options)
	}
	// By field: any, then size.
	b := f.Fields[3]
	if len(b.When) != 2 || len(b.When[0].Values) != 2 || string(b.When[0].Values[0]) != "null" ||
		len(b.When[1].Values) != 1 || string(b.When[1].Values[0]) != "null" {
		t.Errorf("x-when on null: %+v", b.When)
	}
}
