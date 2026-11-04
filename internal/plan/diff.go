package plan

import (
	"encoding/json"
	"maps"
	"slices"
	"strconv"

	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
)

// Version is what a pipeline version pins.
type Version struct {
	Spec                                  *apiv1.PipelineSpec
	Source, Destination                   string
	SourceConnector, DestinationConnector string
	// Each dbt project's bundle, by name.
	Dbt map[string]string
	// Each Spark cluster's version, by name.
	SparkClusters map[string]string
}

// Diff lists the fields that differ between two versions, by name.
func Diff(before, after Version) ([]*apiv1.PlanChange, error) {
	b, err := before.fields()
	if err != nil {
		return nil, err
	}
	a, err := after.fields()
	if err != nil {
		return nil, err
	}
	fields := slices.Collect(maps.Keys(a))
	for f := range b {
		if _, ok := a[f]; !ok {
			fields = append(fields, f)
		}
	}
	slices.Sort(fields)
	var out []*apiv1.PlanChange
	for _, f := range fields {
		if b[f] != a[f] {
			out = append(out, &apiv1.PlanChange{Field: f, Before: b[f], After: a[f]})
		}
	}
	return out, nil
}

// fields flattens the spec into its leaves as JSON, keyed by path, and adds
// the pins. List items with a name are keyed by it.
func (v Version) fields() (map[string]string, error) {
	out := map[string]string{}
	j, err := protojson.Marshal(v.Spec)
	if err != nil {
		return nil, err
	}
	var spec any
	if err := json.Unmarshal(j, &spec); err != nil {
		return nil, err
	}
	flatten("", spec, out)
	pins := map[string]string{
		"source.version": v.Source, "destination.version": v.Destination,
		"source.connector": v.SourceConnector, "destination.connector": v.DestinationConnector,
	}
	for name, digest := range v.Dbt {
		pins["dbt."+name] = digest
	}
	for name, version := range v.SparkClusters {
		pins["sparkClusters."+name] = version
	}
	for k, p := range pins {
		if p != "" {
			out[k] = jsonString(p)
		}
	}
	return out, nil
}

func flatten(path string, v any, out map[string]string) {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			if path != "" {
				k = path + "." + k
			}
			flatten(k, x, out)
		}
	case []any:
		for i, x := range t {
			key := strconv.Itoa(i)
			if m, ok := x.(map[string]any); ok {
				if n, ok := m["name"].(string); ok {
					key = n
				}
			}
			flatten(path+"["+key+"]", x, out)
		}
	default:
		b, _ := json.Marshal(t)
		out[path] = string(b)
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
