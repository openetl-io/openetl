package plan

import (
	"encoding/json"
	"maps"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
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
	// Each Flink cluster's version, by name.
	FlinkClusters map[string]string
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
	flatten("", spec, v.Spec.ProtoReflect().Descriptor(), nil, out)
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
	for name, version := range v.FlinkClusters {
		pins["flinkClusters."+name] = version
	}
	for k, p := range pins {
		if p != "" {
			out[k] = jsonString(p)
		}
	}
	return out, nil
}

// flatten adds v's leaves to out by path. md is the message v is, and fd the
// field v is a value or an item of, so that enums are in the spec's words;
// either is nil where it's unknown, as inside a google.protobuf.Struct.
func flatten(path string, v any, md protoreflect.MessageDescriptor, fd protoreflect.FieldDescriptor, out map[string]string) {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			var xfd protoreflect.FieldDescriptor
			switch {
			case fd != nil && fd.IsMap():
				xfd = fd.MapValue()
			case md != nil:
				xfd = md.Fields().ByJSONName(k)
			}
			var xmd protoreflect.MessageDescriptor
			if xfd != nil && !xfd.IsMap() {
				xmd = xfd.Message()
			}
			if path != "" {
				k = path + "." + k
			}
			flatten(k, x, xmd, xfd, out)
		}
	case []any:
		for i, x := range t {
			key := strconv.Itoa(i)
			if m, ok := x.(map[string]any); ok {
				if n, ok := m["name"].(string); ok {
					key = n
				}
			}
			flatten(path+"["+key+"]", x, md, fd, out)
		}
	default:
		if s, ok := t.(string); ok && fd != nil && fd.Kind() == protoreflect.EnumKind {
			t = enumWord(fd.Enum(), s)
		}
		b, _ := json.Marshal(t)
		out[path] = string(b)
	}
}

// enumWord is how the spec writes an enum's value: DELIVERY_LEVEL_EXACTLY_ONCE
// is exactly_once.
func enumWord(e protoreflect.EnumDescriptor, name string) string {
	zero := e.Values().ByNumber(0)
	if zero == nil {
		return name
	}
	prefix, ok := strings.CutSuffix(string(zero.Name()), "UNSPECIFIED")
	if !ok || !strings.HasPrefix(name, prefix) {
		return name
	}
	return strings.ToLower(strings.TrimPrefix(name, prefix))
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
