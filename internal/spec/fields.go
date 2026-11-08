package spec

import (
	"reflect"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"
)

// FieldPaths lists the fields a kind's spec takes, objects and lists of
// objects included, as dotted paths with [] after a list of objects and
// NAME for a map's keys: source.streams[].columns.NAME.type. A field whose
// value isn't read field by field here, such as a connector's config or a
// size, ends its path. Nil for kinds other than Source, Destination and
// Pipeline.
func FieldPaths(kind string) []string {
	var t reflect.Type
	switch kind {
	case KindPipeline:
		t = reflect.TypeFor[pipelineYAML]()
	case KindSource, KindDestination:
		t = reflect.TypeFor[connectorYAML]()
	default:
		return nil
	}
	var paths []string
	fieldPaths(t, "", &paths)
	slices.Sort(paths)
	return paths
}

func fieldPaths(t reflect.Type, prefix string, paths *[]string) {
	for i := range t.NumField() {
		f := t.Field(i)
		name, opts, _ := strings.Cut(f.Tag.Get("yaml"), ",")
		if opts == "inline" {
			fieldPaths(f.Type, prefix, paths)
			continue
		}
		path := prefix + name
		*paths = append(*paths, path)
		switch ft := deref(f.Type); {
		case object(ft):
			fieldPaths(ft, path+".", paths)
		case ft.Kind() == reflect.Slice && object(deref(ft.Elem())):
			fieldPaths(deref(ft.Elem()), path+"[].", paths)
		case ft.Kind() == reflect.Map && object(deref(ft.Elem())):
			*paths = append(*paths, path+".NAME")
			fieldPaths(deref(ft.Elem()), path+".NAME.", paths)
		}
	}
}

func deref(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

// object is a struct read field by field; a yaml.Node is read by hand.
func object(t reflect.Type) bool {
	return t.Kind() == reflect.Struct && t != reflect.TypeFor[yaml.Node]()
}
