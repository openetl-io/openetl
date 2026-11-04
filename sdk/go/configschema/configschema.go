// Package configschema reads what the platform needs from a connector's
// config schema: which values are secrets.
package configschema

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// SecretPaths lists the properties a config schema marks "x-secret": true,
// as dotted paths (auth.client_secret), in objects at any depth and in
// oneOf, anyOf and allOf branches. A secret inside an array, or under a
// property whose name has a dot, is an error.
func SecretPaths(schema []byte) ([]string, error) {
	if len(schema) == 0 {
		return nil, nil
	}
	var root any
	if err := json.Unmarshal(schema, &root); err != nil {
		return nil, fmt.Errorf("config schema: %w", err)
	}
	var paths []string
	if err := walk(root, "", false, &paths); err != nil {
		return nil, err
	}
	slices.Sort(paths)
	return slices.Compact(paths), nil
}

func walk(node any, path string, inArray bool, paths *[]string) error {
	n, ok := node.(map[string]any)
	if !ok {
		return nil
	}
	if s, _ := n["x-secret"].(bool); s {
		switch {
		case path == "":
			return errors.New("config schema: the config itself is marked x-secret; mark its properties")
		case inArray:
			return fmt.Errorf("config schema: %s is a secret inside an array, which isn't supported", path)
		}
		*paths = append(*paths, path)
	}
	if props, ok := n["properties"].(map[string]any); ok {
		for k, p := range props {
			child := k
			if path != "" {
				child = path + "." + k
			}
			if strings.Contains(k, ".") {
				var under []string
				if err := walk(p, child, inArray, &under); err != nil {
					return err
				}
				if len(under) > 0 {
					return fmt.Errorf("config schema: property %q has a secret, and its name has a dot", k)
				}
				continue
			}
			if err := walk(p, child, inArray, paths); err != nil {
				return err
			}
		}
	}
	for _, key := range []string{"oneOf", "anyOf", "allOf"} {
		branches, _ := n[key].([]any)
		for _, b := range branches {
			if err := walk(b, path, inArray, paths); err != nil {
				return err
			}
		}
	}
	for _, key := range []string{"items", "prefixItems", "additionalItems", "contains"} {
		switch v := n[key].(type) {
		case []any:
			for _, b := range v {
				if err := walk(b, path+"[]", true, paths); err != nil {
					return err
				}
			}
		default:
			if err := walk(v, path+"[]", true, paths); err != nil {
				return err
			}
		}
	}
	return nil
}

// Lookup returns the value at a dotted path in a decoded JSON object.
func Lookup(config map[string]any, path string) (any, bool) {
	parent, key, ok := parentOf(config, path)
	if !ok {
		return nil, false
	}
	v, ok := parent[key]
	return v, ok
}

// Replace sets the value at a dotted path that's already set, and reports
// whether it was.
func Replace(config map[string]any, path string, v any) bool {
	parent, key, ok := parentOf(config, path)
	if !ok {
		return false
	}
	if _, ok := parent[key]; !ok {
		return false
	}
	parent[key] = v
	return true
}

func parentOf(config map[string]any, path string) (map[string]any, string, bool) {
	parts := strings.Split(path, ".")
	m := config
	for _, p := range parts[:len(parts)-1] {
		next, ok := m[p].(map[string]any)
		if !ok {
			return nil, "", false
		}
		m = next
	}
	return m, parts[len(parts)-1], true
}
