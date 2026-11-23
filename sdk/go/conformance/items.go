package conformance

import (
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/configschema"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

var (
	itemName   = regexp.MustCompile(`^[a-z0-9]+([._-][a-z0-9]+)*$`)
	reasonName = regexp.MustCompile(`^[a-z0-9]+(_[a-z0-9]+)*$`)
	// What looks like a placeholder, though its name may not be one's.
	nearPlaceholder = regexp.MustCompile(`\{[A-Za-z0-9_.-]+\}`)
	placeholderName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

// answer is a Check, PreflightSource or PreflightDestination response.
type answer interface {
	GetError() *protocolv1.Error
	GetItems() []*protocolv1.CheckItem
}

// checkAnswer reports what resp, the connector's answer with config, breaks
// of the protocol, as checkItems does, but leaves out of the leak check the
// secrets refuseUntestable refuses. It gives config's secret values, for
// shown.
func (c *common) checkAnswer(t T, with string, config []byte, resp answer) []string {
	t.Helper()
	c.refuseUntestable(t)
	schema := c.spec.GetConfigSchema()
	var secrets, tested []string
	if _, s, err := splitSecrets(schema, config); err == nil {
		untestable := untestableSecrets(schema, config)
		for _, k := range slices.Sorted(maps.Keys(s)) {
			secrets = append(secrets, s[k])
			if _, ok := untestable[k]; !ok {
				tested = append(tested, s[k])
			}
		}
	}
	checkSecrets(t, with, resp, tested)
	checkForm(t, with, resp, secrets, fieldPaths(schema))
	return secrets
}

// minSecretLength is the fewest characters of a secret value the leak check
// tests for: a shorter one, such as "openetl", turns up in text that doesn't
// show it, such as a path or a bucket's name, and fails a connector that
// leaks nothing.
const minSecretLength = 12

// untestableSecrets gives, by dotted path, each non-empty secret of config
// that the leak check can't tell from other text, with why: it's shorter
// than minSecretLength, or one of config's other values holds it. It's nil
// when there's none, or config can't be read.
func untestableSecrets(schema, config []byte) map[string]string {
	_, secrets, err := splitSecrets(schema, config)
	if err != nil || len(secrets) == 0 {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(string(config)))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil
	}
	for k := range secrets {
		configschema.Replace(m, k, nil)
	}
	var others []string
	values(m, &others)
	var out map[string]string
	for k, v := range secrets {
		why := ""
		switch {
		case v == "":
			continue
		case utf8.RuneCountInString(v) < minSecretLength:
			why = fmt.Sprintf("is shorter than %d characters", minSecretLength)
		case slices.ContainsFunc(others, func(o string) bool { return strings.Contains(o, v) }):
			why = "appears in another of the config's values"
		default:
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
		out[k] = why
	}
	return out
}

// values appends the strings and numbers in v, a decoded JSON value, at any
// depth.
func values(v any, out *[]string) {
	switch v := v.(type) {
	case string:
		*out = append(*out, v)
	case json.Number:
		*out = append(*out, v.String())
	case map[string]any:
		for _, x := range v {
			values(x, out)
		}
	case []any:
		for _, x := range v {
			values(x, out)
		}
	}
}

// refuseUntestable fails t, once a suite, with each secret of the suite's
// configs that the leak check can't test, naming it by its path and never
// by its value.
func (c *common) refuseUntestable(t T) {
	t.Helper()
	c.mu.Lock()
	done := c.refused
	c.refused = true
	c.mu.Unlock()
	if done {
		return
	}
	for _, cfg := range c.configs() {
		untestable := untestableSecrets(c.spec.GetConfigSchema(), cfg.b)
		for _, k := range slices.Sorted(maps.Keys(untestable)) {
			t.Errorf("%s's secret config.%s %s, so the kit can't tell a leak of it from text that only happens to hold it: give it a distinctive value of at least %d characters that appears nowhere else in the config",
				cfg.name, k, untestable[k], minSecretLength)
		}
	}
}

// hidden is what a report shows in place of a connector's text that holds a
// secret's value, which checkSecrets fails.
const hidden = "(not shown: it holds the value of a secret)"

// shown is text as a report shows it: hidden if it holds one of secrets.
func shown(text string, secrets []string) string {
	if holds(text, secrets) {
		return hidden
	}
	return text
}

func holds(text string, secrets []string) bool {
	return slices.ContainsFunc(secrets, func(s string) bool { return s != "" && strings.Contains(text, s) })
}

// checkItems reports what resp breaks of the protocol; with names the
// config, such as "with bad credentials". No item string, param or error
// holds one of secrets, the config's secret values. An item's field is one of
// fields, the config schema's properties by dotted path, unless fields is nil
// for a schema that declares none. A placeholder without a param is flagged,
// not failed. The response's error is the first failed item's when one
// failed; with none failed, it's set only when the checks couldn't finish:
// they ran out of time or were cancelled, so the items they didn't reach are
// skipped, or they couldn't run at all, so there are no items.
func checkItems(t T, with string, resp answer, secrets []string, fields map[string]bool) {
	t.Helper()
	checkSecrets(t, with, resp, secrets)
	checkForm(t, with, resp, secrets, fields)
}

// checkForm is checkItems without the leak check: it shows no text that
// holds one of secrets.
func checkForm(t T, with string, resp answer, secrets []string, fields map[string]bool) {
	t.Helper()
	items := resp.GetItems()
	seen := map[string]int{}
	for i, it := range items {
		name := label(i, it, secrets)
		switch n := it.GetName(); {
		case n == "":
			t.Errorf("%s, %s has no name", with, name)
		case !itemName.MatchString(n):
			t.Errorf("%s, %s's name isn't lowercase letters and digits in words joined by \".\", \"_\" or \"-\"", with, name)
		default:
			if j, ok := seen[n]; ok {
				t.Errorf("%s, items %d and %d have the same name %q: each needs its own", with, j+1, i+1, shown(n, secrets))
			} else {
				seen[n] = i
			}
		}
		if it.GetTitle() == "" {
			t.Errorf("%s, %s has no title", with, name)
		}
		if _, ok := protocolv1.CheckKind_name[int32(it.GetKind())]; !ok {
			t.Errorf("%s, %s has the kind %d, which CheckKind doesn't have", with, name, it.GetKind())
		}
		st := it.GetStatus()
		switch st {
		case protocolv1.CheckStatus_CHECK_STATUS_UNSPECIFIED:
			t.Errorf("%s, %s has no status", with, name)
		case protocolv1.CheckStatus_CHECK_STATUS_PASSED, protocolv1.CheckStatus_CHECK_STATUS_WARNING,
			protocolv1.CheckStatus_CHECK_STATUS_FAILED, protocolv1.CheckStatus_CHECK_STATUS_SKIPPED:
		default:
			t.Errorf("%s, %s has the status %d, which CheckStatus doesn't have: a host counts it as FAILED", with, name, st)
		}
		if r := it.GetReason(); r != "" && !reasonName.MatchString(r) {
			t.Errorf("%s, %s's reason %q isn't lowercase words joined by \"_\"", with, name, shown(r, secrets))
		}
		e := it.GetError()
		switch {
		case st == protocolv1.CheckStatus_CHECK_STATUS_FAILED && e == nil:
			t.Errorf("%s, %s failed without an error", with, name)
		case (st == protocolv1.CheckStatus_CHECK_STATUS_PASSED || st == protocolv1.CheckStatus_CHECK_STATUS_SKIPPED) && e != nil:
			t.Errorf("%s, %s %s with an error: only a failed or warning item has one", with, name, statusName(st))
		}
		if e != nil {
			if e.GetCode() == protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED {
				t.Errorf("%s, %s's error has no code", with, name)
			}
			if want := check.Render(it.GetMessage(), it.GetParams()); e.GetMessage() != want {
				t.Errorf("%s, %s's error message is %q, not its message with its params filled in, %q: the detail has a field of its own", with, name, shown(e.GetMessage(), secrets), shown(want, secrets))
			}
		}
		for _, text := range []struct{ field, template string }{
			{"title", it.GetTitle()}, {"description", it.GetDescription()}, {"message", it.GetMessage()}, {"fix", it.GetFix()},
		} {
			for _, m := range nearPlaceholder.FindAllString(text.template, -1) {
				if !placeholderName.MatchString(m[1 : len(m)-1]) {
					t.Errorf("%s, %s's %s has %s, which looks like a placeholder but isn't one: a placeholder's name is lowercase letters, digits and \"_\", starting with a letter", with, name, text.field, shown(m, secrets))
				}
			}
			for _, p := range check.Placeholders(text.template) {
				if _, ok := it.GetParams()[p]; !ok {
					flagf(t, "%s, %s's %s uses {%s} without a param of that name, so it shows as written", with, name, text.field, shown(p, secrets))
				}
			}
		}
		if f := it.GetField(); f != "" && fields != nil && !fields[f] && !fields[withoutIndexes(f)] {
			t.Errorf("%s, %s's field %q isn't a property of the config schema: name one by its dotted path, such as consumer.name", with, name, shown(f, secrets))
		}
	}
	i, first := firstFailed(items)
	e := resp.GetError()
	switch {
	case first == nil && e != nil && len(items) > 0 && !slices.ContainsFunc(items, func(it *protocolv1.CheckItem) bool {
		return it.GetStatus() == protocolv1.CheckStatus_CHECK_STATUS_SKIPPED
	}):
		t.Errorf("%s, the response has an error (%s: %s) but every item passed or warned: set it when an item failed, or when the checks couldn't finish, which leaves the items they didn't reach skipped, or no items at all", with, e.GetCode(), shown(e.GetMessage(), secrets))
	case first != nil && e == nil:
		t.Errorf("%s, %s failed but the response has no error: set it to the first failed item's, which a host from before items shows", with, label(i, first, secrets))
	case first.GetStatus() == protocolv1.CheckStatus_CHECK_STATUS_FAILED:
		if e.GetCode() != first.GetError().GetCode() {
			t.Errorf("%s, the response's error is %s, not %s of %s, the first that failed", with, e.GetCode(), first.GetError().GetCode(), label(i, first, secrets))
		}
		if want := errorMessages(first); !slices.Contains(want, strings.TrimSpace(e.GetMessage())) {
			t.Errorf("%s, the response's error message %q isn't %q, the title, message and detail of %s, the first that failed: a host from before items shows it, as \"title: message: detail\"", with, shown(e.GetMessage(), secrets), shown(want[0], secrets), label(i, first, secrets))
		}
	}
}

// errorMessages are the messages the response's error may have for it, the
// first failed item: "title: message", placeholders filled in, then ": "
// and its detail when it has one, the message's final "." kept or dropped,
// as the SDK drops it.
func errorMessages(it *protocolv1.CheckItem) []string {
	m := check.Render(it.GetMessage(), it.GetParams())
	if title := check.Render(it.GetTitle(), it.GetParams()); title != "" {
		m = title + ": " + m
	}
	d := it.GetDetail()
	if d == "" {
		return []string{strings.TrimSpace(m)}
	}
	return []string{strings.TrimSpace(strings.TrimSuffix(m, ".") + ": " + d), strings.TrimSpace(m + ": " + d)}
}

// checkSecrets fails an answer that shows a secret's value, as it is or
// filled into a template. It doesn't say which value, so the log doesn't
// either.
func checkSecrets(t T, with string, resp answer, secrets []string) {
	t.Helper()
	type text struct{ where, s string }
	texts := []text{{"the response's error", resp.GetError().GetMessage()}}
	for i, it := range resp.GetItems() {
		where := fmt.Sprintf("item %d's ", i+1)
		texts = append(texts, text{where + "name", it.GetName()}, text{where + "field", it.GetField()}, text{where + "reason", it.GetReason()},
			text{where + "detail", it.GetDetail()}, text{where + "error", it.GetError().GetMessage()})
		for _, f := range []struct{ field, template string }{
			{"title", it.GetTitle()}, {"description", it.GetDescription()}, {"message", it.GetMessage()}, {"fix", it.GetFix()},
		} {
			texts = append(texts, text{where + f.field, f.template}, text{where + f.field, check.Render(f.template, it.GetParams())})
		}
		for _, k := range slices.Sorted(maps.Keys(it.GetParams())) {
			texts = append(texts, text{where + "param", k}, text{where + "param", it.GetParams()[k]})
		}
	}
	for _, s := range secrets {
		if s == "" {
			continue
		}
		for _, x := range texts {
			if strings.Contains(x.s, s) {
				t.Errorf("%s, %s shows the value of a secret in the config: no item text, param or error may", with, x.where)
				break
			}
		}
	}
}

// label names item i of an answer to the connector's author: by its name
// and title, or by its place when it has no name or they hold one of
// secrets.
func label(i int, it *protocolv1.CheckItem, secrets []string) string {
	title := check.Render(it.GetTitle(), it.GetParams())
	if it.GetName() == "" || holds(it.GetName(), secrets) || holds(title, secrets) {
		return fmt.Sprintf("item %d", i+1)
	}
	return fmt.Sprintf("item %q (%s)", it.GetName(), title)
}

func statusName(s protocolv1.CheckStatus) string {
	switch s {
	case protocolv1.CheckStatus_CHECK_STATUS_PASSED:
		return "passed"
	case protocolv1.CheckStatus_CHECK_STATUS_SKIPPED:
		return "was skipped"
	}
	return strings.ToLower(strings.TrimPrefix(s.String(), "CHECK_STATUS_"))
}

// firstFailed is the first item a host counts as failed: FAILED, or a
// status outside the four.
func firstFailed(items []*protocolv1.CheckItem) (int, *protocolv1.CheckItem) {
	for i, it := range items {
		switch it.GetStatus() {
		case protocolv1.CheckStatus_CHECK_STATUS_PASSED, protocolv1.CheckStatus_CHECK_STATUS_WARNING, protocolv1.CheckStatus_CHECK_STATUS_SKIPPED:
		default:
			return i, it
		}
	}
	return -1, nil
}

// itemText is a failed or warning item's message, filled in, then its
// detail.
func itemText(it *protocolv1.CheckItem) string {
	text := check.Render(it.GetMessage(), it.GetParams())
	if d := it.GetDetail(); d != "" {
		text = strings.TrimSuffix(text, ".") + ": " + d
	}
	return text
}

// fieldPaths are the dotted paths of the properties a config schema
// declares, at any depth, in its branches, dependent schemas, arrays' items
// and the local definitions it refers to too; nil for a schema that declares
// none.
func fieldPaths(schema []byte) map[string]bool {
	var root any
	if json.Unmarshal(schema, &root) != nil {
		return nil
	}
	paths := map[string]bool{}
	var walk func(node any, prefix string, depth int)
	walk = func(node any, prefix string, depth int) {
		n, ok := node.(map[string]any)
		if !ok || depth > 32 {
			return
		}
		if ref, ok := n["$ref"].(string); ok {
			walk(pointer(root, ref), prefix, depth+1)
		}
		if props, ok := n["properties"].(map[string]any); ok {
			for k, p := range props {
				paths[prefix+k] = true
				walk(p, prefix+k+".", depth+1)
			}
		}
		for _, key := range []string{"oneOf", "anyOf", "allOf"} {
			branches, _ := n[key].([]any)
			for _, b := range branches {
				walk(b, prefix, depth+1)
			}
		}
		for _, key := range []string{"if", "then", "else"} {
			walk(n[key], prefix, depth+1)
		}
		if deps, ok := n["dependentSchemas"].(map[string]any); ok {
			for _, d := range deps {
				walk(d, prefix, depth+1)
			}
		}
		// An array's items' properties go under the array's path.
		for _, key := range []string{"items", "prefixItems"} {
			switch v := n[key].(type) {
			case map[string]any:
				walk(v, prefix, depth+1)
			case []any:
				for _, b := range v {
					walk(b, prefix, depth+1)
				}
			}
		}
	}
	walk(root, "", 0)
	if len(paths) == 0 {
		return nil
	}
	return paths
}

// pointer is what a local JSON pointer, such as #/$defs/auth, points to in
// root; nil for any other reference.
func pointer(root any, ref string) any {
	rest, ok := strings.CutPrefix(ref, "#")
	if !ok {
		return nil
	}
	node := root
	for _, p := range strings.Split(strings.TrimPrefix(rest, "/"), "/") {
		if p == "" {
			continue
		}
		switch n := node.(type) {
		case map[string]any:
			node = n[strings.ReplaceAll(strings.ReplaceAll(p, "~1", "/"), "~0", "~")]
		case []any:
			i, err := strconv.Atoi(p)
			if err != nil || i < 0 || i >= len(n) {
				return nil
			}
			node = n[i]
		default:
			return nil
		}
	}
	return node
}

// withoutIndexes is a dotted path without its array indexes: tables.0.name
// is tables.name, as fieldPaths has an array's items' properties.
func withoutIndexes(path string) string {
	parts := strings.Split(path, ".")
	return strings.Join(slices.DeleteFunc(parts, func(p string) bool {
		_, err := strconv.Atoi(p)
		return err == nil
	}), ".")
}
