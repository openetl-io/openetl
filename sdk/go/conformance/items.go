package conformance

import (
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/openetl-io/openetl/sdk/go/check"
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
// of the protocol, as checkItems does.
func (c *common) checkAnswer(t T, with string, config []byte, resp answer) {
	t.Helper()
	var secrets []string
	if _, s, err := splitSecrets(c.spec.GetConfigSchema(), config); err == nil {
		secrets = slices.Collect(maps.Values(s))
	}
	checkItems(t, with, resp, secrets, fieldPaths(c.spec.GetConfigSchema()))
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
	items := resp.GetItems()
	seen := map[string]int{}
	for i, it := range items {
		name := label(i, it)
		switch n := it.GetName(); {
		case n == "":
			t.Errorf("%s, %s has no name", with, name)
		case !itemName.MatchString(n):
			t.Errorf("%s, %s's name isn't lowercase letters and digits in words joined by \".\", \"_\" or \"-\"", with, name)
		default:
			if j, ok := seen[n]; ok {
				t.Errorf("%s, items %d and %d have the same name %q: each needs its own", with, j+1, i+1, n)
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
			t.Errorf("%s, %s's reason %q isn't lowercase words joined by \"_\"", with, name, r)
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
				t.Errorf("%s, %s's error message is %q, not its message with its params filled in, %q: the detail has a field of its own", with, name, e.GetMessage(), want)
			}
		}
		for _, text := range []struct{ field, template string }{
			{"title", it.GetTitle()}, {"description", it.GetDescription()}, {"message", it.GetMessage()}, {"fix", it.GetFix()},
		} {
			for _, m := range nearPlaceholder.FindAllString(text.template, -1) {
				if !placeholderName.MatchString(m[1 : len(m)-1]) {
					t.Errorf("%s, %s's %s has %s, which looks like a placeholder but isn't one: a placeholder's name is lowercase letters, digits and \"_\", starting with a letter", with, name, text.field, m)
				}
			}
			for _, p := range check.Placeholders(text.template) {
				if _, ok := it.GetParams()[p]; !ok {
					flagf(t, "%s, %s's %s uses {%s} without a param of that name, so it shows as written", with, name, text.field, p)
				}
			}
		}
		if f := it.GetField(); f != "" && fields != nil && !fields[f] {
			t.Errorf("%s, %s's field %q isn't a property of the config schema: name one by its dotted path, such as consumer.name", with, name, f)
		}
	}
	i, first := firstFailed(items)
	e := resp.GetError()
	switch {
	case first == nil && e != nil && len(items) > 0 && !slices.ContainsFunc(items, func(it *protocolv1.CheckItem) bool {
		return it.GetStatus() == protocolv1.CheckStatus_CHECK_STATUS_SKIPPED
	}):
		t.Errorf("%s, the response has an error (%s: %s) but every item passed or warned: set it when an item failed, or when the checks couldn't finish, which leaves the items they didn't reach skipped, or no items at all", with, e.GetCode(), e.GetMessage())
	case first != nil && e == nil:
		t.Errorf("%s, %s failed but the response has no error: set it to the first failed item's, which a host from before items shows", with, label(i, first))
	case first.GetStatus() == protocolv1.CheckStatus_CHECK_STATUS_FAILED:
		if e.GetCode() != first.GetError().GetCode() {
			t.Errorf("%s, the response's error is %s, not %s of %s, the first that failed", with, e.GetCode(), first.GetError().GetCode(), label(i, first))
		}
		if title := check.Render(first.GetTitle(), first.GetParams()); !strings.HasPrefix(e.GetMessage(), title) {
			t.Errorf("%s, the response's error message %q doesn't start with %q, the title of %s, the first that failed: a host from before items shows it, as \"title: message\"", with, e.GetMessage(), title, label(i, first))
		}
	}
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

// label names item i of an answer to the connector's author: by its name,
// or by its place when it has none.
func label(i int, it *protocolv1.CheckItem) string {
	if it.GetName() == "" {
		return fmt.Sprintf("item %d", i+1)
	}
	return fmt.Sprintf("item %q (%s)", it.GetName(), check.Render(it.GetTitle(), it.GetParams()))
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
// declares, at any depth, in its branches and the local definitions it
// refers to too; nil for a schema that declares none.
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
		m, ok := node.(map[string]any)
		if !ok {
			return nil
		}
		node = m[strings.ReplaceAll(strings.ReplaceAll(p, "~1", "/"), "~0", "~")]
	}
	return node
}
