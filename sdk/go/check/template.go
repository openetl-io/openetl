package check

import (
	"regexp"
	"slices"
	"strings"
)

// Params are placeholder values by name. Never secrets.
type Params map[string]string

var placeholder = regexp.MustCompile(`\{([a-z][a-z0-9_]*)\}`)

// Render fills {name} from p. A placeholder without a param stays as
// written, and a value isn't rendered again.
func Render(template string, p Params) string {
	return placeholder.ReplaceAllStringFunc(template, func(m string) string {
		if v, ok := p[m[1:len(m)-1]]; ok {
			return v
		}
		return m
	})
}

// Placeholders are the names template uses, in order, each once.
func Placeholders(template string) []string {
	var names []string
	for _, m := range placeholder.FindAllStringSubmatch(template, -1) {
		if !slices.Contains(names, m[1]) {
			names = append(names, m[1])
		}
	}
	return names
}

// missing is the value of a name given without one.
const missing = "!MISSING"

func pairs(kv []string) Params {
	p := make(Params, (len(kv)+1)/2)
	for i := 0; i < len(kv); i += 2 {
		if i+1 < len(kv) {
			p[kv[i]] = kv[i+1]
		} else {
			p[kv[i]] = missing
		}
	}
	return p
}

// clean is s as valid UTF-8, which the protocol's strings must be.
func clean(s string) string { return strings.ToValidUTF8(s, "\uFFFD") }

func cleanParams(p Params) Params {
	c := make(Params, len(p))
	for k, v := range p {
		c[clean(k)] = clean(v)
	}
	return c
}
