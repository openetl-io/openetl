package hostservices

import (
	"bytes"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"time"
)

const (
	maxTraces    = 64
	maxTraceBody = 4 << 10
	redacted     = "[redacted]"
)

// Trace is one try of an HTTP request, redacted for heal: no credentials
// or secret values, and a successful response's body only as its shape.
type Trace struct {
	Time   time.Time
	Try    int
	Method string
	URL    string
	// The request's own headers; credentials go on at egress, unrecorded.
	RequestHeader  http.Header
	Status         int
	ResponseHeader http.Header
	// The start of the body. A failure's text, or a success's JSON with
	// every string, number and boolean replaced.
	Body     []byte
	Err      string
	Duration time.Duration
}

// Headers whose values are always redacted, besides an API key's own.
var sensitiveHeaders = []string{"Authorization", "Proxy-Authorization", "Cookie", "Set-Cookie", "X-Amz-Security-Token"}

// Traces returns the instance's latest tries, oldest first.
func (s *Services) Traces() []Trace {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Trace, 0, len(s.traces))
	out = append(out, s.traces[s.nextTrace:]...)
	return append(out, s.traces[:s.nextTrace]...)
}

func (s *Services) addTrace(t Trace) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.traces) < maxTraces {
		s.traces = append(s.traces, t)
		return
	}
	s.traces[s.nextTrace] = t
	s.nextTrace = (s.nextTrace + 1) % maxTraces
}

// remember adds secret values to scrub from traces and logs.
func (s *Services) remember(values ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range values {
		// Shorter ones would scrub ordinary text.
		if len(v) >= 4 && !slices.Contains(s.known, v) {
			s.known = append(s.known, v)
		}
	}
	// Longest first, so a secret that contains another goes whole.
	slices.SortFunc(s.known, func(a, b string) int { return len(b) - len(a) })
}

func (s *Services) scrub(v string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range s.known {
		v = strings.ReplaceAll(v, k, redacted)
	}
	return v
}

func (s *Services) redactHeader(h http.Header, also string) http.Header {
	out := make(http.Header, len(h))
	for k, vs := range h {
		if k == also || slices.Contains(sensitiveHeaders, k) {
			out[k] = []string{redacted}
			continue
		}
		for _, v := range vs {
			out[k] = append(out[k], s.scrub(v))
		}
	}
	return out
}

// shape rewrites a JSON prefix, which may be cut short, keeping its keys,
// nesting and nulls. Anything that isn't JSON gives nil.
func shape(b []byte) []byte {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var out bytes.Buffer
	// Per open container: whether it's an object, and the tokens in it.
	type frame struct {
		obj bool
		n   int
	}
	var stack []frame
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		closing := tok == json.Delim('}') || tok == json.Delim(']')
		key := false
		if len(stack) > 0 && !closing {
			f := &stack[len(stack)-1]
			switch {
			case f.obj && f.n%2 == 1:
				out.WriteByte(':')
			case f.n > 0:
				out.WriteByte(',')
			}
			key = f.obj && f.n%2 == 0
			f.n++
		}
		switch t := tok.(type) {
		case json.Delim:
			out.WriteRune(rune(t))
			switch t {
			case '{', '[':
				stack = append(stack, frame{obj: t == '{'})
			default:
				stack = stack[:len(stack)-1]
			}
		case string:
			if key {
				k, _ := json.Marshal(t)
				out.Write(k)
			} else {
				out.WriteString(`""`)
			}
		case json.Number:
			out.WriteByte('0')
		case bool:
			out.WriteString("false")
		case nil:
			out.WriteString("null")
		}
	}
	if out.Len() == 0 {
		return nil
	}
	return out.Bytes()
}
