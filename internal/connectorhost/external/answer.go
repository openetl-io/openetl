package external

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/openetl-io/openetl/sdk/go/check"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// The most of a connector's answer to a check or a preflight that the host
// keeps: items, and each text's bytes, "…" included.
const (
	maxItems      = 2000
	maxName       = 200
	maxTitle      = 300
	maxField      = 300
	maxReason     = 100
	maxText       = 2000 // a message, fix or description, and an item's error's message
	maxDetail     = 4000 // a detail, and the answer's error's message
	maxParams     = 50   // per item
	maxParamName  = 64
	maxParamValue = 1000
	// The most bytes of the strings of an answer's items together, params'
	// names and values included. An answer becomes a workflow result, which
	// must stay well under the orchestrator's payload limit (Temporal's is
	// 2 MB).
	maxAnswerBytes = 1 << 20
)

// answer is a connector's answer to a check or a preflight as the host takes
// it, whatever the connector is written in: without the values of secrets,
// its texts valid UTF-8 without control characters, and cut to their
// lengths. It keeps the first maxItems items, and of those the first that
// fit in maxAnswerBytes; an answer with more fails, whatever its items say.
// check.FromProto then reads it, failing closed; cutting texts changes no
// item's status. A text much longer than its limit is fitted on its own
// first, which redacts only its start.
func answer(secrets []string, resp interface {
	GetError() *protocolv1.Error
	GetItems() []*protocolv1.CheckItem
}) check.Report {
	items, e := resp.GetItems(), resp.GetError()
	n := len(items)
	items = items[:min(n, maxItems)]
	r := newRedactor(secrets)
	long := func(s string, limit int, detail bool) string {
		if r.cuts(s, limit) {
			return r.fit(s, limit, detail)
		}
		return s
	}
	for _, it := range items {
		eachText(it, long)
		for k, v := range it.Params {
			if r.cuts(k, maxParamName) || r.cuts(v, maxParamValue) {
				p := make(map[string]string, len(it.Params))
				for k, v := range it.Params {
					p[long(k, maxParamName, false)] = long(v, maxParamValue, false)
				}
				it.Params = p
				break
			}
		}
	}
	if e != nil {
		e.Message = long(e.Message, maxDetail, false)
	}
	check.Redact(secrets, items, e)
	size, kept := 0, len(items)
	for i, it := range items {
		tidyItem(it)
		if size += itemBytes(it); size > maxAnswerBytes {
			kept = min(kept, i)
		}
	}
	items = items[:kept]
	if e != nil {
		e.Message = tidy(e.Message, maxDetail, false)
	}
	var over string
	switch {
	case n > maxItems && kept < maxItems:
		over = fmt.Sprintf("%d items and more than %d bytes of them, and an answer has at most %d items and %d bytes: the first %d are kept",
			n, maxAnswerBytes, maxItems, maxAnswerBytes, kept)
	case n > maxItems:
		over = fmt.Sprintf("%d items, and an answer has at most %d: the first %d are kept", n, maxItems, maxItems)
	case kept < n:
		over = fmt.Sprintf("%d bytes of items, and an answer has at most %d: the first %d items are kept", size, maxAnswerBytes, kept)
	}
	if over != "" {
		e = &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_INTERNAL, Message: "the connector answered with " + over}
	}
	return check.FromProto(&protocolv1.CheckResponse{Items: items, Error: e})
}

// itemBytes is the bytes of the strings an item carries.
func itemBytes(it *protocolv1.CheckItem) int {
	n := len(it.Name) + len(it.Title) + len(it.Field) + len(it.Reason) + len(it.Message) + len(it.Fix) + len(it.Description) + len(it.Detail) +
		len(it.GetError().GetMessage())
	for k, v := range it.Params {
		n += len(k) + len(v)
	}
	return n
}

// eachText puts f of each of it's texts but its params in its place, with
// the text's limit and whether it's a detail.
func eachText(it *protocolv1.CheckItem, f func(s string, limit int, detail bool) string) {
	it.Name, it.Title, it.Field, it.Reason = f(it.Name, maxName, false), f(it.Title, maxTitle, false), f(it.Field, maxField, false), f(it.Reason, maxReason, false)
	it.Message, it.Fix, it.Description = f(it.Message, maxText, false), f(it.Fix, maxText, false), f(it.Description, maxText, false)
	it.Detail = f(it.Detail, maxDetail, true)
	if it.Error != nil {
		it.Error.Message = f(it.Error.Message, maxText, false)
	}
}

func tidyItem(it *protocolv1.CheckItem) {
	eachText(it, tidy)
	if len(it.Params) > 0 {
		names := slices.Sorted(maps.Keys(it.Params))
		p := make(map[string]string, min(len(names), maxParams))
		for _, k := range names[:min(len(names), maxParams)] {
			p[tidy(k, maxParamName, false)] = tidy(it.Params[k], maxParamValue, false)
		}
		it.Params = p
	}
}

// tidy is clean of s, cut at a rune boundary to at most limit bytes, "…"
// ending what's cut.
func tidy(s string, limit int, detail bool) string {
	s = clean(s, detail)
	if len(s) <= limit {
		return s
	}
	return ellipsis(s, limit-len("…"))
}

// clean is s as valid UTF-8 without Unicode's bidirectional marks and tag
// characters, and with a space for each other control character: C0's and
// C1's, DEL, Unicode's bidirectional overrides and isolates, and its line
// and paragraph separators. A detail keeps newline and tab, and has a
// newline for each separator.
func clean(s string, detail bool) string {
	return strings.Map(func(r rune) rune {
		switch {
		case detail && (r == '\n' || r == '\t'):
			return r
		case detail && (r == '\u2028' || r == '\u2029'):
			return '\n'
		case r == '\u061c', r == '\u200e', r == '\u200f', r >= 0xe0000 && r <= 0xe007f:
			return -1
		case r < 0x20, r >= 0x7f && r <= 0x9f, r >= 0x202a && r <= 0x202e, r >= 0x2066 && r <= 0x2069, r == '\u2028', r == '\u2029':
			return ' '
		}
		return r
	}, strings.ToValidUTF8(s, "�"))
}

// ellipsis is the first n bytes of s, fewer to end at a rune boundary, and
// "…". n is less than len(s).
func ellipsis(s string, n int) string {
	n = max(n, 0)
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "…"
}

// redacted is what check.Redact puts in a secret's place.
var redacted = func() string {
	e := &protocolv1.Error{Message: "s"}
	check.Redact([]string{"s"}, nil, e)
	return e.Message
}()

// formBytes bounds the bytes of all the forms check.Redact finds a secret
// in, for each byte of the secret: it finds it as written and escaped a few
// ways, none more than 6 bytes for each byte, as JSON escapes a control
// character.
const formBytes = 32

// redactor takes the values of secrets out of a connector's texts.
type redactor struct {
	secrets []string
	// The most bytes at the end of a redacted text that cutting the text
	// first can change. A cut can split a form of a secret, and the forms
	// redacted after it can then match differently, each up to its length
	// further back: at most all the forms' bytes, each of which comes out as
	// at most len(redacted) bytes. -1 when a secret is part of a run of
	// redacted, so that redacting can redact what it put in, without that
	// bound.
	margin int
}

func newRedactor(secrets []string) redactor {
	n := 0
	for _, s := range secrets {
		if s != "" && strings.Contains(strings.Repeat(redacted, len(s)/len(redacted)+2), s) {
			return redactor{secrets: secrets, margin: -1}
		}
		n += len(s)
	}
	return redactor{secrets: secrets, margin: max(len(redacted), len("�")) * (formBytes*n + utf8.UTFMax)}
}

// cuts is whether fit redacts only the start of s, which it does when s is
// longer than 4 times limit and r.margin.
func (r redactor) cuts(s string, limit int) bool {
	return r.margin >= 0 && len(s) > 4*(limit+r.margin)
}

// fit is tidy of s without the secrets' values, as the host keeps a
// connector's text. Of an s it cuts, it redacts and cleans only that start,
// and keeps what the cut can't change, all but its last r.margin bytes, up
// to limit: what fitting s whole keeps, unless redacting and cleaning take
// the start to under about a quarter of its length, when it keeps less.
func (r redactor) fit(s string, limit int, detail bool) string {
	if !r.cuts(s, limit) {
		return tidy(r.redact(s), limit, detail)
	}
	u := clean(r.redact(s[:4*(limit+r.margin)]), detail)
	return ellipsis(u, min(len(u)-r.margin, limit-len("…")))
}

func (r redactor) redact(s string) string {
	e := &protocolv1.Error{Message: s}
	check.Redact(r.secrets, nil, e)
	return e.Message
}
