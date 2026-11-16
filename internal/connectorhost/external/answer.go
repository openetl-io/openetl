package external

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/openetl-io/openetl/internal/platform/errs"
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
// item's status.
func answer(secrets []string, resp interface {
	GetError() *protocolv1.Error
	GetItems() []*protocolv1.CheckItem
}) check.Report {
	items, e := resp.GetItems(), resp.GetError()
	n := len(items)
	items = items[:min(n, maxItems)]
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

// unanswered is why a connector didn't answer a check or a preflight, as
// answer takes an answer's error: err is what a call to it returned.
func (p *process) unanswered(err error) error {
	err = p.failure(errs.FromStatus(err))
	e, ok := errors.AsType[*errs.Error](err)
	if !ok {
		return err
	}
	pe := &protocolv1.Error{Message: e.Msg}
	check.Redact(p.secrets, nil, pe)
	c := *e
	c.Msg = tidy(pe.Message, maxDetail, false)
	return &c
}

func tidyItem(it *protocolv1.CheckItem) {
	it.Name, it.Title, it.Field, it.Reason = tidy(it.Name, maxName, false), tidy(it.Title, maxTitle, false), tidy(it.Field, maxField, false), tidy(it.Reason, maxReason, false)
	it.Message, it.Fix, it.Description = tidy(it.Message, maxText, false), tidy(it.Fix, maxText, false), tidy(it.Description, maxText, false)
	it.Detail = tidy(it.Detail, maxDetail, true)
	if it.Error != nil {
		it.Error.Message = tidy(it.Error.Message, maxText, false)
	}
	if len(it.Params) > 0 {
		names := slices.Sorted(maps.Keys(it.Params))
		p := make(map[string]string, min(len(names), maxParams))
		for _, k := range names[:min(len(names), maxParams)] {
			p[tidy(k, maxParamName, false)] = tidy(it.Params[k], maxParamValue, false)
		}
		it.Params = p
	}
}

// tidy is s as valid UTF-8 without Unicode's bidirectional marks and tag
// characters, and with a space for each other control character, and cut at
// a rune boundary to at most limit bytes, "…" ending what's cut. The other
// control characters are C0's and C1's, DEL, Unicode's bidirectional
// overrides and isolates, and its line and paragraph separators. A detail
// keeps newline and tab, and has a newline for each separator.
func tidy(s string, limit int, detail bool) string {
	s = strings.Map(func(r rune) rune {
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
	if len(s) <= limit {
		return s
	}
	end := limit - len("…")
	for end > 0 && !utf8.RuneStart(s[end]) {
		end--
	}
	return s[:end] + "…"
}
