package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"unicode"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/sdk/go/check"
)

const (
	statusPassed  = "passed"
	statusWarning = "warning"
	statusFailed  = "failed"
	statusSkipped = "skipped"
)

// checkItem is one thing a connector's check looked at, from the API or from
// the connector itself: its templates filled in, and every string printable.
type checkItem struct {
	// passed, warning, failed or skipped.
	status                                           string
	title, message, detail, fix, field, code, reason string
}

func (it checkItem) failing() bool { return it.status == statusFailed }

// text is what a failed item says on one line: "CODE: title: message".
func (it checkItem) text() string {
	var parts []string
	for _, s := range []string{it.code, it.title, strings.TrimSuffix(it.message, ".")} {
		if s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, ": ")
}

// apiCheckItems are the API's items, read failing closed: a status other
// than passed, warning or skipped is failed.
func apiCheckItems(items []*apiv1.CheckItem) []checkItem {
	out := make([]checkItem, 0, len(items))
	for _, it := range items {
		p := it.GetParams()
		c := checkItem{
			status: it.GetStatus(), title: printable(cmp.Or(check.Render(it.GetTitle(), p), it.GetName())),
			message: printable(check.Render(it.GetMessage(), p)), detail: printableLines(it.GetDetail()),
			fix: printable(check.Render(it.GetFix(), p)), field: printable(it.GetField()),
			code: printable(it.GetCode()), reason: printable(it.GetReason()),
		}
		switch c.status {
		case statusPassed, statusWarning, statusFailed, statusSkipped:
		default:
			c.status = statusFailed
			note := printable(fmt.Sprintf("the item's status is %q, which counts as failed", it.GetStatus()))
			if c.detail != "" {
				note = c.detail + "\n" + note
			}
			c.detail = note
		}
		out = append(out, c)
	}
	return out
}

// reportItems are a connector's items as it answered, filled in.
func reportItems(items []check.Item) []checkItem {
	out := make([]checkItem, 0, len(items))
	for _, it := range items {
		it = it.Rendered()
		c := checkItem{status: it.Status.String(), title: printable(cmp.Or(it.Title, it.Name)), message: printable(it.Message), field: printable(it.Field)}
		if f := it.Failure; f != nil {
			c.code, c.reason, c.fix, c.detail = errs.Code(f.ErrorCode()).String(), printable(f.Reason), printable(f.Fix), printableLines(f.Detail)
		}
		out = append(out, c)
	}
	return out
}

func anyFailing(items []checkItem) bool {
	for _, it := range items {
		if it.failing() {
			return true
		}
	}
	return false
}

func firstFailing(items []checkItem) (checkItem, bool) {
	for _, it := range items {
		if it.failing() {
			return it, true
		}
	}
	return checkItem{}, false
}

// printCheckItems prints a check's items, one a line under indent with its
// status and title; then, on lines of their own, a passed item's message,
// a skipped item's reason, and a failed or warning item's message, detail,
// fix, field, and code and reason.
func printCheckItems(out io.Writer, indent string, items []checkItem) {
	under := indent + strings.Repeat(" ", 9)
	for _, it := range items {
		fmt.Fprintf(out, "%s%-8s %s\n", indent, it.status, it.title)
		if it.message != "" {
			fmt.Fprintf(out, "%s%s\n", under, it.message)
		}
		if it.status != statusFailed && it.status != statusWarning {
			continue
		}
		if it.detail != "" {
			fmt.Fprintf(out, "%sdetail: %s\n", under, strings.ReplaceAll(it.detail, "\n", "\n"+under+"        "))
		}
		if it.fix != "" {
			fmt.Fprintf(out, "%sfix: %s\n", under, it.fix)
		}
		if it.field != "" {
			fmt.Fprintf(out, "%sfield: %s\n", under, it.field)
		}
		switch {
		case it.code != "" && it.reason != "":
			fmt.Fprintf(out, "%scode: %s (%s)\n", under, it.code, it.reason)
		case it.code != "":
			fmt.Fprintf(out, "%scode: %s\n", under, it.code)
		case it.reason != "":
			fmt.Fprintf(out, "%sreason: %s\n", under, it.reason)
		}
	}
}

// printable is s with what could drive a terminal removed: C0 and C1
// controls, DEL, and the bidirectional marks, embeddings, overrides and
// isolates. Tabs, newlines and other line breaks become spaces, and bytes
// that aren't UTF-8 become U+FFFD.
func printable(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r == '\t' || r == '\n' || r == '\v' || r == '\f' || r == '\r':
			return ' '
		case unicode.IsControl(r) || bidi(r):
			return -1
		}
		return r
	}, s)
}

// printableLines is s made printable line by line, its line breaks kept.
func printableLines(s string) string {
	lines := strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n")
	for i, l := range lines {
		lines[i] = strings.TrimRight(printable(l), " ")
	}
	return strings.Join(lines, "\n")
}

// printableCopy is a copy of m whose strings, at any depth, are printable.
// A map's keys are left as they are.
func printableCopy[M proto.Message](m M) M {
	c := proto.CloneOf(m)
	makePrintable(c.ProtoReflect())
	return c
}

func makePrintable(m protoreflect.Message) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList():
			l := v.List()
			for i := range l.Len() {
				l.Set(i, printableValue(fd, l.Get(i)))
			}
		case fd.IsMap():
			v.Map().Range(func(k protoreflect.MapKey, e protoreflect.Value) bool {
				v.Map().Set(k, printableValue(fd.MapValue(), e))
				return true
			})
		case fd.Kind() == protoreflect.StringKind:
			m.Set(fd, printableValue(fd, v))
		default:
			printableValue(fd, v)
		}
		return true
	})
}

// printableValue is v made printable if it's a string. A message is made
// printable in place.
func printableValue(fd protoreflect.FieldDescriptor, v protoreflect.Value) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(printable(v.String()))
	case protoreflect.MessageKind, protoreflect.GroupKind:
		makePrintable(v.Message())
	}
	return v
}

// bidi reports whether r is a bidirectional control: the Arabic letter
// mark, LRM and RLM, the embeddings and overrides, and the isolates.
func bidi(r rune) bool {
	return r == 0x61c || r == 0x200e || r == 0x200f || r >= 0x202a && r <= 0x202e || r >= 0x2066 && r <= 0x2069
}

const (
	formatText = "text"
	formatJSON = "json"
)

func formatFlag(fs *flag.FlagSet) *string {
	return fs.String("format", formatText, "text, or json for the result as one JSON document, its templates unfilled")
}

func checkFormat(format string) error {
	if format != formatText && format != formatJSON {
		return fmt.Errorf("--format %s: want text or json", format)
	}
	return nil
}

// printJSON prints m as one indented JSON document. Its strings keep what
// came from a connector, with the characters printable removes escaped.
func printJSON(out io.Writer, m proto.Message) error {
	b, err := protojson.Marshal(m)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, b, "", "  "); err != nil {
		return err
	}
	var s strings.Builder
	// protojson escapes C0 controls; what's left can only be in a string.
	for _, r := range buf.String() {
		if r >= 0x7f && (unicode.IsControl(r) || bidi(r)) {
			fmt.Fprintf(&s, `\u%04x`, r)
			continue
		}
		s.WriteRune(r)
	}
	s.WriteByte('\n')
	_, err = io.WriteString(out, s.String())
	return err
}

// exitError is an error that exits with its own status; any other exits
// with 1.
type exitError struct {
	status int
	err    error
}

// Error is printable: an error can carry what a connector said.
func (e *exitError) Error() string { return printableLines(e.err.Error()) }
func (e *exitError) Unwrap() error { return e.err }

func exitStatus(err error) int {
	if e := (*exitError)(nil); errors.As(err, &e) {
		return e.status
	}
	return 1
}

// checkFailed is the error of a check or a preflight that ran and failed.
func checkFailed(format string, args ...any) error {
	return &exitError{status: 1, err: fmt.Errorf(format, args...)}
}

// checkStatus gives a check's or a preflight's error its exit status: 1 when
// it ran and failed, and 2 when it couldn't run.
func checkStatus(err error) error {
	if err == nil || errors.Is(err, flag.ErrHelp) || errors.As(err, new(*exitError)) {
		return err
	}
	return &exitError{status: 2, err: err}
}

const exitStatusUsage = `
Exit status: 0 when it passed, warnings included; 1 when it failed; 2 when
it couldn't run: a usage error, the API or a runner unreachable, an
operation that timed out, or a connector that didn't start or answer.
`

const connectorCheckExitUsage = `
Exit status: 0 when it passed, warnings included; 1 when it failed; 2 when
it couldn't run: a usage error, or a connector that didn't start or answer.
`
