// Package sqlplan is what `openetl-engine plan` reads and writes: a SQL
// transform's statements and the streams they read, and what planning them
// gave. The control plane sends a request to a runner of the pipeline's data
// plane, whose engine plans it.
package sqlplan

import (
	"fmt"
	"strings"
)

// Request is what the engine plans.
type Request struct {
	SQL     string   `json:"sql"`
	Streams []Stream `json:"streams,omitempty"`
}

// Stream is a source's stream that statements may read.
type Stream struct {
	// Its whole name, as the source's catalog lists it.
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
	// Its primary key's columns; empty if it has none.
	Key []string `json:"key,omitempty"`
	// Its rows are inserts, updates and deletes of their key, as CDC's are.
	Changelog bool `json:"changelog,omitempty"`
	// Its timestamps keep nanoseconds.
	Nanoseconds bool `json:"nanoseconds,omitempty"`
}

// Column is a stream's column. Its type is Arrow's, named as the catalog
// names it, such as int64 or timestamp[us, tz=UTC]. Statements see every
// column as nullable.
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Field is a column of an output or a view, its type named as Column's.
type Field struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// Answer is what planning gave. With errors, the rest is empty.
type Answer struct {
	Statements []Statement `json:"statements"`
	Views      []View      `json:"views"`
	Notes      []Note      `json:"notes"`
	Errors     []Error     `json:"errors"`
}

// Statement is one INSERT INTO … SELECT, planned.
type Statement struct {
	At Position `json:"at"`
	// The stream it writes: a source stream's whole name if it replaces one,
	// or else a name of its own.
	Output   string `json:"output"`
	Replaces bool   `json:"replaces"`
	// row_wise, group_window, over, join or updating.
	Class string `json:"class"`
	// The SQL surface's tier that runs it.
	Tier int `json:"tier"`
	// The source streams it reads, directly or through views.
	Inputs []string `json:"inputs"`
	Views  []string `json:"views"`
	// The output's, with logical types.
	Columns []Field `json:"columns"`
	// An Arrow IPC stream holding only the output's schema.
	ArrowSchema []byte `json:"arrow_schema"`
	// The output's primary key; empty if it has none.
	Key []string `json:"key"`
	// The columns its rows are exchanged by; nil where it runs in the lane
	// that read them.
	Exchange []string `json:"exchange"`
	// Its stateful operator's ID; empty for none.
	Operator string `json:"operator"`
}

// View is a CREATE VIEW, planned on its own.
type View struct {
	Name    string   `json:"name"`
	At      Position `json:"at"`
	Inputs  []string `json:"inputs"`
	Columns []Field  `json:"columns"`
	Key     []string `json:"key"`
}

// Note is something the plan should say that isn't an error.
type Note struct {
	At      Position `json:"at"`
	Message string   `json:"message"`
}

// Error is what's wrong with the SQL.
type Error struct {
	// A stable code, such as syntax, bind or later_tier.
	Kind string `json:"kind"`
	// A later tier's statement's class and tier.
	Class string `json:"class,omitempty"`
	Tier  int    `json:"tier,omitempty"`
	// Nil where it isn't about the text.
	At      *Position `json:"at,omitempty"`
	Message string    `json:"message"`
}

// Position is a place in the SQL text. Lines and columns count from 1; a
// column counts characters.
type Position struct {
	Line   uint32 `json:"line"`
	Column uint32 `json:"column"`
}

func (p Position) String() string { return fmt.Sprintf("%d:%d", p.Line, p.Column) }

// Where is the error's place in the spec: transform.sql, and its line and
// column if it has them.
func (e Error) Where() string {
	if e.At == nil {
		return "transform.sql"
	}
	return "transform.sql:" + e.At.String()
}

func (e Error) Error() string { return e.Where() + ": " + e.Message }

// Mentions reports whether sql may name the stream: whether its last dotted
// part appears in the text, ignoring case. A statement names a stream by its
// whole name, so one that doesn't appear can't be read, and a request needn't
// carry it.
func Mentions(sql, stream string) bool {
	last := stream[strings.LastIndex(stream, ".")+1:]
	if last == "" || strings.Contains(stream, `"`) {
		return true
	}
	return strings.Contains(strings.ToLower(sql), strings.ToLower(last))
}
