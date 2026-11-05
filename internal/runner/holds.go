package runner

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/sdk/go/destination"
)

// HeldMode is what a held cell shows (ADR-0025).
type HeldMode int

const (
	HeldNull HeldMode = iota
	HeldValue
	// The target keeps its value; rows it doesn't have get Value, or null.
	HeldLast
)

type Held struct {
	Mode HeldMode
	// JSON of the column's delivered type; empty for null.
	Value string
}

// StreamHolds is how a stream holds back a column, or a row's cell, and
// delivers the rest of the row (ADR-0025).
type StreamHolds struct {
	// A value that doesn't fit holds its cell instead of quarantining its
	// row.
	Cells bool
	Held  Held
	// By source column.
	Columns map[string]Held
	// Columns held together, by source name.
	Groups [][]string
}

// HoldPlan is a stream's holds for the dataflow.
type HoldPlan struct {
	StreamHolds
	// Column holds in force; discarded ones' values are dropped.
	Holding []checkpoint.ColumnHold
	Limits  destination.TableLimits
	Replay  *checkpoint.Replay
}

// Release is a hold the user released, or discarded. It applies to the hold
// on Column that started at Since, if it's still open.
type Release struct {
	ID             string
	Stream, Column string
	Since          checkpoint.RecordRef
	Discard        bool
	// A column hold's values go to a new version of the column, col__v2,
	// and the column keeps the type the destination has.
	NewVersion bool
}

// RefusedError: the destination refused columns of an epoch's schemas
// before the epoch was recorded. RunAttempt starts another attempt that
// holds them.
type RefusedError struct {
	Columns []RefusedColumn
}

type RefusedColumn struct {
	destination.RefusedColumn
	// The field the destination has, as ColumnHold.Delivered.
	Delivered []byte
}

func (e *RefusedError) Error() string {
	cols := make([]string, len(e.Columns))
	for i, c := range e.Columns {
		cols[i] = c.describe()
	}
	return strings.Join(cols, "; ")
}

// describe says what the destination refused, and how to make it take it.
func (c RefusedColumn) describe() string {
	s := fmt.Sprintf("stream %s: the destination refused column %s", c.Stream, c.Column)
	switch {
	case c.FromType != "" && c.ToType != "":
		s += fmt.Sprintf(" (%s to %s)", c.FromType, c.ToType)
	case c.ToType != "":
		s += fmt.Sprintf(" (a new %s column)", c.ToType)
	}
	s += ": " + c.Reason
	if c.Fix != "" {
		s += "; to take the change, run this on the destination: " + c.Fix
	}
	return s
}

func (c RefusedColumn) cause() *checkpoint.HoldCause {
	return &checkpoint.HoldCause{Kind: checkpoint.HoldRefused, Refusal: c.Reason, FromType: c.FromType, ToType: c.ToType, Fix: c.Fix}
}

// holdsOn reports whether stream s holds in this job.
func (j *Job) holdsOn(s Stream) bool {
	return s.Holds != nil && j.HeldURL != "" && len(s.PrimaryKey) > 0 && !j.transformed() &&
		j.Destination.Capabilities().Holds.Takes(s.Mode)
}

func (j *Job) checkHolds() error {
	caps := j.Destination.Capabilities().Holds
	for _, s := range j.Streams {
		if s.Holds == nil {
			continue
		}
		last := s.Holds.Held.Mode == HeldLast
		for _, h := range s.Holds.Columns {
			last = last || h.Mode == HeldLast
		}
		switch {
		case len(s.PrimaryKey) == 0:
			return fmt.Errorf("stream %q holds columns, and only a keyed stream can", s.Name)
		case j.transformed():
			return fmt.Errorf("stream %q holds columns, and a transform's inputs can't be held", s.Name)
		case !caps.Takes(s.Mode):
			return fmt.Errorf("stream %q holds columns, and the destination doesn't patch its write mode", s.Name)
		case last && (s.Mode != destination.Merge || !caps.Last):
			return fmt.Errorf("stream %q shows held cells' last values, which needs merge and a destination that keeps them", s.Name)
		}
	}
	return nil
}

// startHolds are the holds each holding stream starts the attempt with:
// restored's, with the job's releases applied and the refused columns
// held; and the dataflow's plans. A released column the destination
// refuses again keeps its hold, isn't patched in, and records the refused
// release, as do its released cells.
func (j *Job) startHolds(ctx context.Context, restored *checkpoint.Record, limits map[string]destination.TableLimits) (map[string]*checkpoint.Holds, map[string]*HoldPlan, error) {
	holds := map[string]*checkpoint.Holds{}
	plans := map[string]*HoldPlan{}
	for _, s := range j.Streams {
		if !j.holdsOn(s) {
			continue
		}
		h := &checkpoint.Holds{}
		if restored != nil && restored.Streams[s.Name].Holds != nil {
			h = cloneHolds(restored.Streams[s.Name].Holds)
		}
		var released []string
		var taken, versioned []checkpoint.ColumnHold
		// The releases' IDs, by column.
		wholes, cells := map[string]string{}, map[string]string{}
		for _, r := range j.Releases {
			if r.Stream != s.Name || h.Replay != nil {
				continue
			}
			again := func(f *checkpoint.Refused) bool { return f != nil && r.ID != "" && f.Release == r.ID && !r.Discard }
			if i := slices.IndexFunc(h.Columns, func(c checkpoint.ColumnHold) bool {
				return c.Column == r.Column && c.Since == r.Since && !c.Discarded && !again(c.Refused)
			}); i >= 0 {
				if r.Discard {
					h.Columns[i].Discarded, h.Columns[i].Refused = true, nil
				} else {
					taken = append(taken, h.Columns[i])
					if r.NewVersion && h.Columns[i].Delivered != nil {
						versioned = append(versioned, h.Columns[i])
					}
					h.Columns = slices.Delete(h.Columns, i, i+1)
					released = append(released, r.Column)
					wholes[r.Column] = r.ID
				}
			}
			if c, ok := h.Cells[r.Column]; ok && c.Since == r.Since && !again(c.Refused) {
				if r.Discard {
					delete(h.Cells, r.Column)
				} else {
					released = append(released, r.Column)
					cells[r.Column] = r.ID
				}
			}
		}
		for _, r := range j.refused {
			if r.Stream != s.Name {
				continue
			}
			is := func(col string) bool { return col == r.Column }
			released = slices.DeleteFunc(released, is)
			versioned = slices.DeleteFunc(versioned, func(c checkpoint.ColumnHold) bool { return is(c.Column) })
			if h.Replay != nil {
				if h.Replay.Columns = slices.DeleteFunc(h.Replay.Columns, is); len(h.Replay.Columns) == 0 {
					h.Replay = nil
				}
			}
			if id, ok := cells[r.Column]; ok {
				c := h.Cells[r.Column]
				c.Refused = &checkpoint.Refused{Release: id, Reason: "the destination refused the column: " + r.Reason}
				h.Cells[r.Column] = c
				delete(cells, r.Column)
			}
			if slices.ContainsFunc(h.Columns, func(c checkpoint.ColumnHold) bool { return c.Column == r.Column }) {
				continue
			}
			if i := slices.IndexFunc(taken, func(c checkpoint.ColumnHold) bool { return c.Column == r.Column }); i >= 0 {
				c := taken[i]
				c.Reason, c.Cause = "the destination refused it: "+r.Reason, r.cause()
				c.Refused = &checkpoint.Refused{Release: wholes[r.Column], Reason: "the destination refused it again"}
				h.Columns = append(h.Columns, c)
			} else {
				h.Columns = append(h.Columns, checkpoint.ColumnHold{
					Column: r.Column, Delivered: r.Delivered, Reason: "the destination refused it: " + r.Reason, Cause: r.cause(),
				})
			}
		}
		if len(released) > 0 {
			slices.Sort(released)
			files, err := j.Log.HeldFiles(ctx, s.Name, h.Epochs)
			if err != nil {
				return nil, nil, err
			}
			h.Replay = &checkpoint.Replay{Columns: slices.Compact(released), Files: files, Versioned: versioned}
			if len(cells) > 0 {
				h.Replay.CellReleases = cells
			}
		}
		holds[s.Name] = h
		plans[s.Name] = &HoldPlan{StreamHolds: *s.Holds, Holding: h.Columns, Limits: limits[s.Name], Replay: h.Replay}
	}
	return holds, plans, nil
}

// mergeHolds is a stream's holds once epoch at is recorded: base's, with
// what the dataflow reported at the stream's barrier. Nil once nothing is
// held.
func mergeHolds(base *checkpoint.Holds, r *ReadyHolds, at checkpoint.RecordRef, now time.Time) *checkpoint.Holds {
	h := &checkpoint.Holds{}
	if base != nil {
		h = cloneHolds(base)
	}
	if r != nil {
		cols := make([]checkpoint.ColumnHold, 0, len(r.Holding))
		for _, c := range r.Holding {
			c.Since, c.Started = at, now
			if i := slices.IndexFunc(h.Columns, func(p checkpoint.ColumnHold) bool { return p.Column == c.Column }); i >= 0 {
				p := h.Columns[i]
				if p.Since != (checkpoint.RecordRef{}) {
					c.Since, c.Started = p.Since, p.Started
				}
				c.Discarded, c.Refused = p.Discarded, p.Refused
				c.Cause = keepRefusal(p.Cause, c.Cause)
			}
			cols = append(cols, c)
		}
		h.Columns = cols
		add := func(cells map[string]checkpoint.CellHold, col string, c checkpoint.CellHold) map[string]checkpoint.CellHold {
			if cells == nil {
				cells = map[string]checkpoint.CellHold{}
			}
			e := cells[col]
			if e.Cells == 0 {
				e.Reason, e.Since, e.Started = c.Reason, at, now
			}
			e.Cells += c.Cells
			cells[col] = e
			return cells
		}
		for col, c := range r.Cells {
			h.Cells = add(h.Cells, col, c)
			if h.Replay != nil && slices.Contains(h.Replay.Columns, col) {
				h.Replay.Held = add(h.Replay.Held, col, c)
			}
		}
		if len(r.Files) > 0 {
			if n := len(h.Epochs); n > 0 && h.Epochs[n-1].Generation == at.Generation {
				h.Epochs[n-1].To = at.Epoch
			} else {
				h.Epochs = append(h.Epochs, checkpoint.HeldEpochs{Generation: at.Generation, From: at.Epoch, To: at.Epoch})
			}
		}
		if p := r.Replayed; p != nil && h.Replay != nil {
			h.Replay.File, h.Replay.Row, h.Replay.StillHeld = p.File, p.Row, p.StillHeld
			if p.Done {
				for _, col := range h.Replay.Columns {
					held, kept := h.Replay.Held[col]
					c, ok := h.Cells[col]
					switch {
					case p.StillHeld == 0 && kept:
						h.Cells[col] = held
					case p.StillHeld == 0:
						delete(h.Cells, col)
					case ok && h.Replay.CellReleases[col] != "":
						c.Refused = &checkpoint.Refused{Release: h.Replay.CellReleases[col], Reason: stillHeld(p.StillHeld)}
						h.Cells[col] = c
					}
				}
				h.Replay = nil
			}
		}
	}
	values := len(h.Cells) > 0 || h.Replay != nil || slices.ContainsFunc(h.Columns, func(c checkpoint.ColumnHold) bool { return !c.Discarded })
	if !values {
		h.Epochs = nil
	}
	if len(h.Columns) == 0 && !values {
		return nil
	}
	return h
}

func stillHeld(n uint64) string {
	if n == 1 {
		return "1 released value still doesn't fit"
	}
	return fmt.Sprintf("%d released values still don't fit", n)
}

// keepRefusal is the engine's cause for a column, with the destination's
// refusal from was, which the engine doesn't carry.
func keepRefusal(was, now *checkpoint.HoldCause) *checkpoint.HoldCause {
	switch {
	case now == nil:
		return was
	case was == nil:
		return now
	}
	c := *now
	if c.Kind == "" {
		c.Kind, c.Policy, c.With = was.Kind, was.Policy, was.With
	}
	if c.Kind == checkpoint.HoldRefused {
		c.Refusal, c.FromType, c.ToType, c.Fix = was.Refusal, was.FromType, was.ToType, was.Fix
	}
	return &c
}

func cloneHolds(h *checkpoint.Holds) *checkpoint.Holds {
	out := &checkpoint.Holds{
		Columns: slices.Clone(h.Columns),
		Cells:   maps.Clone(h.Cells),
		Epochs:  slices.Clone(h.Epochs),
	}
	if h.Replay != nil {
		r := *h.Replay
		r.Columns, r.Files, r.Held = slices.Clone(r.Columns), slices.Clone(r.Files), maps.Clone(r.Held)
		r.Versioned, r.CellReleases = slices.Clone(r.Versioned), maps.Clone(r.CellReleases)
		out.Replay = &r
	}
	return out
}

// refusedColumns are the columns a SchemaApplied refused, each with the
// field the destination last took, from applied.
func refusedColumns(a destination.SchemaApplied, applied map[string]*arrow.Schema) ([]RefusedColumn, error) {
	var out []RefusedColumn
	for _, r := range a.Refused {
		rc := RefusedColumn{RefusedColumn: r}
		if s := applied[r.Stream]; s != nil {
			if fs, ok := s.FieldsByName(r.Column); ok && len(fs) == 1 {
				b, err := encodeSchema(arrow.NewSchema(fs, nil))
				if err != nil {
					return nil, err
				}
				rc.Delivered = b
			}
		}
		out = append(out, rc)
	}
	return out, nil
}
