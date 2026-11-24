package runner

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/streamschema"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// HeldMode is what a held cell shows.
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
// delivers the rest of the row.
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
	// A release that a person discarded: a discard of its hold, unless the
	// replay in the record took it, as a run had started patching it in.
	// Records are where an attempt commits, so one that never recorded the
	// replay patched nothing in. Where the stream holds, that replay goes
	// on; a job in which it doesn't hold takes the release as a discard of
	// the replay.
	Ends bool
	// With Ends, the held cells of the column that the person saw when they
	// discarded it. Cells held after the discard aren't the person's to
	// drop: with more, the replay's cells stay held.
	EndsCells uint64
}

// RefusedError: the destination refused columns of an epoch's schemas
// before the epoch was recorded. RunAttempt starts another attempt that
// holds them, unless one can't be held.
type RefusedError struct {
	Columns []RefusedColumn
}

type RefusedColumn struct {
	destination.RefusedColumn
	// The field the destination has, as ColumnHold.Delivered.
	Delivered []byte
	// Why the column can't be held, as a clause, as notHeld has it; empty
	// if it can.
	NotHeld string
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

// unheld is the error for c, which can't be held: SCHEMA_DRIFT, with what
// the destination refused and how to make it take it.
func (c RefusedColumn) unheld() error {
	return errs.Wrap(errs.SchemaDrift, &RefusedError{Columns: []RefusedColumn{c}}, fmt.Sprintf("runner: column %s can't be held: %s", c.Column, c.NotHeld))
}

func (c RefusedColumn) cause() *checkpoint.HoldCause {
	return &checkpoint.HoldCause{Kind: checkpoint.HoldRefused, Refusal: c.Reason, FromType: c.FromType, ToType: c.ToType, Fix: c.Fix}
}

// holdsOn reports whether s, one of the destination's streams, holds in
// this job. A SQL transform's outputs hold; the outputs of the user's Spark
// or Flink code don't.
func (j *Job) holdsOn(s Stream) bool {
	return s.Holds != nil && j.HeldURL != "" && len(s.PrimaryKey) > 0 && !j.ownCode() &&
		j.Destination.Capabilities().Holds.Takes(s.Mode)
}

// notHeld says why s's column col, by its destination name, can't be held
// whole, as a clause, as far as the runner knows what the engine refuses;
// empty if it can. source is col's source name; empty: col's.
func notHeld(s Stream, limits destination.TableLimits, col, source string) string {
	if source == "" {
		source = col
	}
	shows := Held{}
	if s.Holds != nil {
		shows = s.Holds.Held
		if h, ok := s.Holds.Columns[source]; ok {
			shows = h
		}
	}
	switch {
	case slices.Contains(s.PrimaryKey, source):
		return "it's part of the stream's key"
	case s.CursorField != "" && s.CursorField == source:
		return "it's the stream's cursor"
	case slices.Contains(limits.Unholdable, col):
		return "the destination can't hold it"
	case slices.Contains(limits.NotNull, col) && (shows.Mode == HeldNull || shows.Value == ""):
		return "it can't be null, and the pipeline gives it no held value"
	}
	return ""
}

// markUnheld sets the NotHeld of each of cols, by limits, the destination's
// answer to schemas, the stream schemas it was given.
func (j *Job) markUnheld(cols []RefusedColumn, limits map[string]destination.TableLimits, schemas map[string][]byte) error {
	streams := j.sinkStreams()
	for i, r := range cols {
		k := slices.IndexFunc(streams, func(s Stream) bool { return s.Name == r.Stream })
		if k < 0 {
			continue
		}
		src, err := sourceName(schemas[r.Stream], r.Column)
		if err != nil {
			return fmt.Errorf("stream %s schema: %w", r.Stream, err)
		}
		cols[i].NotHeld = notHeld(streams[k], limits[r.Stream], r.Column, src)
	}
	return nil
}

// sourceName is the source name of col, a destination name in raw, a
// stream schema the engine reported; empty where it's col's, or raw is nil.
func sourceName(raw []byte, col string) (string, error) {
	if raw == nil {
		return "", nil
	}
	s, err := decodeSchema(raw)
	if err != nil {
		return "", err
	}
	names, err := streamschema.SourceNames(s)
	return names[col], err
}

// heldUnholdable is the error for a hold in force on s's column c, which
// can't be held, as why says: one recorded before the runner checked. It
// goes once the user releases it.
func heldUnholdable(s string, c checkpoint.ColumnHold, why string) error {
	then := "once the destination takes the column as the stream has it"
	if c.Cause != nil && c.Cause.Fix != "" {
		then = "once the destination takes the change (run this on it: " + c.Cause.Fix + ")"
	}
	return errs.New(errs.SchemaDrift, "runner: stream %s holds column %s (%s), but it can't be held: %s; %s, release the hold with `openetl holds release PIPELINE %s %s`",
		s, c.Column, c.Reason, why, then, s, c.Column)
}

func (j *Job) checkHolds() error {
	caps := j.Destination.Capabilities().Holds
	if j.transformed() {
		for _, s := range j.Streams {
			if s.Holds != nil {
				return fmt.Errorf("stream %q holds columns, and a transform's inputs can't be held", s.Name)
			}
		}
	}
	for _, s := range j.sinkStreams() {
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
		case j.ownCode():
			return fmt.Errorf("stream %q holds columns, and a Spark or Flink transform's outputs can't be held", s.Name)
		case !caps.Takes(s.Mode):
			return fmt.Errorf("stream %q holds columns, and the destination doesn't patch its write mode", s.Name)
		case last && (s.Mode != destination.Merge || !caps.Last):
			return fmt.Errorf("stream %q shows held cells' last values, which needs merge and a destination that keeps them", s.Name)
		}
	}
	return nil
}

var writeModes = map[destination.WriteMode]string{destination.Append: "append", destination.Overwrite: "overwrite", destination.Merge: "merge"}

// notHolding says why s, one of the destination's streams, doesn't hold in
// this job, as a clause.
func (j *Job) notHolding(s Stream) string {
	switch {
	case j.ownCode():
		return "a Spark or Flink transform writes it"
	case j.HeldURL == "":
		return "the runner has no storage for held values"
	case len(s.PrimaryKey) == 0:
		return "it has no primary key"
	case !j.Destination.Capabilities().Holds.Takes(s.Mode):
		return "the destination doesn't patch a stream it writes with " + writeModes[s.Mode]
	}
	return "it doesn't hold"
}

// keptHolds fails the attempt if restored has held values of a stream that
// doesn't hold in this job, as after a version that changes its write
// mode, key, destination or transform, or that doesn't read it, or a
// catalog without it: the next record would drop its holds, and their held
// files with them. The job's discards apply first, also those of a release
// being patched in, which only a job in which the stream holds can finish.
// A paused stream keeps its holds, and an overwritten one reads its values
// again.
func (j *Job) keptHolds(restored *checkpoint.Record) error {
	if restored == nil {
		return nil
	}
	keeps := map[string]bool{}
	for _, s := range j.Paused {
		keeps[s.Name] = true
	}
	why := map[string]string{}
	for _, s := range j.sinkStreams() {
		if j.holdsOn(s) || s.Mode == destination.Overwrite {
			keeps[s.Name] = true
		} else {
			why[s.Name] = j.notHolding(s)
		}
	}
	gone := "the pipeline doesn't read it now"
	switch {
	case j.ownCode():
		gone = "a Spark or Flink transform writes what the destination gets now"
	case j.transformed():
		gone = "no output of the SQL transform has its name"
	}
	names := slices.Concat(slices.Collect(maps.Keys(restored.Streams)), slices.Collect(maps.Keys(restored.Outputs)))
	slices.Sort(names)
	var problems []string
	for _, name := range slices.Compact(names) {
		if keeps[name] {
			continue
		}
		held, cols := heldValues(restored.HoldsOf(name), j.Releases, name)
		if len(held) == 0 {
			continue
		}
		reason, ok := why[name]
		if !ok {
			reason = gone
		}
		col := "COLUMN"
		if len(cols) == 1 {
			col = cols[0]
		}
		problems = append(problems, fmt.Sprintf("stream %s holds %s, and %s, so this run can't keep them; undo the change, which keeps them, "+
			"or discard them with `openetl holds discard PIPELINE %s %s`", name, strings.Join(held, " and "), reason, name, col))
	}
	if len(problems) == 0 {
		return nil
	}
	return errs.New(errs.SchemaDrift, "runner: %s", strings.Join(problems, "; "))
}

// heldValues are what h holds of stream's values once releases' discards
// apply, as phrases, and the columns a discard takes; none if it holds
// nothing. A discard, or a release a person discarded, names a hold by
// where it started, and doesn't apply to one that refused it. A release
// being patched in ends by a release it took that a person discarded,
// whatever holds its column has since. Its column's cells, which the API
// lists as being released, go with it, unless the column holds more than
// the person saw: some were held since.
func heldValues(h *checkpoint.Holds, releases []Release, stream string) (held, cols []string) {
	if h == nil {
		return nil, nil
	}
	discarded := func(col string, since checkpoint.RecordRef, refused *checkpoint.Refused) bool {
		return slices.ContainsFunc(releases, func(r Release) bool {
			return (r.Discard || r.Ends) && r.Stream == stream && r.Column == col && r.Since == since && (refused == nil || refused.Release != r.ID)
		})
	}
	replayed := func(col string) bool { return h.Replay != nil && slices.Contains(h.Replay.Columns, col) }
	// ended reports whether the replay of col ends, and the cells of col
	// held that the person saw.
	ended := func(col string) (bool, uint64) {
		var ends bool
		var seen uint64
		if !replayed(col) {
			return false, 0
		}
		for _, r := range releases {
			if r.Ends && r.Stream == stream && r.Column == col && h.Replay.Took(col, r.ID) {
				ends, seen = true, max(seen, r.EndsCells)
			}
		}
		return ends, seen
	}
	for _, c := range h.Columns {
		if !c.Discarded && !discarded(c.Column, c.Since, c.Refused) {
			held, cols = append(held, "column "+c.Column+" held whole"), append(cols, c.Column)
		}
	}
	for _, col := range slices.Sorted(maps.Keys(h.Cells)) {
		c := h.Cells[col]
		ends, seen := ended(col)
		if replayed(col) && (!ends || c.Cells > seen) || !replayed(col) && !discarded(col, c.Since, c.Refused) {
			held, cols = append(held, fmt.Sprintf("%d held cells of %s", c.Cells, col)), append(cols, col)
		}
	}
	if h.Replay != nil {
		var releasing []string
		for _, col := range h.Replay.Columns {
			if ends, _ := ended(col); !ends {
				releasing = append(releasing, col)
			}
		}
		if len(releasing) > 0 {
			held, cols = append(held, "the values of "+strings.Join(releasing, ", ")+" that a release is patching in"), append(cols, releasing...)
		}
	}
	slices.Sort(cols)
	return held, slices.Compact(cols)
}

// startHolds are the holds each holding stream starts the attempt with:
// restored's, with the job's releases applied and the refused columns
// held; and the dataflow's plans. A released column the destination
// refuses again keeps its hold, isn't patched in, and records the refused
// release, as do its released cells. It fails, as keptHolds does, if a
// stream that doesn't hold would drop restored's held values.
func (j *Job) startHolds(ctx context.Context, restored *checkpoint.Record, limits map[string]destination.TableLimits) (map[string]*checkpoint.Holds, map[string]*HoldPlan, error) {
	if err := j.keptHolds(restored); err != nil {
		return nil, nil, err
	}
	holds := map[string]*checkpoint.Holds{}
	plans := map[string]*HoldPlan{}
	for _, s := range j.sinkStreams() {
		if !j.holdsOn(s) {
			continue
		}
		h := &checkpoint.Holds{}
		if restored != nil && restored.HoldsOf(s.Name) != nil {
			h = cloneHolds(restored.HoldsOf(s.Name))
		}
		var released []string
		var taken, versioned []checkpoint.ColumnHold
		// The releases' IDs, by column.
		wholes, cells := map[string]string{}, map[string]string{}
		for _, r := range j.Releases {
			// Releases wait for a replay to end. One of the replay's that a
			// person discarded ends it only where the stream doesn't hold
			// (keptHolds); one that no replay took is a discard.
			if r.Stream != s.Name || h.Replay != nil {
				continue
			}
			// A release refused isn't taken again, nor its discard.
			again := func(f *checkpoint.Refused) bool { return f != nil && r.ID != "" && f.Release == r.ID && !r.Discard }
			discard := r.Discard || r.Ends
			if i := slices.IndexFunc(h.Columns, func(c checkpoint.ColumnHold) bool {
				return c.Column == r.Column && c.Since == r.Since && (!c.Discarded || !discard) && !again(c.Refused)
			}); i >= 0 {
				switch {
				case h.Columns[i].Discarded:
					// Released, a discarded hold ends without a replay, as its
					// values are gone; refused again, the column is held anew.
					h.Columns = slices.Delete(h.Columns, i, i+1)
				case discard:
					h.Columns[i].Discarded, h.Columns[i].Refused = true, nil
				default:
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
				if discard {
					delete(h.Cells, r.Column)
				} else {
					released = append(released, r.Column)
					cells[r.Column] = r.ID
				}
			}
		}
		// A hold in force on a column that can't be held, as one recorded
		// before the runner checked, stops the attempt until it's released.
		// A discarded one has no values left, so it goes, and the destination
		// gets the column as the stream has it.
		var raw []byte
		if restored != nil {
			raw = j.recordedSchemas(restored)[s.Name]
		}
		kept := h.Columns[:0]
		for _, c := range h.Columns {
			var src string
			if c.Cause != nil {
				src = c.Cause.Source
			}
			if src == "" {
				var err error
				if src, err = sourceName(raw, c.Column); err != nil {
					return nil, nil, fmt.Errorf("runner: stream %s schema: %w", s.Name, err)
				}
			}
			if why := notHeld(s, limits[s.Name], c.Column, src); why != "" {
				if c.Discarded {
					continue
				}
				return nil, nil, heldUnholdable(s.Name, c, why)
			}
			kept = append(kept, c)
		}
		h.Columns = kept
		for _, r := range j.refused {
			if r.Stream != s.Name {
				continue
			}
			if r.NotHeld == "" && slices.Contains(limits[s.Name].Unholdable, r.Column) {
				r.NotHeld = "the destination can't hold it"
			}
			if r.NotHeld != "" {
				return nil, nil, r.unheld()
			}
			is := func(col string) bool { return col == r.Column }
			released = slices.DeleteFunc(released, is)
			versioned = slices.DeleteFunc(versioned, func(c checkpoint.ColumnHold) bool { return is(c.Column) })
			why := "the destination refused the column: " + r.Reason
			if id, ok := cells[r.Column]; ok {
				c := h.Cells[r.Column]
				c.Refused = &checkpoint.Refused{Release: id, Reason: why}
				h.Cells[r.Column] = c
				delete(cells, r.Column)
			}
			c := checkpoint.ColumnHold{Column: r.Column, Delivered: r.Delivered}
			if i := slices.IndexFunc(taken, func(c checkpoint.ColumnHold) bool { return c.Column == r.Column }); i >= 0 {
				c = taken[i]
				c.Refused = &checkpoint.Refused{Release: wholes[r.Column], Reason: "the destination refused it again"}
			} else {
				c, _ = heldAgain(h, c, why, time.Now())
			}
			if slices.ContainsFunc(h.Columns, func(c checkpoint.ColumnHold) bool { return c.Column == r.Column }) {
				continue
			}
			c.Reason, c.Cause = "the destination refused it: "+r.Reason, r.cause()
			h.Columns = append(h.Columns, c)
		}
		if len(released) > 0 {
			slices.Sort(released)
			released = slices.Compact(released)
			// The replay reads the epochs from the oldest start of the holds
			// it ends: each released column's hold held whole, and its cell
			// hold, released or not, which the replay clears once every cell
			// fits. A discarded cell hold is gone by now, and isn't read.
			var from checkpoint.RecordRef
			// Where each column's hold held whole starts, and its cell hold;
			// absent for one the replay doesn't end.
			whole, cellHolds := map[string]checkpoint.RecordRef{}, map[string]checkpoint.RecordRef{}
			for i, col := range released {
				var starts []checkpoint.RecordRef
				if k := slices.IndexFunc(taken, func(c checkpoint.ColumnHold) bool { return c.Column == col }); k >= 0 {
					whole[col] = taken[k].Since
					starts = append(starts, taken[k].Since)
				}
				if c, ok := h.Cells[col]; ok {
					cellHolds[col] = c.Since
					starts = append(starts, c.Since)
				}
				for k, at := range starts {
					if i == 0 && k == 0 || before(at, from) {
						from = at
					}
				}
			}
			files, err := j.Log.HeldFiles(ctx, s.Name, heldFrom(h.Epochs, from))
			if err != nil {
				return nil, nil, err
			}
			h.Replay = &checkpoint.Replay{Columns: released, Files: files, Versioned: versioned}
			if len(whole) > 0 {
				h.Replay.Since = whole
			}
			for col := range whole {
				if id := wholes[col]; id != "" {
					if h.Replay.WholeReleases == nil {
						h.Replay.WholeReleases = map[string]string{}
					}
					h.Replay.WholeReleases[col] = id
				}
			}
			// A column isn't read from the files before its hold of a kind
			// started, or from any for a kind it holds nothing of: those
			// may hold values of an earlier hold of it, patched in since, or
			// discarded.
			first := func(starts map[string]checkpoint.RecordRef) map[string]int {
				var out map[string]int
				for _, col := range released {
					n := len(files)
					if at, ok := starts[col]; ok {
						n = slices.IndexFunc(files, func(rel string) bool {
							ref, ok := checkpoint.HeldFileRef(rel)
							return !ok || !before(ref, at)
						})
						if n < 0 {
							n = len(files)
						}
					}
					if n > 0 {
						if out == nil {
							out = map[string]int{}
						}
						out[col] = n
					}
				}
				return out
			}
			h.Replay.First, h.Replay.FirstCells = first(whole), first(cellHolds)
			if len(cells) > 0 {
				h.Replay.CellReleases = cells
			}
		}
		holds[s.Name] = h
		plans[s.Name] = &HoldPlan{StreamHolds: *s.Holds, Holding: h.Columns, Limits: limits[s.Name], Replay: h.Replay}
	}
	return holds, plans, nil
}

// heldAgain takes c's column out of h's replay, as the column is held
// whole again while the replay patches it in, and returns c as the hold
// whole it gets: from where the hold the replay released started, if it
// released one, refused by that release, so a later release reads every
// value held since then, and the refused one isn't taken again. The
// column's released cells, which the replay doesn't patch in now, are
// refused by their release. why says why. Not ok if the replay doesn't
// patch the column in.
func heldAgain(h *checkpoint.Holds, c checkpoint.ColumnHold, why string, now time.Time) (checkpoint.ColumnHold, bool) {
	r := h.Replay
	if r == nil || !slices.Contains(r.Columns, c.Column) {
		return c, false
	}
	if r.Columns = slices.DeleteFunc(r.Columns, func(col string) bool { return col == c.Column }); len(r.Columns) == 0 {
		h.Replay = nil
	}
	if id := r.CellReleases[c.Column]; id != "" {
		if cells, ok := h.Cells[c.Column]; ok {
			cells.Refused = &checkpoint.Refused{Release: id, Reason: why}
			h.Cells[c.Column] = cells
		}
	}
	if at, ok := r.Since[c.Column]; ok {
		// The replay doesn't keep when the hold started.
		c.Since, c.Started = at, now
		if id := r.WholeReleases[c.Column]; id != "" {
			c.Refused = &checkpoint.Refused{Release: id, Reason: why}
		}
	}
	return c, true
}

// heldFrom are the epochs of es from at on.
func heldFrom(es []checkpoint.HeldEpochs, at checkpoint.RecordRef) []checkpoint.HeldEpochs {
	var out []checkpoint.HeldEpochs
	for _, e := range es {
		switch {
		case e.Generation < at.Generation, e.Generation == at.Generation && e.To < at.Epoch:
			continue
		case e.Generation == at.Generation && e.From < at.Epoch:
			e.From = at.Epoch
		}
		out = append(out, e)
	}
	return out
}

func before(a, b checkpoint.RecordRef) bool {
	return a.Generation < b.Generation || a.Generation == b.Generation && a.Epoch < b.Epoch
}

// mergeHolds is a stream's holds once epoch at is recorded: base's, with
// what the dataflow reported at the stream's barrier. delivered is the
// stream's schema as the destination has it there, which gives a column
// held again the field the destination has. Nil once nothing is held.
func mergeHolds(base *checkpoint.Holds, r *ReadyHolds, at checkpoint.RecordRef, now time.Time, delivered []byte) *checkpoint.Holds {
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
			} else {
				// The engine held a column whole again while its release is
				// patched in, as its schema changed again: the replay skips
				// its values from then on.
				c, _ = heldAgain(h, c, "the column was held whole again while the release patched it in", now)
			}
			cols = append(cols, c)
		}
		// One the engine doesn't hold yet, as one held again when a replay
		// ended, stays held; the engine holds it from the next attempt.
		for _, p := range h.Columns {
			if !slices.ContainsFunc(cols, func(c checkpoint.ColumnHold) bool { return c.Column == p.Column }) {
				cols = append(cols, p)
			}
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
				h.Columns = append(h.Columns, leftHeld(h, p.StillHeld, now, delivered)...)
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

// leftHeld are the columns that h's replay released whole, held again as
// it ends with n of its values that still don't fit: from where their holds
// started, refused by their releases, as values of theirs are left in the
// held files, which nothing else would keep. The engine counts the values
// left of all the replay's columns in one, so any held again, though only
// some may have values left; the next release patches in again, by their
// IDs, those that fit. A count by column would hold only those.
func leftHeld(h *checkpoint.Holds, n uint64, now time.Time, delivered []byte) []checkpoint.ColumnHold {
	if n == 0 {
		return nil
	}
	var out []checkpoint.ColumnHold
	for _, col := range h.Replay.Columns {
		at, ok := h.Replay.Since[col]
		if !ok || slices.ContainsFunc(h.Columns, func(c checkpoint.ColumnHold) bool { return c.Column == col }) {
			continue
		}
		c := checkpoint.ColumnHold{Column: col, Delivered: fieldOf(delivered, col)}
		if i := slices.IndexFunc(h.Replay.Versioned, func(v checkpoint.ColumnHold) bool { return v.Column == col }); i >= 0 {
			c = h.Replay.Versioned[i]
		}
		c.Since, c.Started, c.Reason = at, now, "its released values don't all fit its type"
		if id := h.Replay.WholeReleases[col]; id != "" {
			c.Refused = &checkpoint.Refused{Release: id, Reason: stillHeld(n)}
		}
		out = append(out, c)
	}
	return out
}

// fieldOf is col's field in schema, an Arrow IPC stream, as a schema with
// only it; nil if schema has none.
func fieldOf(schema []byte, col string) []byte {
	if schema == nil {
		return nil
	}
	s, err := decodeSchema(schema)
	if err != nil {
		return nil
	}
	fs, ok := s.FieldsByName(col)
	if !ok || len(fs) != 1 {
		return nil
	}
	b, err := encodeSchema(arrow.NewSchema(fs, nil))
	if err != nil {
		return nil
	}
	return b
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
		r.Versioned, r.CellReleases, r.WholeReleases = slices.Clone(r.Versioned), maps.Clone(r.CellReleases), maps.Clone(r.WholeReleases)
		r.First, r.FirstCells, r.Since = maps.Clone(r.First), maps.Clone(r.FirstCells), maps.Clone(r.Since)
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
