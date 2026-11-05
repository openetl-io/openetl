package api

import (
	"fmt"
	"regexp"
	"strconv"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
)

// explain sets h's reason and ways out from its cause. A column hold
// without a cause, from an older record, keeps its reason.
func explain(h *apiv1.Hold) {
	c := h.GetCause()
	if !h.GetWhole() {
		if c == nil {
			c = &apiv1.HoldCause{Value: h.GetReason()}
		}
		c.Kind = apiv1.HoldCause_KIND_VALUE
		if c.Value == "" {
			c.Value = h.GetReason()
		}
		h.Cause = c
	}
	if r := holdReason(h); r != "" {
		h.Reason = r
	}
	if !h.GetDiscarded() && !h.GetReleasing() {
		h.WaysOut = waysOut(h)
	}
}

// holdReason is what's held and why, as a sentence; empty without a cause.
func holdReason(h *apiv1.Hold) string {
	c, col := h.GetCause(), h.GetColumn()
	change := func() string {
		var s string
		switch {
		case c.GetFrom() == "" && c.GetTo() != "":
			s = fmt.Sprintf("%s is a new %s column", col, c.GetTo())
		case c.GetFrom() == "":
			s = col + " is a new column"
		case c.GetFrom() != c.GetTo():
			s = fmt.Sprintf("%s changed from %s to %s", col, c.GetFrom(), c.GetTo())
		default:
			s = col + "'s type changed"
		}
		switch {
		case c.GetFromType() != "" && c.GetToType() != "":
			s += fmt.Sprintf(" (%s to %s in the destination)", c.GetFromType(), c.GetToType())
		case c.GetToType() != "":
			s += fmt.Sprintf(" (%s in the destination)", c.GetToType())
		}
		return s
	}
	switch c.GetKind() {
	case apiv1.HoldCause_KIND_POLICY:
		return fmt.Sprintf("%s, and schema policy %s doesn't take it.", change(), c.GetPolicy())
	case apiv1.HoldCause_KIND_REFUSED:
		return fmt.Sprintf("%s, and the destination refused it: %s.", change(), c.GetRefusal())
	case apiv1.HoldCause_KIND_FIXED:
		return change() + ", and the destination can't change its type."
	case apiv1.HoldCause_KIND_COLUMN_LIMIT:
		return change() + ", and the destination takes no more columns."
	case apiv1.HoldCause_KIND_GROUP:
		return fmt.Sprintf("%s is held with %s, which is held.", col, c.GetWith())
	case apiv1.HoldCause_KIND_VALUE:
		n := "1 value"
		if h.GetCells() != 1 {
			n = strconv.FormatUint(h.GetCells(), 10) + " values"
		}
		return fmt.Sprintf("%s of %s don't fit its type (%s).", n, col, c.GetValue())
	}
	return ""
}

// waysOut are the ways to clear h, the likeliest first.
func waysOut(h *apiv1.Hold) []*apiv1.HoldWayOut {
	c, col := h.GetCause(), h.GetColumn()
	patched := "Patches its held values in, and delivers the column again from the next run."
	if !h.GetWhole() {
		patched = "Patches the held values in."
	}
	release := &apiv1.HoldWayOut{Action: apiv1.HoldWayOut_ACTION_RELEASE, Effect: patched}
	switch c.GetKind() {
	case apiv1.HoldCause_KIND_POLICY:
		release.Effect = "Approves the change: the destination takes it, and the held values are patched in."
	case apiv1.HoldCause_KIND_REFUSED, apiv1.HoldCause_KIND_FIXED:
		to := c.GetToType()
		if to == "" {
			to = c.GetTo()
		}
		switch {
		case c.GetFix() != "":
			release.First, release.Statement = "Run this on the destination, so it takes the change.", c.GetFix()
		case to != "" && c.GetFrom() != "":
			release.First = fmt.Sprintf("Change %s's type on the destination to %s.", col, to)
		case to != "":
			release.First = fmt.Sprintf("Make the destination take %s as a %s column.", col, to)
		default:
			release.First = fmt.Sprintf("Change %s on the destination so it takes the change.", col)
		}
	case apiv1.HoldCause_KIND_COLUMN_LIMIT:
		release.First = "Make room on the destination: drop columns its table doesn't need."
	case apiv1.HoldCause_KIND_GROUP:
		release.First = fmt.Sprintf("Clear %s's hold, which this one is held with.", c.GetWith())
	case apiv1.HoldCause_KIND_VALUE:
		source := c.GetSourceColumn()
		if source == "" {
			source = col
		}
		release.First = fmt.Sprintf("Give %s a type its values fit: a type override in the pipeline's spec "+
			"(columns.%s.type: string or float64), or a wider type on the destination.", col, source)
	}
	out := []*apiv1.HoldWayOut{release}
	if h.GetWhole() && c.GetFrom() != "" && c.GetKind() != apiv1.HoldCause_KIND_GROUP {
		out = append(out, &apiv1.HoldWayOut{
			Action: apiv1.HoldWayOut_ACTION_RELEASE_NEW_VERSION,
			Effect: fmt.Sprintf("Sends its held values, and those after, to a new column, %s, and leaves %s as the destination has it.", nextVersion(col), col),
		})
	}
	discard := &apiv1.HoldWayOut{Action: apiv1.HoldWayOut_ACTION_DISCARD, Effect: "Drops its held values; the column stays as the destination has it."}
	if !h.GetWhole() {
		discard.Effect = "Drops the held values; the cells stay as they are."
	}
	return append(out, discard)
}

var columnVersion = regexp.MustCompile(`^(.+)__v(\d+)$`)

// nextVersion is the column a release to a new version sends col's values
// to.
func nextVersion(col string) string {
	if m := columnVersion.FindStringSubmatch(col); m != nil {
		if k, err := strconv.Atoi(m[2]); err == nil {
			return fmt.Sprintf("%s__v%d", m[1], k+1)
		}
	}
	return col + "__v2"
}
