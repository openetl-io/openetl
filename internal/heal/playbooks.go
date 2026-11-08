package heal

import (
	"fmt"
	"strings"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

// pauses reports whether the class's playbook pauses the pipeline: its
// syncs would fail the same way until someone changes something.
func pauses(c Class) bool {
	return c == Auth || c == SchemaBlocked || c == PositionLost
}

// describe is the incident's summary, and what to do about it.
func describe(o Observation, t target) (summary, next string) {
	name, ws := t.workspace+"/"+t.pipeline, "--workspace "+t.workspace+" "
	if t.workspace == "default" {
		name, ws = t.pipeline, ""
	}
	resume := fmt.Sprintf("then resume it (openetl resume %s%s)", ws, t.pipeline)
	logs := fmt.Sprintf("openetl logs %s%s", ws, t.pipeline)
	if o.Sync != nil {
		logs = fmt.Sprintf("openetl logs %s--sync %s %s", ws, o.Sync, t.pipeline)
	}
	switch o.Class {
	case Auth:
		var secrets []string
		for _, s := range append(append([]string(nil), t.sourceSecrets...), t.destinationSecrets...) {
			secrets = append(secrets, "openetl secret set "+s)
		}
		fix := "Fix the credentials in the source's or destination's config and apply it"
		if len(secrets) > 0 {
			fix = "Update the credentials the source or destination uses (" + strings.Join(secrets, ", ") + ")"
		}
		return fmt.Sprintf("%s's credentials were refused (%s)", name, o.Code),
			fix + ", " + resume + ". The incident resolves once a sync succeeds."
	case SchemaBlocked:
		return fmt.Sprintf("A schema change in stream %s of %s can't be taken", o.Stream, name),
			"The error says what changed and what stopped it. If it's the schema policy, apply the pipeline with a schemaPolicy " +
				"that takes the change (allow_columns takes new columns and widenings, allow_all every change). If the destination " +
				"refused it, make the destination take it, with the statement the error gives if there is one. Then " +
				strings.TrimPrefix(resume, "then ") + ". The incident resolves once a sync succeeds."
	case PositionLost:
		return fmt.Sprintf("%s's source no longer has the position its read resumes from", name),
			"The read can't go on from where it was: a replication slot was lost, or the source discarded what the read hadn't " +
				"acknowledged. To read every stream again from the start (for Postgres, a new slot and snapshot), delete the " +
				"pipeline and apply it again. Changes made in the meantime arrive as current rows, not as change history."
	case RateLimited:
		slow := "After 3 syncs in a row fail this way, and every 3 after, heal slows the pipeline down: it halves a declarative " +
			"source's declared rate limit, down to an eighth, and doubles how often syncs start (schedule.every), up to a day, " +
			"if there's no rate limit left to lower or the sync ran that long. "
		if t.mode == apiv1.HealMode_HEAL_MODE_OFF {
			slow = ""
		}
		return fmt.Sprintf("%s's source kept limiting its requests, past every retry", name),
			slow + "To fix it yourself, sync less often, select fewer streams, or ask the API's owner for a higher limit. " +
				fmt.Sprintf("What heal changed stays until you remove it (openetl overrides remove %s%s FIELD), or apply a spec that changes the field. ", ws, t.pipeline) +
				"The incident resolves once a sync succeeds."
	case CrashLoop:
		return fmt.Sprintf("%s's job keeps failing without committing", name),
			fmt.Sprintf("It's retried, up to every 5 minutes, until it commits. Its log has each failure (%s).", logs)
	case Lagging:
		return fmt.Sprintf("%s's read is behind its source", name),
			"Look for a slow destination, a runner short of CPU, or a burst of changes at the source. The incident resolves once the read is back within half the threshold."
	case RetentionAtRisk:
		return fmt.Sprintf("%s's read has used %s of what its source keeps for it", name, o.Details["used"]),
			"If the read doesn't catch up before the source discards the rest, it loses its position and needs a new start. " +
				"Resume the pipeline if it's paused, and look for what holds the read back. The incident resolves under 40%."
	case Quarantine:
		where := "The rows are under _quarantine in the data plane's storage"
		if f := o.Details["quarantine_file"]; f != "" {
			where += " (the newest: " + f + ")"
		}
		next := where + ", each with the reason it was refused: a value its column's type can't hold, such as a new enum " +
			"value or a date in another format. Fix the source's data, or give the column a type that holds it."
		resumeStream := fmt.Sprintf("openetl resume %s--stream %s %s", ws, o.Stream, t.pipeline)
		switch s, why := quarantineStops(o, t); {
		case t.mode == apiv1.HealMode_HEAL_MODE_OFF:
		case s == stopStream && t.mode == apiv1.HealMode_HEAL_MODE_SUGGEST:
			next += fmt.Sprintf(" To keep it from quarantining more meanwhile, pause the stream (openetl pause %s--stream %s %s), and resume it after (%s).", ws, o.Stream, t.pipeline, resumeStream)
		case s == stopStream:
			next += " Heal paused the stream, so it quarantines no more rows while the pipeline's other streams carry on. Resume it once it's fixed (" + resumeStream + ")."
		case s == stopPipeline && t.mode == apiv1.HealMode_HEAL_MODE_SUGGEST:
			next += " To keep it from quarantining more meanwhile, pause the pipeline, as it reads only this stream, and resume it after (openetl resume " + ws + t.pipeline + ")."
		case s == stopPipeline:
			next += " Heal paused the pipeline, as it reads only this stream. " + strings.ToUpper(resume[:1]) + resume[1:] + " once it's fixed."
		default:
			next += " Heal can't pause the stream on its own: " + why + "."
		}
		return fmt.Sprintf("Stream %s of %s quarantined %s of its rows in the last hour", o.Stream, name, o.Details["share"]),
			next + " The incident resolves once the stream's share falls under half the threshold."
	case Held:
		next := fmt.Sprintf("The rest of each row is delivered, and the held values are kept (openetl holds %s%s).", ws, t.pipeline)
		for _, h := range o.Holds {
			if steps := holdSteps(h, ws, t.pipeline); steps != "" {
				next += " For " + h.GetColumn() + ": " + steps
			}
		}
		if len(o.Holds) == 0 {
			next += fmt.Sprintf(" Once the destination can take the change, release each one with openetl holds release %s%s %s COLUMN, "+
				"which patches the held values in; or drop them with openetl holds discard.", ws, t.pipeline, o.Stream)
		}
		return fmt.Sprintf("Stream %s of %s holds back %s", o.Stream, name, o.Details["held"]),
			next + " The incident turns critical after a day, and resolves when the stream holds nothing."
	}
	if o.Code != "" {
		return fmt.Sprintf("A sync of %s failed (%s)", name, o.Code), fmt.Sprintf("See the sync's log (%s). The incident resolves once a sync succeeds.", logs)
	}
	return fmt.Sprintf("A sync of %s failed", name), fmt.Sprintf("See the sync's log (%s). The incident resolves once a sync succeeds.", logs)
}

// holdSteps are h's ways out, as commands.
func holdSteps(h *apiv1.Hold, ws, pipeline string) string {
	var steps []string
	for _, w := range h.GetWaysOut() {
		var sub string
		switch w.GetAction() {
		case apiv1.HoldWayOut_ACTION_RELEASE:
			sub = "release"
		case apiv1.HoldWayOut_ACTION_RELEASE_NEW_VERSION:
			sub = "release --new-version"
		case apiv1.HoldWayOut_ACTION_DISCARD:
			sub = "discard"
		default:
			continue
		}
		cmd := fmt.Sprintf("openetl holds %s %s%s %s %s", sub, ws, pipeline, h.GetStream(), h.GetColumn())
		first := w.GetFirst()
		if w.GetStatement() != "" {
			first = strings.TrimSuffix(first, ".") + ": " + w.GetStatement() + "."
		}
		step := cmd + "."
		if e := w.GetEffect(); e != "" {
			step = cmd + ": " + strings.ToLower(e[:1]) + e[1:]
		}
		if first != "" {
			step = first + " Then " + cmd + "."
		}
		if len(steps) > 0 {
			step = "Or " + step
		}
		steps = append(steps, step)
	}
	return strings.Join(steps, " ")
}
