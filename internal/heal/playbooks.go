package heal

import (
	"fmt"
	"strings"
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
		return fmt.Sprintf("A schema change in stream %s of %s is blocked by its schema policy", o.Stream, name),
			"To take the change, apply the pipeline with a schemaPolicy that allows it (allow_columns takes new columns, allow_all " +
				"also widens types), " + resume + ". The incident resolves once a sync succeeds."
	case PositionLost:
		return fmt.Sprintf("%s's source no longer has the position its read resumes from", name),
			"The read can't go on from where it was: a replication slot was lost, or the source discarded what the read hadn't " +
				"acknowledged. To read every stream again from the start (for Postgres, a new slot and snapshot), delete the " +
				"pipeline and apply it again. Changes made in the meantime arrive as current rows, not as change history."
	case RateLimited:
		return fmt.Sprintf("%s's source kept limiting its requests, past every retry", name),
			"Sync less often (schedule.every), select fewer streams, or ask the API's owner for a higher limit. The incident resolves once a sync succeeds."
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
		return fmt.Sprintf("Stream %s of %s quarantined %s of its rows in the last hour", o.Stream, name, o.Details["share"]),
			"The rows are under _quarantine in the data plane's storage, each with the reason it was refused: a value its " +
				"column's type can't hold, such as a new enum value or a date in another format. The incident resolves once " +
				"the share falls under half the threshold."
	}
	if o.Code != "" {
		return fmt.Sprintf("A sync of %s failed (%s)", name, o.Code), fmt.Sprintf("See the sync's log (%s). The incident resolves once a sync succeeds.", logs)
	}
	return fmt.Sprintf("A sync of %s failed", name), fmt.Sprintf("See the sync's log (%s). The incident resolves once a sync succeeds.", logs)
}
