package runner

import (
	"slices"
	"strings"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

const versionsPrefix = "versions."

// VersionsOperator is the operator whose state keeps the versions of
// stream's records: by record ID, the hash of the version that holds it,
// so that another version read at the same cursor value gets an ID of its
// own (Plan.Versions).
func VersionsOperator(stream string) string { return versionsPrefix + stream }

func isVersions(op string) bool { return strings.HasPrefix(op, versionsPrefix) }

// versions are the operators that keep the versions of the destination's
// streams' records, by stream: those with a primary key read with a
// cursor, which the destination dedupes by _openetl_id against what it
// has, so that a version read again at the cursor's floor with other values
// would be skipped. Only where the job keeps state, and not past a
// transform, SQL or the user's code: an output named as its input, as
// one Spark code doesn't declare, isn't the input, and the plan keeps
// none for it.
func (j *Job) versions() map[string]string {
	if j.StateURL == "" || j.transformed() {
		return nil
	}
	caps := j.Destination.Capabilities()
	var out map[string]string
	for _, s := range j.Streams {
		if len(s.PrimaryKey) == 0 || s.Read != source.Incremental || s.CursorField == "" || s.Transformed {
			continue
		}
		if delivery.Dedupe(caps, s.delivery(), j.Settings.Require, false) != destination.DedupeTarget {
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
		out[s.Name] = VersionsOperator(s.Name)
	}
	return out
}

// versionsState is snapshots as an attempt's engine restores them: without
// the versions' state of streams not in versions, which is for no operator
// of the attempt, and with that of those in versions from from.
func versionsState(snapshots []checkpoint.Snapshot, versions map[string]string, from []checkpoint.Snapshot) []checkpoint.Snapshot {
	out := slices.DeleteFunc(slices.Clone(snapshots), func(s checkpoint.Snapshot) bool { return isVersions(s.Operator) })
	for _, s := range from {
		if isVersions(s.Operator) && versions[strings.TrimPrefix(s.Operator, versionsPrefix)] == s.Operator {
			out = append(out, s)
		}
	}
	return out
}
