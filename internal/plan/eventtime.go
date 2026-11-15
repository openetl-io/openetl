package plan

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

// IdleAfter is how long a split of a continuous read with event time has
// nothing to read before its watermark moves on with the clock.
const IdleAfter = time.Minute

// OrderedRead says a stream read with read, by cursor, has its rows in the
// order of column's values: a log, or a cursor that's the column.
func OrderedRead(read apiv1.SyncMode, cursor, column string) bool {
	switch read {
	case apiv1.SyncMode_SYNC_MODE_CDC, apiv1.SyncMode_SYNC_MODE_STREAM:
		return true
	case apiv1.SyncMode_SYNC_MODE_INCREMENTAL:
		return cursor != "" && strings.EqualFold(cursor, column)
	}
	return false
}

// EventTimes says, for each stream of a pipeline with a SQL transform that
// has event time, which column it is and how its watermark goes.
func EventTimes(spec *apiv1.PipelineSpec, cat *apiv1.Catalog) []string {
	if spec.GetTransform().GetSql() == nil {
		return nil
	}
	var out []string
	for _, ps := range spec.GetSource().GetStreams() {
		column := ps.GetEventTime()
		if column == "" {
			continue
		}
		i := slices.IndexFunc(cat.GetStreams(), func(cs *apiv1.CatalogStream) bool { return cs.GetName() == ps.GetName() })
		if i < 0 {
			continue
		}
		cs := cat.GetStreams()[i]
		read := ReadMode(spec, cs.GetSyncModes(), ps)
		if !OrderedRead(read, cmp.Or(ps.GetCursor(), cs.GetCursorField()), column) {
			out = append(out, fmt.Sprintf("stream %s: event time %s; read %s, not in its order, so its watermark holds until each read ends", ps.GetName(), column, syncName(read)))
			continue
		}
		delay := ps.GetWatermark().GetDelay().AsDuration()
		note := fmt.Sprintf("stream %s: event time %s; each split's watermark trails the latest it has read by %s", ps.GetName(), column, delay)
		if Continuous(spec) {
			note += fmt.Sprintf(", and moves on with the clock after %s with nothing to read", IdleAfter)
		}
		out = append(out, note)
	}
	return out
}
