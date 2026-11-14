package plan

import (
	"fmt"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// Transform kinds, as WorkerJob names them.
const (
	TransformOwnCode = "own code"
	TransformSQL     = "sql"
)

// WorkerJob is what decides whether a job runs on workers.
type WorkerJob struct {
	Workers int
	// TransformOwnCode for a Spark or Flink transform, TransformSQL, or
	// empty for none.
	Transform string
	Caps      destination.Capabilities
	// The engine writes the destination's data.
	EngineWritten bool
	// One of the destination's streams that holds columns or cells; empty if
	// none does.
	Holding string
}

// OnePod says why a job that asks for workers runs on one pod; empty if it
// can run on them. What needs rows of one key to meet across workers, or one
// writer, stays on one pod.
func OnePod(j WorkerJob) string {
	caps := j.Caps
	switch {
	case j.Transform == TransformOwnCode:
		return "a Spark or Flink transform runs on one pod"
	case j.Transform == TransformSQL:
		return "a SQL transform runs on one pod"
	case caps.Lanes() == 1 && j.Workers > 1:
		return "the destination declares one writer lane"
	case caps.Lanes() < j.Workers:
		return fmt.Sprintf("the destination takes %d writer lanes, fewer than %d workers", caps.Lanes(), j.Workers)
	case caps.Class == destination.TransactionalStream:
		return "a transactional destination's writer lanes commit on their own, which only one pod recovers"
	case caps.Delivery == destination.DeliveryStaged:
		return "a destination that loads staged epochs runs on one pod"
	case j.EngineWritten:
		return "a destination the engine writes runs on one pod"
	case j.Holding != "":
		return fmt.Sprintf("stream %q holds columns, which runs on one pod", j.Holding)
	}
	return ""
}

// OneWorker says why an attempt on workers reads all of a stream's splits on
// one worker; empty if it spreads them.
func OneWorker(mode destination.WriteMode, keyed, splitsByKey bool, read apiv1.SyncMode) string {
	switch {
	case mode == destination.Overwrite:
		return "an overwrite's read ends on one worker"
	case keyed && read == apiv1.SyncMode_SYNC_MODE_CDC:
		return "the changes to each key must reach one writer lane in order"
	case keyed && !splitsByKey:
		return "the source's splits don't partition it by key, and the rows of each key must meet on one writer lane"
	}
	return ""
}

// Workers plans a job's workers: how many each attempt runs on (0: one pod),
// why it runs on one pod although its spec asks for more, and a warning for
// each of the destination's streams an attempt reads on one worker. Without
// a catalog, it can't tell which streams hold columns or stay on one worker.
func Workers(spec *apiv1.PipelineSpec, cat *apiv1.Catalog, caps destination.Capabilities, engineWritten bool) (int, string, []*apiv1.PlanWarning) {
	asked := int(spec.GetResources().GetWorkers())
	if asked <= 1 {
		return 0, "", nil
	}
	j := WorkerJob{Workers: asked, Caps: caps, EngineWritten: engineWritten}
	switch {
	case ownCode(spec):
		j.Transform = TransformOwnCode
	case spec.GetTransform().GetSql() != nil:
		j.Transform = TransformSQL
	}
	var oneWorker []*apiv1.PlanWarning
	if j.Transform == "" {
		for _, s := range selected(spec, cat) {
			keyed := len(s.cs.GetPrimaryKey()) > 0
			read := ReadMode(spec, s.cs.GetSyncModes(), s.ps)
			mode := StreamMode(spec, caps, read, keyed)
			if j.Holding == "" && Holds(caps, mode, keyed) {
				j.Holding = s.cs.GetName()
			}
			if why := OneWorker(mode, keyed, s.cs.GetSplitsByKey(), read); why != "" {
				oneWorker = append(oneWorker, &apiv1.PlanWarning{Stream: s.cs.GetName(), Message: fmt.Sprintf(
					"each attempt reads it on one of its %d workers: %s", asked, why)})
			}
		}
	}
	if why := OnePod(j); why != "" {
		return 0, why, nil
	}
	return asked, "", oneWorker
}
