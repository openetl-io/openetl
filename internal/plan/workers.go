package plan

import (
	"cmp"
	"fmt"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// WorkerJob is what decides whether a job runs on workers.
type WorkerJob struct {
	Workers int
	// A Spark or Flink transform.
	OwnCode bool
	Caps    destination.Capabilities
	// The engine writes the destination's data.
	EngineWritten bool
}

// OnePod says why a job that asks for workers runs on one pod; empty if it
// can run on them. What needs one writer, or what only one pod runs, stays
// on one pod. A SQL transform runs on workers, a group window's too, whose
// rows the workers' engines exchange by key group to the keyed lanes that
// keep its state; and so do holds: the attempt's coordinator owns each
// stream's schema and what it holds.
func OnePod(j WorkerJob) string {
	caps := j.Caps
	switch {
	case j.OwnCode:
		return "a Spark or Flink transform runs on one pod"
	case caps.Lanes() == 1 && j.Workers > 1:
		return "the destination declares one writer lane"
	case caps.Lanes() < j.Workers:
		return fmt.Sprintf("the destination takes %d writer lanes, fewer than %d workers", caps.Lanes(), j.Workers)
	case caps.Class == destination.TransactionalStream && !caps.LaneCommits:
		return "a transactional destination runs on one pod unless its writer lanes commit on their own"
	case caps.Delivery == destination.DeliveryStaged:
		return "a destination that loads staged epochs runs on one pod"
	case j.EngineWritten:
		return "a destination the engine writes runs on one pod"
	}
	return ""
}

// Why an attempt on workers reads all of a stream's splits on one worker.
const (
	whyOverwrite = "an overwrite's read ends on one worker"
	whyChanges   = "the source reads its changes from one log, with those of every stream read with cdc, which one worker reads"
)

// OneWorker says why an attempt on workers reads all of a stream's splits on
// one worker, where it's written in mode, or a SQL statement writes an
// output from it in mode; empty if it spreads them. An exchange before the
// writer sends each row to the worker whose writer lane owns its key group,
// so a keyed stream whose splits don't partition it by key spreads too.
func OneWorker(mode destination.WriteMode, read apiv1.SyncMode) string {
	switch {
	case mode == destination.Overwrite:
		return whyOverwrite
	case read == apiv1.SyncMode_SYNC_MODE_CDC:
		return whyChanges
	}
	return ""
}

// Workers plans a job's workers: how many each attempt runs on (0: one pod),
// why it runs on one pod although its spec asks for more, and a warning for
// each stream an attempt reads on one worker. Without a catalog it can't
// tell which streams one worker reads, nor with a SQL transform without
// the statements' plan, a.
func Workers(spec *apiv1.PipelineSpec, cat *apiv1.Catalog, caps destination.Capabilities, engineWritten bool, a *sqlplan.Answer) (int, string, []*apiv1.PlanWarning) {
	asked := int(spec.GetResources().GetWorkers())
	if asked <= 1 {
		return 0, "", nil
	}
	j := WorkerJob{Workers: asked, Caps: caps, EngineWritten: engineWritten, OwnCode: ownCode(spec)}
	if why := OnePod(j); why != "" {
		return 0, why, nil
	}
	sql := spec.GetTransform().GetSql() != nil
	if sql && a == nil {
		return asked, "", nil
	}
	whys := map[string]string{}
	var inputs []Input
	for _, s := range selected(spec, cat) {
		name := s.cs.GetName()
		keyed := len(s.cs.GetPrimaryKey()) > 0
		read := ReadMode(spec, s.cs.GetSyncModes(), s.ps)
		if sql {
			inputs = append(inputs, Input{Name: name, PrimaryKey: s.cs.GetPrimaryKey(), Read: read, Replay: Replay(replay(s.cs.GetReplay()), read),
				Cursor: cmp.Or(s.ps.GetCursor(), s.cs.GetCursorField()), Spec: s.ps})
			if !Lands(spec, a, name) {
				continue
			}
		}
		whys[name] = OneWorker(StreamMode(spec, caps, read, keyed), read)
	}
	if sql {
		outs, _ := SQLOutputs(spec, caps, a, inputs)
		for i, st := range a.Statements {
			if len(st.Inputs) != 1 || whys[st.Inputs[0]] != "" {
				continue
			}
			for _, in := range inputs {
				if in.Name == st.Inputs[0] {
					whys[in.Name] = OneWorker(outs[i].Mode, in.Read)
				}
			}
		}
	}
	var oneWorker []*apiv1.PlanWarning
	for _, s := range selected(spec, cat) {
		if why := whys[s.cs.GetName()]; why != "" {
			oneWorker = append(oneWorker, &apiv1.PlanWarning{Stream: s.cs.GetName(), Message: fmt.Sprintf(
				"each attempt reads it on one of its %d workers: %s", asked, why)})
		}
	}
	return asked, "", oneWorker
}
