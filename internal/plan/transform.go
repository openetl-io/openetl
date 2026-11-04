package plan

import (
	"fmt"
	"slices"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// Input is one of a transform's input streams.
type Input struct {
	Name       string
	PrimaryKey []string
	Read       apiv1.SyncMode
	Replay     source.Replay
	// The spec's settings for it; nil if it doesn't name it.
	Spec *apiv1.PipelineStream
}

// Output is one of a transform's outputs, which are the destination's
// streams.
type Output struct {
	Name       string
	PrimaryKey []string
	Mode       destination.WriteMode
	// 0: the pipeline's.
	Require delivery.Level
	// Every input's replay is exact.
	Exact bool
}

func (o Output) Delivery() delivery.Stream {
	r := source.Superset
	if o.Exact {
		r = source.Exact
	}
	return delivery.Stream{Name: o.Name, Replay: r, Mode: o.Mode, Keyed: len(o.PrimaryKey) > 0, Require: o.Require, Transformed: true}
}

// Outputs are a transform's outputs: the declared ones, or else each
// input. An output's unset settings are those of the input with its name.
// Without a write mode anywhere, an output is overwritten when every input
// is read with full_refresh, merged when its input is a keyed stream read
// with cdc, and appended to otherwise, where the destination has the mode.
func Outputs(p *apiv1.PipelineSpec, caps destination.Capabilities, inputs []Input) []Output {
	exact, refresh := true, true
	for _, in := range inputs {
		exact = exact && in.Replay == source.Exact
		refresh = refresh && in.Read == apiv1.SyncMode_SYNC_MODE_FULL_REFRESH
	}
	declared := p.GetTransform().GetSpark().GetOutputs()
	if f := p.GetTransform().GetFlink(); f != nil {
		declared = f.GetOutputs()
	}
	if len(declared) == 0 {
		for _, in := range inputs {
			declared = append(declared, &apiv1.TransformOutput{Name: in.Name})
		}
	}
	out := make([]Output, 0, len(declared))
	for _, d := range declared {
		var in *Input
		if i := slices.IndexFunc(inputs, func(in Input) bool { return in.Name == d.GetName() }); i >= 0 {
			in = &inputs[i]
		}
		o := Output{Name: d.GetName(), PrimaryKey: d.GetPrimaryKey(), Require: Level(d.GetRequire()), Exact: exact}
		if len(o.PrimaryKey) == 0 && in != nil {
			o.PrimaryKey = in.PrimaryKey
		}
		if o.Require == 0 && in != nil {
			o.Require = Level(in.Spec.GetRequire())
		}
		o.Mode = WriteMode(&apiv1.PipelineSpec{Destination: &apiv1.PipelineDestination{Write: d.GetWrite()}})
		switch {
		case o.Mode != 0:
		case WriteMode(p) != 0:
			o.Mode = WriteMode(p)
		case refresh && len(inputs) > 0 && caps.SupportsMode(destination.Overwrite):
			o.Mode = destination.Overwrite
		case in != nil && in.Read == apiv1.SyncMode_SYNC_MODE_CDC && len(o.PrimaryKey) > 0 && caps.SupportsMode(destination.Merge):
			o.Mode = destination.Merge
		default:
			o.Mode = destination.Append
		}
		out = append(out, o)
	}
	return out
}

// OutputProblem fails an overwritten output with an input whose read doesn't
// hold all its data: overwrite replaces the table with what one sync wrote.
func OutputProblem(o Output, inputs []Input) error {
	if o.Mode != destination.Overwrite {
		return nil
	}
	for _, in := range inputs {
		if in.Read != apiv1.SyncMode_SYNC_MODE_FULL_REFRESH {
			return fmt.Errorf("output %q is overwritten, which replaces its table with what one sync wrote, so every input needs full_refresh, and stream %q is read with %s",
				o.Name, in.Name, syncName(in.Read))
		}
	}
	return nil
}

// InputProblem fails a Flink transform's input that doesn't replay exactly
// from a resume point: the job's checkpoints hold its inputs' resume points,
// and a restore reads each again from its own.
func InputProblem(p *apiv1.PipelineSpec, in Input) error {
	if p.GetTransform().GetFlink() == nil || in.Replay == source.Exact {
		return nil
	}
	return fmt.Errorf("stream %q doesn't replay exactly from a resume point, and a Flink transform's inputs must, since its checkpoints restore them", in.Name)
}
