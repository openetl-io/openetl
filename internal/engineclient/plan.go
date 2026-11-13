package engineclient

import (
	"context"
	"fmt"
	"log/slog"

	enginev1 "github.com/openetl-io/openetl/gen/openetl/engine/v1"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/source"
)

var _ runner.SQLPlanner = (*Dataflow)(nil)

// PlanSQL plans a SQL transform's statements against the streams they read,
// with their declared and restored schemas, in an engine of its own.
func (d *Dataflow) PlanSQL(ctx context.Context, sql string, streams []runner.Stream, restored map[string][]byte) ([]runner.ExpectedOutput, error) {
	opts := d.Options
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	req := &enginev1.PlanOutputsRequest{Sql: sql}
	for _, s := range streams {
		declared, err := encodeSchema(s.Declared())
		if err != nil {
			return nil, fmt.Errorf("engineclient: %s declared schema: %w", s.Name, err)
		}
		req.Streams = append(req.Streams, &enginev1.StreamPlan{
			Name:                 s.Name,
			PrimaryKey:           s.PrimaryKey,
			ArrowSchema:          declared,
			RestoredSchema:       restored[s.Name],
			NanosecondTimestamps: s.NanosecondTimestamps,
			Changelog:            s.Read == source.CDC,
		})
	}
	p, err := start(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer p.close()
	resp, err := p.client.PlanOutputs(ctx, req)
	if err != nil {
		return nil, p.explain(ctx, err)
	}
	out := make([]runner.ExpectedOutput, 0, len(resp.GetOutputs()))
	for _, o := range resp.GetOutputs() {
		e := o.GetOutput()
		schema, err := arrowipc.DecodeSchema(e.GetArrowSchema())
		if err != nil {
			return nil, fmt.Errorf("engineclient: output %s's schema: %w", e.GetName(), err)
		}
		out = append(out, runner.ExpectedOutput{Name: e.GetName(), Schema: schema, PrimaryKey: e.GetPrimaryKey(), Input: o.GetInput()})
	}
	return out, nil
}
