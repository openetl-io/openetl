package attempt

import (
	"errors"

	enginev1 "github.com/openetl-io/openetl/gen/openetl/engine/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/ints"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// Ready is what a share carries of its worker's dataflow's part of an
// epoch.
type Ready struct {
	// As openetl.engine.v1.EpochReady's.
	Schemas    map[string][]byte
	Snapshots  []checkpoint.Snapshot
	Quarantine []checkpoint.QuarantineFile
}

// EpochReady is r as a share of epoch e carries it.
func (r Ready) EpochReady(e *protocolv1.Epoch) *enginev1.EpochReady {
	p := &enginev1.EpochReady{Epoch: e, Schemas: r.Schemas}
	for _, s := range r.Snapshots {
		snap := &enginev1.StateSnapshot{Operator: s.Operator, Url: s.URL}
		if kg := s.KeyGroups; kg != nil {
			snap.KeyGroups = KeyGroups(kg.First, kg.Last)
		}
		p.Snapshots = append(p.Snapshots, snap)
	}
	for _, q := range r.Quarantine {
		p.Quarantine = append(p.Quarantine, &enginev1.QuarantineFile{Stream: q.Stream, Url: q.URL, Rows: q.Rows, Output: q.Output})
	}
	return p
}

// ReadyOf is what a share's EpochReady carries. It fails on what a worker
// doesn't run: holds, a transform's outputs, and staged files.
func ReadyOf(p *enginev1.EpochReady) (Ready, error) {
	if len(p.GetDelivered()) > 0 || len(p.GetHolds()) > 0 || len(p.GetOutputSchemas()) > 0 || len(p.GetFiles()) > 0 || len(p.GetStaged()) > 0 {
		return Ready{}, errors.New("a worker's share holds what runs on one pod: holds, a transform's outputs or staged files")
	}
	r := Ready{Schemas: p.GetSchemas()}
	for _, s := range p.GetSnapshots() {
		snap := checkpoint.Snapshot{Operator: s.GetOperator(), URL: s.GetUrl()}
		if kg := s.GetKeyGroups(); kg != nil {
			snap.KeyGroups = &checkpoint.KeyGroupRange{First: int(kg.GetFirst()), Last: int(kg.GetLast())}
		}
		r.Snapshots = append(r.Snapshots, snap)
	}
	for _, q := range p.GetQuarantine() {
		r.Quarantine = append(r.Quarantine, checkpoint.QuarantineFile{Stream: q.GetStream(), Output: q.GetOutput(), URL: q.GetUrl(), Rows: q.GetRows()})
	}
	return r, nil
}

// KeyGroups is key groups first to last as the protocol carries them.
func KeyGroups(first, last int) *enginev1.KeyGroupRange {
	return &enginev1.KeyGroupRange{First: ints.Uint32(first), Last: ints.Uint32(last)}
}
