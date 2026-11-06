package spark

import (
	"cmp"
	"context"
	"encoding/json"
	"slices"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
)

// ContextVersion is the context document's protocol version. openetl-spark
// refuses a major version it doesn't know.
const ContextVersion = 1

// Context is what a post-sync application learns of the sync, as JSON.
type Context struct {
	Version     int         `json:"version"`
	Pipeline    string      `json:"pipeline"`
	Sync        string      `json:"sync"`
	Destination Destination `json:"destination"`
	// The records the sync committed: those after After, through Through,
	// in (epoch, generation) order. Equal if it committed none. After is
	// nil when the log doesn't say, in records from before it did.
	After   *checkpoint.RecordRef `json:"after"`
	Through *checkpoint.RecordRef `json:"through"`
	Streams []Stream              `json:"streams"`
}

type Destination struct {
	Connector string `json:"connector"`
	Namespace string `json:"namespace,omitempty"`
}

type Stream struct {
	Name string `json:"name"`
	// The destination's table, where the record's schema names it.
	Table string `json:"table,omitempty"`
	// What the source emitted in the sync, quarantined rows included.
	Rows uint64 `json:"rows"`
}

// BuildContext is sync's context from the pipeline's latest record, which
// says what the sync committed (checkpoint.Record.SyncStart). Records name
// their sync by its workflow's run, run.
func BuildContext(ctx context.Context, log *checkpoint.Log, pipeline, sync, run string, dst Destination) (*Context, error) {
	head, err := log.Latest(ctx)
	if err != nil {
		return nil, err
	}
	c := &Context{Version: ContextVersion, Pipeline: pipeline, Sync: sync, Destination: dst, Streams: []Stream{}}
	if head == nil {
		c.After, c.Through = &checkpoint.RecordRef{}, &checkpoint.RecordRef{}
		return c, nil
	}
	ref := head.Ref()
	c.Through = &ref
	if head.Sync != run {
		c.After = &ref
		return c, nil
	}
	c.After = head.SyncStart
	for name, rows := range head.SyncRows {
		s := Stream{Name: name, Rows: rows}
		if st, ok := head.Streams[name]; ok && st.Schema != nil {
			if schema, err := arrowipc.DecodeSchema(st.Schema); err == nil {
				s.Table, _ = schema.Metadata().GetValue("openetl.table")
			}
		}
		c.Streams = append(c.Streams, s)
	}
	slices.SortFunc(c.Streams, func(a, b Stream) int { return cmp.Compare(a.Name, b.Name) })
	return c, nil
}

func (c *Context) JSON() ([]byte, error) { return json.Marshal(c) }
