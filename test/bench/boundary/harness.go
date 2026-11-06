package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/util"

	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/arrowipc"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// counted counts the Arrow bytes its source emits.
type counted struct {
	source.Reader
	bytes atomic.Int64
}

func (c *counted) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return c.Reader.Read(ctx, req, countingEmitter{out, &c.bytes})
}

type countingEmitter struct {
	source.Emitter
	n *atomic.Int64
}

func (e countingEmitter) Records(stream string, b arrow.RecordBatch) error {
	e.n.Add(util.TotalRecordSize(b))
	return e.Emitter.Records(stream, b)
}

// Forward needs the runner's emitter, which forwards.
func (e countingEmitter) Forward(stream string, b arrowipc.Encoded) error {
	e.n.Add(b.Size)
	return e.Emitter.(source.Forwarder).Forward(stream, b)
}

// discardEmitter drops what a source emits.
type discardEmitter struct{ bytes int64 }

func (d *discardEmitter) Rows(string, []json.RawMessage) error { return errors.New("unexpected rows") }

func (d *discardEmitter) Records(_ string, b arrow.RecordBatch) error {
	d.bytes += util.TotalRecordSize(b)
	b.Release()
	return nil
}

func (d *discardEmitter) Forward(_ string, b arrowipc.Encoded) error {
	d.bytes += b.Size
	return nil
}

func (d *discardEmitter) ResumePoint(string, []byte) error { return nil }

func (d *discardEmitter) Split(stream, id string) source.SplitEmitter {
	return source.WholeSplit(d, stream, id)
}
func (d *discardEmitter) Complete(string, []byte) error { return nil }

// emitted is a batch, or a resume point if batch is nil.
type emitted struct {
	batch arrow.RecordBatch
	size  int64
	state []byte
}

var errEnough = errors.New("captured enough")

// recorder keeps what a source emits, up to the first resume point after
// limit bytes.
type recorder struct {
	limit int64
	bytes int64
	items []emitted
}

func (r *recorder) Rows(string, []json.RawMessage) error { return errors.New("unexpected rows") }

func (r *recorder) Records(_ string, b arrow.RecordBatch) error {
	n := util.TotalRecordSize(b)
	r.items = append(r.items, emitted{batch: b, size: n})
	r.bytes += n
	return nil
}

func (r *recorder) ResumePoint(_ string, state []byte) error {
	r.items = append(r.items, emitted{state: state})
	if r.bytes >= r.limit {
		return errEnough
	}
	return nil
}

func (r *recorder) Split(stream, id string) source.SplitEmitter {
	return source.WholeSplit(r, stream, id)
}

func (r *recorder) Complete(string, []byte) error { return nil }

func (r *recorder) release() {
	for _, it := range r.items {
		if it.batch != nil {
			it.batch.Release()
		}
	}
}

// replay emits recorded items on one stream, over and over, until it has
// sent at least bytes, then completes the stream.
type replay struct {
	stream string
	items  []emitted
	bytes  int64
}

func (r *replay) Read(ctx context.Context, _ source.ReadRequest, out source.Emitter) error {
	var sent int64
	for sent < r.bytes {
		for _, it := range r.items {
			if err := ctx.Err(); err != nil {
				return err
			}
			if it.batch == nil {
				if err := out.ResumePoint(r.stream, it.state); err != nil {
					return err
				}
				if sent >= r.bytes {
					break
				}
				continue
			}
			it.batch.Retain()
			sent += it.size
			if err := out.Records(r.stream, it.batch); err != nil {
				return err
			}
		}
	}
	return out.Complete(r.stream, nil)
}

// discardDest is a runner sink that drops every batch.
type discardDest struct{}

func (discardDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{
		Class:  destination.StageAndApply,
		Scopes: []destination.Scope{destination.ScopePipeline, destination.ScopeStream},
	}
}

func (discardDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return discardWriter{}, nil
}

// engineDiscard is the engine's discard sink.
type engineDiscard struct{ discardDest }

func (engineDiscard) EngineSink() runner.SinkPlan { return runner.SinkPlan{Discard: true} }

type discardWriter struct{}

func (discardWriter) Write(context.Context, destination.Epoch, string, arrow.RecordBatch) error {
	return nil
}

func (discardWriter) PreCommit(context.Context, destination.Epoch) (destination.Staged, error) {
	return nil, nil
}

func (discardWriter) Commit(context.Context, destination.Epoch, []destination.Staged) (destination.Committed, error) {
	return destination.Committed{}, nil
}

func (discardWriter) Recover(context.Context, uint64, destination.Epoch, []destination.Staged) (destination.Recovery, error) {
	return destination.Applied, nil
}

func (discardWriter) Close(context.Context) error { return nil }
