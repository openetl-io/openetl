// Package stdout is a destination that writes each row as a JSON line:
// {"stream": …, "record": {…}}.
package stdout

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/unsuitable001/openetl/sdk/go/destination"
)

type Destination struct {
	mu sync.Mutex
	w  *bufio.Writer
}

var _ destination.Destination = (*Destination)(nil)

// New takes an empty config.
func New(config []byte) (*Destination, error) {
	return NewWriter(config, os.Stdout)
}

func NewWriter(config []byte, w io.Writer) (*Destination, error) {
	if c := bytes.TrimSpace(config); len(c) > 0 && !bytes.Equal(c, []byte("{}")) && !bytes.Equal(c, []byte("null")) {
		return nil, fmt.Errorf("stdout: takes no config, got %s", c)
	}
	return &Destination{w: bufio.NewWriter(w)}, nil
}

func (d *Destination) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.DeliverAndAck, Scopes: []destination.Scope{destination.ScopeStream}, NanosecondTimestamps: true}
}

func (d *Destination) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return &writer{d: d}, nil
}

type writer struct{ d *Destination }

type line struct {
	Stream string          `json:"stream"`
	Record json.RawMessage `json:"record"`
}

func (w *writer) Write(_ context.Context, _ destination.Epoch, stream string, batch arrow.RecordBatch) error {
	var buf bytes.Buffer
	if err := array.RecordToJSON(batch, &buf); err != nil {
		return fmt.Errorf("stdout: encode %s: %w", stream, err)
	}
	w.d.mu.Lock()
	defer w.d.mu.Unlock()
	enc := json.NewEncoder(w.d.w)
	for rec := range bytes.Lines(buf.Bytes()) {
		if err := enc.Encode(line{Stream: stream, Record: bytes.TrimSpace(rec)}); err != nil {
			return err
		}
	}
	return nil
}

func (w *writer) PreCommit(context.Context, destination.Epoch) (destination.Staged, error) {
	w.d.mu.Lock()
	defer w.d.mu.Unlock()
	return nil, w.d.w.Flush()
}

func (w *writer) Commit(context.Context, destination.Epoch, destination.Staged) error { return nil }

func (w *writer) Recover(context.Context, uint64, destination.Epoch, destination.Staged) (destination.Recovery, error) {
	return destination.Applied, nil
}

func (w *writer) Close(context.Context) error {
	w.d.mu.Lock()
	defer w.d.mu.Unlock()
	return w.d.w.Flush()
}
