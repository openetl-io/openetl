// Package arrowipc is how the connector protocol carries Arrow: each batch
// and each schema as a whole IPC stream.
package arrowipc

import (
	"bytes"
	"errors"

	"github.com/apache/arrow-go/v18/arrow"
	_ "github.com/apache/arrow-go/v18/arrow/extensions" // registers uuid and json
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func EncodeBatch(rec arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	if err := w.Write(rec); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeBatch decodes a stream of one batch. Release the batch.
func DecodeBatch(b []byte, mem memory.Allocator) (arrow.RecordBatch, error) {
	r, err := ipc.NewReader(bytes.NewReader(b), ipc.WithAllocator(mem))
	if err != nil {
		return nil, err
	}
	defer r.Release()
	if !r.Next() {
		if err := r.Err(); err != nil {
			return nil, err
		}
		return nil, errors.New("arrowipc: no batch")
	}
	rec := r.RecordBatch()
	rec.Retain()
	return rec, nil
}

// EncodeSchema returns nil for a nil schema.
func EncodeSchema(s *arrow.Schema) ([]byte, error) {
	if s == nil {
		return nil, nil
	}
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(s))
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeSchema returns nil for an empty b.
func DecodeSchema(b []byte) (*arrow.Schema, error) {
	if len(b) == 0 {
		return nil, nil
	}
	r, err := ipc.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer r.Release()
	return r.Schema(), nil
}
