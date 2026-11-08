package arrowipc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Encoded is a batch as the protocol carries it, checked by Inspect.
type Encoded struct {
	// An Arrow IPC stream: the schema, any dictionaries, one record batch.
	Bytes []byte
	Rows  int64
	// The lengths of the buffers of the batch and its dictionaries: what
	// util.TotalRecordSize counts once it's decoded.
	Size int64
}

// Message header types, from Arrow's Message.fbs.
const (
	schemaMessage     = 1
	dictionaryMessage = 2
	batchMessage      = 3
)

var errTruncated = errors.New("arrowipc: truncated")

// Inspect checks that b holds a schema, any dictionaries and one record
// batch, reading only their headers.
func Inspect(b []byte) (Encoded, error) {
	enc := Encoded{Bytes: b}
	batches := 0
	for i := 0; ; i++ {
		meta, body, rest, err := message(b)
		if err != nil {
			return Encoded{}, err
		}
		if meta == nil {
			break
		}
		typ, header, err := meta.header()
		if err != nil {
			return Encoded{}, err
		}
		switch {
		case i == 0 && typ != schemaMessage:
			return Encoded{}, errors.New("arrowipc: the stream doesn't start with a schema")
		case i == 0:
		case typ == dictionaryMessage && batches == 0:
			data, err := meta.table(header, 1)
			if err != nil {
				return Encoded{}, err
			}
			_, size, err := meta.batch(data, body)
			if err != nil {
				return Encoded{}, err
			}
			enc.Size += size
		case typ == batchMessage && batches == 0:
			rows, size, err := meta.batch(header, body)
			if err != nil {
				return Encoded{}, err
			}
			enc.Rows, enc.Size = rows, enc.Size+size
			batches++
		default:
			return Encoded{}, fmt.Errorf("arrowipc: unexpected message of type %d", typ)
		}
		b = rest
	}
	if batches == 0 {
		return Encoded{}, errors.New("arrowipc: no batch")
	}
	return enc, nil
}

// message splits off b's first message. A nil meta ends the stream.
func message(b []byte) (meta fb, body int64, rest []byte, err error) {
	if len(b) == 0 {
		return nil, 0, nil, nil
	}
	if len(b) < 4 {
		return nil, 0, nil, errTruncated
	}
	n := binary.LittleEndian.Uint32(b)
	b = b[4:]
	if n == 0xFFFFFFFF { // the continuation marker; without it, the pre-1.0 format
		if len(b) < 4 {
			return nil, 0, nil, errTruncated
		}
		n = binary.LittleEndian.Uint32(b)
		b = b[4:]
	}
	if n == 0 {
		if len(b) != 0 {
			return nil, 0, nil, errors.New("arrowipc: bytes after the end of the stream")
		}
		return nil, 0, nil, nil
	}
	if uint64(n) > uint64(len(b)) {
		return nil, 0, nil, errTruncated
	}
	meta, b = fb(b[:n]), b[n:]
	root, err := meta.ref(0)
	if err != nil {
		return nil, 0, nil, err
	}
	at, err := meta.field(root, 3)
	if err != nil {
		return nil, 0, nil, err
	}
	if at != 0 {
		if body, err = meta.i64(at); err != nil {
			return nil, 0, nil, err
		}
	}
	if body < 0 || body > int64(len(b)) {
		return nil, 0, nil, errTruncated
	}
	return meta, body, b[body:], nil
}

// fb reads a flatbuffer, checking every offset against its length.
type fb []byte

var errMalformed = errors.New("arrowipc: malformed message header")

// header returns the message's header type and its table.
func (b fb) header() (byte, int, error) {
	root, err := b.ref(0)
	if err != nil {
		return 0, 0, err
	}
	at, err := b.field(root, 1)
	if err != nil || at == 0 || at >= len(b) {
		return 0, 0, errMalformed
	}
	header, err := b.table(root, 2)
	return b[at], header, err
}

// batch returns a RecordBatch table's rows and the lengths of its buffers,
// each of which must lie in a body of body bytes.
func (b fb) batch(table int, body int64) (rows, size int64, err error) {
	at, err := b.field(table, 0)
	if err != nil {
		return 0, 0, err
	}
	if at != 0 {
		if rows, err = b.i64(at); err != nil {
			return 0, 0, err
		}
	}
	if rows < 0 {
		return 0, 0, errMalformed
	}
	at, err = b.field(table, 2)
	if err != nil || at == 0 {
		return rows, 0, err
	}
	vec, err := b.ref(at)
	if err != nil {
		return 0, 0, err
	}
	n, err := b.u32(vec)
	if err != nil {
		return 0, 0, err
	}
	if int64(n)*16 > int64(len(b)-vec-4) {
		return 0, 0, errMalformed
	}
	for i := range int(n) {
		off, _ := b.i64(vec + 4 + 16*i)
		length, _ := b.i64(vec + 4 + 16*i + 8)
		if off < 0 || length < 0 || length > body-off {
			return 0, 0, errMalformed
		}
		size += length
	}
	return rows, size, nil
}

// table returns the table that field slot of the table at pos refers to.
func (b fb) table(pos, slot int) (int, error) {
	at, err := b.field(pos, slot)
	if err != nil {
		return 0, err
	}
	if at == 0 {
		return 0, errMalformed
	}
	return b.ref(at)
}

// field returns where field slot of the table at pos is, or 0 if it's
// absent.
func (b fb) field(pos, slot int) (int, error) {
	soff, err := b.u32(pos)
	if err != nil {
		return 0, err
	}
	vt := pos - int(signed(soff))
	if vt < 0 || vt+4 > len(b) {
		return 0, errMalformed
	}
	vtLen := int(binary.LittleEndian.Uint16(b[vt:]))
	at := 4 + 2*slot
	if at+2 > vtLen {
		return 0, nil
	}
	if vt+at+2 > len(b) {
		return 0, errMalformed
	}
	off := int(binary.LittleEndian.Uint16(b[vt+at:]))
	if off == 0 {
		return 0, nil
	}
	return pos + off, nil
}

// ref follows the offset at pos.
func (b fb) ref(pos int) (int, error) {
	off, err := b.u32(pos)
	if err != nil {
		return 0, err
	}
	to := pos + int(off)
	if to >= len(b) {
		return 0, errMalformed
	}
	return to, nil
}

func (b fb) u32(pos int) (uint32, error) {
	if pos < 0 || pos+4 > len(b) {
		return 0, errMalformed
	}
	return binary.LittleEndian.Uint32(b[pos:]), nil
}

func (b fb) i64(pos int) (int64, error) {
	if pos < 0 || pos+8 > len(b) {
		return 0, errMalformed
	}
	return signed(binary.LittleEndian.Uint32(b[pos+4:]))<<32 | int64(binary.LittleEndian.Uint32(b[pos:])), nil
}

// signed is u read as a two's complement int32.
func signed(u uint32) int64 {
	if u > math.MaxInt32 {
		return int64(u) - 1<<32
	}
	return int64(u)
}
