package engineclient

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	enginev1 "github.com/openetl-io/openetl/gen/openetl/engine/v1"
)

const (
	headerLen = 12
	// Larger lengths mean a corrupt header.
	maxPayload = 1 << 30
)

func putHeader(h []byte, kind enginev1.FrameKind, stream uint32, n int) {
	h[0] = byte(kind) //nolint:gosec // frame kinds fit in a byte
	h[1], h[2], h[3] = 0, 0, 0
	binary.LittleEndian.PutUint32(h[4:8], stream)
	binary.LittleEndian.PutUint32(h[8:12], uint32(n)) //nolint:gosec // n is checked against maxPayload
}

type frameWriter struct {
	w   *bufio.Writer
	hdr [headerLen]byte
}

func newFrameWriter(w io.Writer) *frameWriter {
	return &frameWriter{w: bufio.NewWriterSize(w, 1<<16)}
}

func (fw *frameWriter) write(kind enginev1.FrameKind, stream uint32, payload []byte) error {
	if len(payload) > maxPayload {
		return fmt.Errorf("engineclient: frame payload of %d bytes", len(payload))
	}
	putHeader(fw.hdr[:], kind, stream, len(payload))
	if _, err := fw.w.Write(fw.hdr[:]); err != nil {
		return err
	}
	_, err := fw.w.Write(payload)
	return err
}

func (fw *frameWriter) flush() error { return fw.w.Flush() }

type frameReader struct {
	r   *bufio.Reader
	hdr [headerLen]byte
	buf []byte
}

func newFrameReader(r io.Reader) *frameReader {
	return &frameReader{r: bufio.NewReaderSize(r, 1<<16)}
}

// next returns io.EOF if the edge closed between frames. The payload is valid
// until the next call.
func (fr *frameReader) next() (enginev1.FrameKind, uint32, []byte, error) {
	if _, err := io.ReadFull(fr.r, fr.hdr[:]); err != nil {
		if err == io.ErrUnexpectedEOF { //nolint:errorlint // ReadFull returns it unwrapped
			err = fmt.Errorf("engineclient: edge closed inside a frame header: %w", err)
		}
		return 0, 0, nil, err
	}
	h := fr.hdr[:]
	kind := enginev1.FrameKind(h[0])
	if _, ok := enginev1.FrameKind_name[int32(kind)]; !ok || kind == enginev1.FrameKind_FRAME_KIND_UNSPECIFIED {
		return 0, 0, nil, fmt.Errorf("engineclient: unknown frame kind %d", h[0])
	}
	if h[1]|h[2]|h[3] != 0 {
		return 0, 0, nil, fmt.Errorf("engineclient: reserved header bytes are set")
	}
	stream := binary.LittleEndian.Uint32(h[4:8])
	n := int(binary.LittleEndian.Uint32(h[8:12]))
	if n > maxPayload {
		return 0, 0, nil, fmt.Errorf("engineclient: frame payload of %d bytes", n)
	}
	if cap(fr.buf) < n {
		fr.buf = make([]byte, n)
	}
	fr.buf = fr.buf[:n]
	if _, err := io.ReadFull(fr.r, fr.buf); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, 0, nil, fmt.Errorf("engineclient: edge closed inside a frame payload: %w", err)
	}
	return kind, stream, fr.buf, nil
}
