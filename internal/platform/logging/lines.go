package logging

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
)

// maxLine is how much of a line of a child's output is kept, in bytes.
const maxLine = 1 << 20

// Lines scans the lines a child process writes to a pipe, without their line
// endings. A line of 1 MiB or more is cut there and says so, and the rest of
// it is dropped: the scanner keeps reading, since a child blocks for good
// writing to a pipe nobody reads, and later lines say why it stopped. Its
// only errors are r's.
func Lines(r io.Reader) *bufio.Scanner { return linesUpTo(r, maxLine) }

func linesUpTo(r io.Reader, limit int) *bufio.Scanner {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, min(64<<10, limit)), limit)
	sc.Split(splitLines(limit))
	return sc
}

// splitLines is bufio.ScanLines, cutting lines at limit. The scanner's buffer
// holds limit bytes at most, so it gets a token before the line outgrows it.
func splitLines(limit int) bufio.SplitFunc {
	dropping := false
	return func(data []byte, atEOF bool) (int, []byte, error) {
		// At EOF the scanner stops at the first call without a token, so the
		// lines after a dropped tail are handled in the same call.
		dropped := 0
		if dropping {
			i := bytes.IndexByte(data, '\n')
			if i < 0 {
				return len(data), nil, nil
			}
			dropped, data, dropping = i+1, data[i+1:], false
		}
		i := bytes.IndexByte(data, '\n')
		switch {
		case i >= limit:
			return dropped + i + 1, cut(data, limit), nil
		case i >= 0:
			return dropped + i + 1, dropCR(data[:i]), nil
		case len(data) >= limit:
			dropping = true
			return dropped + len(data), cut(data, limit), nil
		case atEOF && len(data) > 0:
			return dropped + len(data), dropCR(data), nil
		}
		return dropped, nil, nil
	}
}

func cut(line []byte, limit int) []byte {
	return fmt.Appendf(line[:limit:limit], " [line cut at %d bytes]", limit)
}

func dropCR(line []byte) []byte { return bytes.TrimSuffix(line, []byte("\r")) }
