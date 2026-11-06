package sdk

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Cursor is a cursor value as records carry it: a JSON string or number.
// The zero Cursor means the stream has no floor yet.
type Cursor struct {
	raw json.RawMessage
}

func (c Cursor) IsZero() bool { return len(c.raw) == 0 }

func (c Cursor) JSON() json.RawMessage { return c.raw }

// String is a string cursor's contents, or a number's text.
func (c Cursor) String() string {
	var s string
	if json.Unmarshal(c.raw, &s) == nil {
		return s
	}
	return string(c.raw)
}

// Time parses an RFC 3339 cursor.
func (c Cursor) Time() (time.Time, error) {
	return time.Parse(time.RFC3339Nano, c.String())
}

// compareCursors orders two cursor values of the same JSON type: numbers
// numerically, RFC 3339 strings as instants, other strings bytewise.
func compareCursors(a, b json.RawMessage) (int, error) {
	if isString(a) != isString(b) {
		return 0, errors.New("records mix string and number values")
	}
	if !isString(a) {
		var x, y json.Number
		if err := json.Unmarshal(a, &x); err != nil {
			return 0, err
		}
		if err := json.Unmarshal(b, &y); err != nil {
			return 0, err
		}
		return compareNumbers(x, y)
	}
	var x, y string
	if err := json.Unmarshal(a, &x); err != nil {
		return 0, err
	}
	if err := json.Unmarshal(b, &y); err != nil {
		return 0, err
	}
	tx, errX := time.Parse(time.RFC3339Nano, x)
	ty, errY := time.Parse(time.RFC3339Nano, y)
	if errX == nil && errY == nil {
		return tx.Compare(ty), nil
	}
	return strings.Compare(x, y), nil
}

func isString(v json.RawMessage) bool {
	v = bytes.TrimSpace(v)
	return len(v) > 0 && v[0] == '"'
}

func compareNumbers(x, y json.Number) (int, error) {
	if a, err := x.Int64(); err == nil {
		if b, err := y.Int64(); err == nil {
			switch {
			case a < b:
				return -1, nil
			case a > b:
				return 1, nil
			}
			return 0, nil
		}
	}
	a, err := x.Float64()
	if err != nil {
		return 0, err
	}
	b, err := y.Float64()
	if err != nil {
		return 0, err
	}
	switch {
	case a < b:
		return -1, nil
	case a > b:
		return 1, nil
	}
	return 0, nil
}
