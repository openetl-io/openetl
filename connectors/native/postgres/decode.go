package postgres

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// colBuilder decodes one column's non-null values into an Arrow builder.
// Types without a binary decoder are read as text into a string column.
type colBuilder struct {
	format int16
	b      array.Builder
	add    func(v []byte) error
}

const (
	// Microseconds and days from 1970-01-01 to Postgres's epoch, 2000-01-01.
	pgEpochMicros = 946_684_800_000_000
	pgEpochDays   = 10_957
)

var errInfinite = errors.New("infinite dates and timestamps aren't supported yet")

func fixed(n int, f func(v []byte)) func([]byte) error {
	return func(v []byte) error {
		if len(v) != n {
			return fmt.Errorf("a %d-byte value, want %d", len(v), n)
		}
		f(v)
		return nil
	}
}

func newColBuilder(mem memory.Allocator, c sourceColumn) (*colBuilder, error) {
	be := binary.BigEndian
	cb := &colBuilder{format: pgx.BinaryFormatCode}
	switch c.oid {
	case pgtype.BoolOID:
		b := array.NewBooleanBuilder(mem)
		cb.b, cb.add = b, fixed(1, func(v []byte) { b.Append(v[0] != 0) })
	case pgtype.Int2OID:
		b := array.NewInt16Builder(mem)
		cb.b, cb.add = b, fixed(2, func(v []byte) { b.Append(int16(be.Uint16(v))) }) //nolint:gosec // two's complement
	case pgtype.Int4OID:
		b := array.NewInt32Builder(mem)
		cb.b, cb.add = b, fixed(4, func(v []byte) { b.Append(int32(be.Uint32(v))) }) //nolint:gosec // two's complement
	case pgtype.Int8OID:
		b := array.NewInt64Builder(mem)
		cb.b, cb.add = b, fixed(8, func(v []byte) { b.Append(int64(be.Uint64(v))) }) //nolint:gosec // two's complement
	case pgtype.OIDOID:
		b := array.NewUint32Builder(mem)
		cb.b, cb.add = b, fixed(4, func(v []byte) { b.Append(be.Uint32(v)) })
	case pgtype.Float4OID:
		b := array.NewFloat32Builder(mem)
		cb.b, cb.add = b, fixed(4, func(v []byte) { b.Append(math.Float32frombits(be.Uint32(v))) })
	case pgtype.Float8OID:
		b := array.NewFloat64Builder(mem)
		cb.b, cb.add = b, fixed(8, func(v []byte) { b.Append(math.Float64frombits(be.Uint64(v))) })
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.NameOID:
		b := array.NewStringBuilder(mem)
		cb.b, cb.add = b, func(v []byte) error { b.BinaryBuilder.Append(v); return nil }
	case pgtype.ByteaOID:
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		cb.b, cb.add = b, func(v []byte) error { b.Append(v); return nil }
	case pgtype.DateOID:
		b := array.NewDate32Builder(mem)
		cb.b, cb.add = b, func(v []byte) error {
			if len(v) != 4 {
				return fmt.Errorf("a %d-byte date", len(v))
			}
			d := int32(be.Uint32(v)) //nolint:gosec // two's complement
			if d == math.MaxInt32 || d == math.MinInt32 {
				return errInfinite
			}
			b.Append(arrow.Date32(d + pgEpochDays))
			return nil
		}
	case pgtype.TimeOID:
		b := array.NewTime64Builder(mem, &arrow.Time64Type{Unit: arrow.Microsecond})
		cb.b, cb.add = b, fixed(8, func(v []byte) { b.Append(arrow.Time64(be.Uint64(v))) }) //nolint:gosec // microseconds since midnight
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		t := &arrow.TimestampType{Unit: arrow.Microsecond}
		if c.oid == pgtype.TimestamptzOID {
			t.TimeZone = "UTC"
		}
		b := array.NewTimestampBuilder(mem, t)
		cb.b, cb.add = b, func(v []byte) error {
			if len(v) != 8 {
				return fmt.Errorf("a %d-byte timestamp", len(v))
			}
			us := int64(be.Uint64(v)) //nolint:gosec // two's complement
			if us == math.MaxInt64 || us == math.MinInt64 {
				return errInfinite
			}
			b.Append(arrow.Timestamp(us + pgEpochMicros))
			return nil
		}
	case pgtype.UUIDOID:
		b := array.NewExtensionBuilder(mem, extensions.NewUUIDType())
		fb := b.Builder.(*array.FixedSizeBinaryBuilder)
		cb.b, cb.add = b, fixed(16, func(v []byte) { fb.Append(v) })
	case pgtype.JSONOID, pgtype.JSONBOID:
		jt, err := extensions.NewJSONType(arrow.BinaryTypes.String)
		if err != nil {
			return nil, err
		}
		b := array.NewExtensionBuilder(mem, jt)
		sb := b.Builder.(*array.StringBuilder)
		jsonb := c.oid == pgtype.JSONBOID
		cb.b, cb.add = b, func(v []byte) error {
			if jsonb {
				if len(v) == 0 || v[0] != 1 {
					return errors.New("an unknown jsonb version")
				}
				v = v[1:]
			}
			sb.BinaryBuilder.Append(v)
			return nil
		}
	case pgtype.NumericOID:
		p, s, ok := numericTypmod(c.typmod)
		if !ok {
			break
		}
		b := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: p, Scale: s})
		cb.b, cb.add = b, func(v []byte) error {
			n, err := decodeNumeric(v, s)
			if err != nil {
				return err
			}
			b.Append(n)
			return nil
		}
	}
	if cb.b == nil {
		b := array.NewStringBuilder(mem)
		cb.format, cb.b, cb.add = pgx.TextFormatCode, b, func(v []byte) error { b.BinaryBuilder.Append(v); return nil }
	}
	return cb, nil
}

// numericTypmod returns a numeric column's precision and scale if Decimal128
// can hold it.
func numericTypmod(typmod int32) (p, s int32, ok bool) {
	if typmod < 4 {
		return 0, 0, false
	}
	m := typmod - 4
	p = (m >> 16) & 0xffff
	s = m & 0x7ff
	if s&0x400 != 0 {
		s -= 0x800 // negative scales, from Postgres 15
	}
	return p, s, p >= 1 && p <= 38 && s >= 0 && s <= p
}

// decodeNumeric decodes numeric's binary format as an integer scaled by
// 10^scale. Postgres stores base-10000 digits, most significant first; the
// first has weight `weight`, and trailing zero digits are left out.
func decodeNumeric(v []byte, scale int32) (decimal128.Num, error) {
	var n decimal128.Num
	if len(v) < 8 {
		return n, fmt.Errorf("a %d-byte numeric", len(v))
	}
	be := binary.BigEndian
	ndigits := int(be.Uint16(v))
	weight := int(int16(be.Uint16(v[2:]))) //nolint:gosec // two's complement
	sign := be.Uint16(v[4:])
	switch sign {
	case 0x0000, 0x4000:
	case 0xC000:
		return n, errors.New("NaN isn't supported yet")
	default:
		return n, errors.New("infinite numerics aren't supported yet")
	}
	if len(v) != 8+2*ndigits {
		return n, fmt.Errorf("a %d-byte numeric with %d digits", len(v), ndigits)
	}
	ten4 := decimal128.FromU64(10_000)
	exp := 0 // the power of ten that n's last digit stands for
	for i := range ndigits {
		d := uint64(be.Uint16(v[8+2*i:]))
		if d >= 10_000 {
			return n, errors.New("a numeric digit out of range")
		}
		low := 4*(weight-i) + int(scale) // the power of ten of d's last decimal digit, once scaled
		switch {
		case low >= 0:
			n, exp = n.Mul(ten4).Add(decimal128.FromU64(d)), low
		case low > -4:
			cut := uint64(math.Pow10(-low))
			if d%cut != 0 {
				return n, fmt.Errorf("a numeric with more than %d decimal places", scale)
			}
			n, exp = n.Mul(decimal128.GetScaleMultiplier(4+low)).Add(decimal128.FromU64(d/cut)), 0
		case d != 0:
			return n, fmt.Errorf("a numeric with more than %d decimal places", scale)
		}
	}
	if exp > 38 {
		return n, errors.New("a numeric too large for its precision")
	}
	n = n.Mul(decimal128.GetScaleMultiplier(exp))
	if sign == 0x4000 {
		n = n.Negate()
	}
	return n, nil
}
