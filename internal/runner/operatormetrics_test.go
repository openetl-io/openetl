package runner

import (
	"math"
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/checkpoint"
)

// Watermarks at either end of int64, the end of input among them, record
// without trouble.
func TestOperatorMetricsTakeAnyWatermark(t *testing.T) {
	for _, wm := range []int64{math.MinInt64 + 1, -62135596800000001, 0, 253402300800000000, EndOfInput - 1, EndOfInput} {
		measureOperator(t.Context(), "org", "p", checkpoint.Operator{Operator: "w", Watermark: &wm, Windows: -1}, time.Now())
	}
	measureOperator(t.Context(), "org", "p", checkpoint.Operator{Operator: "w"}, time.Now())
}
