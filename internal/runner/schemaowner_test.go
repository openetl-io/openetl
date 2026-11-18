package runner

import (
	"slices"
	"testing"

	attemptv1 "github.com/openetl-io/openetl/gen/openetl/attempt/v1"
	"github.com/openetl-io/openetl/internal/runner/attempt"
)

// sentSchemas is a coordinator that keeps the schemas it sends, and to
// whom: -1 for every worker.
type sentSchemas struct {
	attempt.Coordinator
	to   []int
	sent []*attemptv1.StreamSchema
}

func (c *sentSchemas) Schema(worker int, s *attemptv1.StreamSchema) error {
	c.to = append(c.to, worker)
	c.sent = append(c.sent, s)
	return nil
}

// The owner seals each of the destination's streams as it takes the shares
// of the first epoch with its columns, in a version of its own that every
// worker gets: a change a worker proposed from before the seal gets it the
// sealed version, not taken, and one from it is taken, still sealed. A
// stream restored with columns is sealed from the start, and a stream a SQL
// transform reads never is. The record's schema is the sealed version's.
func TestTheOwnerSealsAStreamAsItTakesTheFirstEpochWithItsColumns(t *testing.T) {
	co := &sentSchemas{}
	// The owner doesn't read schemas, so any bytes stand for them.
	x, y, z := []byte("x"), []byte("y"), []byte("z")
	o := newSchemaOwner(co, map[ownedKey][]byte{{name: "r"}: x, {name: "in", input: true}: x})
	type want struct {
		to      int
		stream  string
		version uint64
		schema  []byte
		sealed  bool
	}
	checked := 0
	expect := func(what string, w ...want) {
		t.Helper()
		got := co.sent[checked:]
		if len(got) != len(w) {
			t.Fatalf("%s: sent %d schemas, want %d: %v", what, len(got), len(w), got)
		}
		for i, s := range got {
			g := want{co.to[checked+i], s.GetStream(), s.GetVersion(), s.GetArrowSchema(), s.GetSealed()}
			if g.to != w[i].to || g.stream != w[i].stream || g.version != w[i].version || !slices.Equal(g.schema, w[i].schema) || g.sealed != w[i].sealed {
				t.Errorf("%s: sent %+v, want %+v", what, g, w[i])
			}
		}
		checked = len(co.sent)
	}
	propose := func(worker int, stream string, input bool, base uint64, schema []byte) {
		t.Helper()
		if err := o.propose(worker, &attemptv1.SchemaProposal{Stream: stream, Input: input, Base: base, ArrowSchema: schema}); err != nil {
			t.Fatal(err)
		}
	}
	own := func() *Ready {
		t.Helper()
		r := &Ready{}
		if err := o.own(r, false); err != nil {
			t.Fatal(err)
		}
		return r
	}

	propose(1, "s", false, 0, x)
	propose(0, "in", true, 0, y)
	expect("before an epoch", want{-1, "s", 1, x, false}, want{-1, "in", 1, y, false})
	r := own()
	expect("the first epoch's shares", want{-1, "s", 2, x, true})
	if !slices.Equal(r.Schemas["s"], x) || !slices.Equal(r.Schemas["in"], y) {
		t.Errorf("the epoch's schemas: %v", r.Schemas)
	}
	propose(2, "s", false, 1, y)
	expect("a change from before the seal", want{2, "s", 2, x, true})
	propose(2, "s", false, 2, y)
	expect("a change from the sealed version", want{-1, "s", 3, y, true})
	own()
	expect("the next epoch's shares")
	propose(0, "r", false, 0, z)
	propose(0, "in", true, 1, z)
	expect("restored, and read", want{-1, "r", 1, z, true}, want{-1, "in", 2, z, false})
	own()
	expect("shares after them")
}
