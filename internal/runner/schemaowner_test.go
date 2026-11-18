package runner

import (
	"slices"
	"testing"

	attemptv1 "github.com/openetl-io/openetl/gen/openetl/attempt/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
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

// sent is a schema a test expects the owner to send.
type sent struct {
	to      int
	stream  string
	input   bool
	version uint64
	schema  []byte
	sealed  bool
}

// ownerTest drives a schemaOwner and checks what it sends.
type ownerTest struct {
	t       *testing.T
	co      *sentSchemas
	o       *schemaOwner
	checked int
}

func newOwnerTest(t *testing.T, restored map[ownedKey][]byte) *ownerTest {
	co := &sentSchemas{}
	return &ownerTest{t: t, co: co, o: newSchemaOwner(co, restored)}
}

// propose has worker propose schema of stream from version base, holding
// columns.
func (h *ownerTest) propose(worker int, stream string, input bool, base uint64, schema []byte, holding ...string) {
	h.t.Helper()
	p := &attemptv1.SchemaProposal{Stream: stream, Input: input, Base: base, ArrowSchema: schema}
	for _, c := range holding {
		p.Holding = append(p.Holding, attempt.ColumnHold(checkpoint.ColumnHold{Column: c}))
	}
	if err := h.o.propose(worker, p); err != nil {
		h.t.Fatal(err)
	}
}

// own merges an epoch's shares into r, and returns it.
func (h *ownerTest) own(r *Ready) *Ready {
	h.t.Helper()
	if err := h.o.own(r, false); err != nil {
		h.t.Fatal(err)
	}
	return r
}

// expect checks what the owner sent since the last call.
func (h *ownerTest) expect(what string, want ...sent) {
	h.t.Helper()
	got := h.co.sent[h.checked:]
	if len(got) != len(want) {
		h.t.Fatalf("%s: sent %d schemas, want %d: %v", what, len(got), len(want), got)
	}
	for i, s := range got {
		g := sent{h.co.to[h.checked+i], s.GetStream(), s.GetInput(), s.GetVersion(), s.GetArrowSchema(), s.GetSealed()}
		w := want[i]
		if g.to != w.to || g.stream != w.stream || g.input != w.input || g.version != w.version || !slices.Equal(g.schema, w.schema) || g.sealed != w.sealed {
			h.t.Errorf("%s: sent %+v, want %+v", what, g, w)
		}
	}
	h.checked = len(h.co.sent)
}

// Schemas as the owner holds them: it doesn't read them, so any bytes stand
// for them.
var schemaX, schemaY, schemaZ = []byte("x"), []byte("y"), []byte("z")

// The owner takes a change proposed from its newest version, and answers
// one from an older version with its newest. As it takes the first epoch's
// shares at which one of the destination's streams has columns, it seals
// the stream in a version of its own that every worker gets: a change made
// before the seal gets its proposer the sealed version, and one made from
// it is taken, still sealed. A stream restored with columns is sealed from
// the start, and a stream a SQL transform reads never is.
func TestTheOwnerSealsAStreamAsItTakesTheFirstEpochWithItsColumns(t *testing.T) {
	x, y, z := schemaX, schemaY, schemaZ
	h := newOwnerTest(t, map[ownedKey][]byte{{name: "r"}: x, {name: "in", input: true}: x})
	if r := h.own(&Ready{}); !slices.Equal(r.Schemas["r"], x) {
		t.Errorf("a record has the restored stream's schema as %q", r.Schemas["r"])
	}
	h.expect("an epoch with no new columns")
	h.propose(1, "s", false, 0, x)
	h.propose(2, "s", false, 0, y)
	h.propose(0, "in", true, 0, y)
	h.expect("before an epoch with columns",
		sent{-1, "s", false, 1, x, false}, sent{2, "s", false, 1, x, false}, sent{-1, "in", true, 1, y, false})
	r := h.own(&Ready{})
	h.expect("the first epoch with columns", sent{-1, "s", false, 2, x, true})
	if !slices.Equal(r.Schemas["s"], x) || !slices.Equal(r.Schemas["in"], y) {
		t.Errorf("the epoch's schemas: %v", r.Schemas)
	}
	h.propose(2, "s", false, 1, y)
	h.expect("a change from before the seal", sent{2, "s", false, 2, x, true})
	h.propose(2, "s", false, 2, y)
	h.expect("a change from the sealed version", sent{-1, "s", false, 3, y, true})
	h.own(&Ready{})
	h.expect("the next epoch's shares")
	h.propose(0, "r", false, 0, z)
	h.propose(0, "in", true, 1, z)
	h.expect("restored, and read", sent{-1, "r", false, 1, z, true}, sent{-1, "in", true, 2, z, false})
	r = h.own(&Ready{})
	h.expect("shares after them")
	if !slices.Equal(r.Schemas["r"], z) {
		t.Errorf("the restored stream's schema in the record: %q", r.Schemas["r"])
	}
}

// A record has the owner's newest version as it merges the epoch's shares,
// with the columns held whole, whichever epoch's rows the changes in it
// came from: a change taken before the first merge with columns is part of
// the first schema, the protocol's one exception, and the seal puts every
// later change under the policy.
func TestARecordHasTheNewestVersionAtItsMerge(t *testing.T) {
	x, y := schemaX, schemaY
	h := newOwnerTest(t, nil)
	h.propose(1, "s", false, 0, x, "c")
	h.propose(0, "s", false, 1, y, "d")
	h.expect("changes before the first merge", sent{-1, "s", false, 1, x, false}, sent{-1, "s", false, 2, y, false})
	r := h.own(&Ready{Holds: map[string]*ReadyHolds{"s": {}}})
	h.expect("the first merge", sent{-1, "s", false, 3, y, true})
	// The newest version's holds, not the first's.
	holding := r.Holds["s"].Holding
	if !slices.Equal(r.Schemas["s"], y) || len(holding) != 1 || holding[0].Column != "d" {
		t.Errorf("the record has s's schema %q and holds %v", r.Schemas["s"], holding)
	}
}
