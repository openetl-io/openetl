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

// sent is a schema a test expects the owner to send.
type sent struct {
	to      int
	stream  string
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

// propose has worker propose schema of stream from version base, made in
// epoch without the policy, or under it with epoch 0.
func (h *ownerTest) propose(worker int, stream string, input bool, base uint64, schema []byte, epoch uint64) {
	h.t.Helper()
	if err := h.o.propose(worker, &attemptv1.SchemaProposal{Stream: stream, Input: input, Base: base, ArrowSchema: schema, Epoch: epoch}); err != nil {
		h.t.Fatal(err)
	}
}

func (h *ownerTest) own() *Ready {
	h.t.Helper()
	r := &Ready{}
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
		g := sent{h.co.to[h.checked+i], s.GetStream(), s.GetVersion(), s.GetArrowSchema(), s.GetSealed()}
		if g.to != want[i].to || g.stream != want[i].stream || g.version != want[i].version || !slices.Equal(g.schema, want[i].schema) || g.sealed != want[i].sealed {
			h.t.Errorf("%s: sent %+v, want %+v", what, g, want[i])
		}
	}
	h.checked = len(h.co.sent)
}

// Schemas as the owner holds them: it doesn't read them, so any bytes stand
// for them.
var schemaX, schemaY, schemaZ = []byte("x"), []byte("y"), []byte("z")

// The owner seals each of the destination's streams as it takes the shares
// of the first epoch with its columns, in a version of its own that every
// worker gets: a change a worker proposed from before the seal gets it the
// sealed version, not taken, and one from it is taken, still sealed. A
// stream restored with columns is sealed from the start, and a stream a SQL
// transform reads never is. The record's schema is the sealed version's.
func TestTheOwnerSealsAStreamAsItTakesTheFirstEpochWithItsColumns(t *testing.T) {
	x, y, z := schemaX, schemaY, schemaZ
	h := newOwnerTest(t, map[ownedKey][]byte{{name: "r"}: x, {name: "in", input: true}: x})
	h.propose(1, "s", false, 0, x, 1)
	h.propose(0, "in", true, 0, y, 1)
	h.expect("before an epoch", sent{-1, "s", 1, x, false}, sent{-1, "in", 1, y, false})
	r := h.own()
	h.expect("the first epoch's shares", sent{-1, "s", 2, x, true})
	if !slices.Equal(r.Schemas["s"], x) || !slices.Equal(r.Schemas["in"], y) {
		t.Errorf("the epoch's schemas: %v", r.Schemas)
	}
	h.propose(2, "s", false, 1, y, 1)
	h.expect("a change from before the seal", sent{2, "s", 2, x, true})
	h.propose(2, "s", false, 2, y, 0)
	h.expect("a change from the sealed version", sent{-1, "s", 3, y, true})
	h.own()
	h.expect("the next epoch's shares")
	h.propose(0, "r", false, 0, z, 0)
	h.propose(0, "in", true, 1, z, 4)
	h.expect("restored, and read", sent{-1, "r", 1, z, true}, sent{-1, "in", 2, z, false})
	h.own()
	h.expect("shares after them")
}

// A change a lane made without the policy is taken while no change of an
// earlier epoch is: lanes' changes in one epoch join, and one of an earlier
// epoch that comes later still does. One made in a later epoch than a
// change taken seals the stream instead, and every worker gets the sealed
// version, so the lane shapes its batch again under the policy, and its
// change from that is taken. A stream a SQL transform reads never seals.
func TestTheOwnerSealsAStreamOnAChangeOfALaterEpoch(t *testing.T) {
	x, y, z := schemaX, schemaY, schemaZ
	h := newOwnerTest(t, nil)
	h.propose(0, "s", false, 0, x, 1)
	h.propose(1, "s", false, 1, y, 1)
	h.expect("one epoch's changes", sent{-1, "s", 1, x, false}, sent{-1, "s", 2, y, false})
	h.propose(1, "s", false, 2, z, 3)
	h.expect("a later epoch's change", sent{-1, "s", 3, y, true})
	h.propose(1, "s", false, 3, z, 0)
	h.expect("the change under the policy", sent{-1, "s", 4, z, true})
	h.own()
	h.expect("the shares, sealed already")

	h.propose(0, "t", false, 0, x, 4)
	h.propose(1, "t", false, 1, y, 2)
	h.propose(1, "t", false, 2, z, 3)
	h.expect("an earlier epoch's change after a later one's",
		sent{-1, "t", 1, x, false}, sent{-1, "t", 2, y, false}, sent{-1, "t", 3, y, true})

	h.propose(0, "in", true, 0, x, 1)
	h.propose(0, "in", true, 1, y, 5)
	h.own()
	h.expect("read", sent{-1, "in", 1, x, false}, sent{-1, "in", 2, y, false})
}
