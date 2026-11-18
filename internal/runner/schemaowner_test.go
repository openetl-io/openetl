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
	to          int
	stream      string
	input       bool
	version     uint64
	schema      []byte
	sealedAfter uint64
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
// epoch without the policy, or under it with epoch 0, holding columns.
func (h *ownerTest) propose(worker int, stream string, input bool, base uint64, schema []byte, epoch uint64, holding ...string) {
	h.t.Helper()
	p := &attemptv1.SchemaProposal{Stream: stream, Input: input, Base: base, ArrowSchema: schema, Epoch: epoch}
	for _, c := range holding {
		p.Holding = append(p.Holding, attempt.ColumnHold(checkpoint.ColumnHold{Column: c}))
	}
	if err := h.o.propose(worker, p); err != nil {
		h.t.Fatal(err)
	}
}

// own merges epoch n's shares into r, and returns it.
func (h *ownerTest) own(r *Ready, n uint64) *Ready {
	h.t.Helper()
	if err := h.o.own(r, false, n); err != nil {
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
		g := sent{h.co.to[h.checked+i], s.GetStream(), s.GetInput(), s.GetVersion(), s.GetArrowSchema(), s.GetSealedAfter()}
		w := want[i]
		if g.to != w.to || g.stream != w.stream || g.input != w.input || g.version != w.version || !slices.Equal(g.schema, w.schema) || g.sealedAfter != w.sealedAfter {
			h.t.Errorf("%s: sent %+v, want %+v", what, g, w)
		}
	}
	h.checked = len(h.co.sent)
}

// Schemas as the owner holds them: it doesn't read them, so any bytes stand
// for them.
var schemaX, schemaY, schemaZ, schemaW = []byte("x"), []byte("y"), []byte("z"), []byte("w")

// The owner seals each of the destination's streams after the epoch of its
// first change as it takes that epoch's shares, in a version of its own
// that every worker gets: a change a worker proposed from before the seal
// gets it the sealed version, not taken, and one from it is taken, still
// sealed. A stream restored with columns is under the policy from the start
// on every engine, and isn't sealed again; a stream a SQL transform reads
// never is. The record's schema is the sealed version's.
func TestTheOwnerSealsAStreamAsItTakesTheFirstEpochWithItsColumns(t *testing.T) {
	x, y, z := schemaX, schemaY, schemaZ
	h := newOwnerTest(t, map[ownedKey][]byte{{name: "r"}: x, {name: "in", input: true}: x})
	h.propose(1, "s", false, 0, x, 1)
	h.propose(0, "in", true, 0, y, 1)
	h.expect("before an epoch", sent{-1, "s", false, 1, x, 0}, sent{-1, "in", true, 1, y, 0})
	r := h.own(&Ready{}, 1)
	h.expect("the first epoch's shares", sent{-1, "s", false, 2, x, 1})
	if !slices.Equal(r.Schemas["s"], x) || !slices.Equal(r.Schemas["in"], y) {
		t.Errorf("the epoch's schemas: %v", r.Schemas)
	}
	h.propose(2, "s", false, 1, y, 2)
	h.expect("a change from before the seal", sent{2, "s", false, 2, x, 1})
	h.propose(2, "s", false, 2, y, 0)
	h.expect("a change from the sealed version", sent{-1, "s", false, 3, y, 1})
	h.own(&Ready{}, 2)
	h.expect("the next epoch's shares")
	h.propose(0, "r", false, 0, z, 0)
	h.propose(0, "in", true, 1, z, 4)
	h.expect("restored, and read", sent{-1, "r", false, 1, z, 0}, sent{-1, "in", true, 2, z, 0})
	r = h.own(&Ready{}, 3)
	h.expect("shares after them")
	if !slices.Equal(r.Schemas["r"], z) {
		t.Errorf("the restored stream's schema in the record: %q", r.Schemas["r"])
	}
}

// A change a lane made without the policy is taken while no change of an
// earlier epoch is: lanes' changes in one epoch join, and one of an earlier
// epoch that comes later still does. One made in a later epoch than a
// change taken seals the stream after that change's epoch instead, and
// every worker gets the sealed version, so the lane shapes its batch again
// under the policy, and its change from that is taken. A lane still in that
// epoch makes changes without the policy after the seal too, and they join.
// A stream a SQL transform reads never seals.
func TestTheOwnerSealsAStreamOnAChangeOfALaterEpoch(t *testing.T) {
	x, y, z, w := schemaX, schemaY, schemaZ, schemaW
	h := newOwnerTest(t, nil)
	h.propose(0, "s", false, 0, x, 1)
	h.propose(1, "s", false, 1, y, 1)
	h.expect("one epoch's changes", sent{-1, "s", false, 1, x, 0}, sent{-1, "s", false, 2, y, 0})
	h.propose(1, "s", false, 2, z, 3)
	h.expect("a later epoch's change", sent{-1, "s", false, 3, y, 1})
	h.propose(1, "s", false, 3, z, 0)
	h.expect("the change under the policy", sent{-1, "s", false, 4, z, 1})
	h.propose(0, "s", false, 4, w, 1)
	h.expect("a change of the first epoch, after the seal", sent{-1, "s", false, 5, w, 1})
	h.own(&Ready{}, 1)
	h.expect("the shares, sealed already")

	h.propose(0, "t", false, 0, x, 4)
	h.propose(1, "t", false, 1, y, 2)
	h.propose(1, "t", false, 2, z, 3)
	h.expect("an earlier epoch's change after a later one's",
		sent{-1, "t", false, 1, x, 0}, sent{-1, "t", false, 2, y, 0}, sent{-1, "t", false, 3, y, 2})

	h.propose(0, "in", true, 0, x, 1)
	h.propose(0, "in", true, 1, y, 5)
	h.own(&Ready{}, 1)
	h.expect("read", sent{-1, "in", true, 1, x, 0}, sent{-1, "in", true, 2, y, 0})
}

// A merge of epoch n records a stream as the owner has it, and seals it,
// only if its first change is of epoch n or before. A change of epoch 2
// that comes before epoch 1's shares are merged is in neither epoch 1's
// record nor a seal at its merge: the record keeps what its shares say, as
// they had no rows of the stream. Epoch 2's merge records it, holds and
// all, and seals the stream after epoch 2.
func TestAMergeRecordsOnlyChangesOfItsEpoch(t *testing.T) {
	x := schemaX
	h := newOwnerTest(t, nil)
	h.propose(1, "s", false, 0, x, 2, "c")
	h.expect("epoch 2's change", sent{-1, "s", false, 1, x, 0})
	r := h.own(&Ready{Holds: map[string]*ReadyHolds{"s": {}}}, 1)
	h.expect("epoch 1's shares")
	if _, ok := r.Schemas["s"]; ok || len(r.Holds["s"].Holding) != 0 {
		t.Errorf("epoch 1's record has s's schema %q and holds %v", r.Schemas["s"], r.Holds["s"].Holding)
	}
	r = h.own(&Ready{Holds: map[string]*ReadyHolds{"s": {}}}, 2)
	h.expect("epoch 2's shares", sent{-1, "s", false, 2, x, 2})
	if !slices.Equal(r.Schemas["s"], x) || len(r.Holds["s"].Holding) != 1 {
		t.Errorf("epoch 2's record has s's schema %q and holds %v", r.Schemas["s"], r.Holds["s"].Holding)
	}

	// A change made under the policy says no epoch: the next merge records
	// it, and seals the stream after its own epoch.
	h.propose(0, "u", false, 0, x, 0)
	h.expect("a change under the policy", sent{-1, "u", false, 1, x, 0})
	r = h.own(&Ready{}, 3)
	h.expect("epoch 3's shares", sent{-1, "u", false, 2, x, 3})
	if !slices.Equal(r.Schemas["u"], x) {
		t.Errorf("epoch 3's record has u's schema %q", r.Schemas["u"])
	}
}
