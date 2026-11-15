package engineclient

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/runner"
)

// keyVectors are keys and the group openetl.kg.v1 gives each, which
// crates/normalize asserts too.
type keyVectors struct {
	Groups int `json:"groups"`
	Keys   []struct {
		Type      string   `json:"type"`
		Spellings []string `json:"spellings"`
		Group     int      `json:"group"`
	} `json:"keys"`
}

// The engine's filter by a range of key groups, which a redo of a writer
// lane's part uses, keeps each JSON key of the shared vectors, in every
// spelling, by its group there, and no other key.
func TestTheEngineKeepsJSONKeysByTheSharedGroups(t *testing.T) {
	raw, err := os.ReadFile("../../crates/normalize/testdata/key_groups.json")
	if err != nil {
		t.Fatal(err)
	}
	var v keyVectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	ctx := testContext(t)
	df := &Dataflow{Options: options(t, nil)}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "k", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.NewMetadata([]string{"ARROW:extension:name"}, []string{"arrow.json"})},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	// Every spelling of every JSON key, row n of key owner[n].
	var all []string
	var owner []int
	for i, k := range v.Keys {
		if k.Type != "json" {
			continue
		}
		for _, s := range k.Spellings {
			all = append(all, fmt.Sprintf(`{"k": %s, "n": %d}`, s, len(all)))
			owner = append(owner, i)
		}
	}
	for i, k := range v.Keys {
		if k.Type != "json" {
			continue
		}
		p := plan()
		p.KeyGroups = v.Groups
		p.Streams = []runner.Stream{{Name: "a", PrimaryKey: []string{"k"}, Schema: schema}}
		p.KeyGroupRanges = map[string]checkpoint.KeyGroupRange{"a": {First: k.Group, Last: k.Group}}
		got, err := run(ctx, df, p,
			runner.Frame{Kind: runner.FrameRows, Stream: "a", Rows: rows(all...)},
			runner.Frame{Kind: runner.FrameBarrier, Stream: "a", Epoch: 5},
		)
		if err != nil {
			release(got)
			t.Fatal(err)
		}
		var kept []int
		for _, f := range got["a"] {
			if f.Kind != runner.FrameBatch {
				continue
			}
			idx := f.Batch.Schema().FieldIndices("n")
			if len(idx) != 1 {
				t.Fatalf("key %d: no column n in %v", i, f.Batch.Schema())
			}
			ns := f.Batch.Column(idx[0]).(*array.Int64)
			for r := range ns.Len() {
				kept = append(kept, int(ns.Value(r)))
			}
		}
		release(got)
		var want []int
		for n, o := range owner {
			if v.Keys[o].Group == k.Group {
				want = append(want, n)
			}
		}
		slices.Sort(kept)
		if !slices.Equal(kept, want) {
			t.Errorf("key %d, group %d: the engine kept rows %v, want %v", i, k.Group, kept, want)
		}
	}
}
