package sdk_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	sdk "github.com/openetl-io/openetl/sdk/go"
)

// liveAPI serves records sorted by id, not by their cursor u, one a page,
// to requests for u at or after since. It reads its records at each
// request, and after serving page n it applies change[n], as writes landing
// while a read is under way.
type liveAPI struct {
	u      map[int]int
	change map[int]map[int]int
	served int
}

func (a *liveAPI) stream() sdk.Stream[config] {
	return sdk.Incremental("b", "u", func(_ *sdk.Ctx[config], s sdk.Cursor, out *sdk.Out) error {
		since := -1
		if !s.IsZero() {
			var err error
			if since, err = strconv.Atoi(s.String()); err != nil {
				return err
			}
		}
		for page, err := range sdk.Paginate(out, func(_ context.Context, token string) ([]json.RawMessage, string, error) {
			after := 0
			if token != "" {
				after, _ = strconv.Atoi(token)
			}
			ids := make([]int, 0, len(a.u))
			for id, u := range a.u {
				if id > after && u >= since {
					ids = append(ids, id)
				}
			}
			if len(ids) == 0 {
				return nil, "", nil
			}
			id := slices.Min(ids)
			rec := json.RawMessage(fmt.Sprintf(`{"id":"%d@%d","u":%d}`, id, a.u[id], a.u[id]))
			a.served++
			for k, u := range a.change[a.served] {
				a.u[k] = u
			}
			return []json.RawMessage{rec}, strconv.Itoa(id), nil
		}) {
			if err != nil {
				return err
			}
			if err := out.EmitJSON(page...); err != nil {
				return err
			}
		}
		return nil
	})
}

// readAll reads stream b from state, and gives the versions read, the last
// resume point's state and where the next sync starts.
func readAll(t *testing.T, src *sdk.Source[config], state string) (rows []string, resume, next string) {
	t.Helper()
	for _, c := range read(t, src, "b", state) {
		switch {
		case strings.HasPrefix(c, "b rows "):
			for _, v := range strings.Split(strings.TrimPrefix(c, "b rows "), ",") {
				rows = append(rows, strings.Trim(v, `"`))
			}
		case strings.HasPrefix(c, "b resume "):
			resume = strings.TrimPrefix(c, "b resume ")
		case strings.HasPrefix(c, "b complete "):
			next = strings.TrimPrefix(c, "b complete ")
		}
	}
	return rows, resume, next
}

// A record updated after its page was read, with a later page carrying a
// newer cursor, is read again by a later sync: the floor doesn't pass it.
func TestIncrementalUnorderedReadsUpdatesMadeDuringARead(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state string
		// Stop the first read after this many pages, and read the rest from
		// its last resume point, as a retry does. 0: read it whole.
		stopAfter int
	}{
		{name: "the first sync"},
		{name: "a saved floor", state: `{"cursor":1}`},
		{name: "a floor and the highest of the reads before", state: `{"cursor":1,"high":2}`},
		{name: "a resumed read", state: `{"cursor":1}`, stopAfter: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Record 1 changes to 4 after its page is read, and record 3,
			// on a later page, to 5.
			api := &liveAPI{u: map[int]int{1: 1, 2: 2, 3: 3}, change: map[int]map[int]int{1: {1: 4, 3: 5}}}
			src := &sdk.Source[config]{Name: "acme", Streams: []sdk.Stream[config]{api.stream()}}
			state := tc.state
			var got []string
			if tc.stopAfter > 0 {
				// The first attempt fails after stopAfter pages, and the retry
				// starts from the resume point after them.
				var resumes []string
				for _, c := range read(t, src, "b", state) {
					if s, ok := strings.CutPrefix(c, "b resume "); ok {
						resumes = append(resumes, s)
					}
				}
				got = append(got, "1@1", "2@2")
				state = resumes[tc.stopAfter-1]
			}
			for range 3 {
				rows, _, next := readAll(t, src, state)
				got = append(got, rows...)
				state = next
			}
			if !slices.Contains(got, "1@4") {
				t.Errorf("record 1's update was never read: read %v", got)
			}
		})
	}
}
