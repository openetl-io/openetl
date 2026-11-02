// Package faker is a bounded source of generated rows. Row i of a stream
// depends only on the seed and i, so a resumed read yields exactly the rows
// after its resume point, or with an overlap, those and the ones just before
// it.
package faker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/unsuitable001/openetl/sdk/go/source"
)

type Config struct {
	Seed uint64 `json:"seed"`
	// Rows per stream. Default 1000.
	Count int `json:"count"`
	// Rows per batch; each batch ends at a resume point. Default 100.
	BatchSize int `json:"batchSize"`
	// A pause after each batch, such as "50ms".
	Delay string `json:"delay"`
	// Rows before its resume point that a resumed read emits again, like a
	// cursor's lookback window. Above 0, replay is superset.
	Overlap int `json:"overlap"`
	// The streams declare no primary key, and id as their cursor, like
	// tables without one.
	Keyless bool `json:"keyless"`
	// Every twice-th row is a copy of the row before it, as a table without
	// a primary key can hold a row twice. Only with keyless.
	Twice int `json:"twice"`
}

type Source struct {
	cfg   Config
	delay time.Duration
}

var _ source.Source = (*Source)(nil)

func New(config []byte) (*Source, error) {
	cfg := Config{Count: 1000, BatchSize: 100}
	if len(bytes.TrimSpace(config)) > 0 {
		dec := json.NewDecoder(bytes.NewReader(config))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("faker: config: %w", err)
		}
	}
	s := &Source{cfg: cfg}
	switch {
	case cfg.Count < 0:
		return nil, errors.New("faker: count must not be negative")
	case cfg.BatchSize <= 0:
		return nil, errors.New("faker: batchSize must be positive")
	case cfg.Overlap < 0:
		return nil, errors.New("faker: overlap must not be negative")
	case cfg.Twice < 0 || cfg.Twice > 0 && !cfg.Keyless:
		return nil, errors.New("faker: twice must not be negative, and needs keyless")
	}
	if cfg.Delay != "" {
		d, err := time.ParseDuration(cfg.Delay)
		if err != nil || d < 0 {
			return nil, fmt.Errorf("faker: delay %q is not a duration", cfg.Delay)
		}
		s.delay = d
	}
	return s, nil
}

type stream struct {
	name string
	row  func(r *rand.Rand, i, count int) any
}

var streams = []stream{
	{name: "users", row: user},
	{name: "purchases", row: purchase},
}

func (s *Source) Discover(context.Context) (source.Catalog, error) {
	var c source.Catalog
	replay := source.Exact
	if s.cfg.Overlap > 0 {
		replay = source.Superset
	}
	for _, st := range streams {
		cs := source.CatalogStream{Name: st.name, PrimaryKey: []string{"id"}, Replay: replay, SyncModes: []source.SyncMode{source.FullRefresh}}
		if s.cfg.Keyless {
			cs.PrimaryKey, cs.CursorField = nil, "id"
		}
		c.Streams = append(c.Streams, cs)
	}
	return c, nil
}

type state struct {
	// Rows emitted so far.
	Next int `json:"next"`
}

func (s *Source) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	for _, cs := range req.Streams {
		idx := -1
		for i, st := range streams {
			if st.name == cs.Name {
				idx = i
			}
		}
		if idx < 0 {
			return fmt.Errorf("faker: unknown stream %q", cs.Name)
		}
		var st state
		if cs.State != nil {
			if err := json.Unmarshal(cs.State, &st); err != nil {
				return fmt.Errorf("faker: %s state: %w", cs.Name, err)
			}
		}
		if err := s.readStream(ctx, idx, max(st.Next-s.cfg.Overlap, 0), out); err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) readStream(ctx context.Context, idx, next int, out source.Emitter) error {
	st := streams[idx]
	for next < s.cfg.Count {
		end := min(next+s.cfg.BatchSize, s.cfg.Count)
		rows := make([]json.RawMessage, 0, end-next)
		for i := next; i < end; i++ {
			from := i
			if s.cfg.Twice > 0 && i > 0 && i%s.cfg.Twice == 0 {
				from = i - 1
			}
			r := rand.New(rand.NewPCG(s.cfg.Seed, uint64(from)<<8|uint64(idx))) //nolint:gosec // generated test data must be reproducible
			b, err := json.Marshal(st.row(r, from, s.cfg.Count))
			if err != nil {
				return err
			}
			rows = append(rows, b)
		}
		if err := out.Rows(st.name, rows); err != nil {
			return err
		}
		next = end
		resume, err := json.Marshal(state{Next: next})
		if err != nil {
			return err
		}
		if err := out.ResumePoint(st.name, resume); err != nil {
			return err
		}
		if s.delay > 0 && next < s.cfg.Count {
			t := time.NewTimer(s.delay)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			}
		}
	}
	return out.Complete(st.name, nil)
}

var (
	firstNames = []string{"Ada", "Alan", "Barbara", "Edsger", "Grace", "John", "Ken", "Leslie", "Margaret", "Radia"}
	lastNames  = []string{"Hamilton", "Hopper", "Knuth", "Lamport", "Liskov", "Lovelace", "Perlman", "Ritchie", "Thompson", "Turing"}
	cities     = []string{"Accra", "Berlin", "Kolkata", "Lima", "Osaka", "Oslo", "Toronto"}
	currencies = []string{"EUR", "GBP", "INR", "JPY", "USD"}
	epoch      = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
)

type address struct {
	City string `json:"city"`
	Zip  string `json:"zip"`
}

type userRow struct {
	ID        int         `json:"id"`
	Name      string      `json:"name"`
	Email     string      `json:"email"`
	Age       int         `json:"age"`
	Score     json.Number `json:"score"`
	Active    bool        `json:"active"`
	Address   address     `json:"address"`
	CreatedAt string      `json:"created_at"`
}

func user(r *rand.Rand, i, _ int) any {
	first, last := firstNames[r.IntN(len(firstNames))], lastNames[r.IntN(len(lastNames))]
	return userRow{
		ID:        i + 1,
		Name:      first + " " + last,
		Email:     fmt.Sprintf("%s.%s.%d@example.com", first, last, i+1),
		Age:       18 + r.IntN(70),
		Score:     decimal(r.Float64()*100, 3),
		Active:    r.IntN(4) != 0,
		Address:   address{City: cities[r.IntN(len(cities))], Zip: fmt.Sprintf("%05d", r.IntN(100000))},
		CreatedAt: epoch.Add(time.Duration(i) * 37 * time.Second).Format(time.RFC3339),
	}
}

type purchaseRow struct {
	ID        int         `json:"id"`
	UserID    int         `json:"user_id"`
	Amount    json.Number `json:"amount"`
	Currency  string      `json:"currency"`
	CreatedAt string      `json:"created_at"`
}

func purchase(r *rand.Rand, i, count int) any {
	return purchaseRow{
		ID:        i + 1,
		UserID:    1 + r.IntN(max(count, 1)),
		Amount:    decimal(float64(r.IntN(100000))/100, 2),
		Currency:  currencies[r.IntN(len(currencies))],
		CreatedAt: epoch.Add(time.Duration(i) * 53 * time.Second).Format(time.RFC3339),
	}
}

// decimal always has a fractional part, so the raw lane infers a float.
func decimal(v float64, places int) json.Number {
	return json.Number(strconv.FormatFloat(v, 'f', places, 64))
}
