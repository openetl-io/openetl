package scaling

import (
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/platform/resources"
)

var t0 = time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)

// run is a job's samples every 30 s for d, its lanes busy and backpressured
// for those shares of their time, from start.
func run(start time.Time, d time.Duration, lanes int, busy, backpressured float64) []Sample {
	var out []Sample
	for at := time.Duration(0); at <= d; at += 30 * time.Second {
		lane := at.Seconds() * float64(lanes)
		out = append(out, Sample{
			Time: start.Add(at), Lanes: lanes,
			Busy: lane * busy, Backpressured: lane * backpressured, Idle: lane * (1 - busy - backpressured),
		})
	}
	return out
}

// then carries samples on for d, as busy and backpressured.
func then(samples []Sample, d time.Duration, busy, backpressured float64) []Sample {
	last := samples[len(samples)-1]
	for at := 30 * time.Second; at <= d; at += 30 * time.Second {
		lane := at.Seconds() * float64(last.Lanes)
		samples = append(samples, Sample{
			Time: last.Time.Add(at), Lanes: last.Lanes, Limit: last.Limit,
			Busy: last.Busy + lane*busy, Backpressured: last.Backpressured + lane*backpressured, Idle: last.Idle + lane*(1-busy-backpressured),
		})
	}
	return samples
}

// lagging is a lag every 10 s from start to end, going from one value to
// another.
func lagging(start, end time.Time, from, to float64) []Lag {
	var out []Lag
	span := end.Sub(start)
	for at := time.Duration(0); at <= span; at += 10 * time.Second {
		out = append(out, Lag{Time: start.Add(at), Value: from + (to-from)*at.Seconds()/span.Seconds()})
	}
	return out
}

func TestDecide(t *testing.T) {
	end := t0.Add(40 * time.Minute)
	rise := lagging(t0, end, 1, 60)
	flat := lagging(t0, end, 5, 5)
	limited := run(t0, 40*time.Minute, 2, 0.95, 0)
	for i := range limited {
		limited[i].Limit = 2
	}
	for _, tc := range []struct {
		name string
		in   Input
		want Decision
	}{
		{"nothing yet", Input{Now: t0}, Decision{Busy: -1, Backpressured: -1}},
		{
			"too soon to say",
			Input{Now: t0.Add(4 * time.Minute), Samples: run(t0, 4*time.Minute, 1, 1, 0), Lag: rise},
			Decision{Busy: -1, Backpressured: -1},
		},
		{
			"busy and falling behind: grows",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 1, 0.9, 0), Lag: rise},
			Decision{Lanes: 2, Busy: 0.9, Backpressured: 0},
		},
		{
			"to the next preset's lanes",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 2, 0.9, 0), Lag: rise},
			Decision{Lanes: 4, Busy: 0.9, Backpressured: 0},
		},
		{
			"busy but keeping up: stays",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 1, 0.9, 0), Lag: flat},
			Decision{Busy: 0.9, Backpressured: 0},
		},
		{
			"falling behind but not busy: the source is the limit",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 2, 0.6, 0), Lag: rise},
			Decision{Busy: 0.6, Backpressured: 0},
		},
		{
			"waiting for the destination: a note",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 2, 0.3, 0.6), Lag: rise},
			Decision{Note: DestinationBound, Busy: 0.3, Backpressured: 0.6},
		},
		{
			"waiting for the destination and keeping up: left as it is",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 4, 0.1, 0.6), Lag: flat},
			Decision{Busy: 0.1, Backpressured: 0.6},
		},
		{
			"under half busy for half an hour: shrinks",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 4, 0.3, 0), Lag: flat},
			Decision{Lanes: 2, Busy: 0.3, Backpressured: 0},
		},
		{
			"to one lane",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 2, 0.2, 0), Lag: flat},
			Decision{Lanes: 1, Busy: 0.2, Backpressured: 0},
		},
		{
			"not for less than half an hour",
			Input{Now: t0.Add(20 * time.Minute), Samples: run(t0, 20*time.Minute, 4, 0.3, 0), Lag: flat},
			Decision{Busy: 0.3, Backpressured: 0},
		},
		{
			"not after a busy spell within it",
			Input{Now: end, Samples: then(then(run(t0, 20*time.Minute, 4, 0.3, 0), 5*time.Minute, 0.8, 0), 15*time.Minute, 0.3, 0), Lag: flat},
			Decision{Busy: 0.3, Backpressured: 0},
		},
		{
			"not while falling behind",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 4, 0.3, 0), Lag: rise},
			Decision{Busy: 0.3, Backpressured: 0},
		},
		{
			"a set size stays",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 1, 0.9, 0), Lag: rise, Fixed: true},
			Decision{Busy: 0.9, Backpressured: 0},
		},
		{
			"ten minutes between rescales",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 1, 0.9, 0), Lag: rise, LastRescale: end.Add(-9 * time.Minute)},
			Decision{Busy: 0.9, Backpressured: 0},
		},
		{
			"after ten minutes",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 1, 0.9, 0), Lag: rise, LastRescale: end.Add(-10 * time.Minute)},
			Decision{Lanes: 2, Busy: 0.9, Backpressured: 0},
		},
		{
			"no more than max",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 2, 0.95, 0), Lag: rise, Max: resources.M},
			Decision{Busy: 0.95, Backpressured: 0},
		},
		{
			"no more than xl without max",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 8, 0.95, 0), Lag: rise},
			Decision{Busy: 0.95, Backpressured: 0},
		},
		{
			"no more than its splits",
			Input{Now: end, Samples: limited, Lag: rise},
			Decision{Busy: 0.95, Backpressured: 0},
		},
		{
			"no fewer than min",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 4, 0.1, 0), Lag: flat, Min: resources.M},
			Decision{Lanes: 2, Busy: 0.1, Backpressured: 0},
		},
		{
			"a lag that doesn't go back far enough says nothing",
			Input{Now: end, Samples: run(t0, 40*time.Minute, 1, 0.9, 0), Lag: lagging(end.Add(-time.Minute), end, 1, 60)},
			Decision{Busy: 0.9, Backpressured: 0},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Decide(tc.in)
			round := func(v float64) float64 { return float64(int(v*1000+0.5)) / 1000 }
			if tc.in.Samples != nil && got.Busy >= 0 {
				got.Busy, got.Backpressured = round(got.Busy), round(got.Backpressured)
			}
			if got != tc.want {
				t.Fatalf("Decide = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestRising(t *testing.T) {
	since := t0
	for _, tc := range []struct {
		name string
		lag  []Lag
		want bool
	}{
		{"rising", lagging(t0.Add(-time.Minute), t0.Add(5*time.Minute), 1, 10), true},
		{"falling", lagging(t0.Add(-time.Minute), t0.Add(5*time.Minute), 10, 1), false},
		{"flat", lagging(t0.Add(-time.Minute), t0.Add(5*time.Minute), 3, 3), false},
		{"a spike that settles back", []Lag{{t0.Add(-time.Second), 5}, {t0.Add(time.Minute), 50}, {t0.Add(4 * time.Minute), 4}}, false},
		{"up at the end of a downward trend", []Lag{{t0.Add(-time.Second), 5}, {t0.Add(time.Minute), 50}, {t0.Add(2 * time.Minute), 1}, {t0.Add(4 * time.Minute), 6}}, false},
		{"none before the window", lagging(t0, t0.Add(5*time.Minute), 1, 10), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := rising(tc.lag, since); got != tc.want {
				t.Fatalf("rising = %v, want %v", got, tc.want)
			}
		})
	}
}
