//go:build integration

package heal_test

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/heal"
)

func (f *fixture) pausedStreams(t *testing.T, mode string) map[string]*uuid.UUID {
	t.Helper()
	rows, err := f.conn.Query(t.Context(), `SELECT stream, incident_id FROM paused_streams WHERE pipeline_id = $1`, f.pipelines[mode])
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]*uuid.UUID{}
	for rows.Next() {
		var s string
		var id *uuid.UUID
		if err := rows.Scan(&s, &id); err != nil {
			t.Fatal(err)
		}
		out[s] = id
	}
	return out
}

func (f *fixture) override(t *testing.T, mode, field string) string {
	t.Helper()
	var v string
	err := f.conn.QueryRow(t.Context(), `SELECT coalesce((SELECT value::text FROM pipeline_overrides WHERE pipeline_id = $1 AND field = $2), '')`,
		f.pipelines[mode], field).Scan(&v)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func quarantined(p uuid.UUID, stream string, read apiv1.SyncMode, others int) heal.Observation {
	sync := uuid.New()
	return heal.Observation{
		Pipeline: p, Stream: stream, Class: heal.Quarantine, Severity: heal.Warning, Message: "500 of 1000",
		Details: map[string]string{"share": "50.0%", "quarantine_file": "s3://lake/o/p/_quarantine/1/4/" + stream + ".jsonl"},
		Sync:    &sync, Key: sync.String(), Read: read, OtherStreams: others,
	}
}

// Quarantine's playbook pauses the stream where it can stop on its own,
// the pipeline where it's the only stream, and nothing where it shares the
// source's change log.
func TestQuarantinePausesTheStream(t *testing.T) {
	f := newFixture(t)
	p := f.pipelines[""]
	incremental, cdc := apiv1.SyncMode_SYNC_MODE_INCREMENTAL, apiv1.SyncMode_SYNC_MODE_CDC

	seen := f.observe(t, quarantined(p, "items", incremental, 2))
	if !seen.Opened || !seen.StreamPaused || seen.Paused || f.paused(t, "") {
		t.Fatalf("first sighting: %+v", seen)
	}
	if got := f.pausedStreams(t, ""); len(got) != 1 || got["items"] == nil || *got["items"] != seen.Incident.ID {
		t.Errorf("paused streams %v, want items, by incident %s", got, seen.Incident.ID)
	}
	if as := actions(t, seen.Incident); len(as) != 1 || as[0].Kind != "pause_stream" || as[0].State != "applied" {
		t.Errorf("actions %+v", as)
	}
	for _, want := range []string{"s3://lake/o/p/_quarantine/1/4/items.jsonl", "Heal paused the stream", "openetl resume --stream items p0"} {
		if !strings.Contains(seen.Incident.NextSteps, want) {
			t.Errorf("next steps don't say %q: %s", want, seen.Incident.NextSteps)
		}
	}
	if got := f.events(t); len(got) != 2 || got[0] != events.IncidentOpened || got[1] != events.StreamPaused {
		t.Errorf("events %v", got)
	}
	if again := f.observe(t, quarantined(p, "items", incremental, 2)); again.StreamPaused || len(actions(t, again.Incident)) != 1 {
		t.Errorf("a paused stream was paused again: %+v", again)
	}

	shared := f.observe(t, quarantined(p, "orders", cdc, 2))
	if shared.StreamPaused || shared.Paused || len(actions(t, shared.Incident)) != 0 {
		t.Errorf("a cdc stream was paused: %+v", shared)
	}
	if !strings.Contains(shared.Incident.NextSteps, "can't pause the stream on its own: it's read with cdc") {
		t.Errorf("next steps don't say why: %s", shared.Incident.NextSteps)
	}

	suggest := f.observe(t, quarantined(f.pipelines["HEAL_MODE_SUGGEST"], "items", incremental, 1))
	if as := actions(t, suggest.Incident); suggest.StreamPaused || len(as) != 1 || as[0].State != "proposed" || len(f.pausedStreams(t, "HEAL_MODE_SUGGEST")) != 0 {
		t.Errorf("heal mode suggest: %+v, actions %+v", suggest, as)
	}

	only := f.observe(t, quarantined(p, "solo", incremental, 0))
	if !only.Paused || only.StreamPaused || !f.paused(t, "") || f.pausedStreams(t, "")["solo"] != nil {
		t.Errorf("the only stream: %+v", only)
	}
}

// Syncs that keep failing rate limited slow the pipeline down every third
// time: a declarative source's declared rate limit halves, and the
// schedule's interval doubles where there's no limit to lower or the sync
// ran as long as the interval.
func TestRateLimitedSlowsDown(t *testing.T) {
	limited := func(f *fixture, ran time.Duration) heal.Seen {
		sync := uuid.New()
		return f.observe(t, heal.Observation{
			Pipeline: f.pipelines[""], Class: heal.RateLimited, Code: "RATE_LIMITED", Message: "429", Sync: &sync, Key: sync.String(), Ran: ran,
		})
	}
	hourly := func(t *testing.T, f *fixture) {
		if _, err := f.conn.Exec(t.Context(), `UPDATE pipeline_versions SET spec = '{"schedule": {"every": "3600s"}}' WHERE pipeline_id = $1`, f.pipelines[""]); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("schedule", func(t *testing.T) {
		f := newFixture(t)
		hourly(t, f)
		for i := range 2 {
			if seen := limited(f, time.Minute); seen.Schedule != nil || len(actions(t, seen.Incident)) != 0 {
				t.Fatalf("sync %d slowed down: %+v", i+1, seen)
			}
		}
		seen := limited(f, time.Minute)
		if seen.Schedule.GetEvery().AsDuration() != 2*time.Hour {
			t.Fatalf("the third sync: schedule %v, want every 2h", seen.Schedule)
		}
		if got := f.override(t, "", heal.OverrideEvery); got != `"7200s"` {
			t.Errorf("override %s", got)
		}
		if as := actions(t, seen.Incident); len(as) != 1 || as[0].Kind != "widen_schedule" || !strings.Contains(as[0].Description, "every 2h instead of every 1h") {
			t.Errorf("actions %+v", as)
		}
		if got := f.events(t); len(got) != 2 || got[0] != events.IncidentOpened || got[1] != events.PipelineOverridden {
			t.Errorf("events %v", got)
		}
		for range 3 {
			seen = limited(f, time.Minute)
		}
		if seen.Schedule.GetEvery().AsDuration() != 4*time.Hour || f.override(t, "", heal.OverrideEvery) != `"14400s"` {
			t.Errorf("the sixth sync: schedule %v", seen.Schedule)
		}
	})

	t.Run("declarative", func(t *testing.T) {
		f := newFixture(t)
		hourly(t, f)
		manifest := `
apiVersion: openetl.io/v1alpha1
kind: DeclarativeSource
metadata: { name: acme, version: 0.1.0 }
spec:
  config: { type: object, properties: { base_url: { type: string } } }
  http:
    base_url: "{config.base_url}"
    rate_limit: { rps: 10 }
  streams:
    - { name: a, request: { path: /a } }
`
		if _, err := f.conn.Exec(t.Context(), `INSERT INTO artifacts (org_id, digest, media_type, content) VALUES ($1, 'sha256:s', $2, $3)`,
			f.org, connectorhost.MediaTypeManifest, []byte(manifest)); err != nil {
			t.Fatal(err)
		}
		for range 3 {
			limited(f, time.Minute)
		}
		if got, every := f.override(t, "", heal.OverrideRateLimitScale), f.override(t, "", heal.OverrideEvery); got != "0.5" || every != "" {
			t.Fatalf("after 3 syncs: scale %q, every %q; want 0.5 and none, as the sync was short", got, every)
		}
		var seen heal.Seen
		for range 3 {
			seen = limited(f, 2*time.Hour)
		}
		if got := f.override(t, "", heal.OverrideRateLimitScale); got != "0.25" || seen.Schedule.GetEvery().AsDuration() != 2*time.Hour {
			t.Fatalf("after 6 syncs: scale %q, schedule %v; want 0.25 and every 2h, as the sync ran longer than the interval", got, seen.Schedule)
		}
		for range 6 {
			limited(f, time.Minute)
		}
		if got := f.override(t, "", heal.OverrideRateLimitScale); got != "0.125" {
			t.Errorf("after 12 syncs: scale %q, want the floor, 0.125", got)
		}
	})

	t.Run("off", func(t *testing.T) {
		f := newFixture(t)
		for range 3 {
			sync := uuid.New()
			seen := f.observe(t, heal.Observation{Pipeline: f.pipelines["HEAL_MODE_OFF"], Class: heal.RateLimited, Code: "RATE_LIMITED", Message: "429", Sync: &sync, Key: sync.String()})
			if seen.Schedule != nil || len(actions(t, seen.Incident)) != 0 {
				t.Fatalf("heal mode off: %+v", seen)
			}
		}
	})
}
