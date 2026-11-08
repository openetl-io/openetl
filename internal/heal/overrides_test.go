package heal

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

func every(d time.Duration) *apiv1.Schedule {
	return &apiv1.Schedule{Trigger: &apiv1.Schedule_Every{Every: durationpb.New(d)}, Jitter: durationpb.New(time.Minute)}
}

func TestSchedule(t *testing.T) {
	got, err := Schedule(every(time.Hour), []byte(`"7200s"`))
	if err != nil || got.GetEvery().AsDuration() != 2*time.Hour || got.GetJitter().AsDuration() != time.Minute {
		t.Errorf("every 1h overridden by 2h: %v, %v", got, err)
	}
	cron := &apiv1.Schedule{Trigger: &apiv1.Schedule_Cron{Cron: "0 6 * * *"}}
	if got, _ := Schedule(cron, []byte(`"7200s"`)); got.GetCron() != "0 6 * * *" {
		t.Errorf("a cron was overridden: %v", got)
	}
	if got, _ := Schedule(every(time.Hour), nil); got.GetEvery().AsDuration() != time.Hour {
		t.Errorf("no override changed the schedule: %v", got)
	}
	if _, err := Schedule(every(time.Hour), []byte(`"soon"`)); err == nil {
		t.Error("a bad override parsed")
	}
}

func TestKept(t *testing.T) {
	hourly := &apiv1.PipelineSpec{Schedule: every(time.Hour)}
	for _, c := range []struct {
		field     string
		next      *apiv1.PipelineSpec
		src, nsrc string
		want      bool
		why       string
	}{
		{OverrideEvery, &apiv1.PipelineSpec{Schedule: every(time.Hour), Heal: &apiv1.Heal{Mode: apiv1.HealMode_HEAL_MODE_SUGGEST}}, "a", "a", true, "another field changed"},
		{OverrideEvery, &apiv1.PipelineSpec{Schedule: every(30 * time.Minute)}, "a", "a", false, "the schedule changed"},
		{OverrideEvery, &apiv1.PipelineSpec{Schedule: &apiv1.Schedule{Trigger: &apiv1.Schedule_Cron{Cron: "@daily"}}}, "a", "a", false, "it became a cron"},
		{OverrideEvery, &apiv1.PipelineSpec{Mode: apiv1.PipelineMode_PIPELINE_MODE_CONTINUOUS}, "a", "a", false, "it became continuous"},
		{OverrideRateLimitScale, hourly, "a", "a", true, "the connector is the same"},
		{OverrideRateLimitScale, hourly, "a", "b", false, "the connector changed"},
	} {
		if got := Kept(c.field, hourly, c.next, c.src, c.nsrc); got != c.want {
			t.Errorf("%s when %s: kept %v, want %v", c.field, c.why, got, c.want)
		}
	}
}

func TestShort(t *testing.T) {
	for d, want := range map[time.Duration]string{
		2 * time.Hour: "2h", 90 * time.Minute: "1h30m", 30 * time.Minute: "30m", 45 * time.Second: "45s", 24 * time.Hour: "24h", 61 * time.Second: "1m1s",
	} {
		if got := short(d); got != want {
			t.Errorf("short(%v) = %q, want %q", d, got, want)
		}
	}
}

func TestQuarantineStops(t *testing.T) {
	incremental, cdc := apiv1.SyncMode_SYNC_MODE_INCREMENTAL, apiv1.SyncMode_SYNC_MODE_CDC
	for _, c := range []struct {
		read      apiv1.SyncMode
		transform bool
		others    int
		want      stopping
	}{
		{incremental, false, 2, stopStream},
		{apiv1.SyncMode_SYNC_MODE_STREAM, false, 1, stopStream},
		{incremental, false, 0, stopPipeline},
		{cdc, false, 2, stopNothing},
		{cdc, false, 0, stopNothing},
		{incremental, true, 2, stopNothing},
		{apiv1.SyncMode_SYNC_MODE_UNSPECIFIED, false, 2, stopNothing},
	} {
		got, why := quarantineStops(Observation{Read: c.read, OtherStreams: c.others}, target{transform: c.transform})
		if got != c.want || (got == stopNothing) != (why != "") {
			t.Errorf("read %v, transform %v, %d others: %v (%q), want %v", c.read, c.transform, c.others, got, why, c.want)
		}
	}
}
