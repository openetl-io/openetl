package dbosbackend

import (
	"testing"
	"time"
)

func TestTimetable(t *testing.T) {
	start := time.Date(2026, 11, 7, 10, 7, 30, 0, time.UTC)
	inIST := start.In(time.FixedZone("IST", 5*60*60+30*60))
	hour := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	hourly := timetable{every: time.Hour}
	for _, tc := range []struct {
		name string
		t    timetable
		now  time.Time
		want time.Time
	}{
		{"on time", hourly, hour, hour.Add(time.Hour)},
		{"a little late", hourly, hour.Add(time.Minute), hour.Add(time.Hour)},
		{"many missed make up one", hourly, hour.Add(5*time.Hour + time.Minute), hour.Add(6 * time.Hour)},
		{"intervals count from the epoch", hourly, start, hour.Add(time.Hour)},
		{"as Temporal's do", timetable{every: 7 * time.Hour}, start, time.Unix(0, 0).Add(7 * time.Hour * time.Duration(start.Unix()/(7*3600)+1))},
		{"in any zone", timetable{every: 10 * time.Minute}, inIST, time.Date(2026, 11, 7, 10, 10, 0, 0, time.UTC)},
		{"cron goes by the clock", timetable{cron: "0 12 * * *"}, start, time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)},
		{"cron's descriptors", timetable{cron: "@hourly"}, start, time.Date(2026, 11, 7, 11, 0, 0, 0, time.UTC)},
		{"cron is in UTC wherever this runs", timetable{cron: "0 12 * * *"}, inIST, time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)},
		{"cron can name a zone", timetable{cron: "CRON_TZ=Asia/Kolkata 0 12 * * *"}, start, time.Date(2026, 11, 8, 6, 30, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.t.next(tc.now)
			if err != nil || !got.Equal(tc.want) {
				t.Errorf("got %v, %v; want %v", got, err, tc.want)
			}
		})
	}
	if _, err := (timetable{cron: "not a cron"}).next(start); err == nil {
		t.Error("a bad cron parsed")
	}
	jittered := timetable{every: time.Hour, jitter: time.Minute}
	for range 100 {
		if at := jittered.fireAt(start); at.Before(start) || !at.Before(start.Add(time.Minute)) {
			t.Fatalf("fires at %v", at)
		}
	}
	if at := hourly.fireAt(start); !at.Equal(start) {
		t.Errorf("fires at %v without jitter", at)
	}
}

func TestSystemDatabaseURL(t *testing.T) {
	got, err := SystemDatabaseURL("postgres://u:p@host:5432/openetl?sslmode=require")
	if want := "postgres://u:p@host:5432/openetl_dbos?sslmode=require"; err != nil || got != want {
		t.Errorf("got %q, %v; want %q", got, err, want)
	}
	for _, bad := range []string{"host=x dbname=y", "postgres://host"} {
		if _, err := SystemDatabaseURL(bad); err == nil {
			t.Errorf("%q has no name to go by", bad)
		}
	}
}
