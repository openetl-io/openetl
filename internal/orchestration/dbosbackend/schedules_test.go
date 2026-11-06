package dbosbackend

import (
	"testing"
	"time"
)

func TestTimetable(t *testing.T) {
	start := time.Date(2026, 11, 7, 10, 7, 30, 0, time.UTC)
	hourly := timetable{every: time.Hour}
	for _, tc := range []struct {
		name string
		t    timetable
		due  time.Time
		now  time.Time
		want time.Time
	}{
		{"on time", hourly, start, start, start.Add(time.Hour)},
		{"a little late", hourly, start, start.Add(time.Minute), start.Add(time.Hour)},
		{"many missed make up one", hourly, start, start.Add(5*time.Hour + time.Minute), start.Add(6 * time.Hour)},
		{"cron goes by the clock", timetable{cron: "0 12 * * *"}, start, start, time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)},
		{"cron's descriptors", timetable{cron: "@hourly"}, start, start, time.Date(2026, 11, 7, 11, 0, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.t.after(tc.due, tc.now)
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
