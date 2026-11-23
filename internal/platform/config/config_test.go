package config

import (
	"flag"
	"slices"
	"testing"
)

func TestSettingsListEveryFlag(t *testing.T) {
	fs := flag.NewFlagSet("openetld", flag.ContinueOnError)
	if _, err := Parse(fs, nil, nil); err != nil {
		t.Fatal(err)
	}
	var flags, settings []string
	fs.VisitAll(func(f *flag.Flag) { flags = append(flags, f.Name) })
	for _, s := range Settings() {
		if s.Flag != "" {
			settings = append(settings, s.Flag)
		}
	}
	slices.Sort(settings)
	if !slices.Equal(flags, settings) {
		t.Errorf("Settings has flags %v; Parse defines %v", settings, flags)
	}
	for f := range DevDefaults() {
		if fs.Lookup(f) == nil {
			t.Errorf("DevDefaults has %s, which isn't a flag", f)
		}
	}
}

func TestContinuousFailureLimit(t *testing.T) {
	c, err := Parse(flag.NewFlagSet("openetld", flag.ContinueOnError), nil, nil)
	if err != nil || c.ContinuousFailureLimit != 5 {
		t.Errorf("the default is %d, %v", c.ContinuousFailureLimit, err)
	}
	if _, err := Parse(flag.NewFlagSet("openetld", flag.ContinueOnError), []string{"--continuous-failure-limit", "0"}, nil); err == nil {
		t.Error("a limit of 0 parsed")
	}
}
