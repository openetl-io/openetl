package conformance

import "context"

// RunMatching runs the suite's checks whose names match, as Run does.
func (s SourceSuite) RunMatching(ctx context.Context, match func(check string) bool, report func(Result)) {
	r := newSourceRun(s)
	defer r.close()
	run(ctx, matching(r.checks(), match), report)
}

// RunMatching runs the suite's checks whose names match, as Run does.
func (s DestinationSuite) RunMatching(ctx context.Context, match func(check string) bool, report func(Result)) {
	r := newDestinationRun(s)
	defer r.close()
	run(ctx, matching(r.checks(), match), report)
}

func matching(checks []Check, match func(string) bool) []Check {
	if match == nil {
		return checks
	}
	var out []Check
	for _, c := range checks {
		if match(c.Name) {
			out = append(out, c)
		}
	}
	return out
}
