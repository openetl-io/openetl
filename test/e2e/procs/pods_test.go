package procs

import (
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
)

// The testdata are pod launchers' output from chaos runs, as the suite
// keeps it: on Temporal, a stream's attempt on one pod and another's on two
// workers, a sync's on workers, and an attempt whose pods never started an
// engine; on DBOS, a stream's on one pod, two attempts of a sync on three
// workers, and a sync whose pods never started an engine.
type pipelinePods struct {
	pipeline string
	pod      int
	workers  map[int]int
	engine   int
}

var launcherLogs = map[string][]pipelinePods{
	"testdata/launcher-temporal.log": {
		{pipeline: "965bd3db-9a28-4ac6-86fa-e51a6244d0c2", pod: 263502, engine: 263537},
		{pipeline: "2647813d-916d-486b-a54e-98930426c1f3", pod: 276313, workers: map[int]int{1: 276311, 2: 276312}, engine: 276516},
		{pipeline: "3cf9f6ce-467c-40e1-86d2-c798b95a467a", pod: 222712, workers: map[int]int{1: 222709, 2: 222710}, engine: 223541},
		// Its pods found the attempt wasn't theirs any more.
		{pipeline: "ea585031-19aa-42c9-bd8e-6f06a1a3e4b0"},
	},
	"testdata/launcher-dbos.log": {
		{pipeline: "fd885879-00c3-44d4-9b9b-bd250b54d83b", pod: 276717, engine: 276753},
		// The second attempt's, which started the latest engine.
		{pipeline: "e3d4993f-f564-49f2-a455-d2ed41cc78f9", pod: 22068, workers: map[int]int{1: 22065, 2: 22066, 3: 22067}, engine: 22411},
		{pipeline: "7e3c69e3-7912-4c0b-b38b-5cf24ca13c42"},
	},
}

func lines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

func check(t *testing.T, p *Pods, want []pipelinePods) {
	t.Helper()
	for _, w := range want {
		bound := w.pod != 0 || len(w.workers) > 0
		if got := p.Pod(w.pipeline); got != w.pod {
			t.Errorf("%s: pod %d, want %d", w.pipeline, got, w.pod)
		}
		if got := p.Workers(w.pipeline); !maps.Equal(got, w.workers) {
			t.Errorf("%s: workers %v, want %v", w.pipeline, got, w.workers)
		}
		if got := p.Engine(w.pipeline); got != w.engine {
			t.Errorf("%s: engine %d, want %d", w.pipeline, got, w.engine)
		}
		if got := p.Bound(w.pipeline); got != bound {
			t.Errorf("%s: bound %v, want %v", w.pipeline, got, bound)
		}
	}
}

// Every pod's and worker's pod's first line parses, on either
// orchestrator, whatever the shape of its attempt's ID.
func TestEveryPodsFirstLineParses(t *testing.T) {
	for path := range launcherLogs {
		pods, workers := 0, 0
		for _, l := range lines(t, path) {
			switch {
			case strings.Contains(l, `msg="runner running one attempt"`):
				pods++
				if podStarted.FindStringSubmatch(l) == nil {
					t.Errorf("%s: a pod's first line doesn't parse: %s", path, l)
				}
			case strings.Contains(l, `msg="runner running one worker of an attempt"`):
				workers++
				if workerStarted.FindStringSubmatch(l) == nil {
					t.Errorf("%s: a worker's pod's first line doesn't parse: %s", path, l)
				}
			case strings.Contains(l, `msg="engine ready"`):
				if engineReady.FindStringSubmatch(l) == nil || engineOwner.FindStringSubmatch(l) == nil {
					t.Errorf("%s: an engine's line doesn't parse: %s", path, l)
				}
			}
		}
		if pods == 0 || workers == 0 {
			t.Errorf("%s: %d pods' and %d workers' first lines", path, pods, workers)
		}
	}
}

// A pipeline's pod and workers' pods are those of the attempt that started
// its latest engine, on Temporal, whose attempt IDs don't carry the
// pipeline, as on DBOS, whose do. Pods that never started an engine are no
// pipeline's.
func TestPodsAreTheirEnginesPipelines(t *testing.T) {
	for path, want := range launcherLogs {
		t.Run(path, func(t *testing.T) {
			var p Pods
			for _, l := range lines(t, path) {
				p.Line(l)
			}
			check(t, &p, want)
		})
	}
}

// Engines' lines may come before their pods', as two processes' output
// interleaves, and bind them all the same.
func TestPodsBindInEitherOrder(t *testing.T) {
	for path, want := range launcherLogs {
		t.Run(path, func(t *testing.T) {
			all := lines(t, path)
			engines := slices.DeleteFunc(slices.Clone(all), func(l string) bool { return !strings.Contains(l, `msg="engine ready"`) })
			others := slices.DeleteFunc(slices.Clone(all), func(l string) bool { return strings.Contains(l, `msg="engine ready"`) })
			var p Pods
			for _, l := range append(engines, others...) {
				p.Line(l)
			}
			check(t, &p, want)
		})
	}
}

// A placement of the attempt's workers again replaces the workers of the
// one before, and a pod of the same attempt placed again replaces its pod.
func TestAPlacementAgainReplacesItsPods(t *testing.T) {
	all := lines(t, "testdata/launcher-temporal.log")
	var p Pods
	for _, l := range all {
		p.Line(l)
	}
	const pipeline, attempt = "2647813d-916d-486b-a54e-98930426c1f3", "attempt=93ee6905-4446-4ca2-96cd-1bbcd24808d0/job-01a21cc2-0bda-77b6-afd9-6b7ffe0a03aa/1"
	again := []string{
		`time=2026-11-27T11:27:50.000Z level=INFO msg="runner running one worker of an attempt" role=runner id=runnervm0hi8g:290001 ` + attempt + ` placement=2 worker=1`,
		`time=2026-11-27T11:27:50.010Z level=INFO msg="runner running one attempt" role=runner id=runnervm0hi8g:290003 ` + attempt + ` placement=2`,
	}
	for _, l := range again {
		p.Line(l)
	}
	if got := p.Pod(pipeline); got != 290003 {
		t.Errorf("pod %d, want 290003", got)
	}
	if got, want := p.Workers(pipeline), map[int]int{1: 290001}; !maps.Equal(got, want) {
		t.Errorf("workers %v, want %v", got, want)
	}
}

// An engine whose line names no owner, as a planner's, is still the
// pipeline's latest, and binds no pod.
func TestAnEngineWithoutAnOwnerBindsNothing(t *testing.T) {
	var p Pods
	p.Line(`time=2026-11-27T11:20:00.000Z level=INFO msg="runner running one attempt" role=runner id=h:10 attempt=a/1 placement=1`)
	p.Line(`time=2026-11-27T11:20:00.100Z level=INFO msg="engine ready" role=runner pipeline=p component=engine pid=11 version=0.0.0`)
	if p.Engine("p") != 11 || p.Pod("p") != 0 || p.Bound("p") {
		t.Errorf("engine %d, pod %d, bound %v; want 11, 0, false", p.Engine("p"), p.Pod("p"), p.Bound("p"))
	}
}

// Engine finds a runner's latest engine for the pipeline in its whole
// output.
func TestEngineIsTheLatestInTheOutput(t *testing.T) {
	for path, want := range launcherLogs {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range want {
			if got := Engine(string(b), w.pipeline); got != w.engine {
				t.Errorf("%s: %s's engine %d, want %d", path, w.pipeline, got, w.engine)
			}
		}
	}
}
