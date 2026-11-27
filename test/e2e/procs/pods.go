package procs

import (
	"maps"
	"regexp"
	"strconv"
	"strings"
)

// Pods keeps what a pod launcher's output, its pods' among it, says of the
// pods: each pipeline's latest engine, the pod of the attempt that started
// that engine, and the attempt's workers' pods. A pod's first line names
// only its attempt, whose ID the orchestrator makes and needn't carry the
// pipeline: on DBOS it starts with the pipeline's workflow, on Temporal
// with the workflow's run. So an attempt is bound to its pipeline once one
// of its engines is ready, as that line names both, and a pod that never
// starts an engine is bound to none. The lines may come in any order. The
// zero value is ready to use. It isn't safe for concurrent use.
type Pods struct {
	// By pipeline: the PID of its latest engine, and the attempt that
	// started it.
	engines map[string]int
	latest  map[string]*attempt
	// Whether a pod or a worker's pod was ever bound to it.
	bound map[string]bool
	// By attempt ID.
	attempts map[string]*attempt
}

type attempt struct {
	// Empty until one of its engines is ready.
	pipeline string
	// Its latest placement's pod's PID, on workers the coordinator's, and
	// that placement.
	pod, podPlacement int
	// Its latest placement's workers' pods, by their number in the log,
	// from 1: worker 1 is the attempt's first, which commits.
	placement int
	workers   map[int]int
}

var (
	// A pod's first line: its PID, attempt and placement.
	podStarted = regexp.MustCompile(`msg="runner running one attempt" .*\bid=\S+:(\d+) attempt=(\S+) placement=(\d+)`)
	// A worker's pod's: its PID, attempt, placement and number.
	workerStarted = regexp.MustCompile(`msg="runner running one worker of an attempt" .*\bid=\S+:(\d+) attempt=(\S+) placement=(\d+) worker=(\d+)`)
	// An engine's: its pipeline and PID, and in a job, the job's owner,
	// which is the attempt and a tag.
	engineReady = regexp.MustCompile(`msg="engine ready" .*pipeline=(\S+) .* pid=(\d+)`)
	engineOwner = regexp.MustCompile(` owner=(\S+)/[^/\s]+(?:\s|$)`)
)

// Line notes what one line of the output says.
func (p *Pods) Line(line string) {
	switch {
	case strings.Contains(line, `msg="runner running one attempt"`):
		if m := podStarted.FindStringSubmatch(line); m != nil {
			a := p.attempt(m[2])
			// A placement's line may come after a later one's.
			if placement, _ := strconv.Atoi(m[3]); placement >= a.podPlacement {
				a.podPlacement = placement
				a.pod, _ = strconv.Atoi(m[1])
				p.bind(a)
			}
		}
	case strings.Contains(line, `msg="runner running one worker of an attempt"`):
		if m := workerStarted.FindStringSubmatch(line); m != nil {
			a := p.attempt(m[2])
			placement, _ := strconv.Atoi(m[3])
			switch {
			case placement < a.placement:
				return
			case placement > a.placement || a.workers == nil:
				a.placement, a.workers = placement, map[int]int{}
			}
			n, _ := strconv.Atoi(m[4])
			a.workers[n], _ = strconv.Atoi(m[1])
			p.bind(a)
		}
	case strings.Contains(line, `msg="engine ready"`):
		pipeline, pid := engine(line)
		if pipeline == "" {
			return
		}
		if p.engines == nil {
			p.engines, p.latest = map[string]int{}, map[string]*attempt{}
		}
		p.engines[pipeline] = pid
		m := engineOwner.FindStringSubmatch(line)
		if m == nil {
			// Its attempt, and so its pod, is unknown.
			delete(p.latest, pipeline)
			return
		}
		a := p.attempt(m[1])
		a.pipeline, p.latest[pipeline] = pipeline, a
		p.bind(a)
	}
}

func (p *Pods) attempt(id string) *attempt {
	if p.attempts == nil {
		p.attempts = map[string]*attempt{}
	}
	a := p.attempts[id]
	if a == nil {
		a = &attempt{}
		p.attempts[id] = a
	}
	return a
}

func (p *Pods) bind(a *attempt) {
	if a.pipeline == "" || a.pod == 0 && len(a.workers) == 0 {
		return
	}
	if p.bound == nil {
		p.bound = map[string]bool{}
	}
	p.bound[a.pipeline] = true
}

// Engine is the PID of the pipeline's latest engine, or 0.
func (p *Pods) Engine(pipeline string) int { return p.engines[pipeline] }

// Pod is the PID of the pod of the attempt that started the pipeline's
// latest engine, or 0.
func (p *Pods) Pod(pipeline string) int {
	if a := p.latest[pipeline]; a != nil {
		return a.pod
	}
	return 0
}

// Workers are the PIDs of the workers' pods of the attempt that started the
// pipeline's latest engine, by their number from 1; none if it runs on one
// pod.
func (p *Pods) Workers(pipeline string) map[int]int {
	if a := p.latest[pipeline]; a != nil {
		return maps.Clone(a.workers)
	}
	return nil
}

// Bound reports whether a pod or a worker's pod was ever bound to the
// pipeline.
func (p *Pods) Bound(pipeline string) bool { return p.bound[pipeline] }

// Engine is the PID of the last engine output says is ready for pipeline,
// or 0.
func Engine(output, pipeline string) int {
	pid := 0
	for _, m := range engineReady.FindAllStringSubmatch(output, -1) {
		if m[1] == pipeline {
			pid, _ = strconv.Atoi(m[2])
		}
	}
	return pid
}

func engine(line string) (pipeline string, pid int) {
	m := engineReady.FindStringSubmatch(line)
	if m == nil {
		return "", 0
	}
	pid, _ = strconv.Atoi(m[2])
	return m[1], pid
}
