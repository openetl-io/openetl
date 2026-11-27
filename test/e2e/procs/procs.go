// Package procs finds the chaos suite's targets among processes: a
// pipeline's pods, workers' pods and engines, by what their launcher's
// output says of them (see Pods); and its connectors, only where they can
// be told apart from every other pipeline's. A connector's process carries
// nothing of its pipeline, so it's the pipeline's only by its parent: a
// pod, which runs one attempt, or a runner that runs no other pipeline.
package procs

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
)

// Host is what starts connectors: a launcher's pod, or a runner, and the
// one pipeline the runner is dedicated to, if any.
type Host struct {
	PID int
	Pod bool
	// The only pipeline the runner runs; empty if it may run others.
	Only string
}

// Owner is the PID whose children are pipeline's connectors and no other
// pipeline's: a pod's, or a runner's dedicated to it; 0 if there's none.
func Owner(h Host, pipeline string) int {
	if h.PID <= 0 || pipeline == "" || !h.Pod && h.Only != pipeline {
		return 0
	}
	return h.PID
}

// pgrep is where macOS and Linux's procps both install it, so no directory
// on PATH chooses what runs.
const pgrep = "/usr/bin/pgrep"

// Children are the PIDs of parent's child processes whose names start with
// prefix; none for a parent of 0.
func Children(ctx context.Context, parent int, prefix string) []int {
	if parent <= 0 {
		return nil
	}
	out, _ := exec.CommandContext(ctx, pgrep, "-P", strconv.Itoa(parent), "^"+prefix).Output()
	var pids []int
	for _, f := range strings.Fields(string(out)) {
		if pid, err := strconv.Atoi(f); err == nil {
			pids = append(pids, pid)
		}
	}
	return pids
}
