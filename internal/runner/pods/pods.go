// Package pods makes a pod for each attempt placed on a pod launcher:
// on Kubernetes, or as a process of its own, for development and tests.
// The pod runs openetld runner for that one attempt, which it takes from the
// API.
package pods

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/openetl-io/openetl/internal/platform/resources"
)

// Launch is a pod to make for an attempt.
type Launch struct {
	Attempt   string
	Placement int32
	// RunSyncAttempt or RunStreamJob.
	Kind string
	Size resources.Size
	// Its pipeline's ID, if the input names one.
	Pipeline string
	// How long its runner takes to stop at a checkpoint.
	StopTimeout int64
	// Set: the attempt runs on this many workers, each in a pod of Size, and
	// its own pod, the coordinator's, is of Coordinator. The launcher makes
	// them all, or none.
	Workers     int
	Coordinator resources.Size
}

// Pod is a pod a launcher made, as it is now.
type Pod struct {
	Name      string
	Attempt   string
	Placement int32
	// Which of the attempt's workers it runs, counting from 1; 0: the
	// attempt's own runner, which coordinates its workers if it has any.
	Worker int
	// It has ended. Failure says why, if it didn't end well: a worker's pod
	// that ends well has none, as its coordinator reports the attempt.
	Ended   bool
	Failure error
	// Why it hasn't started, when something stops it, such as an image that
	// can't be pulled.
	Stuck string
}

// Launcher makes and deletes pods.
type Launcher interface {
	// Launch makes l's pod. A pod that's there already counts.
	Launch(ctx context.Context, l Launch) error
	// Pods lists the pods it made that are still there.
	Pods(ctx context.Context) ([]Pod, error)
	// Delete deletes a pod, which stops at a checkpoint first if it's
	// running; a pod that's gone counts.
	Delete(ctx context.Context, p Pod) error
}

// Name is the pod of an attempt's placement: its attempt's ID can't be one.
func Name(attempt string, placement int32) string {
	sum := sha256.Sum256([]byte(attempt))
	return fmt.Sprintf("openetl-attempt-%s-%d", hex.EncodeToString(sum[:8]), placement)
}

// WorkerName is the pod of worker, counting from 1, of an attempt's
// placement; 0 is Name's.
func WorkerName(attempt string, placement int32, worker int) string {
	if worker == 0 {
		return Name(attempt, placement)
	}
	return fmt.Sprintf("%s-w%d", Name(attempt, placement), worker)
}

// AttemptPort is where a coordinator's pod serves its workers the attempt
// protocol.
const AttemptPort = 7720

// Env is what a pod's runner needs to run its one attempt, or worker, from
// 1, of it, as openetld's environment.
func Env(l Launch, pool string, worker int) []string {
	size := l.Size
	if l.Workers > 0 && worker == 0 {
		size = l.Coordinator
	}
	env := []string{
		"OPENETL_ATTEMPT=" + l.Attempt,
		fmt.Sprintf("OPENETL_PLACEMENT=%d", l.Placement),
		"OPENETL_POOL=" + pool,
		"OPENETL_CAPACITY=" + size.Flag(),
	}
	if worker > 0 {
		env = append(env, fmt.Sprintf("OPENETL_WORKER=%d", worker))
	}
	return env
}

// Pods are the pods of l, its workers' first: worker 1 up to l.Workers, then
// 0, the attempt's own runner, which they wait for.
func (l Launch) pods() []int {
	out := make([]int, 0, l.Workers+1)
	for i := range l.Workers {
		out = append(out, i+1)
	}
	return append(out, 0)
}
