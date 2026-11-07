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
}

// Pod is a pod a launcher made, as it is now.
type Pod struct {
	Name      string
	Attempt   string
	Placement int32
	// It has ended. Failure says why, if it didn't end well.
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

// Env is what a pod's runner needs to run its one attempt, as openetld's
// environment.
func Env(l Launch, pool string) []string {
	return []string{
		"OPENETL_ATTEMPT=" + l.Attempt,
		fmt.Sprintf("OPENETL_PLACEMENT=%d", l.Placement),
		"OPENETL_POOL=" + pool,
		"OPENETL_CAPACITY=" + l.Size.Flag(),
	}
}
