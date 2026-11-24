package runner

import "github.com/openetl-io/openetl/internal/checkpoint"

// OnWait sets what w calls as the coordinator starts to wait on its
// workers, with the step: "recover", then "assign".
func OnWait(w *Workers, f func(step string)) { w.onWait = f }

// ReadTarget is what j's read of s replaces, where it's an overwrite.
func ReadTarget(j *Job, s Stream) *checkpoint.ReadTarget { return j.readTarget(s) }
