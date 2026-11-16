package runner

// OnWait sets what w calls as the coordinator starts to wait on its
// workers, with the step: "recover", then "assign".
func OnWait(w *Workers, f func(step string)) { w.onWait = f }
