package pods

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/openetl-io/openetl/internal/platform/errs"
)

// Processes runs each attempt's runner as a process of its own on this
// machine, as a pod would on Kubernetes. It's for development and tests: its
// processes stop with it.
type Processes struct {
	// openetld's path, and its arguments from the name on, which make it a
	// runner like the launcher.
	Command []string
	// The environment the processes get besides Env's.
	Environ []string
	Pool    string

	mu    sync.Mutex
	procs map[string]*process
}

type process struct {
	pod  Pod
	cmd  *exec.Cmd
	done chan struct{}
	// How long it has to stop once it's deleted.
	grace time.Duration
}

// Launch starts l's processes: its workers', if it has any, and its own.
// If one can't start, it stops those it started.
func (p *Processes) Launch(_ context.Context, l Launch) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.procs == nil {
		p.procs = map[string]*process{}
	}
	var started []*process
	for _, worker := range l.pods() {
		name := WorkerName(l.Attempt, l.Placement, worker)
		if _, ok := p.procs[name]; ok {
			continue
		}
		pr, err := p.start(l, worker, name)
		if err != nil {
			for _, s := range started {
				delete(p.procs, s.pod.Name)
				go stop(s)
			}
			return err
		}
		started = append(started, pr)
	}
	return nil
}

// start starts one process of l; p.mu is held.
func (p *Processes) start(l Launch, worker int, name string) (*process, error) {
	// Not the launch's context: the process outlives the call.
	cmd := &exec.Cmd{
		Path: p.Command[0], Args: p.Command, Env: append(slices.Clone(p.Environ), Env(l, p.Pool, worker)...),
		Stdout: os.Stderr, Stderr: os.Stderr,
	}
	if err := cmd.Start(); err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "pods: start the attempt's process")
	}
	pr := &process{pod: Pod{Name: name, Attempt: l.Attempt, Placement: l.Placement, Worker: worker}, cmd: cmd, done: make(chan struct{}), grace: time.Duration(l.StopTimeout+30) * time.Second}
	p.procs[name] = pr
	go func() {
		err := cmd.Wait()
		p.mu.Lock()
		defer p.mu.Unlock()
		pr.pod.Ended = true
		switch {
		case err != nil:
			pr.pod.Failure = errs.Wrap(errs.Transient, err, "pods: its process "+name+" failed")
		case worker == 0:
			pr.pod.Failure = errs.New(errs.Transient, "its process %s ended without the attempt's result", name)
		}
		close(pr.done)
	}()
	return pr, nil
}

func (p *Processes) Pods(context.Context) ([]Pod, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Pod, 0, len(p.procs))
	for _, pr := range p.procs {
		out = append(out, pr.pod)
	}
	return out, nil
}

// Delete stops a running process as Kubernetes stops a pod: SIGTERM, then
// SIGKILL after its grace.
func (p *Processes) Delete(_ context.Context, pod Pod) error {
	p.mu.Lock()
	pr, ok := p.procs[pod.Name]
	delete(p.procs, pod.Name)
	p.mu.Unlock()
	if ok {
		go stop(pr)
	}
	return nil
}

func stop(pr *process) {
	if err := pr.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		_ = pr.cmd.Process.Kill()
	}
	select {
	case <-pr.done:
	case <-time.After(pr.grace):
		_ = pr.cmd.Process.Kill()
	}
}

// Close stops every process, and waits for them.
func (p *Processes) Close() {
	p.mu.Lock()
	procs := p.procs
	p.procs = nil
	p.mu.Unlock()
	var wg sync.WaitGroup
	for _, pr := range procs {
		wg.Go(func() {
			stop(pr)
			<-pr.done
		})
	}
	wg.Wait()
}
