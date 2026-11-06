package flink

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
)

// client calls Flink's REST API at base: a session cluster's, or a
// JobManager's through the Kubernetes API server's proxy.
type client struct {
	base  *url.URL
	http  *http.Client
	token string
}

// The API answered 404 or 409.
var (
	errNotFound = errors.New("flink: not found")
	errConflict = errors.New("flink: conflict")
)

func (cl *client) do(ctx context.Context, method, p string, body io.Reader, contentType string, out any) error {
	u := *cl.base
	u.Path = path.Join(u.Path, p)
	if strings.Contains(p, "?") {
		before, query, _ := strings.Cut(p, "?")
		u.Path, u.RawQuery = path.Join(cl.base.Path, before), query
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return errs.Wrap(errs.Internal, err, "flink")
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if cl.token != "" {
		req.Header.Set("Authorization", "Bearer "+cl.token)
	}
	res, err := cl.http.Do(req)
	if err != nil {
		return errs.Wrap(errs.Transient, err, "flink: "+method+" "+redactURL(&u))
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	if err != nil {
		return errs.Wrap(errs.Transient, err, "flink: "+method+" "+redactURL(&u))
	}
	switch {
	case res.StatusCode == http.StatusNotFound:
		return fmt.Errorf("%w: %s %s", errNotFound, method, p)
	case res.StatusCode == http.StatusConflict:
		return fmt.Errorf("%w: %s %s: %s", errConflict, method, p, restErrors(b))
	case res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden:
		return errs.New(errs.ConfigInvalid, "flink: %s %s: %s", method, redactURL(&u), res.Status)
	case res.StatusCode >= 400:
		code := errs.Transient
		if res.StatusCode == http.StatusBadRequest {
			code = errs.ConfigInvalid
		}
		return errs.New(code, "flink: %s %s: %s: %s", method, redactURL(&u), res.Status, restErrors(b))
	}
	if out == nil || len(b) == 0 {
		return nil
	}
	if err := json.Unmarshal(b, out); err != nil {
		return errs.Wrap(errs.Transient, err, "flink: "+method+" "+redactURL(&u))
	}
	return nil
}

// restErrors is a failed call's "errors", each a stack trace cut to its
// first line and its root cause, or its body.
func restErrors(b []byte) string {
	var e struct {
		Errors []string `json:"errors"`
	}
	if json.Unmarshal(b, &e) != nil || len(e.Errors) == 0 {
		return string(b[:min(len(b), 2000)])
	}
	out := make([]string, len(e.Errors))
	for i, msg := range e.Errors {
		out[i] = firstLine(msg)
		if j := strings.LastIndex(msg, "Caused by: "); j >= 0 {
			out[i] += " … " + firstLine(msg[j:])
		}
	}
	return strings.Join(out, "; ")
}

func (cl *client) json(ctx context.Context, method, p string, in, out any) error {
	var body io.Reader
	ct := ""
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return errs.Wrap(errs.Internal, err, "flink")
		}
		body, ct = bytes.NewReader(b), "application/json"
	}
	return cl.do(ctx, method, p, body, ct, out)
}

type jobState struct {
	ID    string `json:"jid"`
	Name  string `json:"name"`
	State string `json:"state"`
}

func terminal(state string) bool {
	return state == "FAILED" || state == "CANCELED" || state == "FINISHED"
}

func (cl *client) job(ctx context.Context, id string) (jobState, error) {
	var out jobState
	return out, cl.json(ctx, http.MethodGet, "jobs/"+id, nil, &out)
}

// exception is the job's latest exception, as Flink's exception history has
// it.
func (cl *client) exception(ctx context.Context, id string) string {
	var out struct {
		History struct {
			Entries []struct {
				Name       string `json:"exceptionName"`
				Stacktrace string `json:"stacktrace"`
			} `json:"entries"`
		} `json:"exceptionHistory"`
	}
	if err := cl.json(ctx, http.MethodGet, "jobs/"+id+"/exceptions?maxExceptions=1", nil, &out); err != nil || len(out.History.Entries) == 0 {
		return "the job failed, and Flink kept no exception"
	}
	e := out.History.Entries[0]
	if e.Stacktrace != "" {
		return e.Stacktrace
	}
	return e.Name
}

// cancel asks for the job to be cancelled; a job that's gone or over is
// left.
func (cl *client) cancel(ctx context.Context, id string) error {
	err := cl.json(ctx, http.MethodPatch, "jobs/"+id+"?mode=cancel", nil, nil)
	if errors.Is(err, errNotFound) || errors.Is(err, errConflict) {
		return nil
	}
	return err
}

// stop stops the job with a synchronous savepoint into dir, and returns
// once the savepoint is complete.
func (cl *client) stop(ctx context.Context, c *Cluster, id, dir string) error {
	var trigger struct {
		RequestID string `json:"request-id"`
	}
	in := map[string]any{"targetDirectory": dir, "drain": false}
	if err := cl.json(ctx, http.MethodPost, "jobs/"+id+"/stop", in, &trigger); err != nil {
		return err
	}
	for {
		var st struct {
			Status struct {
				ID string `json:"id"`
			} `json:"status"`
			Operation struct {
				Location string `json:"location"`
				Failure  *struct {
					Class      string `json:"class"`
					StackTrace string `json:"stack-trace"`
				} `json:"failure-cause"`
			} `json:"operation"`
		}
		if err := cl.json(ctx, http.MethodGet, "jobs/"+id+"/savepoints/"+trigger.RequestID, nil, &st); err != nil {
			return err
		}
		if st.Status.ID == "COMPLETED" {
			if f := st.Operation.Failure; f != nil {
				return errs.New(errs.Transient, "flink: stopping job %s with a savepoint failed: %s", id, firstLine(cmp.Or(f.StackTrace, f.Class)))
			}
			return nil
		}
		if err := wait(ctx, c.poll); err != nil {
			return err
		}
	}
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
}

// waitTerminal polls the job until it's terminal, or gone.
func (cl *client) waitTerminal(ctx context.Context, c *Cluster, id string) (string, error) {
	for {
		st, err := cl.job(ctx, id)
		switch {
		case errors.Is(err, errNotFound):
			return "", nil
		case err != nil:
			return "", err
		case terminal(st.State):
			return st.State, nil
		}
		if err := wait(ctx, c.poll); err != nil {
			return "", err
		}
	}
}

// rest is a standalone session cluster.
type rest struct {
	c  *Cluster
	cl *client
	ui string
	// The cluster's settings are checked once.
	checked sync.Once
	check   error
}

func newREST(spec *apiv1.FlinkRest, c *Cluster) (*rest, error) {
	u, err := url.Parse(spec.GetUrl())
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "flink: rest.url")
	}
	return &rest{c: c, cl: &client{base: u, http: c.http, token: spec.GetAuth().GetStringValue()}, ui: strings.TrimSuffix(spec.GetUrl(), "/")}, nil
}

// clusterOnly are options a session cluster reads from its own
// configuration, not a job's, with the values the runner needs.
var clusterOnly = map[string]func(string) bool{
	"jobmanager.execution.failover-strategy": func(v string) bool { return strings.EqualFold(v, "full") },
	"execution.checkpointing.num-retained": func(v string) bool {
		n, err := strconv.Atoi(v)
		return err == nil && n >= 2
	},
}

// checkConfig refuses a session cluster whose own settings the runner
// can't override per job.
func (r *rest) checkConfig(ctx context.Context) error {
	r.checked.Do(func() {
		var entries []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := r.cl.json(ctx, http.MethodGet, "jobmanager/config", nil, &entries); err != nil {
			r.check = err
			return
		}
		conf := map[string]string{}
		for _, e := range entries {
			conf[e.Key] = e.Value
		}
		// The option's name before 1.20.
		if v, ok := conf["state.checkpoints.num-retained"]; ok && conf["execution.checkpointing.num-retained"] == "" {
			conf["execution.checkpointing.num-retained"] = v
		}
		var bad []string
		for _, k := range slices.Sorted(maps.Keys(clusterOnly)) {
			if !clusterOnly[k](conf[k]) {
				bad = append(bad, fmt.Sprintf("%s (%q)", k, conf[k]))
			}
		}
		if len(bad) > 0 {
			r.check = errs.New(errs.ConfigInvalid, "flink: the session cluster at %s must set jobmanager.execution.failover-strategy: full and execution.checkpointing.num-retained: 2 or more, which a job can't set for itself; it has %s",
				r.ui, strings.Join(bad, ", "))
		}
	})
	return r.check
}

// jar uploads the code copy, unless the cluster has it, and returns its ID.
func (r *rest) jar(ctx context.Context, key string) (string, error) {
	name := strings.ReplaceAll(strings.TrimPrefix(key, "code/"), "/", "-")
	var list struct {
		Files []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"files"`
	}
	if err := r.cl.json(ctx, http.MethodGet, "jars", nil, &list); err != nil {
		return "", err
	}
	for _, f := range list.Files {
		if f.Name == name {
			return f.ID, nil
		}
	}
	src, err := r.c.area.Read(ctx, key)
	if err != nil {
		return "", errs.Wrap(errs.Transient, err, "flink: read the code's copy")
	}
	defer func() { _ = src.Close() }()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("jarfile", name)
	if err != nil {
		return "", errs.Wrap(errs.Internal, err, "flink")
	}
	if _, err := io.Copy(fw, src); err != nil {
		return "", errs.Wrap(errs.Transient, err, "flink: read the code's copy")
	}
	if err := mw.Close(); err != nil {
		return "", errs.Wrap(errs.Internal, err, "flink")
	}
	var up struct {
		Filename string `json:"filename"`
	}
	if err := r.cl.do(ctx, http.MethodPost, "jars/upload", &body, mw.FormDataContentType(), &up); err != nil {
		return "", err
	}
	return path.Base(up.Filename), nil
}

func (r *rest) deploy(ctx context.Context, j *job) (runner.FlinkJob, error) {
	if err := r.checkConfig(ctx); err != nil {
		return nil, err
	}
	jar, err := r.jar(ctx, j.jar)
	if err != nil {
		return nil, err
	}
	conf := map[string]string{}
	for k, v := range j.conf {
		// The request's own fields carry these.
		if k != "$internal.pipeline.job-id" && k != "execution.state-recovery.path" && k != "execution.state-recovery.claim-mode" {
			conf[k] = v
		}
	}
	in := map[string]any{
		"entryClass": j.class, "programArgsList": j.args, "jobId": j.id, "flinkConfiguration": conf,
	}
	if j.parallelism > 0 {
		in["parallelism"] = j.parallelism
	}
	if j.restore != "" {
		in["savepointPath"], in["claimMode"] = j.restore, "NO_CLAIM"
	}
	var out struct {
		JobID string `json:"jobid"`
	}
	if err := r.cl.json(ctx, http.MethodPost, "jars/"+jar+"/run", in, &out); err != nil {
		return nil, err
	}
	if out.JobID != j.id {
		return nil, errs.New(errs.Internal, "flink: the cluster ran job %s, not %s", out.JobID, j.id)
	}
	return &restJob{r: r, id: j.id}, nil
}

func (r *rest) cancelAll(ctx context.Context, _ string, ids []string) error {
	for _, id := range ids {
		if err := r.cl.cancel(ctx, id); err != nil {
			return err
		}
	}
	for _, id := range ids {
		if _, err := r.cl.waitTerminal(ctx, r.c, id); err != nil {
			return err
		}
	}
	return nil
}

type restJob struct {
	r  *rest
	id string
}

func (j *restJob) ID() string { return j.id }

func (j *restJob) UI() string { return j.r.ui + "/#/job/running/" + j.id + "/overview" }

func (j *restJob) Wait(ctx context.Context) error {
	state, err := j.r.cl.waitTerminal(ctx, j.r.c, j.id)
	switch {
	case err != nil:
		return err
	case state == "FINISHED":
		return nil
	case state == "FAILED":
		return failed(j.r.cl.exception(ctx, j.id))
	case state == "":
		return errs.New(errs.Transient, "flink: job %s is gone from the session cluster", j.id)
	}
	return errs.New(errs.Transient, "flink: job %s was cancelled", j.id)
}

func (j *restJob) Stop(ctx context.Context, dir string) error {
	if err := j.r.cl.stop(ctx, j.r.c, j.id, dir); err != nil {
		return err
	}
	_, err := j.r.cl.waitTerminal(ctx, j.r.c, j.id)
	return err
}

func (j *restJob) Cancel(ctx context.Context) error {
	if err := j.r.cl.cancel(ctx, j.id); err != nil {
		return err
	}
	_, err := j.r.cl.waitTerminal(ctx, j.r.c, j.id)
	return err
}
