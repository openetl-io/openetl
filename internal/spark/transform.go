package spark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// Protocol is the version of the files through which the runner and
// openetl-spark's driver hand each other epochs.
const Protocol = 1

// TransformDriver is the main class of a jar's transform, which openetl-spark
// holds; spark.openetl.transform names the user's Transform.
const TransformDriver = "io.openetl.spark.TransformDriver"

type submission struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// Failed is a Spark application that ran the user's code and failed, which
// no retry changes.
type Failed struct{ Message string }

func (f *Failed) Error() string { return f.Message }

// TransformName is the application's name for a pipeline's attempt: unique
// to its generation, lowercase, and a valid Kubernetes name.
func TransformName(pipeline string, gen uint64) string {
	return "openetl-" + strings.ToLower(pipeline) + "-g" + strconv.FormatUint(gen, 10)
}

// Transform is one attempt's Spark transform (ADR-0022 §2): one application,
// which openetl-spark's driver runs, and the files below {g}/ of the
// pipeline's area through which the runner hands it each epoch:
//   - _alive, rewritten while the attempt runs, and _end once it's over;
//   - {N}/in/{stream}/, the inputs the first dataflow staged, and
//     {N}/in/_ready.json, which lists them;
//   - {N}/out/, the outputs, and {N}/out/_done.json, which lists them;
//   - _failed.json, the user's exception;
//   - _submission.json, the application's ID.
type Transform struct {
	area    *Area
	staged  string
	sub     Submitter
	app     Application
	outputs []string
	emr     bool
	log     *slog.Logger
	// Zero: 1 s, 15 s and 30 s.
	Poll, Watch, Alive time.Duration
	// How long the driver has to see _end and exit before Stop kills it.
	// Zero: 20 s.
	StopGrace time.Duration

	gen       uint64
	id        string
	stopAlive context.CancelFunc
	alive     sync.WaitGroup
}

var _ runner.Transform = (*Transform)(nil)

// NewTransform prepares a pipeline's transform below location, the data
// plane's store as Open takes it. app names the code and its properties;
// Start names it and adds the runner's.
func NewTransform(area *Area, location string, sub Submitter, app Application, outputs []string, log *slog.Logger) (*Transform, error) {
	staged, err := engineURL(location, area)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Transform{area: area, staged: staged, sub: sub, app: app, outputs: outputs, emr: sub.Kind() == "emr", log: log}, nil
}

// engineURL is the area as the engine writes it: the store's URL, with its
// query, below the area's prefix.
func engineURL(location string, a *Area) (string, error) {
	if !strings.Contains(location, "://") || a.base.Scheme == "file" {
		return a.base.String(), nil
	}
	u, err := url.Parse(location)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimSuffix(a.base.Path, "/")
	return u.String(), nil
}

func (t *Transform) URL() string { return t.staged }

func genDir(g uint64) string { return strconv.FormatUint(g, 10) + "/" }

func epochDir(e destination.Epoch) string {
	return genDir(e.Generation) + strconv.FormatUint(e.N, 10) + "/"
}

func (t *Transform) Start(ctx context.Context, gen uint64) error {
	t.gen = gen
	t.endOlder(ctx)
	if err := t.touch(ctx); err != nil {
		return err
	}
	actx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	t.stopAlive = cancel
	t.alive.Go(func() { t.keepAlive(actx) })

	app := t.app
	app.Name = TransformName(t.area.Pipeline(), gen)
	app.Conf = maps.Clone(app.Conf)
	if app.Conf == nil {
		app.Conf = map[string]string{}
	}
	maps.Copy(app.Conf, t.runnerConf(gen))
	if !app.Python {
		app.Conf["spark.openetl.transform"] = app.MainClass
		app.MainClass = TransformDriver
	}
	t.log.Info("submitting the Spark transform", "name", app.Name, "cluster_kind", t.sub.Kind(), "code", app.Code)
	id, err := t.sub.Submit(ctx, app)
	if err != nil {
		return err
	}
	t.id = id
	t.log.Info("submitted the Spark transform", "submission", id)
	b, _ := json.Marshal(submission{Kind: t.sub.Kind(), ID: id})
	if err := t.area.bucket.WriteAll(ctx, genDir(gen)+"_submission.json", b, nil); err != nil {
		t.log.Warn("recording the transform's submission failed; the next attempt finds it by name if the cluster can", "err", err)
	}
	return nil
}

// runnerConf are the properties the runner sets on a transform: where its
// files are, and Parquet's timestamps as microseconds, without Spark's
// rebasing of old dates.
func (t *Transform) runnerConf(gen uint64) map[string]string {
	c := map[string]string{
		"spark.openetl.root":                          t.area.URI(genDir(gen), t.emr),
		"spark.openetl.protocol":                      strconv.Itoa(Protocol),
		"spark.sql.parquet.outputTimestampType":       "TIMESTAMP_MICROS",
		"spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
		"spark.sql.parquet.datetimeRebaseModeInRead":  "CORRECTED",
		"spark.sql.parquet.int96RebaseModeInWrite":    "CORRECTED",
		"spark.sql.parquet.int96RebaseModeInRead":     "CORRECTED",
	}
	// S3A's magic committer writes without renames, which are copies on S3.
	// EMRFS's S3-optimized committer is on by default.
	if strings.HasPrefix(c["spark.openetl.root"], "s3a://") {
		for k, v := range map[string]string{
			"spark.hadoop.fs.s3a.committer.name":          "magic",
			"spark.hadoop.fs.s3a.committer.magic.enabled": "true",
			"spark.sql.sources.commitProtocolClass":       "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
			"spark.sql.parquet.output.committer.class":    "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
		} {
			if _, set := t.app.Conf[k]; !set {
				c[k] = v
			}
		}
	}
	return c
}

// endOlder ends older generations' applications, which their _end and a
// kill both do, and drops their files. It's best effort: a later attempt
// does what it leaves.
func (t *Transform) endOlder(ctx context.Context) {
	names, err := t.area.children(ctx, "")
	if err != nil {
		t.log.Warn("listing older generations' Spark files failed", "err", err)
	}
	for _, name := range names {
		g, err := strconv.ParseUint(name, 10, 64)
		if err != nil || g >= t.gen {
			continue
		}
		dir := genDir(g)
		if err := t.area.bucket.WriteAll(ctx, dir+"_end", nil, nil); err != nil {
			t.log.Warn("ending an older generation's Spark transform failed", "generation", g, "err", err)
		}
		t.killOlder(ctx, g)
		if err := t.area.drop(ctx, dir); err != nil {
			t.log.Warn("dropping an older generation's Spark files failed", "generation", g, "err", err)
		}
	}
}

func (t *Transform) killOlder(ctx context.Context, g uint64) {
	var s submission
	b, err := t.area.bucket.ReadAll(ctx, genDir(g)+"_submission.json")
	id := ""
	if err == nil && json.Unmarshal(b, &s) == nil && s.Kind == t.sub.Kind() {
		id = s.ID
	} else if id, err = t.sub.Find(ctx, TransformName(t.area.Pipeline(), g)); err != nil {
		t.log.Warn("finding an older generation's Spark transform failed", "generation", g, "err", err)
		return
	}
	if id == "" {
		return
	}
	st, err := t.sub.Status(ctx, id)
	if err == nil && st.Done {
		return
	}
	if err := t.sub.Kill(ctx, id); err != nil {
		t.log.Warn("killing an older generation's Spark transform failed", "generation", g, "submission", id, "err", err)
		return
	}
	t.log.Info("killed an older generation's Spark transform", "generation", g, "submission", id)
}

func (t *Transform) touch(ctx context.Context) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := t.area.bucket.WriteAll(ctx, genDir(t.gen)+"_alive", []byte(now), nil); err != nil {
		return errs.Wrap(errs.Transient, err, "spark: write the transform's _alive")
	}
	return nil
}

// keepAlive rewrites _alive until ctx ends. The driver exits once it's five
// minutes old.
func (t *Transform) keepAlive(ctx context.Context) {
	every := t.Alive
	if every == 0 {
		every = 30 * time.Second
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if err := t.touch(ctx); err != nil && ctx.Err() == nil {
				t.log.Warn("rewriting the transform's _alive failed", "err", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

type readyInput struct {
	Name   string          `json:"name"`
	Schema json.RawMessage `json:"schema"`
	Files  []string        `json:"files"`
	Rows   uint64          `json:"rows"`
}

type readyDoc struct {
	Version    int          `json:"version"`
	Generation uint64       `json:"generation"`
	Epoch      uint64       `json:"epoch"`
	Inputs     []readyInput `json:"inputs"`
	Outputs    []string     `json:"outputs"`
}

type doneDoc struct {
	Version int    `json:"version"`
	Epoch   uint64 `json:"epoch"`
	Outputs []struct {
		Name  string   `json:"name"`
		Files []string `json:"files"`
	} `json:"outputs"`
}

type failedDoc struct {
	Epoch uint64 `json:"epoch"`
	Error string `json:"error"`
	Trace string `json:"trace"`
}

func (t *Transform) Epoch(ctx context.Context, e destination.Epoch, inputs []runner.TransformInput, emit func(string, arrow.RecordBatch) error) error {
	dir := epochDir(e)
	ready := readyDoc{Version: Protocol, Generation: e.Generation, Epoch: e.N, Outputs: t.outputs}
	for _, in := range inputs {
		r := readyInput{Name: in.Stream, Schema: json.RawMessage("null"), Files: []string{}}
		if in.Schema != nil {
			s, err := SchemaJSON(in.Schema)
			if err != nil {
				return errs.Wrap(errs.ConfigInvalid, err, fmt.Sprintf("spark: input %q", in.Stream))
			}
			r.Schema = s
		}
		for _, f := range in.Files {
			rel, ok := strings.CutPrefix(f.Path, dir)
			if !ok || path.Clean(f.Path) != f.Path {
				return errs.New(errs.Internal, "spark: input %q's file %q isn't in epoch %d-g%d", in.Stream, f.Path, e.N, e.Generation)
			}
			r.Files = append(r.Files, rel)
			r.Rows += f.Rows
		}
		ready.Inputs = append(ready.Inputs, r)
	}
	b, err := json.Marshal(ready)
	if err != nil {
		return err
	}
	if err := t.area.bucket.WriteAll(ctx, dir+runner.TransformInputs+"/_ready.json", b, &blob.WriterOptions{ContentType: "application/json"}); err != nil {
		return errs.Wrap(errs.Transient, err, "spark: write the epoch's _ready.json")
	}
	start := time.Now()
	done, err := t.wait(ctx, e)
	if err != nil {
		return err
	}
	t.log.Debug("the Spark transform finished an epoch", "epoch", e.N, "seconds", time.Since(start).Seconds())
	for _, o := range done.Outputs {
		for _, f := range o.Files {
			if err := t.read(ctx, dir+f, func(b arrow.RecordBatch) error { return emit(o.Name, b) }); err != nil {
				return err
			}
		}
	}
	return nil
}

// wait polls for the epoch's _done.json, and the transform's _failed.json,
// and checks the application now and then.
func (t *Transform) wait(ctx context.Context, e destination.Epoch) (*doneDoc, error) {
	poll, watch := t.Poll, t.Watch
	if poll == 0 {
		poll = time.Second
	}
	if watch == 0 {
		watch = 15 * time.Second
	}
	key := epochDir(e) + "out/_done.json"
	failed := genDir(e.Generation) + "_failed.json"
	var lastWatch time.Time
	var ended *State
	for {
		// On a directory store a file shows before its content, so one that
		// isn't JSON yet is still being written, until the application ends.
		if b, err := t.area.bucket.ReadAll(ctx, key); err == nil && (json.Valid(b) || ended != nil) {
			return t.done(b, e)
		} else if err != nil && gcerrors.Code(err) != gcerrors.NotFound {
			t.log.Warn("reading the epoch's _done.json failed", "epoch", e.N, "err", err)
		}
		if b, err := t.area.bucket.ReadAll(ctx, failed); err == nil && (json.Valid(b) || ended != nil) {
			var f failedDoc
			if json.Unmarshal(b, &f) != nil || f.Error == "" {
				f.Error = strings.TrimSpace(string(b))
			}
			t.log.Warn("the Spark transform failed", "epoch", f.Epoch, "err", f.Error, "trace", f.Trace)
			return nil, &Failed{Message: fmt.Sprintf("the Spark transform failed in epoch %d: %s", f.Epoch, f.Error)}
		}
		if ended != nil {
			return nil, errs.New(errs.Transient, "the Spark transform's application ended %s without transforming epoch %d%s", ended.State, e.N, message(ended.Message))
		}
		if time.Since(lastWatch) >= watch {
			lastWatch = time.Now()
			st, err := t.sub.Status(ctx, t.id)
			switch {
			case ctx.Err() != nil:
			case err != nil:
				t.log.Warn("checking the Spark transform's application failed", "submission", t.id, "err", err)
			case st.Done:
				// Look for its files once more, written as it ended.
				ended = &st
				continue
			}
		}
		select {
		case <-time.After(poll):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func message(m string) string {
	if m == "" {
		return ""
	}
	return ": " + m
}

func (t *Transform) done(b []byte, e destination.Epoch) (*doneDoc, error) {
	var d doneDoc
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, errs.Wrap(errs.Internal, err, "spark: the epoch's _done.json")
	}
	names := make([]string, len(d.Outputs))
	for i, o := range d.Outputs {
		names[i] = o.Name
		for _, f := range o.Files {
			if !strings.HasPrefix(f, "out/") || path.Clean(f) != f {
				return nil, errs.New(errs.Internal, "spark: output %q's file %q isn't in the epoch's out/", o.Name, f)
			}
		}
	}
	switch {
	case d.Version != Protocol:
		return nil, errs.New(errs.ConfigInvalid, "spark: the transform speaks openetl-spark's protocol %d, and this runner %d", d.Version, Protocol)
	case d.Epoch != e.N || !slices.Equal(names, t.outputs):
		return nil, errs.New(errs.Internal, "spark: epoch %d's _done.json is for epoch %d, with outputs %v, not %v", e.N, d.Epoch, names, t.outputs)
	}
	return &d, nil
}

// read emits a Parquet file's batches.
func (t *Transform) read(ctx context.Context, key string, emit func(arrow.RecordBatch) error) error {
	attrs, err := t.area.bucket.Attributes(ctx, key)
	if err != nil {
		return errs.Wrap(errs.Transient, err, "spark: read the transform's output "+key)
	}
	pf, err := file.NewParquetReader(&rangeReader{ctx: ctx, b: t.area.bucket, key: key, size: attrs.Size})
	if err != nil {
		return errs.Wrap(errs.Transient, err, "spark: read the transform's output "+key)
	}
	defer func() { _ = pf.Close() }()
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 64 << 10}, memory.DefaultAllocator)
	if err != nil {
		return errs.Wrap(errs.Internal, err, "spark: read the transform's output "+key)
	}
	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return errs.Wrap(errs.Transient, err, "spark: read the transform's output "+key)
	}
	defer rr.Release()
	for rr.Next() {
		b := rr.RecordBatch()
		b.Retain()
		if err := emit(b); err != nil {
			return err
		}
	}
	if err := rr.Err(); err != nil && !errors.Is(err, io.EOF) {
		return errs.Wrap(errs.Transient, err, "spark: read the transform's output "+key)
	}
	return nil
}

func (t *Transform) Drop(ctx context.Context, e destination.Epoch) error {
	return t.area.drop(ctx, epochDir(e))
}

// Stop writes _end, which ends the driver at its next poll, and kills the
// application if it hasn't ended within StopGrace, as when it's busy with
// an epoch the attempt won't commit.
func (t *Transform) Stop(ctx context.Context) error {
	if t.stopAlive != nil {
		t.stopAlive()
		t.alive.Wait()
	}
	grace := t.StopGrace
	if grace == 0 {
		grace = 20 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, grace+time.Minute)
	defer cancel()
	if err := t.area.bucket.WriteAll(ctx, genDir(t.gen)+"_end", nil, nil); err != nil {
		t.log.Warn("writing the transform's _end failed; the next attempt ends it", "err", err)
	}
	if t.id == "" {
		return nil
	}
	poll := t.Poll
	if poll == 0 {
		poll = time.Second
	}
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if st, err := t.sub.Status(ctx, t.id); err == nil && st.Done {
			t.log.Info("the Spark transform's application ended", "submission", t.id, "state", st.State)
			return nil
		}
		select {
		case <-time.After(poll):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := t.sub.Kill(ctx, t.id); err != nil {
		return fmt.Errorf("spark: kill the transform's application %s: %w", t.id, err)
	}
	t.log.Info("killed the Spark transform's application, which didn't end at _end", "submission", t.id)
	return nil
}

// rangeReader reads an object by ranges, for Parquet's footer and column
// chunks.
type rangeReader struct {
	ctx  context.Context //nolint:containedctx // the read's, for io's interfaces
	b    *blob.Bucket
	key  string
	size int64
	off  int64
}

func (r *rangeReader) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}
	n := min(int64(len(p)), r.size-off)
	rd, err := r.b.NewRangeReader(r.ctx, r.key, off, n, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rd.Close() }()
	read, err := io.ReadFull(rd, p[:n])
	if err == nil && n < int64(len(p)) {
		err = io.EOF
	}
	return read, err
}

func (r *rangeReader) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.off)
	r.off += int64(n)
	return n, err
}

func (r *rangeReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.off
	case io.SeekEnd:
		offset += r.size
	default:
		return 0, errors.New("spark: bad whence")
	}
	if offset < 0 {
		return 0, errors.New("spark: seek before the start")
	}
	r.off = offset
	return offset, nil
}
