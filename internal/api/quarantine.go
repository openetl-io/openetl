package api

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/internal/store/db"
)

// indexedCause is a cause as quarantine_files.causes keeps it.
type indexedCause struct {
	Code    string `json:"code"`
	Column  string `json:"column,omitempty"`
	NotHeld string `json:"not_held,omitempty"`
	Rows    uint64 `json:"rows"`
}

// quarantined are the quarantine files of one stream, or of one of a
// transform's outputs, at a checkpoint.
type quarantined struct {
	stream string
	output bool
	files  []*dataplanev1.CheckpointQuarantineFile
}

func quarantinedIn(m *dataplanev1.ReportCheckpointRequest) []quarantined {
	var out []quarantined
	for _, st := range m.GetStreams() {
		if len(st.GetQuarantineFiles()) > 0 {
			out = append(out, quarantined{stream: st.GetName(), files: st.GetQuarantineFiles()})
		}
	}
	for _, o := range m.GetOutputs() {
		if len(o.GetQuarantineFiles()) > 0 {
			out = append(out, quarantined{stream: o.GetName(), output: true, files: o.GetQuarantineFiles()})
		}
	}
	return out
}

// validQuarantinePath reports whether p is a path below a pipeline's
// _quarantine directory, as the runner names a file there.
func validQuarantinePath(p string) bool {
	if strings.ContainsAny(p, "\\\x00") {
		return false
	}
	for _, part := range strings.Split(p, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

// indexQuarantine adds a committed checkpoint's quarantine files to the
// pipeline's index, and records a stream.quarantined event for each stream
// they're of, in one transaction. A file already indexed is skipped and the
// event is recorded once by its key, so reporting the checkpoint again adds
// nothing. It also drops the files older than the pipeline's
// quarantine.retention, which the data plane deletes by the same rule.
func (s *Server) indexQuarantine(ctx context.Context, pipeline uuid.UUID, m *dataplanev1.ReportCheckpointRequest, generation, epoch int64) error {
	groups := quarantinedIn(m)
	for i, g := range groups {
		// A file the index can't name is left out of it, and the checkpoint
		// is mirrored all the same.
		groups[i].files = slices.DeleteFunc(g.files, func(f *dataplanev1.CheckpointQuarantineFile) bool {
			_, ok := ints.Int64(f.GetRows())
			if !ok || !validQuarantinePath(f.GetPath()) {
				s.log().Warn("leaving a quarantine file out of the index", "pipeline", pipeline, "stream", g.stream, "path", f.GetPath(), "url", f.GetUrl())
				return true
			}
			return false
		})
	}
	groups = slices.DeleteFunc(groups, func(g quarantined) bool { return len(g.files) == 0 })
	commit := m.GetCreateTime().AsTime()
	return s.Store.Tx(ctx, func(q *db.Queries) error {
		if _, err := q.DropExpiredQuarantineFiles(ctx, db.DropExpiredQuarantineFilesParams{OrgID: s.Tenant.Org, PipelineID: pipeline}); err != nil {
			return err
		}
		if len(groups) == 0 {
			return nil
		}
		names, err := q.GetPipelineNames(ctx, db.GetPipelineNamesParams{OrgID: s.Tenant.Org, ID: pipeline})
		if err != nil {
			return err
		}
		name := resourceName(names.Workspace, "pipelines", names.Name)
		var syncName string
		switch sync, err := q.GetSyncByRun(ctx, db.GetSyncByRunParams{OrgID: s.Tenant.Org, PipelineID: pipeline, RunID: m.GetRunId()}); {
		case err == nil:
			syncName = name + "/syncs/" + sync.ID.String()
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}
		for _, g := range groups {
			data := events.Quarantine{
				Pipeline: name, Sync: syncName, Stream: g.stream, Output: g.output,
				Generation: m.GetGeneration(), Epoch: m.GetEpoch(),
			}
			for _, f := range g.files {
				causes := sortedCauses(f.GetCauses())
				raw, err := json.Marshal(causes)
				if err != nil {
					return err
				}
				rows, _ := ints.Int64(f.GetRows())
				_, err = q.InsertQuarantineFile(ctx, db.InsertQuarantineFileParams{
					OrgID: s.Tenant.Org, PipelineID: pipeline, Path: f.GetPath(), Url: f.GetUrl(), Stream: g.stream, Output: g.output,
					Generation: generation, Epoch: epoch, RunID: m.GetRunId(), CommitTime: commit, Rows: rows, Causes: raw,
				})
				if err != nil {
					return err
				}
				ef := events.QuarantineFile{Path: f.GetPath(), URL: f.GetUrl(), Rows: f.GetRows(), Causes: []events.QuarantineCause{}}
				for _, c := range causes {
					ef.Causes = append(ef.Causes, events.QuarantineCause{Code: c.Code, Column: c.Column, NotHeld: c.NotHeld, Rows: c.Rows})
				}
				data.Rows += f.GetRows()
				data.Files = append(data.Files, ef)
			}
			_, err := events.Record(ctx, q, s.Tenant.Org, events.Event{
				Workspace: names.WorkspaceID, Type: events.StreamQuarantined, Pipeline: names.Name, Data: data,
				Key: fmt.Sprintf("%s/%d/%d/%s/%t", pipeline, m.GetGeneration(), m.GetEpoch(), g.stream, g.output),
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// The most of a stream's files the quarantine incident names. Past that, it
// gives their count, and its next steps say how to list them.
const incidentFiles = 10

// quarantineDetails sets a quarantine incident's quarantine_files, how many
// files the stream's commits since quarantined rows to, and
// quarantine_paths, the newest incidentFiles of them, newest first.
func quarantineDetails(ctx context.Context, q *db.Queries, org, pipeline uuid.UUID, stream string, since time.Time, details map[string]string) error {
	n, err := q.CountQuarantineFiles(ctx, db.CountQuarantineFilesParams{OrgID: org, PipelineID: pipeline, Stream: stream, Since: since})
	if err != nil || n == 0 {
		return err
	}
	rows, err := q.ListQuarantineFiles(ctx, db.ListQuarantineFilesParams{OrgID: org, PipelineID: pipeline, Stream: &stream, Since: &since, MaxRows: incidentFiles})
	if err != nil {
		return err
	}
	paths := make([]string, len(rows))
	for i, r := range rows {
		paths[i] = r.Path
	}
	details["quarantine_files"] = strconv.FormatInt(n, 10)
	details["quarantine_paths"] = strings.Join(paths, ", ")
	return nil
}

// sortedCauses are a file's causes, the most rows first.
func sortedCauses(in []*apiv1.QuarantineCause) []indexedCause {
	out := make([]indexedCause, 0, len(in))
	for _, c := range in {
		out = append(out, indexedCause{Code: c.GetCode(), Column: c.GetColumn(), NotHeld: c.GetNotHeld(), Rows: c.GetRows()})
	}
	slices.SortStableFunc(out, func(a, b indexedCause) int {
		return cmp.Or(cmp.Compare(b.Rows, a.Rows), strings.Compare(a.Code, b.Code), strings.Compare(a.Column, b.Column), strings.Compare(a.NotHeld, b.NotHeld))
	})
	return out
}

func (s *Server) ListQuarantine(ctx context.Context, req *connect.Request[apiv1.ListQuarantineRequest]) (*connect.Response[apiv1.ListQuarantineResponse], error) {
	_, p, err := s.getPipeline(ctx, req.Msg.GetParent())
	if err != nil {
		return nil, err
	}
	after, n, err := page(req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	params := db.ListQuarantineFilesParams{OrgID: s.Tenant.Org, PipelineID: p.ID, MaxRows: n}
	if st := req.Msg.GetStream(); st != "" {
		params.Stream = &st
	}
	if since := req.Msg.GetSince(); since != nil {
		if !since.IsValid() {
			return nil, invalid("since isn't a valid time")
		}
		t := since.AsTime()
		params.Since = &t
	}
	if after != "" {
		nanos, path, ok := strings.Cut(after, " ")
		ns, err := strconv.ParseInt(nanos, 10, 64)
		if !ok || err != nil {
			return nil, invalid("bad page_token")
		}
		t := time.Unix(0, ns).UTC()
		params.BeforeTime, params.BeforePath = &t, path
	}
	rows, err := s.Store.ListQuarantineFiles(ctx, params)
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListQuarantineResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		last := rows[len(rows)-1]
		res.NextPageToken = nextToken(strconv.FormatInt(last.CommitTime.UnixNano(), 10) + " " + last.Path)
	}
	for _, r := range rows {
		f := &apiv1.QuarantineFile{
			Stream: r.Stream, Output: r.Output, Path: r.Path, Url: r.Url, Rows: ints.Uint64(r.Rows),
			Generation: ints.Uint64(r.Generation), Epoch: ints.Uint64(r.Epoch), CommitTime: timestamppb.New(r.CommitTime),
		}
		if r.SyncID != nil {
			f.Sync = req.Msg.GetParent() + "/syncs/" + r.SyncID.String()
		}
		var causes []indexedCause
		if err := json.Unmarshal(r.Causes, &causes); err != nil {
			return nil, fmt.Errorf("quarantine file %s: %w", r.Path, err)
		}
		for _, c := range causes {
			f.Causes = append(f.Causes, &apiv1.QuarantineCause{Code: c.Code, Column: c.Column, NotHeld: c.NotHeld, Rows: c.Rows})
		}
		res.Files = append(res.Files, f)
	}
	return connect.NewResponse(res), nil
}
