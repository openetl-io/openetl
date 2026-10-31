//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/checkpoint"
)

// Every checkpoint a sync commits is mirrored to the control plane, with
// the rows each stream read, a schema its history has, and the sync's run.
func TestCheckpointMirror(t *testing.T) {
	startRunner(t)
	name := unique("mirror")
	apply(t, pipelineDocs(name, slowFaker, `{"commit": {"interval": "1s"}}`)...)
	if status, stats := waitSync(t, name, 90*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	ctx := t.Context()
	conn, err := pgx.Connect(ctx, env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var org, id string
	if err := conn.QueryRow(ctx, `SELECT org_id::text, id::text FROM pipelines WHERE name = $1`, name).Scan(&org, &id); err != nil {
		t.Fatal(err)
	}

	store, err := checkpoint.NewFSStore(env.checkpoints)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := store.List(ctx, org+"/"+id+"/_checkpoints/")
	if err != nil {
		t.Fatal(err)
	}
	logged := map[string]*checkpoint.Record{}
	for _, k := range keys {
		if !strings.HasSuffix(k, ".json") {
			continue
		}
		data, _, err := store.Get(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		var rec checkpoint.Record
		if err := json.Unmarshal(data, &rec); err != nil {
			t.Fatal(err)
		}
		logged[fmt.Sprintf("%d-g%d", rec.Epoch, rec.Generation)] = &rec
	}

	rows, err := conn.Query(ctx, `SELECT epoch, generation, previous_epoch, previous_generation, streams
		FROM checkpoint_mirror WHERE pipeline_id = $1 ORDER BY epoch, generation`, id)
	if err != nil {
		t.Fatal(err)
	}
	type stream struct {
		Name string
		Rows uint64
	}
	total := map[string]uint64{}
	mirrored := 0
	for rows.Next() {
		var epoch, gen int64
		var prevEpoch, prevGen *int64
		var streams []stream
		if err := rows.Scan(&epoch, &gen, &prevEpoch, &prevGen, &streams); err != nil {
			t.Fatal(err)
		}
		mirrored++
		ref := fmt.Sprintf("%d-g%d", epoch, gen)
		rec := logged[ref]
		if rec == nil {
			t.Errorf("mirrored checkpoint %s isn't in the log", ref)
			continue
		}
		if (rec.Previous == nil) != (prevEpoch == nil) ||
			rec.Previous != nil && (uint64(*prevEpoch) != rec.Previous.Epoch || uint64(*prevGen) != rec.Previous.Generation) { //nolint:gosec // small
			t.Errorf("checkpoint %s: previous %v/%v in the mirror, %+v in the log", ref, prevEpoch, prevGen, rec.Previous)
		}
		if len(streams) != len(rec.Streams) {
			t.Errorf("checkpoint %s: mirrored streams %+v, logged %+v", ref, streams, rec.Streams)
		}
		for _, s := range streams {
			if s.Rows != rec.Streams[s.Name].Rows {
				t.Errorf("checkpoint %s, stream %s: %d rows mirrored, %d logged", ref, s.Name, s.Rows, rec.Streams[s.Name].Rows)
			}
			total[s.Name] += s.Rows
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if mirrored != len(logged) || mirrored < 2 {
		t.Errorf("%d checkpoints mirrored, %d logged; want the same, and at least 2", mirrored, len(logged))
	}
	if total["users"] != 2000 || total["purchases"] != 2000 {
		t.Errorf("mirrored rows %v, want 2000 for each stream", total)
	}

	for what, q := range map[string]string{
		"a schema digest its history lacks": `SELECT count(*) FROM checkpoint_mirror m, jsonb_array_elements(m.streams) s
			WHERE m.pipeline_id = $1 AND s->>'schema_digest' <> '' AND NOT EXISTS (SELECT 1 FROM stream_schemas h
			WHERE h.org_id = m.org_id AND h.pipeline_id = m.pipeline_id AND h.stream = s->>'name' AND h.digest = s->>'schema_digest')`,
		"no schema digest": `SELECT count(*) FROM checkpoint_mirror m, jsonb_array_elements(m.streams) s
			WHERE m.pipeline_id = $1 AND s->>'schema_digest' = '' AND (s->>'rows')::bigint > 0`,
		"a run no sync has": `SELECT count(*) FROM checkpoint_mirror m
			WHERE m.pipeline_id = $1 AND NOT EXISTS (SELECT 1 FROM syncs s WHERE s.org_id = m.org_id AND s.run_id = m.run_id)`,
	} {
		var n int
		if err := conn.QueryRow(ctx, q, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("%d mirrored checkpoints or streams have %s", n, what)
		}
	}

	api := dataplanev1connect.NewDataPlaneServiceClient(http.DefaultClient, env.apiURL)
	report := func(pipeline string, epoch uint64, at time.Time) (*dataplanev1.ReportCheckpointResponse, error) {
		res, err := api.ReportCheckpoint(ctx, connect.NewRequest(&dataplanev1.ReportCheckpointRequest{
			PipelineId: pipeline, RunId: "run", Generation: 99, Epoch: epoch, CreateTime: timestamppb.New(at),
		}))
		if err != nil {
			return nil, err
		}
		return res.Msg, nil
	}
	now := time.Now()
	if res, err := report(id, 1000, now); err != nil || !res.GetAdded() {
		t.Fatalf("first report: %v, %v", res, err)
	}
	if res, err := report(id, 1000, now); err != nil || res.GetAdded() {
		t.Errorf("the same report again: %v, %v; want it skipped", res, err)
	}
	if _, err := report(id, 1001, now.Add(-48*time.Hour)); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("a report from two days ago: %v, want InvalidArgument", err)
	}
	if _, err := report(uuid.NewString(), 1, now); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("a report for no pipeline: %v, want NotFound", err)
	}

	// Concurrent reports on a day no partition covers yet all land.
	other := now.Add(-23 * time.Hour)
	if now.UTC().Hour() >= 12 {
		other = now.Add(23 * time.Hour)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := range uint64(8) {
		wg.Go(func() {
			if res, err := report(id, 2000+i, other); err != nil || !res.GetAdded() {
				errs <- fmt.Errorf("report %d: %v, %w", i, res, err)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	partition := "checkpoint_mirror_" + other.UTC().Format("20060102")
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, partition).Scan(&exists); err != nil || !exists {
		t.Errorf("partition %s exists: %v (%v)", partition, exists, err)
	}
}
