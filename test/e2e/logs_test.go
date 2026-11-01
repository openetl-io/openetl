//go:build e2e

package e2e

import (
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

func syncs() apiv1connect.SyncServiceClient {
	return apiv1connect.NewSyncServiceClient(env.http, env.apiURL)
}

func latestSync(t *testing.T, pipeline string) *apiv1.Sync {
	t.Helper()
	res, err := syncs().ListSyncs(t.Context(), connect.NewRequest(&apiv1.ListSyncsRequest{Parent: "workspaces/default/pipelines/" + pipeline, PageSize: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Msg.GetSyncs()) == 0 {
		return nil
	}
	return res.Msg.GetSyncs()[0]
}

// readLog reads a sync's log from token for as long as there are entries,
// and returns them and the token to go on from; "" once the log has ended.
func readLog(t *testing.T, sync, token string) ([]*apiv1.LogEntry, string) {
	t.Helper()
	var entries []*apiv1.LogEntry
	for {
		res, err := syncs().ReadSyncLog(t.Context(), connect.NewRequest(&apiv1.ReadSyncLogRequest{Name: sync, PageToken: token}))
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, res.Msg.GetEntries()...)
		if token = res.Msg.GetNextPageToken(); token == "" || len(res.Msg.GetEntries()) == 0 {
			return entries, token
		}
	}
}

func wholeLog(t *testing.T, sync string) []*apiv1.LogEntry {
	t.Helper()
	entries, token := readLog(t, sync, "")
	if token != "" {
		t.Fatalf("%s's log hasn't ended", sync)
	}
	return entries
}

// logged counts the entries with the message from the step's attempt, and
// those from the component.
func logged(entries []*apiv1.LogEntry, step, attempt int32, message string) int {
	n := 0
	for _, e := range entries {
		if e.GetPostSyncStep() == step && e.GetAttempt() == attempt && e.GetMessage() == message {
			n++
		}
	}
	return n
}

func fromComponent(entries []*apiv1.LogEntry, component string) bool {
	for _, e := range entries {
		if e.GetAttrs().GetFields()["component"].GetStringValue() == component {
			return true
		}
	}
	return false
}

// A running sync's log can be read as it grows, and once the sync ends, to
// its end, each entry once. ListSyncs pages through syncs, newest first.
func TestSyncLogs(t *testing.T) {
	startRunner(t)
	name := unique("logs")
	// Long enough to outlast the log's first flush, at 5 s.
	apply(t, pipelineDocs(name, `{"seed": 5, "count": 1500, "batchSize": 50, "delay": "200ms"}`, `{}`)...)
	var sync *apiv1.Sync
	eventually(t, 30*time.Second, "the first sync", func() bool { sync = latestSync(t, name); return sync != nil })
	if sync.GetState() != apiv1.SyncState_SYNC_STATE_RUNNING || sync.GetDataPlane() != "default" || sync.GetVersion() == "" || sync.GetEndTime() != nil {
		t.Fatalf("sync %v", sync)
	}

	var entries []*apiv1.LogEntry
	token := ""
	eventually(t, 30*time.Second, "entries while the sync runs", func() bool {
		var more []*apiv1.LogEntry
		more, token = readLog(t, sync.GetName(), token)
		entries = append(entries, more...)
		return len(entries) > 0
	})
	got, err := syncs().GetSync(t.Context(), connect.NewRequest(&apiv1.GetSyncRequest{Name: sync.GetName()}))
	if err != nil {
		t.Fatal(err)
	}
	if got.Msg.GetSync().GetState() != apiv1.SyncState_SYNC_STATE_RUNNING || token == "" {
		t.Fatalf("after reading its log, the sync is %v, and the token %q; slow the faker", got.Msg.GetSync().GetState(), token)
	}
	eventually(t, 90*time.Second, "the log to end", func() bool {
		var more []*apiv1.LogEntry
		more, token = readLog(t, sync.GetName(), token)
		entries = append(entries, more...)
		return token == ""
	})

	if n := logged(entries, 0, 1, "sync attempt finished"); n != 1 {
		t.Errorf(`"sync attempt finished" %d times, want once`, n)
	}
	if !fromComponent(entries, "engine") {
		t.Error("no entries from the engine")
	}
	for _, e := range entries {
		if e.GetTime() == nil || e.GetLevel() == "" || e.GetAttempt() != 1 || e.GetPostSyncStep() != 0 {
			t.Errorf("entry %v", e)
			break
		}
	}
	if whole := wholeLog(t, sync.GetName()); len(whole) != len(entries) {
		t.Errorf("read %d entries as the sync ran, %d after", len(entries), len(whole))
	}

	apply(t, pipelineDocs(name, `{"seed": 5, "count": 10}`, `{}`)[0])
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	parent := "workspaces/default/pipelines/" + name
	first, err := syncs().ListSyncs(t.Context(), connect.NewRequest(&apiv1.ListSyncsRequest{Parent: parent, PageSize: 1}))
	if err != nil {
		t.Fatal(err)
	}
	second, err := syncs().ListSyncs(t.Context(), connect.NewRequest(&apiv1.ListSyncsRequest{Parent: parent, PageSize: 1, PageToken: first.Msg.GetNextPageToken()}))
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Msg.GetSyncs()) != 1 || second.Msg.GetSyncs()[0].GetName() != sync.GetName() || second.Msg.GetNextPageToken() != "" ||
		first.Msg.GetSyncs()[0].GetName() == sync.GetName() || first.Msg.GetSyncs()[0].GetState() != apiv1.SyncState_SYNC_STATE_SUCCEEDED {
		t.Errorf("pages %v and %v", first.Msg, second.Msg)
	}
	_, err = syncs().GetSync(t.Context(), connect.NewRequest(&apiv1.GetSyncRequest{Name: parent + "/syncs/019a0000-0000-7000-8000-000000000000"}))
	wantCode(t, err, connect.CodeNotFound, "a sync that doesn't exist")
}
