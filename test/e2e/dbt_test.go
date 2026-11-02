//go:build e2e

package e2e

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/dbt"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
)

// dbtProject is a DbtProject document with a project of models, which read
// the pipeline's tables as source raw.
func dbtProject(t *testing.T, name string, models map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"dbt_project.yml": "name: shop\nprofile: shop\nconfig-version: 2\n",
		"models/sources.yml": `version: 2
sources:
  - name: raw
    schema: "{{ env_var('OPENETL_NAMESPACE') }}"
    tables: [{name: users}, {name: purchases}]
`,
	}
	for m, sql := range models {
		files["models/"+m+".sql"] = sql
	}
	for f, content := range files {
		p := filepath.Join(dir, filepath.FromSlash(f))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	bundle, err := dbt.Pack(dir)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "DbtProject", "metadata": {"name": %q}, "spec": {"bundle": %q}}`,
		name, base64.StdEncoding.EncodeToString(bundle))
}

func lastSyncError(t *testing.T, pipeline string) string {
	t.Helper()
	v, err := query[string](t, `SELECT coalesce(s.error, '') FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id
		WHERE p.name = $1 ORDER BY s.start_time DESC LIMIT 1`, pipeline)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// A sync runs its dbt step against the destination after its data lands,
// on the bundle current when it started. A model that fails fails the sync,
// but not the load.
func TestDbtPostSync(t *testing.T) {
	if env.dbt == "" {
		t.Skip("set OPENETL_DBT to a dbt with dbt-postgres to run dbt steps")
	}
	startRunner(t)
	name := unique("dbt")
	marts := schema(name) + "_marts"
	pipeline := func(name string) []string {
		docs := pipelineDocs(name, `{"seed": 3, "count": 300}`, `{}`)
		docs[2] = strings.Replace(docs[2], `"delivery": {}`,
			fmt.Sprintf(`"delivery": {}, "postSync": [{"dbt": {"project": %q, "select": "user_count", "schema": %q}}]`, name, marts), 1)
		return docs
	}
	project := dbtProject(t, name, map[string]string{"user_count": "select count(*) as n from {{ source('raw', 'users') }}"})
	res := apply(t, append([]string{project}, pipeline(name)...)...)
	if res[0].GetAction() != apiv1.ApplyAction_APPLY_ACTION_CREATED || !strings.HasPrefix(res[0].GetVersion(), "sha256:") {
		t.Fatalf("applied %v", res[0])
	}
	digest := res[0].GetVersion()
	status, stats := waitSync(t, name, 90*time.Second)
	if status != "succeeded" {
		t.Fatalf("sync %s: %s %s", status, stats, lastSyncError(t, name))
	}
	if !strings.Contains(stats, `"PostSync"`) || !strings.Contains(stats, digest) || !strings.Contains(stats, `"success": 1`) {
		t.Errorf("stats %s: want the dbt step's result", stats)
	}
	n, err := query[int](t, "SELECT n FROM "+pgx.Identifier{marts, "user_count"}.Sanitize())
	if err != nil || n != 300 {
		t.Fatalf("user_count = %d, %v; want 300", n, err)
	}
	log := wholeLog(t, latestSync(t, name).GetName())
	if logged(log, 1, 1, "dbt finished") != 1 || !fromComponent(log, "dbt") {
		t.Errorf("the log has no dbt step: %v", log)
	}

	broken := dbtProject(t, name, map[string]string{
		"user_count": "select count(*) as n from {{ source('raw', 'users') }}",
		"broken":     "select * from no_such_table",
	})
	applied := applyAll(t, false, broken)
	res = applied.GetResources()
	if res[0].GetAction() != apiv1.ApplyAction_APPLY_ACTION_UPDATED {
		t.Fatalf("applied %v", res[0])
	}
	if deps := applied.GetDependents(); len(deps) != 1 || deps[0].GetName() != "workspaces/default/pipelines/"+name {
		t.Errorf("changing the project: dependents %v", deps)
	}
	// The step selects only user_count, so the broken model doesn't run.
	if status, stats := syncNow(t, name); status != "succeeded" || !strings.Contains(stats, res[0].GetVersion()) {
		t.Fatalf("sync %s: %s %s; want it to run %s", status, stats, lastSyncError(t, name), res[0].GetVersion())
	}
	apply(t, broken, strings.Replace(pipeline(name)[2], `"select": "user_count", `, "", 1))
	users := func() int {
		n, err := query[int](t, "SELECT count(*) FROM "+pgx.Identifier{schema(name), "users"}.Sanitize())
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := users()
	status, stats = syncNow(t, name)
	msg := lastSyncError(t, name)
	if status != "failed" || !strings.Contains(msg, "dbt project "+name) || !strings.Contains(msg, "model.shop.broken") || !strings.Contains(msg, contract.DbtFailed) {
		t.Fatalf("sync %s, error %q", status, msg)
	}
	if !strings.Contains(stats, `"Committed"`) {
		t.Errorf("stats %s: want the attempt that loaded the data", stats)
	}
	// The faker appends its rows on every sync.
	if after := users(); after != before+300 {
		t.Errorf("the failed sync loaded %d users, want 300", after-before)
	}
}
