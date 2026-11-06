//go:build e2e

package e2e

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
)

// A dbt project can't be deleted while a pipeline's current version runs
// it, and older versions keep the bundle they pin.
func TestDbtProjects(t *testing.T) {
	startRunner(t)
	name := unique("dbtproj")
	project := dbtProject(t, name, map[string]string{"user_count": "select count(*) as n from {{ source('raw', 'users') }}"})
	docs := pipelineDocs(name, `{"seed": 6, "count": 10}`, `{}`)
	withStep := strings.Replace(docs[2], `"delivery": {}`, fmt.Sprintf(`"delivery": {}, "postSync": [{"dbt": {"project": %q}}]`, name), 1)
	res := apply(t, project, docs[0], docs[1], withStep)
	digest, v1 := res[0].GetVersion(), res[3].GetVersion()
	// Whether dbt is there or not, nothing is left running.
	waitSync(t, name, 90*time.Second)

	projects := apiv1connect.NewDbtProjectServiceClient(env.http, env.apiURL)
	projectName, pipelineName := "workspaces/default/dbtProjects/"+name, "workspaces/default/pipelines/"+name
	get := func() (*apiv1.DbtProject, error) {
		res, err := projects.GetDbtProject(t.Context(), connect.NewRequest(&apiv1.GetDbtProjectRequest{Name: projectName}))
		if err != nil {
			return nil, err
		}
		return res.Msg.GetDbtProject(), nil
	}
	p, err := get()
	if err != nil {
		t.Fatal(err)
	}
	if p.GetName() != projectName || p.GetDigest() != digest || p.GetEtag() == "" || len(p.GetUsedBy()) != 1 || p.GetUsedBy()[0] != pipelineName {
		t.Errorf("got %v", p)
	}
	var listed *apiv1.DbtProject
	req := &apiv1.ListDbtProjectsRequest{Parent: "workspaces/default", PageSize: 1}
	for {
		res, err := projects.ListDbtProjects(t.Context(), connect.NewRequest(req))
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range res.Msg.GetDbtProjects() {
			if p.GetName() == projectName {
				listed = p
			}
		}
		if req.PageToken = res.Msg.GetNextPageToken(); req.PageToken == "" {
			break
		}
	}
	if listed.GetDigest() != digest || len(listed.GetUsedBy()) != 1 {
		t.Errorf("listed %v", listed)
	}

	del := func(etag string) error {
		_, err := projects.DeleteDbtProject(t.Context(), connect.NewRequest(&apiv1.DeleteDbtProjectRequest{Name: projectName, Etag: etag}))
		return err
	}
	if err := del(""); connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "is used by "+pipelineName) {
		t.Fatalf("deleting a project in use: %v", err)
	}
	apply(t, docs[2])
	if p, err = get(); err != nil || len(p.GetUsedBy()) != 0 {
		t.Fatalf("after the pipeline dropped its step: %v, %v", p, err)
	}
	if err := del("999"); connect.CodeOf(err) != connect.CodeAborted {
		t.Errorf("deleting with a stale etag: %v", err)
	}
	if err := del(p.GetEtag()); err != nil {
		t.Fatal(err)
	}
	if _, err := get(); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("getting the deleted project: %v", err)
	}
	if err := applyErr(t, withStep); connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "dbt project") {
		t.Errorf("applying a step that names the deleted project: %v", err)
	}

	old, err := apiv1connect.NewPipelineServiceClient(env.http, env.apiURL).GetPipelineVersion(t.Context(),
		connect.NewRequest(&apiv1.GetPipelineVersionRequest{Name: pipelineName + "/versions/" + v1}))
	if err != nil {
		t.Fatal(err)
	}
	if d := old.Msg.GetPipelineVersion().GetDbt(); len(d) != 1 || d[name] != digest {
		t.Errorf("the first version pins %v, want %s's %s", d, name, digest)
	}
}

// restCall sends a REST request with c, and decodes the JSON it answers.
func restCall(t *testing.T, c *http.Client, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, env.apiURL+path, r)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("%s %s: %d %q: %v", method, path, res.StatusCode, b, err)
	}
	return res.StatusCode, out
}

// at follows keys and indexes into decoded JSON.
func at(v any, path ...any) any {
	for _, p := range path {
		switch p := p.(type) {
		case string:
			m, _ := v.(map[string]any)
			v = m[p]
		case int:
			s, _ := v.([]any)
			if p >= len(s) {
				return nil
			}
			v = s[p]
		}
	}
	return v
}

// REST is the google.api.http rules, transcoded by Vanguard to the same
// handlers, with the same auth and errors.
func TestREST(t *testing.T) {
	name := unique("rest")
	var documents []any
	for i, d := range pipelineDocs(name, `{"seed": 7, "count": 10}`, `{}`)[:2] {
		documents = append(documents, map[string]any{"origin": fmt.Sprintf("doc %d", i+1), "content": base64.StdEncoding.EncodeToString([]byte(d))})
	}
	code, res := restCall(t, env.http, "POST", "/v1/workspaces/default:apply", map[string]any{"documents": documents})
	if code != http.StatusOK || at(res, "resources", 0, "action") != "APPLY_ACTION_CREATED" || at(res, "resources", 1, "name") != "workspaces/default/destinations/"+name {
		t.Fatalf("apply: %d %v", code, res)
	}
	source := "/v1/workspaces/default/sources/" + name
	code, res = restCall(t, env.http, "GET", source, nil)
	if code != http.StatusOK || at(res, "source", "name") != source[len("/v1/"):] || at(res, "source", "spec", "config", "count") != 10.0 {
		t.Errorf("get: %d %v", code, res)
	}
	code, res = restCall(t, env.http, "GET", "/v1/workspaces/default/sources?pageSize=1", nil)
	if s, _ := at(res, "sources").([]any); code != http.StatusOK || len(s) != 1 {
		t.Errorf("list: %d %v", code, res)
	}
	code, res = restCall(t, env.http, "GET", "/v1/workspaces/default/sources/no-such-source", nil)
	if code != http.StatusNotFound || at(res, "code") != float64(connect.CodeNotFound) || !strings.Contains(fmt.Sprint(at(res, "message")), "doesn't exist") {
		t.Errorf("get a source that doesn't exist: %d %v", code, res)
	}
	if code, res = restCall(t, http.DefaultClient, "GET", source, nil); code != http.StatusUnauthorized {
		t.Errorf("get without a key: %d %v", code, res)
	}
	destination := "/v1/workspaces/default/destinations/" + name
	if code, res = restCall(t, env.http, "DELETE", destination+"?etag=999", nil); code != http.StatusConflict {
		t.Errorf("delete with a stale etag: %d %v", code, res)
	}
	if code, res = restCall(t, env.http, "DELETE", destination, nil); code != http.StatusOK {
		t.Errorf("delete: %d %v", code, res)
	}
	secret := "/v1/workspaces/default/secrets/" + unique("rest")
	code, res = restCall(t, env.http, "PATCH", secret+"?allowMissing=true", map[string]any{"value": "hunter2"})
	if code != http.StatusOK || at(res, "created") != true || at(res, "secret", "value") != nil {
		t.Errorf("set a secret: %d %v", code, res)
	}

	// The other protocols reach the handlers as they are.
	sources := apiv1connect.NewSourceServiceClient(env.http, env.apiURL, connect.WithGRPCWeb())
	if _, err := sources.GetSource(t.Context(), connect.NewRequest(&apiv1.GetSourceRequest{Name: source[len("/v1/"):]})); err != nil {
		t.Errorf("gRPC-Web: %v", err)
	}

	req, err := http.NewRequestWithContext(t.Context(), "GET", env.apiURL+"/openapi.yaml", nil)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = doc.Body.Close() }()
	b, _ := io.ReadAll(doc.Body)
	if doc.StatusCode != http.StatusOK || !bytes.Contains(b, []byte("openapi: 3.0.3")) || !bytes.Contains(b, []byte("/v1/workspaces/{workspace}/pipelines/{pipeline}/versions:")) {
		t.Errorf("the OpenAPI document: %d %.200s", doc.StatusCode, b)
	}
}

// Requests are refused past a size: 4 MiB, but 32 MiB for Apply, which
// carries dbt bundles.
func TestRequestSizeLimits(t *testing.T) {
	secrets := apiv1connect.NewSecretServiceClient(env.http, env.apiURL)
	_, err := secrets.UpdateSecret(t.Context(), connect.NewRequest(&apiv1.UpdateSecretRequest{
		Secret: &apiv1.Secret{Name: "workspaces/default/secrets/" + unique("big"), Value: strings.Repeat("x", 5<<20)}, AllowMissing: true,
	}))
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Errorf("a 5 MiB secret: %v", err)
	}
	code, res := restCall(t, env.http, "PATCH", "/v1/workspaces/default/secrets/"+unique("big")+"?allowMissing=true", map[string]any{"value": strings.Repeat("x", 5<<20)})
	if code != http.StatusTooManyRequests {
		t.Errorf("a 5 MiB secret over REST: %d %v", code, res)
	}

	applyDoc := func(size int) error {
		_, err := apiv1connect.NewApplyServiceClient(env.http, env.apiURL).Apply(t.Context(), connect.NewRequest(&apiv1.ApplyRequest{
			Documents: []*apiv1.Document{{Origin: "big", Content: append([]byte("# "), bytes.Repeat([]byte("x"), size)...)}},
		}))
		return err
	}
	if err := applyDoc(20 << 20); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("a 20 MiB document: %v; want it parsed, and refused", err)
	}
	if err := applyDoc(33 << 20); connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Errorf("a 33 MiB document: %v", err)
	}
}
