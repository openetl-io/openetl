//go:build e2e

package e2e

import (
	"bytes"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

// Configs name secrets, which the API keeps sealed and gives only to
// runners.
func TestSecrets(t *testing.T) {
	startRunner(t)
	ctx := t.Context()
	name := unique("secrets")
	secrets := apiv1connect.NewSecretServiceClient(env.http, env.apiURL)
	destinations := apiv1connect.NewDestinationServiceClient(env.http, env.apiURL)
	dsn := setSecret(t, env.dbURL)
	secretName := "workspaces/default/secrets/" + dsn
	ds := pipelineDocs(name, `{"seed": 5, "count": 200}`, `{}`)
	ref := `{"secret": ` + strconv.Quote(dsn) + `}`
	ds[1] = strings.Replace(ds[1], `{"secret": "e2e-dsn"}`, ref, 1)

	if err := applyErr(t, strings.Replace(ds[1], ref, strconv.Quote(env.dbURL), 1)); err == nil || !strings.Contains(err.Error(), "config.dsn is a secret") || strings.Contains(err.Error(), env.dbURL) {
		t.Errorf("a DSN in the config: %v", err)
	}
	if err := applyErr(t, strings.Replace(ds[1], dsn, "no-such-secret", 1)); err == nil || !strings.Contains(err.Error(), `secret "no-such-secret" doesn't exist`) {
		t.Errorf("a secret that doesn't exist: %v", err)
	}

	apply(t, ds...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkRows(t, schema(name), 200, "users", "purchases")

	// The API returns the reference, not the value, and the store holds the
	// value only sealed.
	u, err := url.Parse(env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := u.User.Password()
	got, err := destinations.GetDestination(ctx, connect.NewRequest(&apiv1.GetDestinationRequest{Name: "workspaces/default/destinations/" + name}))
	if err != nil {
		t.Fatal(err)
	}
	b, err := protojson.Marshal(got.Msg)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b, []byte(password)) || !bytes.Contains(b, []byte(dsn)) {
		t.Errorf("GetDestination: %s", b)
	}
	sec, err := secrets.GetSecret(ctx, connect.NewRequest(&apiv1.GetSecretRequest{Name: secretName}))
	if err != nil {
		t.Fatal(err)
	}
	if s := sec.Msg.GetSecret(); s.GetValue() != "" || !slices.Equal(s.GetUsedBy(), []string{"workspaces/default/destinations/" + name}) {
		t.Errorf("GetSecret: %v", s)
	}
	sealed, err := query[[]byte](t, "SELECT ciphertext FROM secrets WHERE name = $1", dsn)
	if err != nil || len(sealed) == 0 || bytes.Contains(sealed, []byte(password)) {
		t.Errorf("the stored secret: %v", err)
	}
	if spec, err := query[string](t, "SELECT spec::text FROM destinations WHERE name = $1", name); err != nil || strings.Contains(spec, password) {
		t.Errorf("the stored destination: %s, %v", spec, err)
	}

	// A new value is used from the next operation on, without applying
	// again.
	check := func() *apiv1.CheckResult {
		t.Helper()
		r, err := destinations.CheckDestination(ctx, connect.NewRequest(&apiv1.CheckDestinationRequest{Name: "workspaces/default/destinations/" + name}))
		if err != nil {
			t.Fatal(err)
		}
		return r.Msg.GetResult()
	}
	set := func(value, etag string) error {
		_, err := secrets.UpdateSecret(ctx, connect.NewRequest(&apiv1.UpdateSecretRequest{Secret: &apiv1.Secret{Name: secretName, Value: value, Etag: etag}}))
		return err
	}
	if err := set(wrongPassword(t), sec.Msg.GetSecret().GetEtag()); err != nil {
		t.Fatal(err)
	}
	if r := check(); r.GetOk() || r.GetCode() != "AUTH_INVALID" {
		t.Errorf("check with the wrong password: %v", r)
	}
	if err := set(env.dbURL, sec.Msg.GetSecret().GetEtag()); connect.CodeOf(err) != connect.CodeAborted {
		t.Errorf("an update with a stale etag: %v", err)
	}
	if err := set(env.dbURL, ""); err != nil {
		t.Fatal(err)
	}
	if r := check(); !r.GetOk() {
		t.Errorf("check with the password back: %v", r)
	}

	// A secret a config names can't be deleted.
	_, err = secrets.DeleteSecret(ctx, connect.NewRequest(&apiv1.DeleteSecretRequest{Name: secretName}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "destinations/"+name) {
		t.Errorf("delete while in use: %v", err)
	}
	pipelines := apiv1connect.NewPipelineServiceClient(env.http, env.apiURL)
	if _, err := pipelines.DeletePipeline(ctx, connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name})); err != nil {
		t.Fatal(err)
	}
	if _, err := destinations.DeleteDestination(ctx, connect.NewRequest(&apiv1.DeleteDestinationRequest{Name: "workspaces/default/destinations/" + name})); err != nil {
		t.Fatal(err)
	}
	if _, err := secrets.DeleteSecret(ctx, connect.NewRequest(&apiv1.DeleteSecretRequest{Name: secretName})); err != nil {
		t.Fatal(err)
	}
	_, err = secrets.GetSecret(ctx, connect.NewRequest(&apiv1.GetSecretRequest{Name: secretName}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("get after delete: %v", err)
	}
}
