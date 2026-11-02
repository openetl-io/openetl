package activities

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/secretstore"
	"github.com/unsuitable001/openetl/internal/secretstore/secretstoretest"
)

type specAPI struct {
	dataplanev1connect.UnimplementedDataPlaneServiceHandler
	ref string
}

func (f *specAPI) GetSyncSpec(context.Context, *connect.Request[dataplanev1.GetSyncSpecRequest]) (*connect.Response[dataplanev1.GetSyncSpecResponse], error) {
	cfg := func(field string) *apiv1.ConnectorConfig {
		c, _ := structpb.NewStruct(map[string]any{field: map[string]any{"secret": "db"}, "host": "h"})
		return &apiv1.ConnectorConfig{Connector: "postgres", Config: c}
	}
	ext := func(field string) []*dataplanev1.ExternalSecret {
		return []*dataplanev1.ExternalSecret{{Field: field, Secret: "workspaces/w/secrets/db", Ref: f.ref}}
	}
	return connect.NewResponse(&dataplanev1.GetSyncSpecResponse{
		OrgId: "org", Source: cfg("dsn"), Destination: cfg("password"),
		SourceSecrets: ext("dsn"), DestinationSecrets: ext("password"),
	}), nil
}

func TestReadSecrets(t *testing.T) {
	fake := secretstoretest.NewAWS()
	defer fake.Close()
	fake.Set("prod/db", `{"dsn": "postgres://u:hunter2@h/db"}`)
	fake.Setenv(t)
	api := &specAPI{ref: "aws-sm://prod/db?region=eu-west-1&field=dsn"}
	mux := http.NewServeMux()
	mux.Handle(dataplanev1connect.NewDataPlaneServiceHandler(api))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	client := dataplanev1connect.NewDataPlaneServiceClient(srv.Client(), srv.URL)
	ctx := t.Context()

	read := func(r *secretstore.Reader) (*dataplanev1.GetSyncSpecResponse, error) {
		spec, err := syncSpec(ctx, client, "org", "p", "v")
		if err != nil {
			return nil, err
		}
		if err := readSecrets(ctx, r, spec.GetSource(), spec.GetSourceSecrets()); err != nil {
			return nil, err
		}
		return spec, readSecrets(ctx, r, spec.GetDestination(), spec.GetDestinationSecrets())
	}
	spec, err := read(&secretstore.Reader{})
	if err != nil {
		t.Fatal(err)
	}
	if got := spec.GetSource().GetConfig().AsMap(); got["dsn"] != "postgres://u:hunter2@h/db" || got["host"] != "h" {
		t.Errorf("source config: %v", got)
	}
	if got := spec.GetDestination().GetConfig().AsMap(); got["password"] != "postgres://u:hunter2@h/db" {
		t.Errorf("destination config: %v", got)
	}
	if n := len(fake.Requests()); n != 2 {
		t.Errorf("%d reads", n)
	}

	if _, err := read(nil); errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "can't read those") {
		t.Errorf("no reader: %v", err)
	}
	api.ref = "aws-sm://prod/gone?region=eu-west-1"
	_, err = read(&secretstore.Reader{})
	if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "config.dsn names workspaces/w/secrets/db: ") || !strings.Contains(err.Error(), "aws-sm://prod/gone") {
		t.Errorf("a secret the store doesn't have: %v", err)
	}
}
