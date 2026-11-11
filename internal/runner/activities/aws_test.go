package activities

import (
	"strings"
	"testing"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/awsroles"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/connectorhost/external"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

// A connection without a role leaves the env as it was, and one with a
// role the runner can't assume fails before any connector starts.
func TestLeaseAWS(t *testing.T) {
	base := connectorhost.Env{AWS: map[string]external.AWS{"source": {URL: "http://127.0.0.1:1/other"}}, InheritAWS: true}
	env := base
	release, err := leaseAWS(t.Context(), nil, &env, "default", "orders", awsConnection{"source", "events", nil}, awsConnection{"destination", "lake", nil})
	if err != nil {
		t.Fatal(err)
	}
	release()
	if env.AWS["destination"].URL != "" || !env.InheritAWS {
		t.Errorf("connections without roles changed the env: %+v", env)
	}

	role := &apiv1.AWSAccess{Role: "arn:aws:iam::222222222222:role/lake"}
	_, err = leaseAWS(t.Context(), nil, &env, "default", "orders", awsConnection{"destination", "lake", role})
	if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "destination lake names an aws role, and this runner can't assume roles") {
		t.Errorf("no broker: %v", err)
	}
	b, err := awsroles.New("")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leaseAWS(t.Context(), b, &env, "default", "orders", awsConnection{"destination", "lake", role}); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("a role the runner may not assume: %v", err)
	}
	if len(base.AWS) != 1 {
		t.Errorf("leasing changed the runner's shared env: %+v", base.AWS)
	}
}
