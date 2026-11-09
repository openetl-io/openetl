//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"connectrpc.com/connect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/oci"
	"github.com/openetl-io/openetl/internal/spec"
)

// What a release publishes, so a missing package fails the run.
var connectorPackages = []string{"postgres", "kafka", "kinesis", "iceberg", "s3"}

func connectorRegistryClient() *oci.Client {
	c := &oci.Client{HTTP: &http.Client{Timeout: 10 * time.Minute}}
	if token := os.Getenv("OPENETL_E2E_CONNECTOR_TOKEN"); token != "" {
		user := os.Getenv("OPENETL_E2E_CONNECTOR_USER")
		c.Credentials = func(context.Context, oci.Ref) (oci.Credential, error) {
			return oci.Credential{Username: user, Password: token}, nil
		}
	}
	return c
}

// pullConnectors writes each package's executables for this platform into
// dir, named as a connectors directory names them, for the tests that run a
// connector themselves. Runners don't see dir: they download what's applied.
func pullConnectors(ctx context.Context, dir string) error {
	ns, err := oci.Parse(env.connectorRegistry)
	if err != nil {
		return err
	}
	c := connectorRegistryClient()
	for _, name := range connectorPackages {
		p, err := c.Pull(ctx, ns.Child(name).At(env.connectorVersion))
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		doc, err := spec.Parse(p.Descriptor)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		for _, role := range doc.Descriptor.Kinds() {
			bin, ok := doc.Descriptor.Roles[role].Binaries[connectorhost.Platform()]
			if !ok {
				return fmt.Errorf("%s's %s has no executable for %s", name, role, connectorhost.Platform())
			}
			blob, err := oci.Parse(bin.URL)
			if err != nil {
				return err
			}
			f, err := os.OpenFile(filepath.Join(dir, role+"-"+name), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
			if err != nil {
				return err
			}
			err = c.FetchBlob(ctx, blob, f, connectorhost.MaxExecutable)
			if cerr := f.Close(); err == nil {
				err = cerr
			}
			if err != nil {
				return fmt.Errorf("%s's %s: %w", name, role, err)
			}
		}
	}
	return nil
}

// applyRegistryConnectors gives the official source the registry's
// credential, if it's private, and applies each package from it.
func applyRegistryConnectors(ctx context.Context) error {
	apply := apiv1connect.NewApplyServiceClient(env.http, env.apiURL)
	if token := os.Getenv("OPENETL_E2E_CONNECTOR_TOKEN"); token != "" {
		const secret = "connector-registry-token"
		if _, err := apiv1connect.NewSecretServiceClient(env.http, env.apiURL).UpdateSecret(ctx, connect.NewRequest(&apiv1.UpdateSecretRequest{
			Secret: &apiv1.Secret{Name: "workspaces/default/secrets/" + secret, Value: token}, AllowMissing: true,
		})); err != nil {
			return err
		}
		source := fmt.Sprintf("apiVersion: openetl.io/v1alpha1\nkind: ConnectorSource\nmetadata:\n  name: official\nspec:\n  url: %s\n  username: %s\n  secret: %s\n",
			env.connectorRegistry, os.Getenv("OPENETL_E2E_CONNECTOR_USER"), secret)
		if _, err := apply.Apply(ctx, connect.NewRequest(&apiv1.ApplyRequest{Documents: []*apiv1.Document{{Origin: "official source", Content: []byte(source)}}})); err != nil {
			return fmt.Errorf("the official source's credential: %w", err)
		}
	}
	req := &apiv1.ApplyRequest{}
	for _, name := range connectorPackages {
		ref := env.connectorRegistry + "/" + name + ":" + env.connectorVersion
		req.Documents = append(req.Documents, &apiv1.Document{Origin: ref, Reference: ref})
	}
	_, err := apply.Apply(ctx, connect.NewRequest(req))
	return err
}
