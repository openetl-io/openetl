package activities

import (
	"context"
	"fmt"
	"log/slog"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/flink"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

// flinkCluster prepares a continuous attempt's Flink job: the cluster the
// version pins, with its secrets read, and the pipeline's grant on its
// _flink/ prefix. Close it when the attempt ends.
func (a *Sync) flinkCluster(ctx context.Context, spec *dataplanev1.GetSyncSpecResponse, log *slog.Logger) (*flink.Cluster, error) {
	t := spec.GetPipeline().GetTransform().GetFlink()
	switch {
	case a.Bridge == nil:
		return nil, errs.New(errs.ConfigInvalid, "this runner serves no bridge to Flink jobs; start it with --bridge-listen")
	case a.Storage == "":
		return nil, errs.New(errs.ConfigInvalid, "a Flink transform needs the data plane's object store, and this runner has none")
	}
	pc := spec.GetFlinkClusters()[t.GetCluster()]
	if pc == nil {
		return nil, errs.New(errs.ConfigInvalid, "the pipeline version pins no version of Flink cluster %s", t.GetCluster())
	}
	read := func(ctx context.Context, cfg *apiv1.ConnectorConfig, ext []*dataplanev1.ExternalSecret) error {
		return readSecrets(ctx, a.Secrets, cfg, ext)
	}
	if err := flink.ResolveSecrets(ctx, pc, read); err != nil {
		return nil, err
	}
	s := flink.Settings{Org: spec.GetOrgId(), Pipeline: spec.GetPipelineId(), Store: a.Storage, Cluster: pc.GetSpec(), Transform: t}
	access := spec.GetPipeline().GetFlinkAccess()
	s.EMRRole = access.GetEmrRole()
	if keys := access.GetKeys(); keys != nil {
		if err := read(ctx, &apiv1.ConnectorConfig{Config: keys}, spec.GetFlinkAccessSecrets()); err != nil {
			return nil, fmt.Errorf("flinkAccess.keys: %w", err)
		}
		s.AccessKeyID, s.SecretAccessKey = keys.GetFields()["accessKeyId"].GetStringValue(), keys.GetFields()["secretAccessKey"].GetStringValue()
		if s.AccessKeyID == "" || s.SecretAccessKey == "" {
			return nil, errs.New(errs.Internal, "flink: flinkAccess.keys' secrets weren't read")
		}
	}
	return flink.New(ctx, s, flink.Options{Logger: log.With("component", "flink"), HTTP: a.HTTP})
}
