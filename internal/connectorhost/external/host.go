package external

import (
	"slices"
	"strings"

	"golang.org/x/mod/module"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/telemetry"
	connspec "github.com/openetl-io/openetl/internal/spec"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// Protocol is the connector protocol this platform speaks.
const Protocol = "v1"

// Features are this platform's additions to the protocol after the alpha.
var Features []string

// Host is what this platform tells a connector about itself.
func Host() *protocolv1.Host {
	return &protocolv1.Host{Version: Release(), Protocol: Protocol, Features: Features}
}

// Release is this build's release, such as 0.1.0, or "dev": a build from
// an untagged commit has a pseudo-version, which Go stamps from the VCS.
func Release() string {
	v := telemetry.Version()
	if module.IsPseudoVersion(v) {
		return "dev"
	}
	v = strings.TrimPrefix(v, "v")
	if !connspec.ValidVersion(v) {
		return "dev"
	}
	return v
}

// checkSpec refuses a connector that speaks another protocol, or needs a
// feature this platform hasn't.
func checkSpec(name string, s *protocolv1.SpecResponse) error {
	if s.GetProtocolVersion() != Protocol {
		return errs.New(errs.ConfigInvalid, "connector %s speaks protocol %q, and this runner speaks %s", name, s.GetProtocolVersion(), Protocol)
	}
	for _, f := range s.GetRequiredFeatures() {
		if !slices.Contains(Features, f) {
			return errs.New(errs.ConfigInvalid, "connector %s needs the feature %s, which this runner doesn't have", name, f)
		}
	}
	return nil
}
