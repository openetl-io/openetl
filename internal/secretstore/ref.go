// Package secretstore reads secrets from cloud secret stores by reference:
// AWS Secrets Manager (aws-sm://), GCP Secret Manager (gcp-sm://) and Azure
// Key Vault (azure-kv://). Runners read them with their own cloud identity,
// so the control plane keeps only the reference.
package secretstore

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
)

const (
	AWS   = "aws-sm"
	GCP   = "gcp-sm"
	Azure = "azure-kv"
)

const maxRefSize = 2048

// Ref is a parsed reference.
type Ref struct {
	Scheme string
	// AWS: the secret's name or ARN. GCP: projects/{p}/secrets/{s}, or
	// projects/{p}/locations/{l}/secrets/{s} for a regional secret. Azure:
	// the secret's name.
	Name string
	// AWS: the region, from the reference or the ARN. Empty: the runner's.
	Region string
	// GCP: a regional secret's location.
	Location string
	// Azure: the vault's URL.
	Vault string
	// Empty: the latest. AWS: a version ID. GCP: a version number or alias.
	Version string
	// AWS: a staging label, such as AWSPREVIOUS.
	Stage string
	// A top-level field of a JSON object value. Empty: the whole value.
	Field string
}

var (
	awsName   = regexp.MustCompile(`^[A-Za-z0-9/_+=.@-]{1,512}$`)
	awsRegion = regexp.MustCompile(`^[a-z]{2,4}(-[a-z]+)+-[0-9]+$`)
	gcpPart   = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,255}$`)
	gcpLoc    = regexp.MustCompile(`^[a-z]+-[a-z]+[0-9]+$`)
	azureName = regexp.MustCompile(`^[A-Za-z0-9-]{1,127}$`)
	azureHost = regexp.MustCompile(`^[a-z0-9-]{3,24}$`)
)

// Key Vault's domains, by cloud. A reference names a vault in one of them,
// so runners send their Azure tokens only to Key Vault.
var azureDomains = []string{".vault.azure.net", ".vault.azure.cn", ".vault.usgovcloudapi.net", ".vault.microsoftazure.de"}

// Parse parses a reference:
//
//	aws-sm://{name or ARN}[?region=…][&version=…][&stage=…][&field=…]
//	gcp-sm://projects/{p}/[locations/{l}/]secrets/{s}[/versions/{v}][?version=…][&field=…]
//	azure-kv://{vault}/{name}[?version=…][&field=…]
//
// An Azure vault is its name, or its host in another cloud, such as
// myvault.vault.azure.cn.
func Parse(ref string) (Ref, error) {
	if len(ref) > maxRefSize {
		return Ref{}, fmt.Errorf("a reference is at most %d bytes", maxRefSize)
	}
	scheme, rest, ok := strings.Cut(ref, "://")
	if !ok {
		return Ref{}, fmt.Errorf("%q isn't a reference; use aws-sm://, gcp-sm:// or azure-kv://", ref)
	}
	p, query, _ := strings.Cut(rest, "?")
	if strings.Contains(p, "#") {
		return Ref{}, fmt.Errorf("%s: a reference has no fragment", ref)
	}
	p, err := url.PathUnescape(p)
	if err != nil {
		return Ref{}, fmt.Errorf("%s: %w", ref, err)
	}
	params, err := url.ParseQuery(query)
	if err != nil {
		return Ref{}, fmt.Errorf("%s: %w", ref, err)
	}
	r := Ref{Scheme: scheme}
	allowed := []string{"version", "field"}
	switch scheme {
	case AWS:
		allowed = append(allowed, "region", "stage")
		err = r.parseAWS(p, params.Get("region"))
	case GCP:
		err = r.parseGCP(p, params.Get("version"))
	case Azure:
		err = r.parseAzure(p)
	default:
		return Ref{}, fmt.Errorf("%q isn't a reference; use aws-sm://, gcp-sm:// or azure-kv://", ref)
	}
	if err != nil {
		return Ref{}, fmt.Errorf("%s: %w", ref, err)
	}
	for k, v := range params {
		switch {
		case !slices.Contains(allowed, k):
			return Ref{}, fmt.Errorf("%s: unknown parameter %q; %s:// takes %s", ref, k, scheme, strings.Join(allowed, ", "))
		case len(v) != 1 || v[0] == "":
			return Ref{}, fmt.Errorf("%s: give %s once, with a value", ref, k)
		}
	}
	if r.Version == "" {
		r.Version = params.Get("version")
	}
	r.Stage, r.Field = params.Get("stage"), params.Get("field")
	return r, nil
}

func (r *Ref) parseAWS(name, region string) error {
	r.Name, r.Region = name, region
	if region != "" && !awsRegion.MatchString(region) {
		return fmt.Errorf("%q isn't an AWS region", region)
	}
	if !strings.HasPrefix(name, "arn:") {
		if !awsName.MatchString(name) {
			return errors.New("an AWS secret's name is 1 to 512 letters, digits and /_+=.@-")
		}
		return nil
	}
	// arn:{partition}:secretsmanager:{region}:{account}:secret:{name}
	parts := strings.Split(name, ":")
	if len(parts) != 7 || parts[2] != "secretsmanager" || parts[5] != "secret" || !awsRegion.MatchString(parts[3]) || !awsName.MatchString(parts[6]) {
		return errors.New("not a Secrets Manager secret's ARN")
	}
	if region != "" && region != parts[3] {
		return fmt.Errorf("region %s isn't the ARN's, %s", region, parts[3])
	}
	r.Region = parts[3]
	return nil
}

func (r *Ref) parseGCP(name, version string) error {
	const form = "name a secret as projects/{project}/secrets/{secret}, with /versions/{version} or locations/{location}/ if you like"
	s := strings.Split(name, "/")
	if len(s) >= 6 && s[2] == "locations" {
		r.Location = s[3]
		if !gcpLoc.MatchString(r.Location) {
			return fmt.Errorf("%q isn't a GCP location", r.Location)
		}
		s = append(s[:2], s[4:]...)
	}
	switch {
	case len(s) == 6 && s[4] == "versions":
		if version != "" {
			return errors.New("give the version in the name or as version=, not both")
		}
		r.Version = s[5]
		if !gcpPart.MatchString(r.Version) {
			return errors.New(form)
		}
	case len(s) != 4:
		return errors.New(form)
	}
	if s[0] != "projects" || s[2] != "secrets" || !gcpPart.MatchString(s[1]) || !gcpPart.MatchString(s[3]) {
		return errors.New(form)
	}
	r.Name = "projects/" + s[1] + "/secrets/" + s[3]
	if r.Location != "" {
		r.Name = "projects/" + s[1] + "/locations/" + r.Location + "/secrets/" + s[3]
	}
	return nil
}

func (r *Ref) parseAzure(p string) error {
	vault, name, ok := strings.Cut(p, "/")
	vault = strings.ToLower(vault)
	if !ok || !azureName.MatchString(name) {
		return errors.New("name a secret as {vault}/{name}; a name is letters, digits and dashes")
	}
	host := vault + azureDomains[0]
	if i := strings.IndexByte(vault, '.'); i >= 0 {
		vault, host = vault[:i], vault
		if !slices.Contains(azureDomains, host[i:]) {
			return fmt.Errorf("%s isn't a Key Vault host; Key Vault's domains are %s", host, strings.Join(azureDomains, ", "))
		}
	}
	if !azureHost.MatchString(vault) {
		return fmt.Errorf("%q isn't a vault's name", vault)
	}
	r.Vault, r.Name = "https://"+host+"/", name
	return nil
}
