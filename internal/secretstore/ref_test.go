package secretstore

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	for _, tc := range []struct {
		ref  string
		want Ref
	}{
		{"aws-sm://prod/db", Ref{Scheme: AWS, Name: "prod/db"}},
		{"aws-sm://prod/db?region=eu-west-1&field=password&stage=AWSPREVIOUS", Ref{Scheme: AWS, Name: "prod/db", Region: "eu-west-1", Stage: "AWSPREVIOUS", Field: "password"}},
		{"aws-sm://arn:aws:secretsmanager:us-gov-west-1:123456789012:secret:prod/db-AbCdEf?version=0b1c", Ref{Scheme: AWS, Name: "arn:aws:secretsmanager:us-gov-west-1:123456789012:secret:prod/db-AbCdEf", Region: "us-gov-west-1", Version: "0b1c"}},
		{"gcp-sm://projects/shop/secrets/db", Ref{Scheme: GCP, Name: "projects/shop/secrets/db"}},
		{"gcp-sm://projects/123/secrets/db/versions/4?field=dsn", Ref{Scheme: GCP, Name: "projects/123/secrets/db", Version: "4", Field: "dsn"}},
		{"gcp-sm://projects/shop/locations/europe-west4/secrets/db?version=latest", Ref{Scheme: GCP, Name: "projects/shop/locations/europe-west4/secrets/db", Location: "europe-west4", Version: "latest"}},
		{"azure-kv://ShopVault/db-password", Ref{Scheme: Azure, Vault: "https://shopvault.vault.azure.net/", Name: "db-password"}},
		{"azure-kv://shop.vault.azure.cn/db?version=abc123", Ref{Scheme: Azure, Vault: "https://shop.vault.azure.cn/", Name: "db", Version: "abc123"}},
	} {
		got, err := Parse(tc.ref)
		if err != nil || got != tc.want {
			t.Errorf("Parse(%s) = %+v, %v; want %+v", tc.ref, got, err, tc.want)
		}
	}

	for ref, want := range map[string]string{
		"prod/db":                                   "isn't a reference",
		"vault://secret/db":                         "isn't a reference",
		"aws-sm://":                                 "1 to 512",
		"aws-sm://prod db":                          "1 to 512",
		"aws-sm://prod/db?region=evil.com/":         "isn't an AWS region",
		"aws-sm://prod/db?regoin=us-east-1":         `unknown parameter "regoin"`,
		"aws-sm://prod/db?field=a&field=b":          "give field once",
		"aws-sm://prod/db?field=":                   "give field once",
		"aws-sm://prod/db#x":                        "no fragment",
		"aws-sm://arn:aws:s3:us-east-1:1:secret:db": "ARN",
		"aws-sm://arn:aws:secretsmanager:us-east-1:1:secret:db?region=eu-west-1": "isn't the ARN's",
		"gcp-sm://shop/db": "projects/{project}",
		"gcp-sm://projects/shop/secrets/db/versions/1?version=2": "not both",
		"gcp-sm://projects/shop/secrets/db?stage=x":              `unknown parameter "stage"`,
		"gcp-sm://projects/shop/locations/evil.com/secrets/db":   "isn't a GCP location",
		"azure-kv://shop":                             "{vault}/{name}",
		"azure-kv://shop/a/b":                         "{vault}/{name}",
		"azure-kv://evil.example.com/db":              "isn't a Key Vault host",
		"azure-kv://evil.com#.vault.azure.net/db":     "no fragment",
		"azure-kv://a/db":                             "isn't a vault's name",
		"aws-sm://" + strings.Repeat("a", maxRefSize): "at most",
	} {
		if _, err := Parse(ref); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Parse(%.60s) = %v; want %q", ref, err, want)
		}
	}
}
