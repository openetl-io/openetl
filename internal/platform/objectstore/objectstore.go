// Package objectstore opens buckets on object storage, named by URL.
package objectstore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/blob/s3blob"
)

// Open takes a directory, file:///dir, mem://, or a bucket URL as S3Client,
// GCSClient or AzureClient describes. A directory is created if missing. A
// bucket URL's path puts every key under it (Prefix).
func Open(ctx context.Context, location string) (*blob.Bucket, error) {
	if !strings.Contains(location, "://") {
		if location == "" {
			return nil, errors.New("objectstore: no location")
		}
		return openDir(location)
	}
	u, err := url.Parse(location)
	if err != nil {
		return nil, fmt.Errorf("objectstore: %w", err)
	}
	switch u.Scheme {
	case "file":
		if u.Host != "" && u.Host != "localhost" {
			return nil, fmt.Errorf("objectstore: %q names a remote host", location)
		}
		return openDir(u.Path)
	case "mem":
		return memblob.OpenBucket(nil), nil
	}
	b, err := openBucket(ctx, u)
	if err != nil {
		return nil, err
	}
	if p := Prefix(u); p != "" {
		b = blob.PrefixedBucket(b, p)
	}
	return b, nil
}

// Prefix is the key prefix a bucket URL's path names: "base/" for
// s3://bucket/base, and "" for none. The path is cleaned, so
// s3://bucket//base/ names base/ too.
func Prefix(u *url.URL) string {
	p := strings.TrimPrefix(path.Clean("/"+u.Path), "/")
	if p == "" {
		return ""
	}
	return p + "/"
}

func openBucket(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	switch u.Scheme {
	case "s3":
		c, err := S3Client(u)
		if err != nil {
			return nil, err
		}
		return s3blob.OpenBucket(ctx, c, u.Host, nil)
	case "gs":
		c, err := GCSClient(ctx, u)
		if err != nil {
			return nil, err
		}
		return gcsblob.OpenBucket(ctx, nil, u.Host, &gcsblob.Options{Client: c})
	case "azblob":
		c, err := AzureClient(u)
		if err != nil {
			return nil, err
		}
		return azureblob.OpenBucket(ctx, c, nil)
	}
	return nil, fmt.Errorf("objectstore: unsupported location %q", u.Redacted())
}

func openDir(dir string) (*blob.Bucket, error) {
	// Temporary files next to their targets, so renames never cross mounts.
	b, err := fileblob.OpenBucket(dir, &fileblob.Options{CreateDir: true, NoTempDir: true})
	if err != nil {
		return nil, fmt.Errorf("objectstore: %w", err)
	}
	return b, nil
}

// S3Client returns a client for s3://bucket?region=…&endpoint=…. The region
// defaults to us-east-1, and an endpoint implies path-style addressing.
// Credentials come from AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and
// AWS_SESSION_TOKEN; without them requests are anonymous.
func S3Client(u *url.URL) (*s3.Client, error) {
	if u.Scheme != "s3" || u.Host == "" {
		return nil, fmt.Errorf("objectstore: %q is not s3://bucket", u.Redacted())
	}
	q := u.Query()
	opts := s3.Options{Region: q.Get("region"), Credentials: aws.AnonymousCredentials{}}
	if opts.Region == "" {
		opts.Region = "us-east-1"
	}
	if ep := q.Get("endpoint"); ep != "" {
		opts.BaseEndpoint = aws.String(ep)
		opts.UsePathStyle = true
	}
	if !S3Anonymous() {
		creds := aws.Credentials{AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"), SessionToken: os.Getenv("AWS_SESSION_TOKEN")}
		opts.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return creds, nil })
	}
	return s3.New(opts), nil
}

// S3Anonymous reports whether S3Client's clients send their requests
// unsigned, so they can't presign URLs either.
func S3Anonymous() bool { return os.Getenv("AWS_ACCESS_KEY_ID") == "" }

// S3ObjectURL is the unsigned URL of key in the bucket u names, as
// S3Client addresses it: path-style below an endpoint, and virtual-hosted on
// AWS.
func S3ObjectURL(u *url.URL, key string) string {
	q := u.Query()
	if ep := q.Get("endpoint"); ep != "" {
		base, err := url.Parse(ep)
		if err == nil {
			base.Path = path.Join("/", base.Path, u.Host) + "/" + key
			return base.String()
		}
	}
	region := cmp.Or(q.Get("region"), "us-east-1")
	return (&url.URL{Scheme: "https", Host: u.Host + ".s3." + region + ".amazonaws.com", Path: "/" + key}).String()
}

// GCSClient returns a client for gs://bucket. Credentials are Google's
// application default credentials, unless STORAGE_EMULATOR_HOST names an
// emulator, which is used unauthenticated.
func GCSClient(ctx context.Context, u *url.URL) (*storage.Client, error) {
	if u.Scheme != "gs" || u.Host == "" {
		return nil, fmt.Errorf("objectstore: %q is not gs://bucket", u.Redacted())
	}
	c, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("objectstore: gcs: %w", err)
	}
	return c, nil
}

// AzureClient returns a client for azblob://container?account=…&endpoint=….
// The account defaults to AZURE_STORAGE_ACCOUNT, and the endpoint, the
// account's blob service URL, to https://{account}.blob.core.windows.net.
// With AZURE_STORAGE_KEY requests are signed with the account key, and
// otherwise with Azure's default credential chain.
func AzureClient(u *url.URL) (*container.Client, error) {
	if u.Scheme != "azblob" || u.Host == "" {
		return nil, fmt.Errorf("objectstore: %q is not azblob://container", u.Redacted())
	}
	q := u.Query()
	account := cmp.Or(q.Get("account"), os.Getenv("AZURE_STORAGE_ACCOUNT"))
	if account == "" {
		return nil, fmt.Errorf("objectstore: %q names no account, and AZURE_STORAGE_ACCOUNT is unset", u.Redacted())
	}
	endpoint := cmp.Or(q.Get("endpoint"), "https://"+account+".blob.core.windows.net")
	containerURL := strings.TrimSuffix(endpoint, "/") + "/" + u.Host
	if key := os.Getenv("AZURE_STORAGE_KEY"); key != "" {
		cred, err := container.NewSharedKeyCredential(account, key)
		if err != nil {
			return nil, fmt.Errorf("objectstore: azure: %w", err)
		}
		return container.NewClientWithSharedKeyCredential(containerURL, cred, nil)
	}
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("objectstore: azure: %w", err)
	}
	return container.NewClient(containerURL, cred, nil)
}
