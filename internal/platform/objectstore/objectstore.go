// Package objectstore opens buckets on object storage, named by URL.
package objectstore

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/blob/s3blob"
)

// Open takes a directory, file:///dir, mem://, or
// s3://bucket?region=…&endpoint=…. A directory is created if missing.
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
	case "s3":
		c, err := S3Client(u)
		if err != nil {
			return nil, err
		}
		return s3blob.OpenBucket(ctx, c, u.Host, nil)
	}
	return nil, fmt.Errorf("objectstore: unsupported location %q", location)
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
	if id := os.Getenv("AWS_ACCESS_KEY_ID"); id != "" {
		creds := aws.Credentials{AccessKeyID: id, SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"), SessionToken: os.Getenv("AWS_SESSION_TOKEN")}
		opts.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return creds, nil })
	}
	return s3.New(opts), nil
}
