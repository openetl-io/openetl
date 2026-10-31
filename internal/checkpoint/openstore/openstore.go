// Package openstore opens the checkpoint store a URL names.
package openstore

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/checkpoint/s3store"
)

// Open takes a directory, or s3://bucket?region=…&endpoint=…. An endpoint
// implies path-style addressing. S3 credentials come from AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN; without them requests are
// anonymous.
func Open(_ context.Context, location string) (checkpoint.Store, error) {
	if !strings.Contains(location, "://") {
		if location == "" {
			return nil, errors.New("openstore: no location")
		}
		return checkpoint.NewFSStore(location)
	}
	u, err := url.Parse(location)
	if err != nil {
		return nil, fmt.Errorf("openstore: %w", err)
	}
	if u.Scheme != "s3" || u.Host == "" {
		return nil, fmt.Errorf("openstore: %q is neither a directory nor s3://bucket", location)
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
	return s3store.New(s3.New(opts), u.Host), nil
}
