// Package objectstore opens buckets on object storage, named by URL.
package objectstore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go-v2/credentials/endpointcreds"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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
		c, err := S3Client(ctx, u)
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

// S3Client returns a client for s3://bucket?region=…&endpoint=…&anonymous=true.
// An endpoint implies path-style addressing. Without a region the client
// uses the bucket's, which S3 is asked for once per bucket and process; with
// an endpoint, or if S3 doesn't say, it uses AWS_REGION, AWS_DEFAULT_REGION
// or the profile's region, and else us-east-1.
//
// Requests are signed with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and
// AWS_SESSION_TOKEN when they're set, and otherwise with whatever AWS's
// default credential chain finds: a profile, a web identity (EKS IRSA), the
// container's role (ECS, EKS Pod Identity) or the instance's. anonymous=true
// sends them unsigned.
func S3Client(ctx context.Context, u *url.URL) (*s3.Client, error) {
	if u.Scheme != "s3" || u.Host == "" {
		return nil, fmt.Errorf("objectstore: %q is not s3://bucket", u.Redacted())
	}
	anonymous, err := S3Anonymous(u)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	opts := s3.Options{Region: q.Get("region")}
	if ep := q.Get("endpoint"); ep != "" {
		opts.BaseEndpoint = aws.String(ep)
		opts.UsePathStyle = true
	}
	switch {
	case anonymous:
		opts.Credentials = aws.AnonymousCredentials{}
	case os.Getenv("AWS_ACCESS_KEY_ID") != "":
		creds := aws.Credentials{AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"), SessionToken: os.Getenv("AWS_SESSION_TOKEN")}
		opts.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return creds, nil })
	default:
		cfg, err := awsConfig()
		if err != nil {
			return nil, fmt.Errorf("objectstore: load the AWS config: %w", err)
		}
		opts.Credentials = chainCredentials{cfg.Credentials}
	}
	if opts.Region == "" {
		opts.Region = configuredRegion()
		if opts.BaseEndpoint == nil {
			if r, ok := bucketRegion(ctx, u.Host, opts.Region); ok {
				opts.Region = r
			}
		}
	}
	return s3.New(opts), nil
}

// Resolve returns location with the region of the S3 bucket it names, for
// an s3:// URL with neither a region nor an endpoint, so that processes it's
// handed to, like the engine, needn't find it. Other locations, and a bucket
// S3 gives no region for, come back as they are.
func Resolve(ctx context.Context, location string) string {
	u, err := url.Parse(location)
	if err != nil || u.Scheme != "s3" || u.Host == "" {
		return location
	}
	q := u.Query()
	if q.Get("region") != "" || q.Get("endpoint") != "" {
		return location
	}
	r, ok := bucketRegion(ctx, u.Host, configuredRegion())
	if !ok {
		return location
	}
	q.Set("region", r)
	u.RawQuery = q.Encode()
	return u.String()
}

// S3Anonymous reports whether S3Client's clients for u send their requests
// unsigned, so they can't presign URLs either.
func S3Anonymous(u *url.URL) (bool, error) {
	v := u.Query().Get("anonymous")
	if v == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("objectstore: %q: anonymous=%s is neither true nor false", u.Redacted(), v)
	}
	return b, nil
}

// S3WrongRegion returns the region S3 says a bucket is in when err is its
// answer to a request sent to another region's endpoint.
func S3WrongRegion(err error) (string, bool) {
	re, ok := errors.AsType[*smithyhttp.ResponseError](err)
	if !ok || re.Response == nil || re.Response.StatusCode != http.StatusMovedPermanently {
		return "", false
	}
	r := re.Response.Header.Get("X-Amz-Bucket-Region")
	return r, r != ""
}

var awsConfig = sync.OnceValues(loadAWSConfig)

func loadAWSConfig() (aws.Config, error) { return config.LoadDefaultConfig(context.Background()) }

// configuredRegion is the AWS config's region, from the environment or the
// profile, or us-east-1.
func configuredRegion() string {
	cfg, _ := awsConfig()
	return cmp.Or(os.Getenv("AWS_REGION"), os.Getenv("AWS_DEFAULT_REGION"), cfg.Region, "us-east-1")
}

// chainCredentials says what failed when the default chain's provider gives
// no credentials, and what to set.
type chainCredentials struct{ aws.CredentialsProvider }

func (c chainCredentials) Retrieve(ctx context.Context) (aws.Credentials, error) {
	creds, err := c.CredentialsProvider.Retrieve(ctx)
	if err != nil {
		return creds, &CredentialsError{problem: credentialsProblem(c.CredentialsProvider, err), err: err}
	}
	return creds, nil
}

// CredentialsError is in the error of a request that an S3Client's client
// couldn't sign, as the default chain gave it no AWS credentials.
type CredentialsError struct {
	problem string
	err     error
}

func (e *CredentialsError) Error() string { return "objectstore: " + e.problem + ": " + e.err.Error() }

func (e *CredentialsError) Unwrap() error { return e.err }

const credentialsToSet = "set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, give the process an IAM role, or add anonymous=true to the s3:// URL of a store that takes unsigned requests"

// credentialsProblem names the source that p, a provider the default chain
// chose, failed to get credentials from.
func credentialsProblem(p aws.CredentialsProvider, err error) string {
	var refused smithy.APIError
	var status interface{ HTTPStatusCode() int }
	switch {
	case aws.IsCredentialsProvider(p, (*stscreds.WebIdentityRoleProvider)(nil)) && errors.As(err, &refused):
		return "STS refused the web identity token for the role: on EKS, check that IAM has an OIDC provider for the cluster's issuer, and that the role's trust policy allows the service account"
	case aws.IsCredentialsProvider(p, (*stscreds.WebIdentityRoleProvider)(nil)):
		return "no credentials for the web identity role"
	case aws.IsCredentialsProvider(p, (*endpointcreds.Provider)(nil)):
		return "the container credentials endpoint gave no credentials: on EKS, check the service account's Pod Identity association, and that the Pod Identity agent runs on the node"
	case aws.IsCredentialsProvider(p, (*ec2rolecreds.Provider)(nil)) && errors.As(err, &status) && status.HTTPStatusCode() == http.StatusNotFound:
		return "no AWS credentials, and the EC2 instance has no IAM role: " + credentialsToSet
	case aws.IsCredentialsProvider(p, (*ec2rolecreds.Provider)(nil)):
		return "no AWS credentials: " + credentialsToSet + "; in a container on EC2, the instance's role needs an instance metadata hop limit of 2 or more (HttpPutResponseHopLimit)"
	}
	return "no AWS credentials from the AWS profile"
}

var (
	bucketRegions sync.Map
	// S3's endpoint to ask for a bucket's region; empty for AWS's. For tests.
	regionEndpoint string
)

// bucketRegion asks S3, unsigned, which region bucket is in, at hint's
// endpoint. S3 answers for any bucket that exists, even one it refuses
// access to.
func bucketRegion(ctx context.Context, bucket, hint string) (string, bool) {
	if r, ok := bucketRegions.Load(bucket); ok {
		return r.(string), true
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	opts := s3.Options{Region: hint, Credentials: aws.AnonymousCredentials{}, UsePathStyle: true}
	if regionEndpoint != "" {
		opts.BaseEndpoint = aws.String(regionEndpoint)
	}
	out, err := s3.New(opts).HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bucket})
	var r string
	if err == nil {
		r = aws.ToString(out.BucketRegion)
	} else if re, ok := errors.AsType[*smithyhttp.ResponseError](err); ok && re.Response != nil {
		r = re.Response.Header.Get("X-Amz-Bucket-Region")
	}
	if r == "" {
		return "", false
	}
	bucketRegions.Store(bucket, r)
	return r, true
}

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
	region := cmp.Or(q.Get("region"), configuredRegion())
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
