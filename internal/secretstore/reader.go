package secretstore

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"sync"
	"unicode/utf8"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/smithy-go"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openetl-io/openetl/internal/platform/errs"
)

// Reader reads references with the runner's cloud identity: the AWS SDK's
// default credentials, Google's application default credentials, or Azure's
// default credential. Its zero value is ready. It makes a client for each
// AWS region, GCP location and Azure vault when first needed, and keeps it.
type Reader struct {
	mu        sync.Mutex
	aws       map[string]*secretsmanager.Client
	gcp       map[string]*secretmanager.Client
	azure     map[string]*azsecrets.Client
	azureCred azcore.TokenCredential

	// Tests set these.
	gcpOptions   []option.ClientOption
	azureVault   string
	azureOptions *azsecrets.ClientOptions
}

// Read reads ref's value, or its field. Its errors are classified, and never
// hold the value.
func (r *Reader) Read(ctx context.Context, ref string) (string, error) {
	p, err := Parse(ref)
	if err != nil {
		return "", errs.Wrap(errs.ConfigInvalid, err, "")
	}
	var b []byte
	switch p.Scheme {
	case AWS:
		b, err = r.readAWS(ctx, ref, p)
	case GCP:
		b, err = r.readGCP(ctx, ref, p)
	default:
		b, err = r.readAzure(ctx, ref, p)
	}
	if err != nil {
		return "", err
	}
	v, err := field(b, p.Field)
	if err != nil {
		return "", errs.New(errs.ConfigInvalid, "%s: %v", ref, err)
	}
	return v, nil
}

func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var err error
	for _, c := range r.gcp {
		err = errors.Join(err, c.Close())
	}
	r.gcp = nil
	return err
}

// field is b, or the named field of the JSON object b holds: a string's
// text, or any other value's JSON.
func field(b []byte, name string) (string, error) {
	if name == "" {
		if !utf8.Valid(b) {
			return "", errors.New("the value isn't UTF-8 text; name a field of a JSON value, or store it as text")
		}
		return string(b), nil
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(b, &obj) != nil {
		return "", fmt.Errorf("the value isn't a JSON object, so it has no field %q", name)
	}
	v, ok := obj[name]
	switch {
	case !ok:
		return "", fmt.Errorf("the value has no field %q", name)
	case string(v) == "null":
		return "", fmt.Errorf("field %q is null", name)
	}
	var s string
	if json.Unmarshal(v, &s) == nil {
		return s, nil
	}
	return string(v), nil
}

func (r *Reader) readAWS(ctx context.Context, ref string, p Ref) ([]byte, error) {
	c, err := r.awsClient(ctx, p.Region)
	if err != nil {
		return nil, err
	}
	in := &secretsmanager.GetSecretValueInput{SecretId: &p.Name}
	if p.Version != "" {
		in.VersionId = &p.Version
	}
	if p.Stage != "" {
		in.VersionStage = &p.Stage
	}
	out, err := c.GetSecretValue(ctx, in)
	if err != nil {
		return nil, errs.Wrap(awsCode(err), err, "read "+ref)
	}
	if out.SecretString != nil {
		return []byte(*out.SecretString), nil
	}
	return out.SecretBinary, nil
}

func (r *Reader) awsClient(ctx context.Context, region string) (*secretsmanager.Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.aws[region]; ok {
		return c, nil
	}
	var opts []func(*config.LoadOptions) error
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "load the runner's AWS config")
	}
	if cfg.Region == "" {
		return nil, errs.New(errs.ConfigInvalid, "no AWS region; give region= in the reference, or set AWS_REGION on the runner")
	}
	c := secretsmanager.NewFromConfig(cfg)
	if r.aws == nil {
		r.aws = map[string]*secretsmanager.Client{}
	}
	r.aws[region] = c
	return c, nil
}

func awsCode(err error) errs.Code {
	var ae smithy.APIError
	if !errors.As(err, &ae) {
		return errs.Transient
	}
	switch ae.ErrorCode() {
	case "ResourceNotFoundException", "InvalidParameterException", "InvalidRequestException", "ValidationException":
		return errs.ConfigInvalid
	case "AccessDeniedException", "DecryptionFailure":
		return errs.PermissionDenied
	case "UnrecognizedClientException", "InvalidClientTokenId", "InvalidSignatureException", "IncompleteSignature":
		return errs.AuthInvalid
	case "ExpiredTokenException":
		return errs.AuthExpired
	case "ThrottlingException", "TooManyRequestsException":
		return errs.RateLimited
	}
	return errs.Transient
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func (r *Reader) readGCP(ctx context.Context, ref string, p Ref) ([]byte, error) {
	c, err := r.gcpClient(ctx, p.Location)
	if err != nil {
		return nil, err
	}
	res, err := c.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: p.Name + "/versions/" + cmp.Or(p.Version, "latest")})
	if err != nil {
		return nil, errs.Wrap(grpcCode(status.Code(err)), err, "read "+ref)
	}
	data := res.GetPayload().GetData()
	if sum := res.GetPayload().DataCrc32C; sum != nil && int64(crc32.Checksum(data, castagnoli)) != *sum {
		return nil, errs.New(errs.Transient, "read %s: the value's checksum doesn't match", ref)
	}
	return data, nil
}

func (r *Reader) gcpClient(ctx context.Context, location string) (*secretmanager.Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.gcp[location]; ok {
		return c, nil
	}
	opts := r.gcpOptions
	if location != "" && opts == nil {
		opts = []option.ClientOption{option.WithEndpoint("secretmanager." + location + ".rep.googleapis.com:443")}
	}
	// The client outlives ctx.
	c, err := secretmanager.NewClient(context.WithoutCancel(ctx), opts...)
	if err != nil {
		return nil, errs.Wrap(errs.AuthInvalid, err, "make a GCP Secret Manager client with the runner's credentials")
	}
	if r.gcp == nil {
		r.gcp = map[string]*secretmanager.Client{}
	}
	r.gcp[location] = c
	return c, nil
}

func grpcCode(c codes.Code) errs.Code {
	switch c {
	case codes.NotFound, codes.InvalidArgument, codes.FailedPrecondition:
		return errs.ConfigInvalid
	case codes.PermissionDenied:
		return errs.PermissionDenied
	case codes.Unauthenticated:
		return errs.AuthInvalid
	case codes.ResourceExhausted:
		return errs.RateLimited
	}
	return errs.Transient
}

func (r *Reader) readAzure(ctx context.Context, ref string, p Ref) ([]byte, error) {
	c, err := r.azureClient(p.Vault)
	if err != nil {
		return nil, err
	}
	res, err := c.GetSecret(ctx, p.Name, p.Version, nil)
	if err != nil {
		return nil, errs.Wrap(azureCode(err), err, "read "+ref)
	}
	if res.Value == nil {
		return nil, errs.New(errs.ConfigInvalid, "read %s: it has no value", ref)
	}
	return []byte(*res.Value), nil
}

func (r *Reader) azureClient(vault string) (*azsecrets.Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.azure[vault]; ok {
		return c, nil
	}
	if r.azureCred == nil {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, errs.Wrap(errs.AuthInvalid, err, "the runner's Azure credential")
		}
		r.azureCred = cred
	}
	c, err := azsecrets.NewClient(cmp.Or(r.azureVault, vault), r.azureCred, r.azureOptions)
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "make a Key Vault client")
	}
	if r.azure == nil {
		r.azure = map[string]*azsecrets.Client{}
	}
	r.azure[vault] = c
	return c, nil
}

func azureCode(err error) errs.Code {
	var re *azcore.ResponseError
	if errors.As(err, &re) {
		switch re.StatusCode {
		case http.StatusBadRequest, http.StatusNotFound:
			return errs.ConfigInvalid
		case http.StatusUnauthorized:
			return errs.AuthInvalid
		case http.StatusForbidden:
			return errs.PermissionDenied
		case http.StatusTooManyRequests:
			return errs.RateLimited
		}
		return errs.Transient
	}
	var af *azidentity.AuthenticationFailedError
	if errors.As(err, &af) {
		return errs.AuthInvalid
	}
	return errs.Transient
}
