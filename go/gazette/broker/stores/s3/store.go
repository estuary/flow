package s3

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/schema"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/stores"
	"go.gazette.dev/core/broker/stores/common"
)

// StoreQueryArgs contains fields that are parsed from the query arguments
// of an s3:// fragment store URL.
type StoreQueryArgs struct {
	common.RewriterConfig
	// AWS Profile to extract credentials from the shared credentials file.
	// For details, see:
	//   https://aws.amazon.com/blogs/security/a-new-and-standardized-way-to-manage-credentials-in-the-aws-sdks/
	// If empty, the default credentials are used.
	Profile string
	// Endpoint to connect to S3. If empty, the default S3 service is used.
	Endpoint string
	// ACL applied when persisting new fragments. By default, this is
	// s3.ObjectCannedACLBucketOwnerFullControl.
	ACL string
	// Storage class applied when persisting new fragments. By default,
	// this is s3.ObjectStorageClassStandard.
	StorageClass string
	// SSE is the server-side encryption type to be applied (eg, "AES256").
	// By default, encryption is not used.
	SSE string
	// SSEKMSKeyId specifies the ID for the AWS KMS symmetric customer managed key
	// By default, not used.
	SSEKMSKeyId string
	// Region is the region for the bucket. If empty, the region is determined
	// from `Profile` or the default credentials.
	Region string
}

type store struct {
	bucket string
	prefix string
	args   StoreQueryArgs
	client *s3.S3
}

// New creates a new S3 Store from the provided URL.
func New(ep *url.URL) (stores.Store, error) {
	var args StoreQueryArgs
	if err := parseStoreArgs(ep, &args); err != nil {
		return nil, err
	}
	// Omit leading slash from bucket prefix. Note that FragmentStore already
	// enforces that URL Paths end in '/'.
	var bucket, prefix = ep.Host, ep.Path[1:]

	var awsConfig = aws.NewConfig()
	awsConfig.WithCredentialsChainVerboseErrors(true)

	if args.Region != "" {
		awsConfig.WithRegion(args.Region)
	}

	if args.Endpoint != "" {
		awsConfig.WithEndpoint(args.Endpoint)
		// We must force path style because bucket-named virtual hosts
		// are not compatible with explicit endpoints.
		awsConfig.WithS3ForcePathStyle(true)
	} else {
		// Real S3. Override the default http.Transport's behavior of inserting
		// "Accept-Encoding: gzip" and transparently decompressing client-side.
		awsConfig.WithHTTPClient(&http.Client{
			Transport: &http.Transport{DisableCompression: true},
		})
	}

	awsSession, err := session.NewSessionWithOptions(session.Options{
		Config:  *awsConfig,
		Profile: args.Profile,
	})
	if err != nil {
		return nil, fmt.Errorf("constructing S3 session: %s", err)
	}

	creds, err := awsSession.Config.Credentials.Get()
	if err != nil {
		return nil, fmt.Errorf("fetching AWS credentials for profile %q: %s", args.Profile, err)
	}

	// The aws sdk will always just return an error if this Region is not set, even if
	// the Endpoint was provided explicitly. It's important to fail-fast in this case.
	if awsSession.Config.Region == nil || *awsSession.Config.Region == "" {
		return nil, fmt.Errorf("missing AWS region configuration for profile %q", args.Profile)
	}

	log.WithFields(log.Fields{
		"endpoint":     args.Endpoint,
		"profile":      args.Profile,
		"region":       *awsSession.Config.Region,
		"keyID":        creds.AccessKeyID,
		"providerName": creds.ProviderName,
	}).Info("constructed new aws.Session")

	var client = s3.New(awsSession)

	return &store{
		bucket: bucket,
		prefix: prefix,
		args:   args,
		client: client,
	}, nil
}

func (s *store) SignGet(path string, d time.Duration) (string, error) {
	var getObj = s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.args.RewritePath(s.prefix, path)),
	}
	var req, _ = s.client.GetObjectRequest(&getObj)
	return req.Presign(d)
}

func (s *store) Exists(ctx context.Context, path string) (bool, error) {
	var headObj = s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.args.RewritePath(s.prefix, path)),
	}
	if _, err := s.client.HeadObjectWithContext(ctx, &headObj); err == nil {
		return true, nil
	} else if awsErr, ok := err.(awserr.RequestFailure); ok && awsErr.StatusCode() == http.StatusNotFound {
		return false, nil
	} else {
		return false, err
	}
}

func (s *store) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	var getObj = s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.args.RewritePath(s.prefix, path)),
	}
	var resp *s3.GetObjectOutput
	var err error
	if resp, err = s.client.GetObjectWithContext(ctx, &getObj); err != nil {
		return nil, err
	}
	return resp.Body, err
}

func (s *store) Put(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
	// S3 SDK requires io.ReadSeeker, so we use io.NewSectionReader to adapt io.ReaderAt
	var putObj = s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.args.RewritePath(s.prefix, path)),
		Body:   io.NewSectionReader(content, 0, contentLength),
	}

	if s.args.ACL != "" {
		putObj.ACL = aws.String(s.args.ACL)
	}
	if s.args.StorageClass != "" {
		putObj.StorageClass = aws.String(s.args.StorageClass)
	}
	if s.args.SSE != "" {
		putObj.ServerSideEncryption = aws.String(s.args.SSE)
	}
	if s.args.SSEKMSKeyId != "" {
		putObj.SSEKMSKeyId = aws.String(s.args.SSEKMSKeyId)
	}
	if contentEncoding != "" {
		putObj.ContentEncoding = aws.String(contentEncoding)
	}

	_, err := s.client.PutObjectWithContext(ctx, &putObj)
	return err
}

func (s *store) List(ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
	prefix = s.args.RewritePath(s.prefix, prefix)
	var q = s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}
	var listErr error
	err := s.client.ListObjectsV2PagesWithContext(ctx, &q, func(objs *s3.ListObjectsV2Output, _ bool) bool {
		for _, obj := range objs.Contents {
			if strings.HasSuffix(*obj.Key, "/") {
				continue // Ignore directory-like objects
			}
			// Return path relative to the listing prefix
			var relPath = strings.TrimPrefix(*obj.Key, prefix)
			if err := callback(relPath, *obj.LastModified); err != nil {
				listErr = err
				return false // Stop pagination
			}
		}
		return true // Continue to next page
	})
	if listErr != nil {
		return listErr
	}
	return err
}

func (s *store) Remove(ctx context.Context, path string) error {
	var deleteObj = s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.args.RewritePath(s.prefix, path)),
	}

	_, err := s.client.DeleteObjectWithContext(ctx, &deleteObj)
	return err
}

func (s *store) IsAuthError(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case s3.ErrCodeNoSuchBucket:
			return true
		case s3ErrCodeAccessDenied:
			return true
		}
	}
	if awsErr, ok := err.(awserr.RequestFailure); ok {
		if awsErr.StatusCode() == http.StatusForbidden {
			return true
		}
	}
	return false
}

func parseStoreArgs(ep *url.URL, args interface{}) error {
	var decoder = schema.NewDecoder()
	decoder.IgnoreUnknownKeys(false)

	if q, err := url.ParseQuery(ep.RawQuery); err != nil {
		return err
	} else if err = decoder.Decode(args, q); err != nil {
		return fmt.Errorf("parsing store URL arguments: %s", err)
	}
	return nil
}

const (
	// AWS S3 error codes not defined as constants in the SDK
	s3ErrCodeAccessDenied = "AccessDenied"
)
