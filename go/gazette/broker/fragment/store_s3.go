package fragment

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
)

// S3StoreConfig configures a Fragment store of the "s3://" scheme.
// It is initialized from parsed URL parameters of the pb.FragmentStore.
type S3StoreConfig struct {
	bucket string
	prefix string

	RewriterConfig
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
}

type s3Backend struct {
	clients   map[[2]string]*s3.S3
	clientsMu sync.Mutex
}

func newS3Backend() *s3Backend {
	return &s3Backend{
		clients: make(map[[2]string]*s3.S3),
	}
}

func (s *s3Backend) Provider() string {
	return "s3"
}

func (s *s3Backend) SignGet(ep *url.URL, fragment pb.Fragment, d time.Duration) (string, error) {
	cfg, client, err := s.s3Client(ep)
	if err != nil {
		return "", err
	}

	var getObj = s3.GetObjectInput{
		Bucket: aws.String(cfg.bucket),
		Key:    aws.String(cfg.rewritePath(cfg.prefix, fragment.ContentPath())),
	}
	var req, _ = client.GetObjectRequest(&getObj)
	return req.Presign(d)
}

func (s *s3Backend) Exists(ctx context.Context, ep *url.URL, fragment pb.Fragment) (bool, error) {
	cfg, client, err := s.s3Client(ep)
	if err != nil {
		return false, err
	}
	var headObj = s3.HeadObjectInput{
		Bucket: aws.String(cfg.bucket),
		Key:    aws.String(cfg.rewritePath(cfg.prefix, fragment.ContentPath())),
	}
	if _, err = client.HeadObjectWithContext(ctx, &headObj); err == nil {
		return true, nil
	} else if awsErr, ok := err.(awserr.RequestFailure); ok && awsErr.StatusCode() == http.StatusNotFound {
		err = nil
		return false, nil
	} else {
		return false, err
	}
}

func (s *s3Backend) Open(ctx context.Context, ep *url.URL, fragment pb.Fragment) (io.ReadCloser, error) {
	cfg, client, err := s.s3Client(ep)
	if err != nil {
		return nil, err
	}

	var getObj = s3.GetObjectInput{
		Bucket: aws.String(cfg.bucket),
		Key:    aws.String(cfg.rewritePath(cfg.prefix, fragment.ContentPath())),
	}
	var resp *s3.GetObjectOutput
	if resp, err = client.GetObjectWithContext(ctx, &getObj); err != nil {
		return nil, err
	}
	return resp.Body, err
}

func (s *s3Backend) Persist(ctx context.Context, ep *url.URL, spool Spool) error {
	cfg, client, err := s.s3Client(ep)
	if err != nil {
		return err
	}

	var putObj = s3.PutObjectInput{
		Bucket: aws.String(cfg.bucket),
		Key:    aws.String(cfg.rewritePath(cfg.prefix, spool.ContentPath())),
	}

	if cfg.ACL != "" {
		putObj.ACL = aws.String(cfg.ACL)
	}
	if cfg.StorageClass != "" {
		putObj.StorageClass = aws.String(cfg.StorageClass)
	}
	if cfg.SSE != "" {
		putObj.ServerSideEncryption = aws.String(cfg.SSE)
	}
	if cfg.SSEKMSKeyId != "" {
		putObj.SSEKMSKeyId = aws.String(cfg.SSEKMSKeyId)
	}
	if spool.CompressionCodec == pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION {
		putObj.ContentEncoding = aws.String("gzip")
	}
	if spool.CompressionCodec != pb.CompressionCodec_NONE {
		putObj.Body = io.NewSectionReader(spool.compressedFile, 0, spool.compressedLength)
	} else {
		putObj.Body = io.NewSectionReader(spool.File, 0, spool.ContentLength())
	}
	_, err = client.PutObjectWithContext(ctx, &putObj)
	return err
}

func (s *s3Backend) List(ctx context.Context, store pb.FragmentStore, ep *url.URL, journal pb.Journal, callback func(pb.Fragment)) error {
	var cfg, client, err = s.s3Client(ep)
	if err != nil {
		return err
	}
	var q = s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.bucket),
		Prefix: aws.String(cfg.rewritePath(cfg.prefix, journal.String()) + "/"),
	}
	return client.ListObjectsV2PagesWithContext(ctx, &q, func(objs *s3.ListObjectsV2Output, _ bool) bool {
		for _, obj := range objs.Contents {
			if strings.HasSuffix(*obj.Key, "/") {
				// Ignore directory-like objects, usually created by mounting buckets with a FUSE driver.
			} else if frag, err := pb.ParseFragmentFromRelativePath(journal, (*obj.Key)[len(*q.Prefix):]); err != nil {
				log.WithFields(log.Fields{"bucket": cfg.bucket, "key": *obj.Key, "err": err}).Warning("parsing fragment")
			} else if *obj.Size == 0 && frag.ContentLength() > 0 {
				log.WithFields(log.Fields{"obj": obj}).Warning("zero-length fragment")
			} else {
				frag.ModTime = obj.LastModified.Unix()
				frag.BackingStore = store
				callback(frag)
			}
		}
		return true
	})
}

func (s *s3Backend) Remove(ctx context.Context, fragment pb.Fragment) error {
	cfg, client, err := s.s3Client(fragment.BackingStore.URL())
	if err != nil {
		return err
	}
	var deleteObj = s3.DeleteObjectInput{
		Bucket: aws.String(cfg.bucket),
		Key:    aws.String(cfg.rewritePath(cfg.prefix, fragment.ContentPath())),
	}

	_, err = client.DeleteObjectWithContext(ctx, &deleteObj)
	return err
}

func (s *s3Backend) s3Client(ep *url.URL) (cfg S3StoreConfig, client *s3.S3, err error) {
	if err = parseStoreArgs(ep, &cfg); err != nil {
		return
	}
	// Omit leading slash from bucket prefix. Note that FragmentStore already
	// enforces that URL Paths end in '/'.
	cfg.bucket, cfg.prefix = ep.Host, ep.Path[1:]

	defer s.clientsMu.Unlock()
	s.clientsMu.Lock()

	var key = [2]string{cfg.Endpoint, cfg.Profile}
	if client = s.clients[key]; client != nil {
		return
	}

	var awsConfig = aws.NewConfig()
	awsConfig.WithCredentialsChainVerboseErrors(true)

	if cfg.Endpoint != "" {
		awsConfig.WithEndpoint(cfg.Endpoint)
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
		Profile: cfg.Profile,
	})
	if err != nil {
		err = fmt.Errorf("constructing S3 session: %s", err)
		return
	}

	creds, err := awsSession.Config.Credentials.Get()
	if err != nil {
		err = fmt.Errorf("fetching AWS credentials for profile %q: %s", cfg.Profile, err)
		return
	}

	// The aws sdk will always just return an error if this Region is not set, even if
	// the Endpoint was provided explicitly. It's important to return an error here
	// in that case, before adding this client to the `clients` map.
	if awsSession.Config.Region == nil || *awsSession.Config.Region == "" {
		err = fmt.Errorf("missing AWS region configuration for profile %q", cfg.Profile)
		return
	}

	log.WithFields(log.Fields{
		"endpoint":     cfg.Endpoint,
		"profile":      cfg.Profile,
		"region":       *awsSession.Config.Region,
		"keyID":        creds.AccessKeyID,
		"providerName": creds.ProviderName,
	}).Info("constructed new aws.Session")

	client = s3.New(awsSession)
	s.clients[key] = client

	return
}
