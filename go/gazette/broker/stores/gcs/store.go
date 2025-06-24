package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/gorilla/schema"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/stores"
	"go.gazette.dev/core/broker/stores/common"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// StoreQueryArgs contains fields that are parsed from the query arguments
// of a gs:// fragment store URL.
type StoreQueryArgs struct {
	common.RewriterConfig
}

type store struct {
	bucket           string
	prefix           string
	args             StoreQueryArgs
	client           *storage.Client
	signedURLOptions storage.SignedURLOptions
}

// New creates a new GCS Store from the provided URL.
func New(ep *url.URL) (stores.Store, error) {
	var args StoreQueryArgs
	if err := parseStoreArgs(ep, &args); err != nil {
		return nil, err
	}
	// Omit leading slash from bucket prefix. Note that FragmentStore already
	// enforces that URL Paths end in '/'.
	var bucket, prefix = ep.Host, ep.Path[1:]

	var ctx = context.Background()

	creds, err := google.FindDefaultCredentials(ctx, storage.ScopeFullControl)
	if err != nil {
		return nil, err
	} else if creds.JSON == nil {
		return nil, fmt.Errorf("use of GCS requires that a service-account private key be supplied with application default credentials")
	}
	conf, err := google.JWTConfigFromJSON(creds.JSON, storage.ScopeFullControl)
	if err != nil {
		return nil, err
	}
	client, err := storage.NewClient(ctx,
		option.WithTokenSource(conf.TokenSource(ctx)),
	)
	if err != nil {
		return nil, err
	}
	var opts = storage.SignedURLOptions{
		GoogleAccessID: conf.Email,
		PrivateKey:     conf.PrivateKey,
	}

	log.WithFields(log.Fields{
		"ProjectID":      creds.ProjectID,
		"GoogleAccessID": conf.Email,
		"PrivateKeyID":   conf.PrivateKeyID,
		"Subject":        conf.Subject,
		"Scopes":         conf.Scopes,
	}).Info("constructed new GCS client")

	return &store{
		bucket:           bucket,
		prefix:           prefix,
		args:             args,
		client:           client,
		signedURLOptions: opts,
	}, nil
}

func (s *store) SignGet(path string, d time.Duration) (string, error) {
	var opts = s.signedURLOptions
	opts.Method = "GET"
	opts.Expires = time.Now().Add(d)

	return storage.SignedURL(s.bucket, s.args.RewritePath(s.prefix, path), &opts)
}

func (s *store) Exists(ctx context.Context, path string) (exists bool, err error) {
	_, err = s.client.Bucket(s.bucket).Object(s.args.RewritePath(s.prefix, path)).Attrs(ctx)
	if err == nil {
		exists = true
	} else if err == storage.ErrObjectNotExist {
		err = nil
	}
	return exists, err
}

func (s *store) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	return s.client.Bucket(s.bucket).Object(s.args.RewritePath(s.prefix, path)).NewReader(ctx)
}

func (s *store) Put(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wc = s.client.Bucket(s.bucket).Object(s.args.RewritePath(s.prefix, path)).NewWriter(ctx)

	if contentEncoding != "" {
		wc.ContentEncoding = contentEncoding
	}
	// io.Copy only needs io.Reader, so we use io.NewSectionReader to adapt io.ReaderAt
	var _, err = io.Copy(wc, io.NewSectionReader(content, 0, contentLength))
	if err != nil {
		return err
	}
	return wc.Close()
}

func (s *store) List(ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
	prefix = s.args.RewritePath(s.prefix, prefix)
	var (
		q = storage.Query{
			Prefix: prefix,
		}
		it  = s.client.Bucket(s.bucket).Objects(ctx, &q)
		obj *storage.ObjectAttrs
		err error
	)
	for obj, err = it.Next(); err == nil; obj, err = it.Next() {
		if strings.HasSuffix(obj.Name, "/") {
			continue // Ignore directory-like objects
		}
		// Return path relative to the listing prefix
		var relPath = strings.TrimPrefix(obj.Name, prefix)
		if err := callback(relPath, obj.Updated); err != nil {
			return err
		}
	}
	if err == iterator.Done {
		err = nil
	}
	return err
}

func (s *store) Remove(ctx context.Context, path string) error {
	return s.client.Bucket(s.bucket).Object(s.args.RewritePath(s.prefix, path)).Delete(ctx)
}

func (s *store) IsAuthError(err error) bool {
	if err == nil {
		return false
	}

	if err == storage.ErrBucketNotExist {
		return true
	}

	// Check for Google API errors that indicate AuthZ failures.
	var gErr *googleapi.Error
	if errors.As(err, &gErr) {
		switch gErr.Code {
		case http.StatusForbidden:
			return true
		case http.StatusNotFound:
			// Only treat bucket-level 404s as AuthZ failures, not object-level.
			if strings.Contains(gErr.Message, "bucket") {
				return true
			}
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
