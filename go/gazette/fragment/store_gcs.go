package fragment

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type gcsCfg struct {
	bucket string
	prefix string

	rewriterCfg
}

type gcsBackend struct {
	client           *storage.Client
	signedURLOptions storage.SignedURLOptions
	clientMu         sync.Mutex
}

func (s *gcsBackend) Provider() string {
	return "gcs"
}

func (s *gcsBackend) SignGet(ep *url.URL, fragment pb.Fragment, d time.Duration) (string, error) {
	cfg, _, opts, err := s.gcsClient(ep)
	if err != nil {
		return "", err
	}
	opts.Method = "GET"
	opts.Expires = time.Now().Add(d)

	return storage.SignedURL(cfg.bucket, cfg.rewritePath(cfg.prefix, fragment.ContentPath()), &opts)
}

func (s *gcsBackend) Exists(ctx context.Context, ep *url.URL, fragment pb.Fragment) (exists bool, err error) {
	cfg, client, _, err := s.gcsClient(ep)
	if err != nil {
		return false, err
	}
	_, err = client.Bucket(cfg.bucket).Object(cfg.rewritePath(cfg.prefix, fragment.ContentPath())).Attrs(ctx)
	if err == nil {
		exists = true
	} else if err == storage.ErrObjectNotExist {
		err = nil
	}
	return exists, err
}

func (s *gcsBackend) Open(ctx context.Context, ep *url.URL, fragment pb.Fragment) (io.ReadCloser, error) {
	cfg, client, _, err := s.gcsClient(ep)
	if err != nil {
		return nil, err
	}
	return client.Bucket(cfg.bucket).Object(cfg.rewritePath(cfg.prefix, fragment.ContentPath())).NewReader(ctx)
}

func (s *gcsBackend) Persist(ctx context.Context, ep *url.URL, spool Spool) error {
	cfg, client, _, err := s.gcsClient(ep)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	var wc = client.Bucket(cfg.bucket).Object(cfg.rewritePath(cfg.prefix, spool.ContentPath())).NewWriter(ctx)

	if spool.CompressionCodec == pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION {
		wc.ContentEncoding = "gzip"
	}
	if spool.CompressionCodec != pb.CompressionCodec_NONE {
		_, err = io.Copy(wc, io.NewSectionReader(spool.compressedFile, 0, spool.compressedLength))
	} else {
		_, err = io.Copy(wc, io.NewSectionReader(spool.File, 0, spool.ContentLength()))
	}
	if err != nil {
		cancel() // Abort |wc|.
	} else {
		err = wc.Close()
	}
	return err
}

func (s *gcsBackend) List(ctx context.Context, store pb.FragmentStore, ep *url.URL, name pb.Journal, callback func(pb.Fragment)) error {
	cfg, client, _, err := s.gcsClient(ep)
	if err != nil {
		return err
	}
	var (
		q = storage.Query{
			Prefix: cfg.rewritePath(cfg.prefix, name.String()) + "/",
			// Gazette stores all of a journal's fragment files in a flat
			// structure. Providing a delimiter excludes files in
			// subdirectories from the query results because they will be
			// collapsed into a single synthetic "directory entry".
			Delimiter: "/",
		}
		it    = client.Bucket(cfg.bucket).Objects(ctx, &q)
		strip = len(cfg.prefix)
		obj   *storage.ObjectAttrs
	)
	for obj, err = it.Next(); err == nil; obj, err = it.Next() {
		if obj.Name[strip:] == q.Prefix || obj.Prefix != "" {
			// The parent directory is included in the results because it
			// matches the prefix. Additionally, if there are subdirectories,
			// they will be represented by synthetic "directory entries". Both
			// of these types of objects should be ignored as they never
			// represent fragment files.
			//
			// See:
			// - https://cloud.google.com/storage/docs/json_api/v1/objects/list
			// - https://godoc.org/cloud.google.com/go/storage#ObjectAttrs.Prefix
		} else if frag, err2 := pb.ParseContentPath(obj.Name[strip:]); err2 != nil {
			log.WithFields(log.Fields{"bucket": cfg.bucket, "name": obj.Name, "err": err2}).Warning("parsing fragment")
		} else if obj.Size == 0 && frag.ContentLength() > 0 {
			log.WithFields(log.Fields{"bucket": cfg.bucket, "name": obj.Name}).Warning("zero-length fragment")
		} else {
			frag.ModTime = obj.Updated.Unix()
			frag.BackingStore = store
			callback(frag)
		}
	}
	if err == iterator.Done {
		err = nil
	}
	return err
}

func (s *gcsBackend) Remove(ctx context.Context, fragment pb.Fragment) error {
	cfg, client, _, err := s.gcsClient(fragment.BackingStore.URL())
	if err != nil {
		return err
	}
	return client.Bucket(cfg.bucket).Object(cfg.rewritePath(cfg.prefix, fragment.ContentPath())).Delete(ctx)
}

func (s *gcsBackend) gcsClient(ep *url.URL) (cfg gcsCfg, client *storage.Client, opts storage.SignedURLOptions, err error) {
	if err = parseStoreArgs(ep, &cfg); err != nil {
		return
	}
	// Omit leading slash from bucket prefix. Note that FragmentStore already
	// enforces that URL Paths end in '/'.
	cfg.bucket, cfg.prefix = ep.Host, ep.Path[1:]

	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	if s.client != nil {
		client = s.client
		opts = s.signedURLOptions
		return
	}
	var ctx = context.Background()

	creds, err := google.FindDefaultCredentials(ctx, storage.ScopeFullControl)
	if err != nil {
		return
	} else if creds.JSON == nil {
		err = fmt.Errorf("use of GCS requires that a service-account private key be supplied with application default credentials")
		return
	}
	conf, err := google.JWTConfigFromJSON(creds.JSON, storage.ScopeFullControl)
	if err != nil {
		return
	}
	client, err = storage.NewClient(ctx, option.WithTokenSource(conf.TokenSource(ctx)))
	if err != nil {
		return
	}
	opts = storage.SignedURLOptions{
		GoogleAccessID: conf.Email,
		PrivateKey:     conf.PrivateKey,
	}
	s.client, s.signedURLOptions = client, opts

	log.WithFields(log.Fields{
		"ProjectID":      creds.ProjectID,
		"GoogleAccessID": conf.Email,
		"PrivateKeyID":   conf.PrivateKeyID,
		"Subject":        conf.Subject,
		"Scopes":         conf.Scopes,
	}).Info("constructed new GCS client")

	return
}
