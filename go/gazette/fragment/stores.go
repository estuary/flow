package fragment

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/schema"
	"go.gazette.dev/core/metrics"
	pb "go.gazette.dev/core/protocol"
)

type backend interface {
	Provider() string
	SignGet(ep *url.URL, fragment pb.Fragment, d time.Duration) (string, error)
	Exists(ctx context.Context, ep *url.URL, fragment pb.Fragment) (bool, error)
	Open(ctx context.Context, ep *url.URL, fragment pb.Fragment) (io.ReadCloser, error)
	Persist(ctx context.Context, ep *url.URL, spool Spool) error
	List(ctx context.Context, store pb.FragmentStore, ep *url.URL, name pb.Journal, callback func(pb.Fragment)) error
	Remove(ctx context.Context, fragment pb.Fragment) error
}

var sharedStores = struct {
	s3  *s3Backend
	gcs *gcsBackend
	fs  *fsBackend
}{
	s3:  newS3Backend(),
	gcs: &gcsBackend{},
	fs:  &fsBackend{},
}

func getBackend(scheme string) backend {
	switch scheme {
	case "s3":
		return sharedStores.s3
	case "gs":
		return sharedStores.gcs
	case "file":
		return sharedStores.fs
	default:
		panic("unsupported scheme: " + scheme)
	}
}

// SignGetURL returns a URL authenticating the bearer to perform a GET operation
// of the Fragment for the provided Duration from the current time.
func SignGetURL(fragment pb.Fragment, d time.Duration) (string, error) {
	var ep = fragment.BackingStore.URL()
	var b = getBackend(ep.Scheme)

	var signedURL, err = b.SignGet(ep, fragment, d)
	instrumentStoreOp(b.Provider(), "get_signed_url", err)
	return signedURL, err
}

// Open a Reader of the Fragment on the store. The returned ReadCloser does not
// perform any applicable client-side decompression, but does request server
// decompression in the case of GZIP_OFFLOAD_DECOMPRESSION.
func Open(ctx context.Context, fragment pb.Fragment) (io.ReadCloser, error) {
	var ep = fragment.BackingStore.URL()
	var b = getBackend(ep.Scheme)

	var rc, err = b.Open(ctx, ep, fragment)
	instrumentStoreOp(b.Provider(), "open", err)
	return rc, err
}

// Persist a Spool to its store. If the Spool Fragment is already present,
// this is a no-op. If the Spool has not been compressed incrementally,
// it will be compressed before being persisted.
func Persist(ctx context.Context, spool Spool) error {
	var ep = spool.Fragment.BackingStore.URL()
	var b = getBackend(ep.Scheme)

	var exists, err = b.Exists(ctx, ep, spool.Fragment.Fragment)
	instrumentStoreOp(b.Provider(), "exist", err)
	if err != nil {
		return err
	} else if exists {
		return nil // All done.
	}

	// Ensure |compressedFile| is ready. This is a no-op if compressed incrementally.
	if spool.CompressionCodec != pb.CompressionCodec_NONE {
		spool.finishCompression()
	}

	// We expect persisting individual spools to be fast, but have seen bugs
	// in the past where eg a storage backend behavior change caused occasional
	// multi-part upload failures such that the client wedged retrying
	// indefinitely. Use a generous timeout to detect and recover from this
	// class of error.
	var timeoutCtx, _ = context.WithTimeout(ctx, 5*time.Minute)

	if err = b.Persist(timeoutCtx, ep, spool); err == nil {
		metrics.StorePersistedBytesTotal.WithLabelValues(b.Provider()).Add(float64(spool.ContentLength()))
	}
	instrumentStoreOp(b.Provider(), "persist", err)
	return err
}

// List Fragments of the FragmentStore for a given journal. |callback| is
// invoked with each listed Fragment, and any returned error aborts the listing.
func List(ctx context.Context, store pb.FragmentStore, name pb.Journal, callback func(pb.Fragment)) error {
	var ep = store.URL()
	var b = getBackend(ep.Scheme)

	var err = b.List(ctx, store, ep, name, callback)
	instrumentStoreOp(b.Provider(), "list", err)
	return err
}

// Remove |fragment| from its BackingStore.
func Remove(ctx context.Context, fragment pb.Fragment) error {
	var b = getBackend(fragment.BackingStore.URL().Scheme)
	var err = b.Remove(ctx, fragment)
	instrumentStoreOp(b.Provider(), "remove", err)
	return err
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

func instrumentStoreOp(provider, op string, err error) {
	if err != nil {
		metrics.StoreRequestTotal.WithLabelValues(provider, op, metrics.Fail).Inc()
	} else {
		metrics.StoreRequestTotal.WithLabelValues(provider, op, metrics.Ok).Inc()
	}
}

// rewriterCfg holds a find/replace pair, often populated by parseStoreArgs()
// and provides a convenience function to rewrite the given path.
//
// It is meant to be embedded by other backend store configs.
type rewriterCfg struct {
	Find, Replace string
}

// rewritePath replace the first occurrence of the find string with the replace
// string in journal path |j| and appends it to the fragment store path |s|,
// effectively rewriting the default Journal path. If find is empty or not
// found, the original |j| is appended.
func (cfg rewriterCfg) rewritePath(s, j string) string {
	if cfg.Find == "" {
		return s + j
	}
	return s + strings.Replace(j, cfg.Find, cfg.Replace, 1)
}
