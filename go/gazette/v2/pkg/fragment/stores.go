package fragment

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/metrics"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/gorilla/schema"
)

const (
	getSignedURLOp = "get_signed_url"
	existsOp       = "exist"
	openOp         = "open"
	persistOp      = "persist"
	listOp         = "list"
	removeOp       = "remove"
)

// SignGetURL returns a URL authenticating the bearer to perform a GET operation
// of the Fragment for the provided Duration from the current time.
func SignGetURL(fragment pb.Fragment, d time.Duration) (string, error) {
	var ep = fragment.BackingStore.URL()

	switch ep.Scheme {
	case "s3":
		return s3SignGET(ep, fragment, d)
	case "gs":
		return gcsSignGET(ep, fragment, d)
	case "file":
		return fsURL(ep, fragment)
	default:
		panic("unsupported scheme: " + ep.Scheme)
	}
}

// Open a Reader of the Fragment on the store. The returned ReadCloser does not
// perform any applicable client-side decompression, but does request server
// decompression in the case of GZIP_OFFLOAD_DECOMPRESSION.
func Open(ctx context.Context, fragment pb.Fragment) (io.ReadCloser, error) {
	var ep = fragment.BackingStore.URL()

	switch ep.Scheme {
	case "s3":
		return s3Open(ctx, ep, fragment)
	case "gs":
		return gcsOpen(ctx, ep, fragment)
	case "file":
		return fsOpen(ep, fragment)
	default:
		panic("unsupported scheme: " + ep.Scheme)
	}
}

// Persist a Spool to its store. If the Spool Fragment is already present,
// this is a no-op. If the Spool has not been compressed incrementally,
// it will be compressed before being persisted.
func Persist(ctx context.Context, spool Spool) error {
	var ep = spool.Fragment.BackingStore.URL()

	var exists bool
	var err error

	switch ep.Scheme {
	case "s3":
		exists, err = s3Exists(ctx, ep, spool.Fragment.Fragment)
	case "gs":
		exists, err = gcsExists(ctx, ep, spool.Fragment.Fragment)
	case "file":
		exists, err = fsExists(ep, spool.Fragment.Fragment)
	default:
		panic("unsupported scheme: " + ep.Scheme)
	}

	if err != nil {
		return err
	} else if exists {
		return nil // All done.
	}

	// Ensure |compressedFile| is ready. This is a no-op if compressed incrementally.
	if spool.CompressionCodec != pb.CompressionCodec_NONE {
		spool.finishCompression()
	}

	switch ep.Scheme {
	case "s3":
		return s3Persist(ctx, ep, spool)
	case "gs":
		return gcsPersist(ctx, ep, spool)
	case "file":
		return fsPersist(ep, spool)
	}
	panic("not reached")
}

// List Fragments of the FragmentStore for a given journal. |callback| is
// invoked with each listed Fragment, and any returned error aborts the listing.
func List(ctx context.Context, store pb.FragmentStore, name pb.Journal, callback func(pb.Fragment)) error {
	var ep = store.URL()

	switch ep.Scheme {
	case "s3":
		return s3List(ctx, store, ep, name, callback)
	case "gs":
		return gcsList(ctx, store, ep, name, callback)
	case "file":
		return fsList(store, ep, name, callback)
	default:
		panic("unsupported scheme: " + ep.Scheme)
	}
}

// Remove |fragment| from its BackingStore.
func Remove(ctx context.Context, fragment pb.Fragment) error {
	switch fragment.BackingStore.URL().Scheme {
	case "s3":
		return s3Remove(ctx, fragment)
	case "gs":
		return gcsRemove(ctx, fragment)
	case "file":
		return fsRemove(fragment)
	default:
		panic("unsupported scheme: " + fragment.BackingStore)
	}
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

func instrument(provider, op string, err error) {
	if err != nil {
		metrics.StoreRequestTotal.WithLabelValues(provider, op, metrics.Fail)
	}
	metrics.StoreRequestTotal.WithLabelValues(provider, op, metrics.Ok)
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
