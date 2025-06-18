package fragment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"text/template"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/gorilla/schema"
	pkgerrors "github.com/pkg/errors"
	pb "go.gazette.dev/core/broker/protocol"
)

// ForceFileStore reinterprets journal stores to use the file:// scheme, ignoring
// a configured cloud scheme and bucket such as s3:// or gcs://
var ForceFileStore bool = false

type backend interface {
	Provider() string
	SignGet(ep *url.URL, fragment pb.Fragment, d time.Duration) (string, error)
	Exists(ctx context.Context, ep *url.URL, fragment pb.Fragment) (bool, error)
	Open(ctx context.Context, ep *url.URL, fragment pb.Fragment) (io.ReadCloser, error)
	Persist(ctx context.Context, ep *url.URL, spool Spool) error
	List(ctx context.Context, store pb.FragmentStore, ep *url.URL, name pb.Journal, callback func(pb.Fragment)) error
	Remove(ctx context.Context, fragment pb.Fragment) error
	IsAuthError(error) bool
}

var sharedStores = struct {
	s3    *s3Backend
	gcs   *gcsBackend
	azure *azureBackend
	fs    *fsBackend
}{
	s3:  newS3Backend(),
	gcs: &gcsBackend{},
	azure: &azureBackend{
		pipelines: make(map[string]pipeline.Pipeline),
		clients:   make(map[string]*service.Client),
		udcs:      make(map[string]udcAndExp),
	},
	fs: &fsBackend{},
}

func getBackend(scheme string) backend {
	if ForceFileStore {
		return sharedStores.fs
	}

	switch scheme {
	case "s3":
		return sharedStores.s3
	case "gs":
		return sharedStores.gcs
	case "azure", "azure-ad":
		return sharedStores.azure
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

// Persist a Spool to the JournalSpec's store. If the Spool Fragment is already
// present, this is a no-op. If the Spool has not been compressed incrementally,
// it will be compressed before being persisted. If `isExiting` and the preferred
// store returns an AuthZ error, it will try subsequent stores.
func Persist(ctx context.Context, spool Spool, spec *pb.JournalSpec, isExiting bool) error {
	var stores = spec.Fragment.Stores

	for len(stores) != 0 {
		spool.Fragment.BackingStore, stores = stores[0], stores[1:]

		if postfix, err := evalPathPostfix(spool, spec); err != nil {
			return err
		} else {
			spool.PathPostfix = postfix
		}

		var ep = spool.Fragment.BackingStore.URL()
		var b = getBackend(ep.Scheme)

		var exists, err = b.Exists(ctx, ep, spool.Fragment.Fragment)
		instrumentStoreOp(b.Provider(), "exist", err)

		if err != nil {
			if b.IsAuthError(err) && isExiting && len(stores) != 0 {
				continue // Fall back to the next store.
			}
			return err
		} else if exists {
			return nil // All done.
		}

		// Ensure `compressedFile` is ready. This is a no-op if compressed incrementally.
		if spool.CompressionCodec != pb.CompressionCodec_NONE {
			spool.finishCompression()
		}

		// We expect persisting individual spools to be fast, but have seen bugs
		// in the past where eg a storage backend behavior change caused occasional
		// multi-part upload failures such that the client wedged retrying
		// indefinitely. Use a generous timeout to detect and recover from this
		// class of error.
		var timeoutCtx, cancel = context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()

		err = b.Persist(timeoutCtx, ep, spool)
		instrumentStoreOp(b.Provider(), "persist", err)

		if err != nil {
			if b.IsAuthError(err) && isExiting && len(stores) != 0 {
				continue // During shutdown with auth errors, try next store.
			}
			return err
		}

		storePersistedBytesTotal.WithLabelValues(b.Provider()).Add(float64(spool.ContentLength()))
		return nil // Success
	}
	return nil // No stores.
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
	var cause = errors.Unwrap(err)
	if err != nil {
		if cause != nil {
			storeRequestTotal.WithLabelValues(provider, op, cause.Error()).Inc()
		} else {
			storeRequestTotal.WithLabelValues(provider, op, err.Error()).Inc()
		}
	} else {
		storeRequestTotal.WithLabelValues(provider, op, "ok").Inc()
	}
}

func evalPathPostfix(spool Spool, spec *pb.JournalSpec) (string, error) {
	var tpl, err = template.New("").Parse(spec.Fragment.PathPostfixTemplate)
	if err != nil {
		return "", pkgerrors.WithMessage(err, "parsing PathPostfixTemplate")
	}

	var b bytes.Buffer
	if err = tpl.Execute(&b, struct {
		Spool
		*pb.JournalSpec
	}{spool, spec}); err != nil {
		return "", pkgerrors.WithMessagef(err,
			"executing PathPostfixTemplate (%s)", spec.Fragment.PathPostfixTemplate)
	}
	return b.String(), nil
}

// RewriterConfig rewrites the path under which journal fragments are stored
// by finding and replacing a portion of the journal's name in the final
// constructed path. Its use is uncommon and not recommended, but it can help
// in the implementation of new journal naming taxonomies which don't disrupt
// journal fragments that are already written.
//
//	var cfg = RewriterConfig{
//	    Replace: "/old-path/page-views/
//	    Find:    "/bar/v1/page-views/",
//	}
//	// Remaps journal name => fragment store URL:
//	//  "/foo/bar/v1/page-views/part-000" => "s3://my-bucket/foo/old-path/page-views/part-000" // Matched.
//	//  "/foo/bar/v2/page-views/part-000" => "s3://my-bucket/foo/bar/v2/page-views/part-000"   // Not matched.
type RewriterConfig struct {
	// Find is the string to replace in the unmodified journal name.
	Find string
	// Replace is the string with which Find is replaced in the constructed store path.
	Replace string
}

// rewritePath replace the first occurrence of the find string with the replace
// string in journal path |j| and appends it to the fragment store path |s|,
// effectively rewriting the default Journal path. If find is empty or not
// found, the original |j| is appended.
func (cfg RewriterConfig) rewritePath(s, j string) string {
	if cfg.Find == "" {
		return s + j
	}
	return s + strings.Replace(j, cfg.Find, cfg.Replace, 1)
}
