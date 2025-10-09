package fragment

import (
	"bytes"
	"context"
	"io"
	"text/template"
	"time"

	pkgerrors "github.com/pkg/errors"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/stores"
)

// SignGetURL returns a URL authenticating the bearer to perform a GET operation
// of the Fragment for the provided Duration from the current time.
func SignGetURL(fragment pb.Fragment, d time.Duration) (string, error) {
	var store = stores.Get(fragment.BackingStore)
	store.Mark.Store(true)

	return store.SignGet(fragment.ContentPath(), d)
}

// Open a Reader of the Fragment on the store. The returned ReadCloser does not
// perform any applicable client-side decompression, but does request server
// decompression in the case of GZIP_OFFLOAD_DECOMPRESSION.
func Open(ctx context.Context, fragment pb.Fragment) (io.ReadCloser, error) {
	var store = stores.Get(fragment.BackingStore)
	store.Mark.Store(true)

	return store.Get(ctx, fragment.ContentPath())
}

// Persist a Spool to the JournalSpec's store. If the Spool Fragment is already
// present, this is a no-op. If the Spool has not been compressed incrementally,
// it will be compressed before being persisted. If `isExiting` and the preferred
// store returns an AuthZ error, it will try subsequent stores.
func Persist(ctx context.Context, spool Spool, spec *pb.JournalSpec, isExiting bool) error {
	var fs = spec.Fragment.Stores

	for len(fs) != 0 {
		spool.Fragment.BackingStore, fs = fs[0], fs[1:]

		if postfix, err := evalPathPostfix(spool, spec); err != nil {
			return err
		} else {
			spool.PathPostfix = postfix
		}

		var active = stores.Get(spool.Fragment.BackingStore)
		active.Mark.Store(true)

		exists, err := active.Exists(ctx, spool.ContentPath())
		if err != nil {
			if isExiting && len(fs) != 0 && active.Store != nil && active.Store.IsAuthError(err) {
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

		var (
			contentEncoding string
			contentLength   int64
			reader          io.ReaderAt
		)

		if spool.CompressionCodec != pb.CompressionCodec_NONE {
			reader = spool.compressedFile
			contentLength = spool.compressedLength

			if spool.CompressionCodec == pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION {
				contentEncoding = "gzip" // Request server-side decompression.
			}
		} else {
			reader = spool.Fragment.File
			contentLength = spool.ContentLength()
		}

		err = active.Put(timeoutCtx, spool.ContentPath(), reader, contentLength, contentEncoding)
		if err != nil {
			if isExiting && len(fs) != 0 && active.Store != nil && active.Store.IsAuthError(err) {
				continue // During shutdown with auth errors, try next store.
			}
			return err
		}
		return nil // Success
	}
	return nil // No fragment stores.
}

// Remove `fragment` from its BackingStore.
func Remove(ctx context.Context, fragment pb.Fragment) error {
	var store = stores.Get(fragment.BackingStore)
	store.Mark.Store(true)

	return store.Remove(ctx, fragment.ContentPath())
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
