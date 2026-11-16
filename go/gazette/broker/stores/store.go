// Package stores provides an abstraction over cloud storage systems for journal fragments.
package stores

import (
	"context"
	"io"
	"net/url"
	"time"
)

// Store provides an abstraction over cloud storage systems for journal fragments.
type Store interface {
	// SignGet returns a pre-signed URL for GET operations with the given duration.
	SignGet(path string, d time.Duration) (string, error)

	// Exists checks if content exists at the given path.
	Exists(ctx context.Context, path string) (bool, error)

	// Get returns an io.ReadCloser for content at the given path.
	// The returned reader provides the raw content without any decompression.
	Get(ctx context.Context, path string) (io.ReadCloser, error)

	// Put durably writes content to the store at the given path.
	// contentEncoding is used to set appropriate headers (e.g., "gzip" for compressed content).
	Put(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error

	// List enumerates all objects under the given prefix.
	// The callback receives the path relative to the prefix and modification time for each object.
	// For example, if prefix is "foo/bar/" and an object "foo/bar/baz.txt" exists,
	// the callback will be invoked with "baz.txt" as the path.
	// If the callback returns an error, listing is terminated and that error is returned.
	List(ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error

	// Remove deletes content at the given path.
	Remove(ctx context.Context, path string) error

	// IsAuthError returns true if the error represents an authorization failure
	// (e.g., missing permissions, bucket not found, access denied).
	// This is used to distinguish authorization errors from authentication or
	// other transient errors, particularly during graceful shutdown to allow
	// fallback to alternate stores.
	IsAuthError(error) bool
}

// Constructor is a function that creates a Store instance from a URL.
// Each storage backend provides its own constructor implementation.
type Constructor func(*url.URL) (Store, error)
