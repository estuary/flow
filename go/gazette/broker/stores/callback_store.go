package stores

import (
	"context"
	"io"
	"time"
)

// CallbackStore implements Store for testing with customizable behavior.
// It allows tests to provide callback functions for each Store method.
// Each callback receives the Fallback store as a parameter, allowing it to
// delegate to the fallback for default behavior.
type CallbackStore struct {
	Fallback Store

	SignGetFunc     func(fallback Store, path string, d time.Duration) (string, error)
	GetFunc         func(fallback Store, ctx context.Context, path string) (io.ReadCloser, error)
	ExistsFunc      func(fallback Store, ctx context.Context, path string) (bool, error)
	PutFunc         func(fallback Store, ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error
	ListFunc        func(fallback Store, ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error
	RemoveFunc      func(fallback Store, ctx context.Context, path string) error
	IsAuthErrorFunc func(fallback Store, err error) bool
}

// SignGet calls SignGetFunc if set, otherwise delegates to Fallback.
func (c *CallbackStore) SignGet(path string, d time.Duration) (string, error) {
	if c.SignGetFunc != nil {
		return c.SignGetFunc(c.Fallback, path, d)
	}
	return c.Fallback.SignGet(path, d)
}

// Exists calls ExistsFunc if set, otherwise delegates to Fallback.
func (c *CallbackStore) Exists(ctx context.Context, path string) (bool, error) {
	if c.ExistsFunc != nil {
		return c.ExistsFunc(c.Fallback, ctx, path)
	}
	return c.Fallback.Exists(ctx, path)
}

// Get calls GetFunc if set, otherwise delegates to Fallback.
func (c *CallbackStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	if c.GetFunc != nil {
		return c.GetFunc(c.Fallback, ctx, path)
	}
	return c.Fallback.Get(ctx, path)
}

// Put calls PutFunc if set, otherwise delegates to Fallback.
func (c *CallbackStore) Put(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
	if c.PutFunc != nil {
		return c.PutFunc(c.Fallback, ctx, path, content, contentLength, contentEncoding)
	}
	return c.Fallback.Put(ctx, path, content, contentLength, contentEncoding)
}

// List calls ListFunc if set, otherwise delegates to Fallback.
func (c *CallbackStore) List(ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
	if c.ListFunc != nil {
		return c.ListFunc(c.Fallback, ctx, prefix, callback)
	}
	return c.Fallback.List(ctx, prefix, callback)
}

// Remove calls RemoveFunc if set, otherwise delegates to Fallback.
func (c *CallbackStore) Remove(ctx context.Context, path string) error {
	if c.RemoveFunc != nil {
		return c.RemoveFunc(c.Fallback, ctx, path)
	}
	return c.Fallback.Remove(ctx, path)
}

// IsAuthError calls IsAuthErrorFunc if set, otherwise delegates to Fallback.
func (c *CallbackStore) IsAuthError(err error) bool {
	if c.IsAuthErrorFunc != nil {
		return c.IsAuthErrorFunc(c.Fallback, err)
	}
	return c.Fallback.IsAuthError(err)
}
