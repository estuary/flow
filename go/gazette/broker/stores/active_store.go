package stores

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	pb "go.gazette.dev/core/broker/protocol"
)

// ActiveStore wraps a Store implementation with mark-and-sweep GC support and instrumentation.
type ActiveStore struct {
	Key   pb.FragmentStore // FragmentStore from which this ActiveStore was built
	Store Store
	Mark  atomic.Bool // Mark bit for GC

	initErr error // Initialization error (if any) - checked by all methods

	// Health check state
	health struct {
		mu     sync.Mutex    // Protects health check fields
		err    error         // Error from last health check (nil if successful)
		nextCh chan struct{} // Closed when the next check completes
	}
}

// NewActiveStore creates a new ActiveStore with the given FragmentStore and Store.
// If initErr is non-nil, it will be returned on use of any method.
// Don't use this in production code: use Get() for proper initialization and caching.
// Tests may use this to create ActiveStores with fixture errors or health states.
func NewActiveStore(fs pb.FragmentStore, store Store, initErr error) *ActiveStore {
	var s = &ActiveStore{
		Key:     fs,
		Store:   store,
		initErr: initErr,
	}
	s.Mark.Store(true) // Marked active on creation.
	s.health.err = ErrFirstHealthCheck
	s.health.nextCh = make(chan struct{})

	if s.initErr == nil {
		s.health.err = ErrFirstHealthCheck
	} else {
		// Configure HealthStatus to signal repeatedly with the initialization error.
		s.health.err = initErr
		close(s.health.nextCh)
	}
	return s
}

// HealthStatus returns an error summarizing the health of the store,
// and a channel that will be closed when the next health check completes.
func (s *ActiveStore) HealthStatus() (<-chan struct{}, error) {
	s.health.mu.Lock()
	defer s.health.mu.Unlock()

	return s.health.nextCh, s.health.err
}

// UpdateHealth updates the health status of the store.
// Don't use this in production code: Get() calls it automatically.
// It's intended for tests to manually set health status fixtures.
func (s *ActiveStore) UpdateHealth(err error) {
	s.health.mu.Lock()
	defer s.health.mu.Unlock()

	if err == nil {
		s.health.err = nil
		storeHealthCheckTotal.WithLabelValues(string(s.Key), "ok").Inc()
	} else {
		s.health.err = err
		storeHealthCheckTotal.WithLabelValues(string(s.Key), "error").Inc()
	}

	close(s.health.nextCh)
	s.health.nextCh = make(chan struct{})
}

// SignGet returns a pre-signed URL for GET operations with the given duration.
func (s *ActiveStore) SignGet(path string, d time.Duration) (string, error) {
	if s.initErr != nil {
		return "", s.initErr
	}

	var started = time.Now()
	var signed, err = s.Store.SignGet(path, d)

	var status = "success"
	if err != nil {
		status = "error"
	}

	storeOperationTotal.WithLabelValues(string(s.Key), "signget", status).Inc()
	storeOperationDuration.WithLabelValues(string(s.Key), "signget", status).Observe(time.Since(started).Seconds())

	return signed, err
}

// Exists checks if content exists at the given path.
func (s *ActiveStore) Exists(ctx context.Context, path string) (bool, error) {
	if s.initErr != nil {
		return false, s.initErr
	}

	var started = time.Now()
	var exists, err = s.Store.Exists(ctx, path)

	var status = "success"
	if err != nil {
		status = "error"
	}

	storeOperationTotal.WithLabelValues(string(s.Key), "exists", status).Inc()
	storeOperationDuration.WithLabelValues(string(s.Key), "exists", status).Observe(time.Since(started).Seconds())

	return exists, err
}

// Get returns an io.ReadCloser for content at the given path.
func (s *ActiveStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	if s.initErr != nil {
		return nil, s.initErr
	}

	var started = time.Now()
	var rc, err = s.Store.Get(ctx, path)

	var status = "success"
	if err != nil {
		status = "error"
	}

	storeOperationTotal.WithLabelValues(string(s.Key), "get", status).Inc()
	storeOperationDuration.WithLabelValues(string(s.Key), "get", status).Observe(time.Since(started).Seconds())

	return rc, err
}

// Put durably writes content to the store at the given path.
func (s *ActiveStore) Put(ctx context.Context, path string, content io.ReaderAt, contentLength int64, contentEncoding string) error {
	if s.initErr != nil {
		return s.initErr
	}

	var started = time.Now()
	var err = s.Store.Put(ctx, path, content, contentLength, contentEncoding)

	var status = "success"
	if err != nil {
		status = "error"
	}

	storeOperationTotal.WithLabelValues(string(s.Key), "put", status).Inc()
	storeOperationDuration.WithLabelValues(string(s.Key), "put", status).Observe(time.Since(started).Seconds())

	// Track content size for successful puts
	if err == nil && contentLength > 0 {
		var encoding = contentEncoding
		if encoding == "" {
			encoding = "none"
		}
		storePutBytesTotal.WithLabelValues(string(s.Key), encoding).Add(float64(contentLength))
	}

	return err
}

// List enumerates all objects under the given prefix.
func (s *ActiveStore) List(ctx context.Context, prefix string, callback func(path string, modTime time.Time) error) error {
	if s.initErr != nil {
		return s.initErr
	}

	var started = time.Now()

	// Wrap callback to count items
	var itemCount int64
	var wrappedCallback = func(path string, modTime time.Time) error {
		itemCount++
		return callback(path, modTime)
	}

	var err = s.Store.List(ctx, prefix, wrappedCallback)

	var status = "success"
	if err != nil {
		status = "error"
	}

	storeOperationTotal.WithLabelValues(string(s.Key), "list", status).Inc()
	storeOperationDuration.WithLabelValues(string(s.Key), "list", status).Observe(time.Since(started).Seconds())

	// Track item count for all list operations
	storeListItems.WithLabelValues(string(s.Key)).Observe(float64(itemCount))

	return err
}

// Remove content at the given path.
func (s *ActiveStore) Remove(ctx context.Context, path string) error {
	if s.initErr != nil {
		return s.initErr
	}

	var started = time.Now()
	var err = s.Store.Remove(ctx, path)

	var status = "success"
	if err != nil {
		status = "error"
	}

	storeOperationTotal.WithLabelValues(string(s.Key), "remove", status).Inc()
	storeOperationDuration.WithLabelValues(string(s.Key), "remove", status).Observe(time.Since(started).Seconds())

	return err
}

var (
	// ErrFirstHealthCheck indicates the first health check hasn't completed yet.
	ErrFirstHealthCheck = fmt.Errorf("first health check hasn't completed yet")
	// ErrLastHealthCheck indicates health checks have stopped for this store.
	ErrLastHealthCheck = fmt.Errorf("health checks have stopped")
)
