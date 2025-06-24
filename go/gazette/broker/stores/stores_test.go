package stores

import (
	"errors"
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestGetStore(t *testing.T) {
	RegisterProviders(map[string]Constructor{
		"s3": func(u *url.URL) (Store, error) {
			return NewMemoryStore(u), nil
		},
	})

	// Get store for the first time - should create it
	s1 := Get(pb.FragmentStore("s3://store1/"))
	require.NoError(t, s1.initErr)

	// Get same store again - should return cached instance
	s2 := Get(pb.FragmentStore("s3://store1/"))
	require.NoError(t, s2.initErr)
	require.Same(t, s1, s2) // Same pointer

	// Get different store - should create new instance
	s3 := Get(pb.FragmentStore("s3://store2/"))
	require.NoError(t, s3.initErr)
	require.NotSame(t, s1, s3)
}

func TestConstructorErrors(t *testing.T) {
	RegisterProviders(map[string]Constructor{
		"gs": func(u *url.URL) (Store, error) {
			return nil, errors.New("init failed")
		},
	})

	// Get store that will fail construction
	store := Get(pb.FragmentStore("s3://bucket/")) // Not registered.
	require.Error(t, store.initErr)
	require.Equal(t, "unsupported fragment store scheme: s3", store.initErr.Error())

	// Verify store was not cached
	storesMu.RLock()
	_, exists := stores[pb.FragmentStore("s3://bucket/")]
	storesMu.RUnlock()
	require.False(t, exists)
}

func TestRetryAfterInitError(t *testing.T) {
	// Track constructor calls
	var callCount = 0
	var shouldFail = true
	var mu sync.Mutex

	RegisterProviders(map[string]Constructor{
		"s3": func(u *url.URL) (Store, error) {
			mu.Lock()
			defer mu.Unlock()
			callCount++
			if shouldFail {
				return nil, errors.New("temporary failure")
			}
			return NewMemoryStore(u), nil
		},
	})

	// First attempt - constructor should fail
	store := Get(pb.FragmentStore("s3://test/"))
	require.Error(t, store.initErr)
	require.Equal(t, "temporary failure", store.initErr.Error())

	mu.Lock()
	require.Equal(t, 1, callCount)
	mu.Unlock()

	// Fix the constructor behavior
	mu.Lock()
	shouldFail = false
	mu.Unlock()

	// Second attempt - constructor should be called again and succeed
	s := Get(pb.FragmentStore("s3://test/"))
	require.NoError(t, s.initErr)
	require.NotNil(t, s)

	mu.Lock()
	require.Equal(t, 2, callCount)
	mu.Unlock()

	// Third attempt - should return cached store without calling constructor
	s2 := Get(pb.FragmentStore("s3://test/"))
	require.NoError(t, s2.initErr)
	require.Same(t, s, s2) // Same pointer

	mu.Lock()
	require.Equal(t, 2, callCount) // Constructor not called again
	mu.Unlock()
}

func TestConcurrentAccess(t *testing.T) {
	RegisterProviders(map[string]Constructor{
		"s3": func(u *url.URL) (Store, error) {
			return NewMemoryStore(u), nil
		},
	})

	// Test concurrent access to the same store
	var wg sync.WaitGroup
	const goroutines = 10
	wg.Add(goroutines)

	var stores []*ActiveStore
	var errors []error
	var mu sync.Mutex

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			s := Get(pb.FragmentStore("s3://concurrent/"))

			mu.Lock()
			stores = append(stores, s)
			errors = append(errors, s.initErr)
			mu.Unlock()
		}()
	}

	wg.Wait()

	// All goroutines should get the same store instance
	require.Equal(t, goroutines, len(stores))
	require.Equal(t, goroutines, len(errors))

	for i := 0; i < goroutines; i++ {
		require.NoError(t, errors[i])
		require.Same(t, stores[0], stores[i]) // All should be same pointer
	}
}

func TestSweepRemovesUnmarkedStores(t *testing.T) {
	RegisterProviders(map[string]Constructor{
		"s3": func(u *url.URL) (Store, error) {
			return NewMemoryStore(u), nil
		},
	})

	// Create three stores
	store1 := Get(pb.FragmentStore("s3://store1/"))
	require.NoError(t, store1.initErr)
	store2 := Get(pb.FragmentStore("s3://store2/"))
	require.NoError(t, store2.initErr)
	store3 := Get(pb.FragmentStore("s3://store3/"))
	require.NoError(t, store3.initErr)

	// Verify all stores exist
	storesMu.RLock()
	require.Equal(t, 3, len(stores))
	storesMu.RUnlock()

	// Mark all stores as in-use
	for _, fs := range []pb.FragmentStore{"s3://store1/", "s3://store2/", "s3://store3/"} {
		store := Get(fs)
		require.NoError(t, store.initErr)
		store.Mark.Store(true)
	}

	// First sweep should not remove any stores (all were just accessed)
	var removed = Sweep()
	require.Equal(t, 0, removed)

	// Mark only store1 and store3 as in-use
	store1 = Get(pb.FragmentStore("s3://store1/"))
	require.NoError(t, store1.initErr)
	store1.Mark.Store(true)
	store3 = Get(pb.FragmentStore("s3://store3/"))
	require.NoError(t, store3.initErr)
	store3.Mark.Store(true)

	// Second sweep should remove store2 (not marked)
	removed = Sweep()
	require.Equal(t, 1, removed)

	// Verify only two stores remain
	storesMu.RLock()
	require.Equal(t, 2, len(stores))
	_, exists := stores[pb.FragmentStore("s3://store2/")]
	require.False(t, exists)
	storesMu.RUnlock()

	// Verify health status of removed store returns error
	store2New := Get(pb.FragmentStore("s3://store2/"))
	require.NoError(t, store2New.initErr) // New instance created

	// Verify it's a new instance
	require.NotSame(t, store2, store2New)
}

func TestSweepWithInitErrors(t *testing.T) {
	// Register providers that succeed and fail
	RegisterProviders(map[string]Constructor{
		"s3": func(u *url.URL) (Store, error) {
			return NewMemoryStore(u), nil
		},
		"gs": func(u *url.URL) (Store, error) {
			return nil, errors.New("init failed")
		},
	})

	// Create one successful store
	store1 := Get(pb.FragmentStore("s3://store1/"))
	require.NoError(t, store1.initErr)

	// Create stores that fail to initialize
	store2 := Get(pb.FragmentStore("gs://store2/"))
	require.Error(t, store2.initErr)
	store3 := Get(pb.FragmentStore("gs://store3/"))
	require.Error(t, store3.initErr)

	// Verify only the successful store is cached
	storesMu.RLock()
	require.Equal(t, 1, len(stores))
	_, exists := stores[pb.FragmentStore("s3://store1/")]
	require.True(t, exists)
	storesMu.RUnlock()

	// First sweep should clear the mark but not remove (store was marked on creation)
	var removed = Sweep()
	require.Equal(t, 0, removed)

	// Second sweep should remove the unmarked store
	removed = Sweep()
	require.Equal(t, 1, removed)

	// Verify all stores were removed
	storesMu.RLock()
	require.Equal(t, 0, len(stores))
	storesMu.RUnlock()
}

func TestNewActiveStore(t *testing.T) {
	// Test creating an ActiveStore directly
	var mockStore = &CallbackStore{}

	// Test with no init error
	store := NewActiveStore(pb.FragmentStore("s3://test/"), mockStore, nil)
	require.NoError(t, store.initErr)
	require.Equal(t, pb.FragmentStore("s3://test/"), store.Key)
	require.Same(t, mockStore, store.Store)

	// Check initial health status
	var done, err = store.HealthStatus()
	require.NotNil(t, done)
	require.Error(t, err)
	require.Contains(t, err.Error(), "first health check hasn't completed yet")

	// Test with init error
	var initErr = errors.New("initialization failed")
	store2 := NewActiveStore(pb.FragmentStore("s3://error/"), mockStore, initErr)
	require.Error(t, store2.initErr)
	require.Equal(t, initErr, store2.initErr)

	// Check health status returns init error
	done, err = store2.HealthStatus()
	require.NotNil(t, done)
	require.Equal(t, initErr, err)

	// Done channel should already be closed for error case
	select {
	case <-done:
		// Good, it's closed
	default:
		t.Fatal("done channel should be closed for stores with init error")
	}
}

func TestUpdateHealth(t *testing.T) {
	var mockStore = &CallbackStore{}

	store := NewActiveStore(pb.FragmentStore("s3://test/"), mockStore, nil)

	// Get initial status
	var done1, err1 = store.HealthStatus()
	require.Error(t, err1)

	// Update health to success
	store.UpdateHealth(nil)

	// Original done channel should be closed
	select {
	case <-done1:
		// Good
	default:
		t.Fatal("done channel should be closed after health update")
	}

	// Get new status
	var done2, err2 = store.HealthStatus()
	require.NoError(t, err2)
	require.NotEqual(t, done1, done2) // Should be a new channel

	// Update health to error
	var healthErr = errors.New("health check failed")
	store.UpdateHealth(healthErr)

	// Done2 should be closed
	select {
	case <-done2:
		// Good
	default:
		t.Fatal("done channel should be closed after health update")
	}

	// Get error status
	var _, err3 = store.HealthStatus()
	require.Equal(t, healthErr, err3)
}

func TestMarkAndSweepBehavior(t *testing.T) {
	RegisterProviders(map[string]Constructor{
		"s3": func(u *url.URL) (Store, error) {
			return NewMemoryStore(u), nil
		},
	})

	// Create a store
	store := Get(pb.FragmentStore("s3://test/"))
	require.NoError(t, store.initErr)

	// Initial mark should be true (marked on creation)
	require.True(t, store.Mark.Load())

	// Sweep should not remove marked store
	var removed = Sweep()
	require.Equal(t, 0, removed)

	// Mark should be cleared after sweep
	require.False(t, store.Mark.Load())

	// Another sweep should remove unmarked store
	removed = Sweep()
	require.Equal(t, 1, removed)

	// Get the store again - should be a new instance
	store2 := Get(pb.FragmentStore("s3://test/"))
	require.NoError(t, store2.initErr)
	require.NotSame(t, store, store2)
}

func TestHealthStatusAfterSweep(t *testing.T) {
	RegisterProviders(map[string]Constructor{
		"s3": func(u *url.URL) (Store, error) {
			return NewMemoryStore(u), nil
		},
	})

	// Create a store
	store := Get(pb.FragmentStore("s3://test/"))
	require.NoError(t, store.initErr)

	// Wait for initial health check to complete
	done, _ := store.HealthStatus()
	<-done

	// Update health to success
	store.UpdateHealth(nil)

	// Verify health is good
	var _, err = store.HealthStatus()
	require.NoError(t, err)

	// First sweep clears the mark (store was marked on creation)
	var removed = Sweep()
	require.Equal(t, 0, removed)

	// Second sweep removes the unmarked store
	removed = Sweep()
	require.Equal(t, 1, removed)

	// Health status should now indicate stopped checks
	var done2, err2 = store.HealthStatus()
	require.Error(t, err2)
	require.Contains(t, err2.Error(), "health checks have stopped")

	// Done channel should be closed
	select {
	case <-done2:
		// Good
	default:
		t.Fatal("done channel should be closed for swept stores")
	}
}
