package fragment

import (
	"context"
	"errors"
	"io"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/stores"
)

func TestPersistAuthErrorFallback(t *testing.T) {
	var authError = errors.New("authorization failed")
	var otherError = errors.New("network timeout")

	// Register stores: s3 fails with auth errors, gs with non-auth error, azure succeeds
	stores.RegisterProviders(map[string]stores.Constructor{
		"s3": func(u *url.URL) (stores.Store, error) {
			return &stores.CallbackStore{
				Fallback: stores.NewMemoryStore(u),
				PutFunc: func(stores.Store, context.Context, string, io.ReaderAt, int64, string) error {
					return authError
				},
				IsAuthErrorFunc: func(_ stores.Store, err error) bool {
					return err == authError
				},
			}, nil
		},
		"gs": func(u *url.URL) (stores.Store, error) {
			return &stores.CallbackStore{
				Fallback: stores.NewMemoryStore(u),
				PutFunc: func(stores.Store, context.Context, string, io.ReaderAt, int64, string) error {
					return otherError
				},
				IsAuthErrorFunc: func(_ stores.Store, err error) bool {
					return false
				},
			}, nil
		},
		"azure": func(u *url.URL) (stores.Store, error) {
			return stores.NewMemoryStore(u), nil
		},
	})

	// Create test spool
	var obv testSpoolObserver
	var spool = NewSpool("test/journal", &obv)
	spool.applyContent(&pb.ReplicateRequest{
		Content:      []byte("test content"),
		ContentDelta: 0,
	})
	spool.applyCommit(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "test/journal",
			Begin:            0,
			End:              12,
			Sum:              pb.SHA1SumOf("test content"),
			CompressionCodec: pb.CompressionCodec_NONE,
		}}, true)

	var spec = &pb.JournalSpec{
		Fragment: pb.JournalSpec_Fragment{
			Stores: []pb.FragmentStore{"s3://first/", "s3://second/", "azure://third/"},
		},
	}

	// Case 1: Auth error without exit fails immediately
	err := Persist(context.Background(), spool, spec, false)
	require.Equal(t, authError, err)

	// Verify no stores have data
	for _, storeURL := range spec.Fragment.Stores {
		var store = stores.Get(storeURL)
		exists, err := store.Exists(context.Background(), spool.ContentPath())
		require.NoError(t, err)
		require.False(t, exists)
	}

	// Case 2: Auth errors fall through to the Azure store when exiting.
	err = Persist(context.Background(), spool, spec, true)
	require.NoError(t, err)

	// Verify only azure store has data
	for i, storeURL := range spec.Fragment.Stores {
		var store = stores.Get(storeURL)
		exists, err := store.Exists(context.Background(), spool.ContentPath())
		require.NoError(t, err)
		require.Equal(t, i == 2, exists, "Store %d", i)
	}

	// Case 3: Non-auth error should fail even when exiting
	spec.Fragment.Stores = []pb.FragmentStore{"gs://failing/", "azure://backup/"}
	err = Persist(context.Background(), spool, spec, true)
	require.Equal(t, otherError, err)

	// Verify no stores have data (non-auth error stops immediately)
	for _, storeURL := range spec.Fragment.Stores {
		var store = stores.Get(storeURL)
		exists, err := store.Exists(context.Background(), spool.ContentPath())
		require.NoError(t, err)
		require.False(t, exists)
	}
}

func TestIsAuthError(t *testing.T) {
	var authError = errors.New("access denied")
	var otherError = errors.New("network timeout")

	stores.RegisterProviders(map[string]stores.Constructor{
		// A store which denies deletes with an authorization error.
		"s3": func(u *url.URL) (stores.Store, error) {
			return &stores.CallbackStore{
				Fallback: stores.NewMemoryStore(u),
				RemoveFunc: func(stores.Store, context.Context, string) error {
					return authError
				},
				IsAuthErrorFunc: func(_ stores.Store, err error) bool {
					return err == authError
				},
			}, nil
		},
		// A store which fails to initialize, so its ActiveStore has a nil Store.
		"gs": func(u *url.URL) (stores.Store, error) {
			return nil, errors.New("cannot initialize store")
		},
	})

	var frag = pb.Fragment{
		Journal:          "test/journal",
		Begin:            0,
		End:              12,
		CompressionCodec: pb.CompressionCodec_NONE,
		BackingStore:     "s3://bucket/",
	}

	// The auth error surfaced by Remove is classified as an auth error...
	require.Equal(t, authError, Remove(context.Background(), frag))
	require.True(t, IsAuthError(frag, authError))

	// ...while an unrelated error against the same store is not.
	require.False(t, IsAuthError(frag, otherError))

	// A store which failed to initialize never reports an auth error (and does not panic).
	var uninitFrag = pb.Fragment{
		Journal:          "test/journal",
		CompressionCodec: pb.CompressionCodec_NONE,
		BackingStore:     "gs://bucket/",
	}
	require.False(t, IsAuthError(uninitFrag, authError))
}
