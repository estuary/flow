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
