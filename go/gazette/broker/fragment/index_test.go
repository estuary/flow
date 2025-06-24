package fragment

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/stores"
)

func TestSimpleRemoteAndLocalQueries(t *testing.T) {
	var ind = NewIndex(context.Background())

	var set = buildSet(t, 100, 150, 150, 200, 200, 250)
	ind.ReplaceRemote(set[:2])

	var resp, file, err = ind.Query(context.Background(), &pb.ReadRequest{Offset: 110, Block: true})
	require.Equal(t, &pb.ReadResponse{
		Offset:    110,
		WriteHead: 200,
		Fragment:  &pb.Fragment{Begin: 100, End: 150},
	}, resp)
	require.Nil(t, file)
	require.NoError(t, err)

	// Add a local fragment with backing file. Expect we can query it.
	set[2].File = os.Stdin
	ind.SpoolCommit(set[2])

	resp, file, err = ind.Query(context.Background(), &pb.ReadRequest{Offset: 210, Block: true})
	require.Equal(t, &pb.ReadResponse{
		Offset:    210,
		WriteHead: 250,
		Fragment:  &pb.Fragment{Begin: 200, End: 250},
	}, resp)
	require.Equal(t, os.Stdin, file)
	require.NoError(t, err)
}

func TestRemoteReplacesLocal(t *testing.T) {
	var ind = NewIndex(context.Background())

	var set = buildSet(t, 100, 200)
	set[0].File = os.Stdin
	ind.SpoolCommit(set[0])

	// Precondition: local fragment is queryable.
	var resp, file, err = ind.Query(context.Background(), &pb.ReadRequest{Offset: 110, Block: true})
	require.Equal(t, &pb.ReadResponse{
		Offset:    110,
		WriteHead: 200,
		Fragment:  &pb.Fragment{Begin: 100, End: 200},
	}, resp)
	require.Equal(t, os.Stdin, file)
	require.NoError(t, err)

	// Update remote to cover the same span with more fragments. CoverSet seeks to
	// return the longest overlapping fragment, but as we've removed local
	// fragments covered by remote ones, we should see remote fragments only.
	set = buildSet(t, 100, 150, 150, 200)
	ind.ReplaceRemote(set)

	resp, file, err = ind.Query(context.Background(), &pb.ReadRequest{Offset: 110, Block: true})
	require.Equal(t, &pb.ReadResponse{
		Offset:    110,
		WriteHead: 200,
		Fragment:  &pb.Fragment{Begin: 100, End: 150},
	}, resp)
	require.Nil(t, file)
	require.NoError(t, err)
}

func TestQueryAtHead(t *testing.T) {
	var ind = NewIndex(context.Background())
	ind.SpoolCommit(buildSet(t, 100, 200)[0])

	var resp, _, err = ind.Query(context.Background(), &pb.ReadRequest{Offset: -1, Block: false})
	require.Equal(t, &pb.ReadResponse{
		Status:    pb.Status_OFFSET_NOT_YET_AVAILABLE,
		Offset:    200,
		WriteHead: 200,
	}, resp)
	require.NoError(t, err)

	go ind.SpoolCommit(buildSet(t, 200, 250)[0])

	resp, _, err = ind.Query(context.Background(), &pb.ReadRequest{Offset: 200, Block: true})
	require.Equal(t, &pb.ReadResponse{
		Offset:    200,
		WriteHead: 250,
		Fragment:  &pb.Fragment{Begin: 200, End: 250},
	}, resp)
	require.NoError(t, err)
}

func TestQueryAtMissingByteZero(t *testing.T) {
	var ind = NewIndex(context.Background())
	var now = time.Now().Unix()

	// Establish local fragment fixtures.
	var set = buildSet(t, 100, 200, 200, 300)
	ind.SpoolCommit(set[0])

	// A read from byte zero cannot proceed.
	var resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 0, Block: false})
	require.Equal(t, pb.Status_OFFSET_NOT_YET_AVAILABLE, resp.Status)

	// Set ModTime, marking the fragment as very recently persisted.
	// A read from byte zero now skips forward.
	set[0].ModTime = now
	ind.ReplaceRemote(set)

	resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 0, Block: false})
	require.Equal(t, &pb.ReadResponse{
		Offset:    100,
		WriteHead: 300,
		Fragment:  &pb.Fragment{Begin: 100, End: 200, ModTime: now},
	}, resp)
}

func TestQueryAtMissingMiddle(t *testing.T) {
	var ind = NewIndex(context.Background())
	var baseTime = time.Unix(1500000000, 0)

	// Fix |timeNow| to |baseTime|.
	defer func() { timeNow = time.Now }()
	timeNow = func() time.Time { return baseTime }

	// Establish fixture with zero'd Fragment ModTimes.
	var set = buildSet(t, 100, 200, 300, 400)
	ind.SpoolCommit(set[0])
	ind.SpoolCommit(set[1])

	// Expect before and after the missing span are queryable, but the missing middle is not available.
	var resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 110, Block: false})
	require.Equal(t, pb.Status_OK, resp.Status)
	resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 210, Block: false})
	require.Equal(t, pb.Status_OFFSET_NOT_YET_AVAILABLE, resp.Status)
	resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 310, Block: false})
	require.Equal(t, pb.Status_OK, resp.Status)

	// Update ModTime to |baseTime|. Queries still fail (as we haven't passed the time horizon).
	set[0].ModTime, set[1].ModTime = baseTime.Unix(), baseTime.Unix()
	ind.ReplaceRemote(set)

	resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 210, Block: false})
	require.Equal(t, pb.Status_OFFSET_NOT_YET_AVAILABLE, resp.Status)

	// Perform a blocking query, and arrange for a satisfying Fragment to be added.
	// Expect it's returned.
	go ind.SpoolCommit(buildSet(t, 200, 250)[0])

	resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 210, Block: true})
	require.Equal(t, &pb.ReadResponse{
		Offset:    210,
		WriteHead: 400,
		Fragment:  &pb.Fragment{Begin: 200, End: 250},
	}, resp)

	// Perform a blocking query at the present time, and asynchronously tick
	// time forward and wake the read with an unrelated Fragment update (eg,
	// due to a local commit or remote store refresh). Expect the returned read
	// jumps forward to the next Fragment.
	go func() {
		ind.mu.Lock() // Synchronize |timeNow| access with Query.
		timeNow = func() time.Time { return baseTime.Add(offsetJumpAgeThreshold + time.Second) }
		ind.mu.Unlock()
		ind.SpoolCommit(buildSet(t, 0, 1)[0]) // Wake query, if it's blocked.
	}()

	resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 250, Block: true})
	require.Equal(t, &pb.ReadResponse{
		Offset:    300,
		WriteHead: 400,
		Fragment:  &pb.Fragment{Begin: 300, End: 400, ModTime: baseTime.Unix()},
	}, resp)

	// As the time horizon has been reached, non-blocking reads also offset jump immediately.
	resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 250, Block: false})
	require.Equal(t, pb.Status_OK, resp.Status)
}

func TestQueryWithBeginModTimeConstraint(t *testing.T) {
	const beginTime int64 = 1500000000

	var set = buildSet(t, 100, 200, 200, 300, 300, 400, 400, 500, 500, 600)
	set[0].ModTime = beginTime - 10  // 100-200 not matched.
	set[1].ModTime = beginTime       // 200-300 matched
	set[2].ModTime = beginTime - 1   // 300-400 not matched.
	set[3].ModTime = beginTime + 100 // 400-500 matched.

	var ind = NewIndex(context.Background())
	ind.ReplaceRemote(set[:4])
	ind.SpoolCommit(set[4]) // 500-600 has no ModTime.

	for _, tc := range []struct {
		offset, ind int64
	}{
		{50, 1},
		{150, 1},
		{200, 1},
		{350, 3},
		{400, 3},
		{500, 4},
	} {
		var resp, _, _ = ind.Query(context.Background(),
			&pb.ReadRequest{Offset: tc.offset, BeginModTime: beginTime})
		require.Equal(t, &pb.ReadResponse{
			Offset:    set[tc.ind].Begin,
			WriteHead: 600,
			Fragment: &pb.Fragment{
				Begin:   set[tc.ind].Begin,
				End:     set[tc.ind].End,
				ModTime: set[tc.ind].ModTime,
			},
		}, resp)
	}
}

func TestBlockedContextCancelled(t *testing.T) {
	var indCtx, indCancel = context.WithCancel(context.Background())
	var reqCtx, reqCancel = context.WithCancel(context.Background())

	var ind = NewIndex(indCtx)
	ind.SpoolCommit(buildSet(t, 100, 200)[0])

	// Cancel the request context. Expect the query returns immediately.
	go reqCancel()

	var resp, _, err = ind.Query(reqCtx, &pb.ReadRequest{Offset: -1, Block: true})
	require.Nil(t, resp)
	require.Equal(t, context.Canceled, err)

	// Cancel the Index's context. Same deal.
	reqCtx = context.Background()
	go indCancel()

	resp, _, err = ind.Query(reqCtx, &pb.ReadRequest{Offset: -1, Block: true})
	require.Nil(t, resp)
	require.Equal(t, context.Canceled, err)
}

func TestWalkStoresAndURLSigning(t *testing.T) {
	stores.RegisterProviders(map[string]stores.Constructor{
		"s3": func(ep *url.URL) (stores.Store, error) {

			var ms = stores.NewMemoryStore(ep)

			switch ep.Host {
			case "one":
				ms.Content = map[string][]byte{
					"a/journal/0000000000000000-0000000000000111-0000000000000000000000000000000000000111":     {},
					"a/journal/0000000000000111-0000000000000222-0000000000000000000000000000000000000222.raw": {},
					"a/journal/0000000000000222-0000000000000255-0000000000000000000000000000000000000333.sz":  {}, // Covered.
				}
			case "two":
				ms.Content = map[string][]byte{
					"a/journal/0000000000000222-0000000000000333-0000000000000000000000000000000000000444.gz": {},
					"a/journal/0000000000000444-0000000000000555-0000000000000000000000000000000000000555.gz": {},
				}
			}
			return ms, nil
		},
	})

	var ctx = context.Background()
	var ind = NewIndex(ctx)
	var set CoverSet
	var err error

	set, err = WalkAllStores(ctx, "a/journal", []pb.FragmentStore{
		pb.FragmentStore("s3://empty/"),
	})
	require.NoError(t, err)
	require.Equal(t, CoverSet(nil), set)

	// Gather fixture Fragments from "one" store.
	set, err = WalkAllStores(ctx, "a/journal", []pb.FragmentStore{
		pb.FragmentStore("s3://one/"),
	})
	require.NoError(t, err)
	ind.ReplaceRemote(set)

	// Expect first remote load has completed.
	<-ind.FirstRefreshCh()

	require.Len(t, ind.set, 3)
	var bo, eo, _ = ind.Summary()
	require.Equal(t, int64(0x0), bo)
	require.Equal(t, int64(0x255), eo)

	// Expect "one" provides Fragment 222-255.
	var resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 0x223})
	require.Equal(t, pb.Status_OK, resp.Status)
	require.Equal(t,
		"memory://one/a/journal/0000000000000222-0000000000000255-0000000000000000000000000000000000000333.sz",
		resp.FragmentUrl)

	set, err = WalkAllStores(ctx, "a/journal", []pb.FragmentStore{
		pb.FragmentStore("s3://one/"),
		pb.FragmentStore("s3://two/"),
	})
	require.NoError(t, err)
	ind.ReplaceRemote(set)

	require.Len(t, ind.set, 4) // Combined Fragments are reflected.
	bo, eo, _ = ind.Summary()
	require.Equal(t, int64(0x0), bo)
	require.Equal(t, int64(0x555), eo)

	// Expect "two" now provides Fragment 222-333.
	resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 0x223})
	require.Equal(t, pb.Status_OK, resp.Status)
	require.Equal(t,
		"memory://two/a/journal/0000000000000222-0000000000000333-0000000000000000000000000000000000000444.gz",
		resp.FragmentUrl)
}

func TestInspectCases(t *testing.T) {
	// Case: request context is cancelled before refresh.
	var ind = NewIndex(context.Background())
	var ctx, cancel = context.WithCancel(context.Background())
	cancel()

	require.Equal(t, context.Canceled, ind.Inspect(ctx, func(CoverSet) error {
		panic("not called")
	}))

	// Case: index context is cancelled before refresh.
	ctx, cancel = context.WithCancel(context.Background())
	ind = NewIndex(ctx)
	cancel()

	require.Equal(t, context.Canceled, ind.Inspect(ctx, func(CoverSet) error {
		panic("not called")
	}))

	// Case: index refreshes after call starts.
	var set = buildSet(t, 100, 150, 150, 200, 200, 250)
	ind = NewIndex(context.Background())
	go ind.ReplaceRemote(set)

	require.NoError(t, ind.Inspect(context.Background(), func(s CoverSet) error {
		require.Equal(t, set, s)
		return nil
	}))
}

func buildSet(t *testing.T, offsets ...int64) CoverSet {
	var set CoverSet
	var ok bool

	for i := 0; i < len(offsets); i += 2 {
		var frag = pb.Fragment{Begin: offsets[i], End: offsets[i+1]}

		if set, ok = set.Add(Fragment{Fragment: frag}); !ok {
			t.Fatalf("invalid offset @%d (%d, %d)", i, offsets[i], offsets[i+1])
		}
	}
	return set
}
