package fragment

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type IndexSuite struct{}

func (s *IndexSuite) TestSimpleRemoteAndLocalQueries(c *gc.C) {
	var ind = NewIndex(context.Background())

	var set = buildSet(c, 100, 150, 150, 200, 200, 250)
	ind.ReplaceRemote(set[:2])

	var resp, file, err = ind.Query(context.Background(), &pb.ReadRequest{Offset: 110, Block: true})
	c.Check(resp, gc.DeepEquals, &pb.ReadResponse{
		Offset:    110,
		WriteHead: 200,
		Fragment:  &pb.Fragment{Begin: 100, End: 150},
	})
	c.Check(file, gc.IsNil)
	c.Check(err, gc.IsNil)

	// Add a local fragment with backing file. Expect we can query it.
	set[2].File = os.Stdin
	ind.SpoolCommit(set[2])

	resp, file, err = ind.Query(context.Background(), &pb.ReadRequest{Offset: 210, Block: true})
	c.Check(resp, gc.DeepEquals, &pb.ReadResponse{
		Offset:    210,
		WriteHead: 250,
		Fragment:  &pb.Fragment{Begin: 200, End: 250},
	})
	c.Check(file, gc.Equals, os.Stdin)
	c.Check(err, gc.IsNil)
}

func (s *IndexSuite) TestRemoteReplacesLocal(c *gc.C) {
	var ind = NewIndex(context.Background())

	var set = buildSet(c, 100, 200)
	set[0].File = os.Stdin
	ind.SpoolCommit(set[0])

	// Precondition: local fragment is queryable.
	var resp, file, err = ind.Query(context.Background(), &pb.ReadRequest{Offset: 110, Block: true})
	c.Check(resp, gc.DeepEquals, &pb.ReadResponse{
		Offset:    110,
		WriteHead: 200,
		Fragment:  &pb.Fragment{Begin: 100, End: 200},
	})
	c.Check(file, gc.Equals, os.Stdin)
	c.Check(err, gc.IsNil)

	// Update remote to cover the same span with more fragments. CoverSet seeks to
	// return the longest overlapping fragment, but as we've removed local
	// fragments covered by remote ones, we should see remote fragments only.
	set = buildSet(c, 100, 150, 150, 200)
	ind.ReplaceRemote(set)

	resp, file, err = ind.Query(context.Background(), &pb.ReadRequest{Offset: 110, Block: true})
	c.Check(resp, gc.DeepEquals, &pb.ReadResponse{
		Offset:    110,
		WriteHead: 200,
		Fragment:  &pb.Fragment{Begin: 100, End: 150},
	})
	c.Check(file, gc.IsNil)
	c.Check(err, gc.IsNil)
}

func (s *IndexSuite) TestQueryAtHead(c *gc.C) {
	var ind = NewIndex(context.Background())
	ind.SpoolCommit(buildSet(c, 100, 200)[0])

	var resp, _, err = ind.Query(context.Background(), &pb.ReadRequest{Offset: -1, Block: false})
	c.Check(resp, gc.DeepEquals, &pb.ReadResponse{
		Status:    pb.Status_OFFSET_NOT_YET_AVAILABLE,
		Offset:    200,
		WriteHead: 200,
	})
	c.Check(err, gc.IsNil)

	go ind.SpoolCommit(buildSet(c, 200, 250)[0])

	resp, _, err = ind.Query(context.Background(), &pb.ReadRequest{Offset: 200, Block: true})
	c.Check(resp, gc.DeepEquals, &pb.ReadResponse{
		Offset:    200,
		WriteHead: 250,
		Fragment:  &pb.Fragment{Begin: 200, End: 250},
	})
}

func (s *IndexSuite) TestQueryAtMissingMiddle(c *gc.C) {
	var ind = NewIndex(context.Background())
	var baseTime = time.Unix(1500000000, 0)

	// Fix |timeNow| to |baseTime|.
	defer func() { timeNow = time.Now }()
	timeNow = func() time.Time { return baseTime }

	// Establish fixture with zero'd Fragment ModTimes.
	var set = buildSet(c, 100, 200, 300, 400)
	ind.SpoolCommit(set[0])
	ind.SpoolCommit(set[1])

	// Expect before and after the missing span are queryable, but the missing middle is not available.
	var resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 110, Block: false})
	c.Check(resp.Status, gc.Equals, pb.Status_OK)
	resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 210, Block: false})
	c.Check(resp.Status, gc.Equals, pb.Status_OFFSET_NOT_YET_AVAILABLE)
	resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 310, Block: false})
	c.Check(resp.Status, gc.Equals, pb.Status_OK)

	// Update ModTime to |baseTime|. Queries still fail (as we haven't passed the time horizon).
	set[0].ModTime, set[1].ModTime = baseTime, baseTime
	ind.ReplaceRemote(set)

	resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 210, Block: false})
	c.Check(resp.Status, gc.Equals, pb.Status_OFFSET_NOT_YET_AVAILABLE)

	// Perform a blocking query, and arrange for a satisfying Fragment to be added.
	// Expect it's returned.
	go ind.SpoolCommit(buildSet(c, 200, 250)[0])

	resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 210, Block: true})
	c.Check(resp, gc.DeepEquals, &pb.ReadResponse{
		Offset:    210,
		WriteHead: 400,
		Fragment:  &pb.Fragment{Begin: 200, End: 250},
	})

	// Perform a blocking query at the present time, and asynchronously tick
	// time forward and wake the read with an unrelated Fragment update (eg,
	// due to a local commit or remote store refresh). Expect the returned read
	// jumps forward to the next Fragment.
	go func() {
		ind.mu.Lock() // Synchronize |timeNow| access with Query.
		timeNow = func() time.Time { return baseTime.Add(offsetJumpAgeThreshold + 1) }
		ind.mu.Unlock()

		ind.SpoolCommit(buildSet(c, 400, 420)[0])
	}()

	resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 250, Block: true})
	c.Check(resp, gc.DeepEquals, &pb.ReadResponse{
		Offset:    300,
		WriteHead: 420,
		Fragment:  &pb.Fragment{Begin: 300, End: 400, ModTime: baseTime},
	})

	// As the time horizon has been reached, non-blocking reads also offset jump immediately.
	resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 250, Block: false})
	c.Check(resp.Status, gc.Equals, pb.Status_OK)
}

func (s *IndexSuite) TestBlockedContextCancelled(c *gc.C) {
	var indCtx, indCancel = context.WithCancel(context.Background())
	var reqCtx, reqCancel = context.WithCancel(context.Background())

	var ind = NewIndex(indCtx)
	ind.SpoolCommit(buildSet(c, 100, 200)[0])

	// Cancel the request context. Expect the query returns immediately.
	go reqCancel()

	var resp, _, err = ind.Query(reqCtx, &pb.ReadRequest{Offset: -1, Block: true})
	c.Check(resp, gc.IsNil)
	c.Check(err, gc.Equals, context.Canceled)

	// Cancel the Index's context. Same deal.
	reqCtx, reqCancel = context.WithCancel(context.Background())
	go indCancel()

	resp, _, err = ind.Query(reqCtx, &pb.ReadRequest{Offset: -1, Block: true})
	c.Check(resp, gc.IsNil)
	c.Check(err, gc.Equals, context.Canceled)
}

func (s *IndexSuite) TestWalkStoresAndURLSigning(c *gc.C) {
	var tmpdir, err = ioutil.TempDir("", "IndexSuite.TestWalkStores")
	c.Assert(err, gc.IsNil)

	defer func() { os.RemoveAll(tmpdir) }()
	defer func(s string) { FileSystemStoreRoot = s }(FileSystemStoreRoot)
	FileSystemStoreRoot = tmpdir

	var paths = []string{
		"root/one/a/journal/0000000000000000-0000000000000111-0000000000000000000000000000000000000111",
		"root/one/a/journal/0000000000000111-0000000000000222-0000000000000000000000000000000000000222.raw",
		"root/one/a/journal/0000000000000222-0000000000000255-0000000000000000000000000000000000000333.sz", // Covered.
		"root/two/a/journal/0000000000000222-0000000000000333-0000000000000000000000000000000000000444.gz",
		"root/two/a/journal/0000000000000444-0000000000000555-0000000000000000000000000000000000000555.gz",
	}

	for _, path := range paths {
		path = filepath.Join(tmpdir, filepath.FromSlash(path))
		c.Assert(os.MkdirAll(filepath.Dir(path), 0700), gc.IsNil)
		c.Assert(ioutil.WriteFile(path, []byte("data"), 0600), gc.IsNil)
	}

	var ctx = context.Background()
	var ind = NewIndex(ctx)
	var set CoverSet

	set, err = WalkAllStores(ctx, "a/journal", []pb.FragmentStore{
		pb.FragmentStore("file:///path/does/not/exist/"),
	})
	c.Check(err, gc.NotNil)
	c.Check(set, gc.DeepEquals, CoverSet{})

	// Gather fixture Fragments from "/root/one/" store.
	set, err = WalkAllStores(ctx, "a/journal", []pb.FragmentStore{
		pb.FragmentStore("file:///root/one/"),
	})
	c.Check(err, gc.IsNil)
	ind.ReplaceRemote(set)

	// Expect first remote load has completed.
	c.Check(ind.WaitForFirstRemoteRefresh(context.Background()), gc.IsNil)

	c.Check(ind.set, gc.HasLen, 3)
	c.Check(ind.EndOffset(), gc.Equals, int64(0x255))

	// Expect root/one provides Fragment 222-255.
	var resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 0x223})
	c.Check(resp.Status, gc.Equals, pb.Status_OK)
	c.Check(resp.FragmentUrl, gc.Equals,
		"file:///root/one/a/journal/0000000000000222-0000000000000255-0000000000000000000000000000000000000333.sz")

	set, err = WalkAllStores(ctx, "a/journal", []pb.FragmentStore{
		pb.FragmentStore("file:///root/one/"),
		pb.FragmentStore("file:///root/two/"),
	})
	c.Check(err, gc.IsNil)
	ind.ReplaceRemote(set)

	c.Check(ind.set, gc.HasLen, 4) // Combined Fragments are reflected.
	c.Check(ind.EndOffset(), gc.Equals, int64(0x555))

	// Expect root/two now provides Fragment 222-333.
	resp, _, _ = ind.Query(context.Background(), &pb.ReadRequest{Offset: 0x223})
	c.Check(resp.Status, gc.Equals, pb.Status_OK)
	c.Check(resp.FragmentUrl, gc.Equals,
		"file:///root/two/a/journal/0000000000000222-0000000000000333-0000000000000000000000000000000000000444.gz")
}

func buildSet(c *gc.C, offsets ...int64) CoverSet {
	var set CoverSet
	var ok bool

	for i := 0; i < len(offsets); i += 2 {
		var frag = pb.Fragment{Begin: offsets[i], End: offsets[i+1]}

		if set, ok = set.Add(Fragment{Fragment: frag}); !ok {
			c.Logf("invalid offset @%d (%d, %d)", i, offsets[i], offsets[i+1])
			c.FailNow()
		}
	}
	return set
}

var _ = gc.Suite(&IndexSuite{})
