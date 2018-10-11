package broker

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/codecs"
	"github.com/LiveRamp/gazette/v2/pkg/fragment"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type ReadSuite struct{}

func (s *ReadSuite) TestStreaming(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	// Make |chunkSize| small so we can test for chunking effects.
	defer func(cs int) { chunkSize = cs }(chunkSize)
	chunkSize = 5

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReplica)
	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id)

	var res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal"})
	var spool, err = acquireSpool(tf.ctx, res.replica, false)
	c.Check(err, gc.IsNil)

	stream, err := broker.MustClient().Read(pb.WithDispatchDefault(tf.ctx),
		&pb.ReadRequest{
			Journal:      "a/journal",
			Offset:       0,
			Block:        true,
			DoNotProxy:   true,
			MetadataOnly: false,
		})
	c.Assert(err, gc.IsNil)
	c.Check(stream.CloseSend(), gc.IsNil)

	spool.MustApply(&pb.ReplicateRequest{Content: []byte("foobarbaz")})
	spool.MustApply(&pb.ReplicateRequest{Proposal: boxFragment(spool.Next())})

	expectReadResponse(c, stream, pb.ReadResponse{
		Status:    pb.Status_OK,
		Header:    &res.Header,
		Offset:    0,
		WriteHead: 9,
		Fragment: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              9,
			Sum:              pb.SHA1SumOf("foobarbaz"),
			CompressionCodec: pb.CompressionCodec_NONE,
		},
	})
	expectReadResponse(c, stream, pb.ReadResponse{
		Status:  pb.Status_OK,
		Offset:  0,
		Content: []byte("fooba"),
	})
	expectReadResponse(c, stream, pb.ReadResponse{
		Status:  pb.Status_OK,
		Offset:  5,
		Content: []byte("rbaz"),
	})

	// Commit more content. Expect the committed Fragment metadata is sent,
	// along with new commit content.
	spool.MustApply(&pb.ReplicateRequest{Content: []byte("bing")})
	spool.MustApply(&pb.ReplicateRequest{Proposal: boxFragment(spool.Next())})

	expectReadResponse(c, stream, pb.ReadResponse{
		Status:    pb.Status_OK,
		Offset:    9,
		WriteHead: 13,
		Fragment: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              13,
			Sum:              pb.SHA1SumOf("foobarbazbing"),
			CompressionCodec: pb.CompressionCodec_NONE,
		},
	})
	expectReadResponse(c, stream, pb.ReadResponse{
		Status:  pb.Status_OK,
		Offset:  9,
		Content: []byte("bing"),
	})

	cleanup()
	_, err = stream.Recv()
	c.Check(err, gc.ErrorMatches, `rpc error: code = Canceled .*`)
}

func (s *ReadSuite) TestMetadataAndNonBlocking(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReplica)
	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id)

	var res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal"})
	var spool, err = acquireSpool(tf.ctx, res.replica, false)
	c.Check(err, gc.IsNil)

	spool.MustApply(&pb.ReplicateRequest{Content: []byte("feedbeef")})
	spool.MustApply(&pb.ReplicateRequest{Proposal: boxFragment(spool.Next())})

	var ctx = pb.WithDispatchDefault(tf.ctx)
	stream, err := broker.MustClient().Read(ctx, &pb.ReadRequest{
		Journal:      "a/journal",
		Offset:       3,
		Block:        false,
		MetadataOnly: false,
	})
	c.Assert(err, gc.IsNil)
	c.Check(stream.CloseSend(), gc.IsNil)

	expectReadResponse(c, stream, pb.ReadResponse{
		Status:    pb.Status_OK,
		Header:    &res.Header,
		Offset:    3,
		WriteHead: 8,
		Fragment: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              8,
			Sum:              pb.SHA1SumOf("feedbeef"),
			CompressionCodec: pb.CompressionCodec_NONE,
		},
	})
	expectReadResponse(c, stream, pb.ReadResponse{
		Status:  pb.Status_OK,
		Offset:  3,
		Content: []byte("dbeef"),
	})
	expectReadResponse(c, stream, pb.ReadResponse{
		Status:    pb.Status_OFFSET_NOT_YET_AVAILABLE,
		Offset:    8,
		WriteHead: 8,
	})

	_, err = stream.Recv()
	c.Check(err, gc.Equals, io.EOF)

	// Now, issue a blocking metadata-only request.
	stream, err = broker.MustClient().Read(ctx, &pb.ReadRequest{
		Journal:      "a/journal",
		Offset:       8,
		Block:        true,
		MetadataOnly: true,
	})
	c.Assert(err, gc.IsNil)
	c.Check(stream.CloseSend(), gc.IsNil)

	// Commit more content, unblocking our metadata request.
	spool.MustApply(&pb.ReplicateRequest{Content: []byte("bing")})
	spool.MustApply(&pb.ReplicateRequest{Proposal: boxFragment(spool.Next())})

	expectReadResponse(c, stream, pb.ReadResponse{
		Status:    pb.Status_OK,
		Header:    &res.Header,
		Offset:    8,
		WriteHead: 12,
		Fragment: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              12,
			Sum:              pb.SHA1SumOf("feedbeefbing"),
			CompressionCodec: pb.CompressionCodec_NONE,
		},
	})

	// Expect no data is sent, and the stream is closed.
	_, err = stream.Recv()
	c.Check(err, gc.Equals, io.EOF)
}

func (s *ReadSuite) TestProxyCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReplica)
	var peer = newMockBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})
	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 1}, peer.id)

	var res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal", mayProxy: true})
	var ctx = pb.WithDispatchDefault(tf.ctx)

	// Case: successfully proxies from peer.
	var req = &pb.ReadRequest{
		Journal:      "a/journal",
		Offset:       0,
		Block:        true,
		DoNotProxy:   false,
		MetadataOnly: false,
	}
	var stream, _ = broker.MustClient().Read(ctx, req)

	// Expect initial request is proxied to the peer, with attached Header, followed by client EOF.
	req.Header = &res.Header
	c.Check(<-peer.ReadReqCh, gc.DeepEquals, req)

	peer.ReadRespCh <- &pb.ReadResponse{Offset: 1234}
	peer.ReadRespCh <- &pb.ReadResponse{Offset: 5678}
	peer.ErrCh <- nil

	expectReadResponse(c, stream, pb.ReadResponse{Offset: 1234})
	expectReadResponse(c, stream, pb.ReadResponse{Offset: 5678})

	var _, err = stream.Recv()
	c.Check(err, gc.Equals, io.EOF)

	// Case: proxy is not allowed.
	req = &pb.ReadRequest{
		Journal:    "a/journal",
		Offset:     0,
		DoNotProxy: true,
	}
	stream, _ = broker.MustClient().Read(ctx, req)

	expectReadResponse(c, stream, pb.ReadResponse{
		Status: pb.Status_NOT_JOURNAL_BROKER,
		Header: boxHeaderProcessID(res.Header, broker.id),
	})

	_, err = stream.Recv()
	c.Check(err, gc.Equals, io.EOF)

	// Case: remote broker returns an error.
	req = &pb.ReadRequest{
		Journal: "a/journal",
		Offset:  0,
	}
	stream, _ = broker.MustClient().Read(ctx, req)

	// Peer reads request, and returns an error.
	<-peer.ReadReqCh
	peer.ErrCh <- errors.New("some kind of error")

	_, err = stream.Recv()
	c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = some kind of error`)
}

func (s *ReadSuite) TestRemoteFragmentCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReplica)
	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id)

	// Create a remote fragment fixture with journal content.
	var frag, tmpDir = buildRemoteFragmentFixture(c)

	defer func() { c.Check(os.RemoveAll(tmpDir), gc.IsNil) }()
	defer func(s string) { fragment.FileSystemStoreRoot = s }(fragment.FileSystemStoreRoot)
	fragment.FileSystemStoreRoot = tmpDir

	// Resolve, and update the replica index to reflect the remote fragment fixture.
	var res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal"})
	res.replica.index.ReplaceRemote(fragment.CoverSet{fragment.Fragment{Fragment: frag}})

	// Case: non-blocking read which is permitted to proxy. Expect the remote
	// fragment is decompressed and seek'd to the desired offset.
	stream, err := broker.MustClient().Read(pb.WithDispatchDefault(tf.ctx),
		&pb.ReadRequest{
			Journal:      "a/journal",
			Offset:       100,
			Block:        false,
			DoNotProxy:   false,
			MetadataOnly: false,
		})
	c.Assert(err, gc.IsNil)
	c.Check(stream.CloseSend(), gc.IsNil)

	expectReadResponse(c, stream, pb.ReadResponse{
		Status:      pb.Status_OK,
		Header:      &res.Header,
		Offset:      100,
		WriteHead:   120,
		Fragment:    &frag,
		FragmentUrl: "file:///" + frag.ContentPath(),
	})
	expectReadResponse(c, stream, pb.ReadResponse{
		Status:  pb.Status_OK,
		Offset:  100,
		Content: []byte("remote fragment data"),
	})
	expectReadResponse(c, stream, pb.ReadResponse{
		Status:    pb.Status_OFFSET_NOT_YET_AVAILABLE,
		Offset:    120,
		WriteHead: 120,
	})
	_, err = stream.Recv()
	c.Check(err, gc.Equals, io.EOF)

	// Case: non-blocking read which is not permitted to proxy. Remote fragment is not read.
	stream, err = broker.MustClient().Read(pb.WithDispatchDefault(tf.ctx),
		&pb.ReadRequest{
			Journal:      "a/journal",
			Offset:       100,
			Block:        false,
			DoNotProxy:   true,
			MetadataOnly: false,
		})
	c.Assert(err, gc.IsNil)
	c.Check(stream.CloseSend(), gc.IsNil)

	expectReadResponse(c, stream, pb.ReadResponse{
		Status:      pb.Status_OK,
		Header:      &res.Header,
		Offset:      100,
		WriteHead:   120,
		Fragment:    &frag,
		FragmentUrl: "file:///" + frag.ContentPath(),
	})
	_, err = stream.Recv()
	c.Check(err, gc.Equals, io.EOF)
}

func buildRemoteFragmentFixture(c *gc.C) (frag pb.Fragment, dir string) {
	const data = "XXXXXremote fragment data"

	var err error
	dir, err = ioutil.TempDir("", "BrokerSuite")
	c.Assert(err, gc.IsNil)

	frag = pb.Fragment{
		Journal:          "a/journal",
		Begin:            95,
		End:              120,
		Sum:              pb.SHA1SumOf(data),
		CompressionCodec: pb.CompressionCodec_SNAPPY,
		BackingStore:     pb.FragmentStore("file:///"),
		ModTime:          time.Unix(1234567, 0),
	}

	var path = filepath.Join(dir, frag.ContentPath())
	c.Assert(os.MkdirAll(filepath.Dir(path), 0700), gc.IsNil)

	file, err := os.Create(path)
	c.Assert(err, gc.IsNil)

	comp, err := codecs.NewCodecWriter(file, pb.CompressionCodec_SNAPPY)
	c.Assert(err, gc.IsNil)
	_, err = comp.Write([]byte(data))
	c.Assert(err, gc.IsNil)
	c.Assert(comp.Close(), gc.IsNil)
	c.Assert(file.Close(), gc.IsNil)
	return
}

func boxFragment(f pb.Fragment) *pb.Fragment { return &f }

func expectReadResponse(c *gc.C, stream pb.Journal_ReadClient, expect pb.ReadResponse) {
	var resp, err = stream.Recv()
	c.Check(err, gc.IsNil)

	// time.Time cannot be compared with reflect.DeepEqual. Check separately,
	// without modifying Fragment instance that |expect| points to.
	if expect.Fragment != nil {
		var clone = *expect.Fragment
		expect.Fragment = &clone

		var respModTime time.Time
		if resp.Fragment != nil {
			respModTime = resp.Fragment.ModTime
		}
		c.Check(expect.Fragment.ModTime.Equal(respModTime), gc.Equals, true)
		resp.Fragment.ModTime, expect.Fragment.ModTime = time.Time{}, time.Time{}
	}
	c.Check(*resp, gc.DeepEquals, expect)
}

var _ = gc.Suite(&ReadSuite{})
