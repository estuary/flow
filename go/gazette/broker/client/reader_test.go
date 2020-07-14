package client

import (
	"bufio"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	gc "github.com/go-check/check"
	"go.gazette.dev/core/broker/codecs"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/teststub"
)

type ReaderSuite struct{}

func (s *ReaderSuite) TestOpenFragmentURLCases(c *gc.C) {
	var frag, url, dir, cleanup = buildFragmentFixture(c)
	defer cleanup()
	defer InstallFileTransport(dir)()

	var ctx = context.Background()

	// Case: read entire fragment.
	var rc, err = OpenFragmentURL(ctx, frag, frag.Begin, url)
	c.Check(err, gc.IsNil)

	b, err := ioutil.ReadAll(rc)
	c.Check(err, gc.IsNil)
	c.Check(string(b), gc.Equals, "XXXXXhello, world!!!")
	c.Check(rc.Offset, gc.Equals, rc.Fragment.End)
	c.Check(rc.Close(), gc.IsNil)

	// Case: read a portion of the fragment.
	rc, err = OpenFragmentURL(ctx, frag, frag.Begin+5, url)
	c.Check(err, gc.IsNil)
	c.Check(rc.Offset, gc.Equals, rc.Fragment.Begin+5)

	b, err = ioutil.ReadAll(rc)
	c.Check(err, gc.IsNil)
	c.Check(string(b), gc.Equals, "hello, world!!!")
	c.Check(rc.Offset, gc.Equals, rc.Fragment.End)
	c.Check(rc.Close(), gc.IsNil)

	// Case: stream ends before Fragment.End.
	frag.End += 1
	rc, err = OpenFragmentURL(ctx, frag, frag.Begin+5, url)
	c.Check(err, gc.IsNil)

	b, err = ioutil.ReadAll(rc)
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)
	c.Check(string(b), gc.Equals, "hello, world!!!")
	c.Check(rc.Close(), gc.IsNil)

	// Case: stream continues after Fragment.End.
	frag.End -= 4
	rc, err = OpenFragmentURL(ctx, frag, frag.Begin+5, url)
	c.Check(err, gc.IsNil)

	b, err = ioutil.ReadAll(rc)
	c.Check(err, gc.Equals, ErrDidNotReadExpectedEOF)
	c.Check(string(b), gc.Equals, "hello, world")
	c.Check(rc.Close(), gc.IsNil)

	// Case: doesn't exist.
	rc, err = OpenFragmentURL(ctx, frag, frag.Begin, url+"does-not-exist")
	c.Check(err, gc.ErrorMatches, `!OK fetching \(404 Not Found, "file:///.*\)`)

	// Case: decompression fails.
	frag.CompressionCodec = pb.CompressionCodec_SNAPPY
	rc, err = OpenFragmentURL(ctx, frag, frag.Begin+5, url)
	c.Check(err, gc.ErrorMatches, `snappy: corrupt input`)
}

func (s *ReaderSuite) TestReaderCases(c *gc.C) {
	var frag, url, dir, cleanup = buildFragmentFixture(c)
	defer cleanup()
	defer InstallFileTransport(dir)()

	var broker = teststub.NewBroker(c)
	defer broker.Cleanup()

	var ctx = context.Background()
	var rjc = pb.NewRoutedJournalClient(broker.Client(), NewRouteCache(2, time.Hour))

	go serveReadFixtures(c, broker,
		// Case 1: fixture returns fragment metadata & URL, then EOF.
		readFixture{fragment: &frag, fragmentUrl: url},
		// Case 2: fragment metadata but no URL, at read offset -1.
		readFixture{fragment: &frag, offset: 110},
		// Case 3: wrong broker (and it's not instructed to proxy).
		readFixture{status: pb.Status_NOT_JOURNAL_BROKER},
		// Case 4: read some content, and then OFFSET_NOT_YET_AVAILABLE.
		readFixture{content: "prior content", status: pb.Status_OFFSET_NOT_YET_AVAILABLE},
		// Case 5: immediate OFFSET_NOT_YET_AVAILABLE.
		readFixture{status: pb.Status_OFFSET_NOT_YET_AVAILABLE},
		// Case 6: Status not explicitly mapped to an error.
		readFixture{status: pb.Status_INSUFFICIENT_JOURNAL_BROKERS},
		// Case 7: streamed content, with response offset jump 105 => 110.
		readFixture{content: "foo bar baz bing", offset: 110},
		// Case 8: partial streamed content, then a non-EOF error.
		readFixture{content: "partial read", err: errors.New("potato")},
		// Case 9: invalid offset jump 105 => 100.
		readFixture{content: "invalid", offset: 100},
		// Case 10: ReadResponse validation fails (invalid Fragment).
		readFixture{fragment: &pb.Fragment{End: 1, Begin: 0}},
		// Case 11: fixture returns fragment metadata & URL, then EOF.
		readFixture{fragment: &frag, fragmentUrl: url},
	)

	// Case 1: fragment metadata & URL.
	var r = NewReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal", Offset: 105})

	// Initial read is zero-length, but updates Response with FragmentUrl.
	var n, err = r.Read(nil)
	c.Check(n, gc.Equals, 0)
	c.Check(err, gc.IsNil)
	c.Check(r.Response.Fragment, gc.NotNil)

	// Expect next read opens fragment URL fixture reads directly.
	b, err := ioutil.ReadAll(r)
	c.Check(string(b), gc.Equals, "hello, world!!!")
	c.Check(err, gc.IsNil)

	// And that the Route is cached.
	c.Check(rjc.Route(ctx, "a/journal"), gc.DeepEquals, pb.Route{
		Members:   []pb.ProcessSpec_ID{{Zone: "a", Suffix: "broker"}},
		Endpoints: []pb.Endpoint{broker.Endpoint()},
		Primary:   0,
	})

	// Case 2: fragment metadata, no URL and the offset is jumped.
	r = NewReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal", Offset: -1})
	n, err = r.Read(b)

	c.Check(n, gc.Equals, 0)
	c.Check(err, gc.Equals, ErrOffsetJump)
	c.Check(r.Request.Offset, gc.Equals, int64(110)) // Updated from Response.
	c.Check(r.Response.Fragment, gc.DeepEquals, &frag)

	n, err = r.Read(b)
	c.Check(n, gc.Equals, 0)
	c.Check(err, gc.Equals, io.EOF)

	// Case 3: NOT_JOURNAL_BROKER => ErrNotJournalBroker.
	r = NewReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal", Offset: 105})
	n, err = r.Read(b)

	c.Check(n, gc.Equals, 0)
	c.Check(err, gc.Equals, ErrNotJournalBroker)

	// Case 4: read content, then OFFSET_NOT_YET_AVAILABLE => ErrOffsetNotYetAvailable.
	r = NewReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal", Offset: 105})
	b, err = ioutil.ReadAll(r)

	c.Check(string(b), gc.Equals, "prior content")
	c.Check(err, gc.Equals, ErrOffsetNotYetAvailable)

	// Case 5: immediate OFFSET_NOT_YET_AVAILABLE => ErrOffsetNotYetAvailable.
	r = NewReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal", Offset: 105})
	n, err = r.Read(b)

	c.Check(n, gc.Equals, 0)
	c.Check(err, gc.Equals, ErrOffsetNotYetAvailable)

	// Case 6: expect other statuses are dynamically mapped to error.
	r = NewReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal", Offset: 105})
	n, err = r.Read(b)

	c.Check(n, gc.Equals, 0)
	c.Check(err, gc.ErrorMatches, pb.Status_INSUFFICIENT_JOURNAL_BROKERS.String())

	// Case 7: streamed content with multiple small reads, offset jump & tracked Route update
	r = NewReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal", Offset: 105})

	n, err = r.Read(nil)
	c.Check(n, gc.Equals, 0)
	c.Check(err, gc.Equals, ErrOffsetJump)

	// Expect offset was skipped forward to Response.Offset.
	c.Check(r.Request.Offset, gc.Equals, int64(110))

	// Note the fixture content is split across two ReadResponses.
	// Consume both messages across four small Reads.
	b = make([]byte, 6)
	n, err = r.Read(b[:])
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 0)
	c.Check(r.Request.Offset, gc.Equals, int64(110))

	n, err = r.Read(b[:])
	c.Check(err, gc.IsNil)
	c.Check(string(b[:n]), gc.Equals, "foo ba")
	c.Check(r.Request.Offset, gc.Equals, int64(116))

	n, err = r.Read(b[:])
	c.Check(err, gc.IsNil)
	c.Check(string(b[:n]), gc.Equals, "r ")
	c.Check(r.Request.Offset, gc.Equals, int64(118))

	n, err = r.Read(b[:]) // Read next message.
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 0)
	c.Check(r.Request.Offset, gc.Equals, int64(118))

	n, err = r.Read(b[:])
	c.Check(err, gc.IsNil)
	c.Check(string(b[:n]), gc.Equals, "baz bi")
	c.Check(r.Request.Offset, gc.Equals, int64(124))

	n, err = r.Read(b[:])
	c.Check(err, gc.IsNil)
	c.Check(string(b[:n]), gc.Equals, "ng")
	c.Check(r.Request.Offset, gc.Equals, int64(126))

	// Expect that fragment metadata was carried across multiple content chunks.
	c.Check(r.Response.Fragment, gc.DeepEquals, &pb.Fragment{
		Journal:          "a/journal",
		End:              1024,
		CompressionCodec: pb.CompressionCodec_NONE,
	})
	c.Check(r.Response.WriteHead, gc.Equals, int64(1024))

	// Case 8: content followed by a non-EOF error.
	r = NewReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal", Offset: 105})

	b, err = ioutil.ReadAll(r)
	c.Check(string(b), gc.Equals, "partial read")
	c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = potato`)

	// Case 9: invalid offset jump 105 => 100.
	r = NewReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal", Offset: 105})

	b, err = ioutil.ReadAll(r)
	c.Check(b, gc.HasLen, 0)
	c.Check(err, gc.ErrorMatches, `invalid ReadResponse offset \(100; expected >= 105\)`)

	// Case 10: ReadResponse validation fails (invalid fragment).
	r = NewReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal", Offset: 105})

	n, err = r.Read(nil)
	c.Check(n, gc.Equals, 0)
	c.Check(err, gc.ErrorMatches, `ReadResponse.Fragment.Journal: invalid length .*`)

	// Case 11: fragment metadata & URL. Reader EOF's early due to EndOffset.
	r = NewReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal", Offset: 105, EndOffset: 111})

	b, err = ioutil.ReadAll(r)
	c.Check(string(b), gc.Equals, "hello,")
	c.Check(err, gc.IsNil)
}

func (s *ReaderSuite) TestBufferedOffsetAdjustment(c *gc.C) {
	var broker = teststub.NewBroker(c)
	defer broker.Cleanup()

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	go readFixture{content: "foobar\nbaz\n", offset: 100}.serve(c, broker)

	var r = NewReader(context.Background(), rjc, pb.ReadRequest{Journal: "a/journal", Offset: 100})
	var br = bufio.NewReader(r)

	var b, err = br.ReadBytes('\n')
	c.Check(string(b), gc.Equals, "foobar\n")
	c.Check(err, gc.IsNil)

	// Expect the entire input reader was consumed.
	c.Check(r.Request.Offset, gc.Equals, int64(100+7+4))
	// Expect the adjusted mark reflects just the portion read from |br|.
	c.Check(r.AdjustedOffset(br), gc.Equals, int64(100+7))
}

func (s *ReaderSuite) TestReaderSeekCases(c *gc.C) {
	var frag, url, dir, cleanup = buildFragmentFixture(c)
	defer cleanup()
	defer InstallFileTransport(dir)()

	var broker = teststub.NewBroker(c)
	defer broker.Cleanup()

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})

	// Start read fixture which returns fragment metadata & URL, then EOF,
	// causing Reader to directly open the fragment.
	go readFixture{fragment: &frag, fragmentUrl: url, offset: frag.Begin}.serve(c, broker)

	var r = NewReader(context.Background(), rjc, pb.ReadRequest{Journal: "a/journal"})

	// First zero-byte read causes Reader to read the response fixture.
	var n, err = r.Read(nil)
	c.Check(err, gc.Equals, ErrOffsetJump)
	c.Check(n, gc.Equals, 0)
	c.Check(r.Request.Offset, gc.Equals, int64(frag.Begin))
	c.Check(r.Response.FragmentUrl, gc.Matches, "file:///00000.*")

	// Next read drives Reader to open the fragment directly.
	n, err = r.Read(nil)
	c.Check(n, gc.Equals, 0)
	c.Check(err, gc.IsNil)
	c.Check(r.direct, gc.NotNil)
	c.Check(r.Request.Offset, gc.Equals, frag.Begin)

	// Case: seeking forward works, so long as the Fragment covers the seek'd offset.
	offset, err := r.Seek(5, io.SeekCurrent)
	c.Check(offset, gc.Equals, frag.Begin+5)
	c.Check(err, gc.IsNil)

	offset, err = r.Seek(frag.Begin+6, io.SeekStart)
	c.Check(offset, gc.Equals, frag.Begin+6)
	c.Check(err, gc.IsNil)

	// Case: seeking backwards requires a new reader.
	offset, err = r.Seek(-1, io.SeekCurrent)
	c.Check(offset, gc.Equals, frag.Begin+6)
	c.Check(err, gc.Equals, ErrSeekRequiresNewReader)

	// Case: as does seeking beyond the current fragment End.
	offset, err = r.Seek(frag.End, io.SeekStart)
	c.Check(offset, gc.Equals, frag.Begin+6)
	c.Check(err, gc.Equals, ErrSeekRequiresNewReader)
}

func (s *ReaderSuite) TestContextErrorCases(c *gc.C) {
	var broker = teststub.NewBroker(c)
	defer broker.Cleanup()

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})

	// Case 1: context is cancelled.
	var caseCtx, caseCancel = context.WithCancel(context.Background())
	caseCancel()

	var r = NewReader(caseCtx, rjc, pb.ReadRequest{Journal: "a/journal"})
	var n, err = r.Read(nil)

	c.Check(err, gc.Equals, context.Canceled)
	c.Check(n, gc.Equals, 0)

	// Case 2: context reaches deadline.
	caseCtx, _ = context.WithTimeout(context.Background(), time.Microsecond)
	<-caseCtx.Done() // Block until deadline.

	r = NewReader(caseCtx, rjc, pb.ReadRequest{Journal: "a/journal"})
	n, err = r.Read(nil)

	c.Check(err, gc.Equals, context.DeadlineExceeded)
	c.Check(n, gc.Equals, 0)
}

type readFixture struct {
	status pb.Status
	err    error

	// optional fields
	content     string
	offset      int64
	fragment    *pb.Fragment
	fragmentUrl string
}

func (f readFixture) serve(c *gc.C, broker *teststub.Broker) {
	var req = <-broker.ReadReqCh
	c.Check(req.Journal, gc.Equals, pb.Journal("a/journal"))

	// Start with a basic response template which may be customized.
	var resp = pb.ReadResponse{
		Status:    pb.Status_OK,
		Header:    buildHeaderFixture(broker),
		Offset:    req.Offset,
		WriteHead: 1024,
		Fragment: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              1024,
			CompressionCodec: pb.CompressionCodec_NONE,
		},
	}

	if f.status != pb.Status_OK && len(f.content) == 0 {
		resp.Status = f.status
	}
	if f.offset != 0 {
		resp.Offset = f.offset
	}
	if f.fragment != nil {
		resp.Fragment = f.fragment
	}
	if f.fragmentUrl != "" {
		resp.FragmentUrl = f.fragmentUrl
	}

	broker.ReadRespCh <- resp

	if l := len(f.content); l != 0 {
		broker.ReadRespCh <- pb.ReadResponse{Offset: resp.Offset, Content: []byte(f.content[:l/2])}
		broker.ReadRespCh <- pb.ReadResponse{Offset: resp.Offset + int64(l/2), Content: []byte(f.content[l/2:])}

		if f.status != pb.Status_OK {
			broker.ReadRespCh <- pb.ReadResponse{Status: f.status}
		}
	}
	broker.WriteLoopErrCh <- f.err
}

func serveReadFixtures(c *gc.C, broker *teststub.Broker, fixtures ...readFixture) {
	for _, f := range fixtures {
		f.serve(c, broker)
	}
}

func buildFragmentFixture(c *gc.C) (frag pb.Fragment, url string, dir string, cleanup func()) {
	const data = "XXXXXhello, world!!!"

	var err error
	dir, err = ioutil.TempDir("", "ReaderSuite")
	c.Assert(err, gc.IsNil)

	cleanup = func() {
		c.Check(os.RemoveAll(dir), gc.IsNil)
	}

	frag = pb.Fragment{
		Journal:          "a/journal",
		Begin:            100,
		End:              120,
		Sum:              pb.SHA1SumOf(data),
		CompressionCodec: pb.CompressionCodec_GZIP,
		BackingStore:     pb.FragmentStore("file:///"),
	}
	url = string(frag.BackingStore) + frag.ContentName()

	var path = filepath.Join(dir, frag.ContentName())
	file, err := os.Create(path)
	c.Assert(err, gc.IsNil)

	comp, err := codecs.NewCodecWriter(file, pb.CompressionCodec_GZIP)
	c.Assert(err, gc.IsNil)
	_, err = comp.Write([]byte(data))
	c.Assert(err, gc.IsNil)
	c.Assert(comp.Close(), gc.IsNil)
	c.Assert(file.Close(), gc.IsNil)

	return
}

func buildHeaderFixture(ep interface{ Endpoint() pb.Endpoint }) *pb.Header {
	return &pb.Header{
		ProcessId: pb.ProcessSpec_ID{Zone: "a", Suffix: "broker"},
		Route: pb.Route{
			Members:   []pb.ProcessSpec_ID{{Zone: "a", Suffix: "broker"}},
			Endpoints: []pb.Endpoint{ep.Endpoint()},
			Primary:   0,
		},
		Etcd: pb.Header_Etcd{
			ClusterId: 12,
			MemberId:  34,
			Revision:  56,
			RaftTerm:  78,
		},
	}
}

var _ = gc.Suite(&ReaderSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
