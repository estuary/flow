package fragment

import (
	"errors"
	"io"
	"io/ioutil"
	"time"

	gc "github.com/go-check/check"
	"go.gazette.dev/core/broker/codecs"
	pb "go.gazette.dev/core/broker/protocol"
)

type SpoolSuite struct{}

func (s *SpoolSuite) TestNextCases(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)

	// Case: Newly-initialized spool.
	c.Check(spool.Next(), gc.DeepEquals, pb.Fragment{
		Journal:          "a/journal",
		CompressionCodec: pb.CompressionCodec_NONE,
	})

	// Case: Zero-length fragment.
	spool.MustApply(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            100,
			End:              100,
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
		Registers: &regEmpty,
	})
	c.Check(spool.Next(), gc.DeepEquals, pb.Fragment{
		Journal:          "a/journal",
		Begin:            100,
		End:              100,
		CompressionCodec: pb.CompressionCodec_SNAPPY,
	})

	// Case: Fragment with applied content, ready to be committed.
	var _, err = spool.Apply(&pb.ReplicateRequest{
		Content:      []byte("some"),
		ContentDelta: 0,
	}, false)
	c.Check(err, gc.IsNil)

	_, err = spool.Apply(&pb.ReplicateRequest{
		Content:      []byte(" content"),
		ContentDelta: 4,
	}, false)
	c.Check(err, gc.IsNil)

	c.Check(spool.Next(), gc.DeepEquals, pb.Fragment{
		Journal:          "a/journal",
		Begin:            100,
		End:              112,
		Sum:              pb.SHA1SumOf("some content"),
		CompressionCodec: pb.CompressionCodec_SNAPPY,
	})
}

func (s *SpoolSuite) TestNoCompression(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	runReplicateSequence(c, &spool, pb.CompressionCodec_NONE, true)

	c.Check(obv.completes, gc.HasLen, 1)
	c.Check(obv.commits, gc.HasLen, 2)

	c.Check(obv.completes[0].compressedFile, gc.IsNil)
	c.Check(obv.completes[0].compressor, gc.IsNil)
	c.Check(obv.completes[0].compressedLength, gc.Equals, int64(0))

	c.Check(contentString(c, obv.completes[0], pb.CompressionCodec_NONE),
		gc.Equals, "an initial write final write")
}

func (s *SpoolSuite) TestCompressionAndPrimary(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	runReplicateSequence(c, &spool, pb.CompressionCodec_GZIP, true)

	c.Check(obv.completes, gc.HasLen, 1)
	c.Check(obv.commits, gc.HasLen, 2)

	c.Check(obv.completes[0].compressedFile, gc.NotNil)
	c.Check(obv.completes[0].compressor, gc.IsNil) // Closed.
	c.Check(obv.completes[0].compressedLength, gc.Not(gc.Equals), int64(0))

	c.Check(contentString(c, obv.completes[0], pb.CompressionCodec_GZIP),
		gc.Equals, "an initial write final write")
}

func (s *SpoolSuite) TestCompressionNotPrimary(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	runReplicateSequence(c, &spool, pb.CompressionCodec_GZIP, false)

	c.Check(obv.completes, gc.HasLen, 1)
	c.Check(obv.commits, gc.HasLen, 2)

	c.Check(obv.completes[0].compressedFile, gc.IsNil)
	c.Check(obv.completes[0].compressor, gc.IsNil)

	c.Check(contentString(c, obv.completes[0], pb.CompressionCodec_NONE),
		gc.Equals, "an initial write final write")

	// Though not compressed incrementally, expect it will compress on demand.
	obv.completes[0].finishCompression()

	c.Check(obv.completes[0].compressedFile, gc.NotNil)
	c.Check(obv.completes[0].compressor, gc.IsNil) // Closed.
	c.Check(obv.completes[0].compressedLength, gc.Not(gc.Equals), int64(0))

	c.Check(contentString(c, obv.completes[0], pb.CompressionCodec_GZIP),
		gc.Equals, "an initial write final write")
}

func (s *SpoolSuite) TestRejectRollBeforeCurrentEnd(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	runReplicateSequence(c, &spool, pb.CompressionCodec_NONE, false)

	// Expect offsets prior to the current End (28) fail.
	var resp, err = spool.Apply(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            17 + 11 - 1,
			End:              17 + 11 - 1,
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Registers: &regBar,
	}, false)

	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{
		Status:    pb.Status_PROPOSAL_MISMATCH,
		Fragment:  &spool.Fragment.Fragment,
		Registers: &regBar,
	})
	c.Check(err, gc.IsNil)

	// Expect offsets beyond the current End succeed.
	resp, err = spool.Apply(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            17 + 11 + 1,
			End:              17 + 11 + 1,
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Registers: &regBaz,
	}, false)

	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{Status: pb.Status_OK})
	c.Check(err, gc.IsNil)
}

func (s *SpoolSuite) TestMoreMismatchCases(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)

	// Setup: |spool| has 4 committed bytes.
	spool.MustApply(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			CompressionCodec: pb.CompressionCodec_GZIP,
		},
		Registers: &regEmpty,
	})
	spool.MustApply(&pb.ReplicateRequest{Content: []byte("abcd")})
	var proposal = spool.Next()
	spool.MustApply(newProposal(proposal, regFoo))

	// Case: Re-commit of exactly the current proposal succeeds.
	spool.MustApply(newProposal(proposal, regFoo))

	// Case: However if registers do not match, it's refused.
	var resp, _ = spool.Apply(newProposal(proposal, regBar), false)
	c.Check(resp.Status, gc.Equals, pb.Status_PROPOSAL_MISMATCH)
	c.Check(resp.Registers, gc.DeepEquals, &regFoo)

	// Apply 4 uncommitted bytes
	spool.MustApply(&pb.ReplicateRequest{Content: []byte("efgh")})

	// Case: Attempt a roll-back, but with incorrect registers. It's refused.
	resp, _ = spool.Apply(newProposal(spool.Fragment.Fragment, regBar), false)
	c.Check(resp.Status, gc.Equals, pb.Status_PROPOSAL_MISMATCH)

	// Case: Apply a mismatched proposal which is within current spool bounds.
	// Expect the spool doesn't roll forward.
	proposal = spool.Next()
	proposal.Begin = 1 // Cause proposal to mismatch.

	resp, _ = spool.Apply(newProposal(proposal, regBar), false)
	c.Check(resp.Status, gc.Equals, pb.Status_PROPOSAL_MISMATCH)
	c.Check(spool.Begin, gc.Equals, int64(0))
	c.Check(spool.Registers, gc.DeepEquals, regFoo)
	c.Check(obv.completes, gc.HasLen, 0)

	// Case: Again, but this time the proposal is beyond current bounds.
	// Expect a MISMATCH is still returned, but the spool rolls forward
	// and takes the provided registers.
	proposal.End = 9

	resp, _ = spool.Apply(newProposal(proposal, regBar), false)
	c.Check(resp.Status, gc.Equals, pb.Status_PROPOSAL_MISMATCH)
	c.Check(spool.Begin, gc.Equals, int64(9))
	c.Check(spool.End, gc.Equals, int64(9))
	c.Check(spool.delta, gc.Equals, int64(0))
	c.Check(spool.Registers, gc.DeepEquals, regBar)

	c.Check(obv.completes, gc.HasLen, 1)
	c.Check(contentString(c, obv.completes[0], pb.CompressionCodec_GZIP),
		gc.Equals, "abcd")

	// Case 3: Spool is empty, and sees a proposal within current bounds.
	// Expect it remains unchanged.
	proposal.End = 8

	resp, _ = spool.Apply(newProposal(proposal, regBaz), false)
	c.Check(resp.Status, gc.Equals, pb.Status_PROPOSAL_MISMATCH)
	c.Check(spool.End, gc.Equals, int64(9))
	c.Check(spool.Registers, gc.DeepEquals, regBar)

	// Case 4: Spool is empty, but rolls due to a proposal beyond current bounds.
	// It's not treated as a completion, because the spool is empty.
	proposal.End = 11

	resp, _ = spool.Apply(newProposal(proposal, regBaz), false)
	c.Check(resp.Status, gc.Equals, pb.Status_PROPOSAL_MISMATCH)
	c.Check(spool.Begin, gc.Equals, int64(11))
	c.Check(spool.End, gc.Equals, int64(11))
	c.Check(spool.Registers, gc.DeepEquals, regBaz)

	c.Check(obv.completes, gc.HasLen, 1) // Unchanged.
}

func (s *SpoolSuite) TestRejectNextMismatch(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)

	var resp, err = spool.Apply(&pb.ReplicateRequest{Content: []byte("foobar")}, false)
	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{})
	c.Check(err, gc.IsNil)

	// Incorrect End offset.
	resp, err = spool.Apply(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              5,
			Sum:              pb.SHA1Sum{Part1: 0x8843d7f92416211d, Part2: 0xe9ebb963ff4ce281, Part3: 0x25932878},
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Registers: &regFoo,
	}, false)

	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{
		Status:    pb.Status_PROPOSAL_MISMATCH,
		Fragment:  &spool.Fragment.Fragment,
		Registers: &regEmpty,
	})
	c.Check(err, gc.IsNil)

	// Incorrect SHA1 Sum.
	resp, err = spool.Apply(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              6,
			Sum:              pb.SHA1Sum{Part1: 0xFFFFFFFFFFFFFFFF, Part2: 0xe9ebb963ff4ce281, Part3: 0x25932878},
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Registers: &regFoo,
	}, false)

	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{
		Status:    pb.Status_PROPOSAL_MISMATCH,
		Fragment:  &spool.Fragment.Fragment,
		Registers: &regEmpty,
	})
	c.Check(err, gc.IsNil)

	// Correct Next Fragment.
	resp, err = spool.Apply(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              6,
			Sum:              pb.SHA1Sum{Part1: 0x8843d7f92416211d, Part2: 0xe9ebb963ff4ce281, Part3: 0x25932878},
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Registers: &regFoo,
	}, false)

	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{Status: pb.Status_OK})
	c.Check(err, gc.IsNil)
}

func (s *SpoolSuite) TestContentDeltaMismatch(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)

	var resp, err = spool.Apply(&pb.ReplicateRequest{
		ContentDelta: 0,
		Content:      []byte("foo"),
	}, false)
	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{})
	c.Check(err, gc.IsNil)

	_, err = spool.Apply(&pb.ReplicateRequest{
		ContentDelta: 2,
		Content:      []byte("bar"),
	}, false)
	c.Check(err, gc.ErrorMatches, `invalid ContentDelta \(2; expected 3\)`)
}

func (s *SpoolSuite) TestFileErrorRetries(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	spool.CompressionCodec = pb.CompressionCodec_GZIP

	defer useShortRetryInterval()()

	injectNewSpoolFileFailure() // First open of spool.File fails.
	var _, err = spool.Apply(&pb.ReplicateRequest{Content: []byte("foo")}, true)
	c.Check(err, gc.IsNil)

	var proposal = spool.Next()

	injectNewSpoolFileFailure() // First open of spool.compressedFile fails.
	_, err = spool.Apply(&pb.ReplicateRequest{Proposal: &proposal}, true)
	c.Check(err, gc.IsNil)

	injectFileError(&spool.File) // Next spool WriteAt attempt fails.
	_, err = spool.Apply(&pb.ReplicateRequest{Content: []byte("bar")}, true)
	c.Check(err, gc.IsNil)

	injectFileError(&spool.File)           // Next ReadAt attempt fails, forcing rebuild of compressor.
	injectFileError(&spool.compressedFile) // Next Seek-start attempt fails.

	proposal = spool.Next()
	_, err = spool.Apply(&pb.ReplicateRequest{Proposal: &proposal}, true)
	c.Check(err, gc.IsNil)

	injectFileError(&spool.compressedFile) // Next Write or Seek fails, forcing rebuild of compressor.

	spool.finishCompression()
	c.Check(contentString(c, spool, pb.CompressionCodec_GZIP), gc.Equals, "foobar")
}

func useShortRetryInterval() func() {
	var d = spoolRetryInterval
	spoolRetryInterval = time.Millisecond

	return func() {
		spoolRetryInterval = d
	}
}

func injectNewSpoolFileFailure() {
	var realNewSpoolFile = newSpoolFile

	newSpoolFile = func() (File, error) {
		newSpoolFile = realNewSpoolFile
		return nil, errors.New("spool open error")
	}
}

func injectFileError(file *File) {
	var orig = *file
	*file = errFile{restore: func() { *file = orig }}
}

type errFile struct{ restore func() }

func (f errFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.restore()
	return 0, errors.New("failed ReadAt")
}
func (f errFile) Seek(offset int64, whence int) (int64, error) {
	f.restore()
	return 0, errors.New("failed Seek")
}
func (f errFile) WriteAt(p []byte, off int64) (int, error) {
	f.restore()
	return 0, errors.New("failed WriteAt")
}
func (f errFile) Write(p []byte) (int, error) {
	f.restore()
	return 0, errors.New("failed Write")
}
func (f errFile) Close() error {
	f.restore()
	return errors.New("failed Close")
}

func contentString(c *gc.C, s Spool, codec pb.CompressionCodec) string {
	var rc io.ReadCloser
	var err error

	if s.compressedFile == nil {
		rc = ioutil.NopCloser(io.NewSectionReader(s.File, 0, s.ContentLength()))
	} else {
		rc, err = codecs.NewCodecReader(
			io.NewSectionReader(s.compressedFile, 0, s.compressedLength), codec)
	}
	c.Assert(err, gc.IsNil)

	b, err := ioutil.ReadAll(rc)
	c.Check(err, gc.IsNil)
	c.Check(rc.Close(), gc.IsNil)

	return string(b)
}

func runReplicateSequence(c *gc.C, s *Spool, codec pb.CompressionCodec, primary bool) {

	var fixedTime = time.Time{}.Add(time.Hour * 24)
	defer func(fn func() time.Time) { timeNow = fn }(timeNow)
	timeNow = func() time.Time { return fixedTime }

	var seq = []pb.ReplicateRequest{
		// Commit 0 (synchronize).
		{
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				Begin:            0,
				End:              0,
				Sum:              pb.SHA1Sum{},
				CompressionCodec: codec,
			},
			Registers: &regEmpty,
		},
		{
			Content:      []byte("an init"),
			ContentDelta: 0,
		},
		{
			Content:      []byte("ial write "),
			ContentDelta: 7,
		},
		// Commit 1: "an initial write". Sets FirstAppendTime.
		{
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				Begin:            0,
				End:              17,
				Sum:              pb.SHA1SumOf("an initial write "),
				CompressionCodec: codec,
			},
			Registers: &regFoo,
		},
		// Content which is rolled back.
		{
			Content:      []byte("WHO"),
			ContentDelta: 0,
		},
		{
			Content:      []byte("OPS!"),
			ContentDelta: 3,
		},
		// Roll back to commit 1.
		{
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				Begin:            0,
				End:              17,
				Sum:              pb.SHA1SumOf("an initial write "),
				CompressionCodec: codec,
			},
			Registers: &regFoo,
		},
		{
			Content:      []byte("final write"),
			ContentDelta: 0,
		},
		// Commit 2: "final write"
		{
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				Begin:            0,
				End:              17 + 11,
				Sum:              pb.SHA1SumOf("an initial write final write"),
				CompressionCodec: codec,
			},
			Registers: &regBar,
		},
		// Content which is streamed but never committed.
		{
			Content:      []byte("extra "),
			ContentDelta: 0,
		},
		{
			Content:      []byte("partial content"),
			ContentDelta: 6,
		},
		// Commit 3: roll spool forward, completing prior spool.
		{
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				Begin:            17 + 11,
				End:              17 + 11,
				Sum:              pb.SHA1Sum{},
				CompressionCodec: codec,
			},
			Registers: &regBar,
		},
	}
	for _, req := range seq {
		var resp, err = s.Apply(&req, primary)

		c.Check(err, gc.IsNil)
		c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{Status: pb.Status_OK})

		if resp.Status != pb.Status_OK {
			c.Log(resp.String())
		} else if s.ContentLength() == 0 {
			c.Check(s.FirstAppendTime.IsZero(), gc.Equals, true)
		} else {
			c.Check(s.FirstAppendTime.Equal(fixedTime), gc.Equals, true)
		}
	}
}

type testSpoolObserver struct {
	commits   []Fragment
	completes []Spool
}

func (o *testSpoolObserver) SpoolCommit(f Fragment)        { o.commits = append(o.commits, f) }
func (o *testSpoolObserver) SpoolComplete(s Spool, _ bool) { o.completes = append(o.completes, s) }

// newProposal is a convenience which returns a ReplicateRequest with
// proposed, boxed |fragment| and |registers|.
func newProposal(fragment pb.Fragment, registers pb.LabelSet) *pb.ReplicateRequest {
	return &pb.ReplicateRequest{
		Proposal:  &fragment,
		Registers: &registers,
	}
}

var (
	// LabelSet fixtures for reference in tests.
	regEmpty = pb.MustLabelSet()
	regFoo   = pb.MustLabelSet("reg", "foo")
	regBar   = pb.MustLabelSet("reg", "bar")
	regBaz   = pb.MustLabelSet("reg", "baz")

	_ = gc.Suite(&SpoolSuite{})
)
