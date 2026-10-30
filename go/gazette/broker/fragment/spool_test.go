package fragment

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/codecs"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestNextCases(t *testing.T) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)

	// Case: Newly-initialized spool.
	require.Equal(t, pb.Fragment{
		Journal:          "a/journal",
		CompressionCodec: pb.CompressionCodec_NONE,
	}, spool.Next())

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
	require.Equal(t, pb.Fragment{
		Journal:          "a/journal",
		Begin:            100,
		End:              100,
		CompressionCodec: pb.CompressionCodec_SNAPPY,
	}, spool.Next())

	// Case: Fragment with applied content, ready to be committed.
	var _, err = spool.Apply(&pb.ReplicateRequest{
		Content:      []byte("some"),
		ContentDelta: 0,
	}, false)
	require.NoError(t, err)

	_, err = spool.Apply(&pb.ReplicateRequest{
		Content:      []byte(" content"),
		ContentDelta: 4,
	}, false)
	require.NoError(t, err)

	require.Equal(t, pb.Fragment{
		Journal:          "a/journal",
		Begin:            100,
		End:              112,
		Sum:              pb.SHA1SumOf("some content"),
		CompressionCodec: pb.CompressionCodec_SNAPPY,
	}, spool.Next())
}

func TestNoCompression(t *testing.T) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	runReplicateSequence(t, &spool, pb.CompressionCodec_NONE, true)

	require.Len(t, obv.completes, 1)
	require.Len(t, obv.commits, 2)

	require.Nil(t, obv.completes[0].compressedFile)
	require.Nil(t, obv.completes[0].compressor)
	require.Equal(t, int64(0), obv.completes[0].compressedLength)

	require.Equal(t, "an initial write final write",
		contentString(t, obv.completes[0], pb.CompressionCodec_NONE))
}

func TestCompressionAndPrimary(t *testing.T) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	runReplicateSequence(t, &spool, pb.CompressionCodec_GZIP, true)

	require.Len(t, obv.completes, 1)
	require.Len(t, obv.commits, 2)

	require.NotNil(t, obv.completes[0].compressedFile)
	require.Nil(t, obv.completes[0].compressor) // Closed.
	require.NotEqual(t, int64(0), obv.completes[0].compressedLength)

	require.Equal(t, "an initial write final write",
		contentString(t, obv.completes[0], pb.CompressionCodec_GZIP))
}

func TestCompressionNotPrimary(t *testing.T) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	runReplicateSequence(t, &spool, pb.CompressionCodec_GZIP, false)

	require.Len(t, obv.completes, 1)
	require.Len(t, obv.commits, 2)

	require.Nil(t, obv.completes[0].compressedFile)
	require.Nil(t, obv.completes[0].compressor)

	require.Equal(t, "an initial write final write",
		contentString(t, obv.completes[0], pb.CompressionCodec_NONE))

	// Though not compressed incrementally, expect it will compress on demand.
	obv.completes[0].finishCompression()

	require.NotNil(t, obv.completes[0].compressedFile)
	require.Nil(t, obv.completes[0].compressor) // Closed.
	require.NotEqual(t, int64(0), obv.completes[0].compressedLength)

	require.Equal(t, "an initial write final write",
		contentString(t, obv.completes[0], pb.CompressionCodec_GZIP))
}

func TestGzipBatcherMultipleMembers(t *testing.T) {
	var origBatchSize = compressionBatchSize
	compressionBatchSize = 10
	defer func() { compressionBatchSize = origBatchSize }()

	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)

	var resp, err = spool.Apply(newProposal(pb.Fragment{
		Journal:          "a/journal",
		Begin:            0,
		End:              0,
		CompressionCodec: pb.CompressionCodec_GZIP,
	}, regEmpty), true)
	require.NoError(t, err)
	require.Equal(t, pb.Status_OK, resp.Status)

	// Commit some data. The compression batch size has been artificially
	// lowered so the first commit is compressed, the second commit is buffered,
	// the third commit triggers a second member, the fourth commit triggers
	// a third member, and the fifth commit is smaller than compressionBatchSize
	// and will not trigger compression until spool completion.
	for _, req := range []pb.ReplicateRequest{
		{Content: []byte("first write ")},
		{Content: []byte("second ")},
		{Content: []byte("third ")},
		{Content: []byte("fourth write ")},
		{Content: []byte("final")},
	} {
		var resp, err = spool.Apply(&req, true)
		require.NoError(t, err)
		require.Equal(t, pb.ReplicateResponse{Status: pb.Status_OK}, resp)

		resp, err = spool.Apply(newProposal(spool.Next(), regEmpty), true)
		require.NoError(t, err)
		require.Equal(t, pb.ReplicateResponse{Status: pb.Status_OK}, resp)
	}

	// Complete the spool.
	resp, err = spool.Apply(newProposal(pb.Fragment{
		Journal:          "a/journal",
		Begin:            spool.Fragment.End,
		End:              spool.Fragment.End,
		CompressionCodec: pb.CompressionCodec_GZIP,
	}, regEmpty), true)
	require.NoError(t, err)
	require.Equal(t, pb.ReplicateResponse{Status: pb.Status_OK}, resp)

	require.Len(t, obv.commits, 5)
	require.Len(t, obv.completes, 1)
	require.NotNil(t, obv.completes[0].compressedFile)
	require.NotEqual(t, int64(0), obv.completes[0].compressedLength)

	var expected = "first write second third fourth write final"
	var actual = contentString(t, obv.completes[0], pb.CompressionCodec_GZIP)
	require.Equal(t, expected, actual)

	// Decompress and verify each member.
	var parts []string
	var compressedData = make([]byte, obv.completes[0].compressedLength)
	_, err = obv.completes[0].compressedFile.ReadAt(compressedData, 0)
	require.NoError(t, err)
	var gzipHeader = []byte{0x1f, 0x8b}
	for start := bytes.Index(compressedData, gzipHeader); start != -1; {
		var next = bytes.Index(compressedData[start+len(gzipHeader):], gzipHeader)
		var end int
		if next == -1 {
			end = len(compressedData)
		} else {
			end = start + len(gzipHeader) + next
		}

		var memberData = compressedData[start:end]
		var reader, readerErr = codecs.NewCodecReader(bytes.NewReader(memberData), pb.CompressionCodec_GZIP)
		require.NoError(t, readerErr)
		var decompressed, readErr = io.ReadAll(reader)
		require.NoError(t, readErr)
		require.NoError(t, reader.Close())

		parts = append(parts, string(decompressed))
		if next == -1 {
			break
		}
		start = end
	}

	require.Len(t, parts, 4)
	require.Equal(t, "first write ", parts[0])
	require.Equal(t, "second third ", parts[1])
	require.Equal(t, "fourth write ", parts[2])
	require.Equal(t, "final", parts[3])

	// Now test that compression offset tracking works correctly across spool
	// rolls by verifying that multiple commits written to the new spool can be
	// read.
	for _, req := range []pb.ReplicateRequest{
		{Content: []byte("abc")},
		{Content: []byte("xyz")},
	} {
		var resp, err = spool.Apply(&req, true)
		require.NoError(t, err)
		require.Equal(t, pb.ReplicateResponse{Status: pb.Status_OK}, resp)

		resp, err = spool.Apply(newProposal(spool.Next(), regEmpty), true)
		require.NoError(t, err)
		require.Equal(t, pb.ReplicateResponse{Status: pb.Status_OK}, resp)
	}

	// Complete the second spool.
	resp, err = spool.Apply(newProposal(pb.Fragment{
		Journal:          "a/journal",
		Begin:            spool.Fragment.End,
		End:              spool.Fragment.End,
		CompressionCodec: pb.CompressionCodec_GZIP,
	}, regEmpty), true)
	require.NoError(t, err)
	require.Equal(t, pb.Status_OK, resp.Status)

	require.Len(t, obv.completes, 2)
	require.Equal(t, "abcxyz", contentString(t, obv.completes[1], pb.CompressionCodec_GZIP))

	// The first and only write is below the compression threshold, which will
	// require initializing compression of the spool in finishCompression.
	resp, err = spool.Apply(&pb.ReplicateRequest{Content: []byte("small")}, true)
	require.NoError(t, err)
	require.Equal(t, pb.ReplicateResponse{Status: pb.Status_OK}, resp)

	resp, err = spool.Apply(newProposal(spool.Next(), regEmpty), true)
	require.NoError(t, err)
	require.Equal(t, pb.ReplicateResponse{Status: pb.Status_OK}, resp)

	// Complete the third spool.
	resp, err = spool.Apply(newProposal(pb.Fragment{
		Journal:          "a/journal",
		Begin:            spool.Fragment.End,
		End:              spool.Fragment.End,
		CompressionCodec: pb.CompressionCodec_GZIP,
	}, regEmpty), true)
	require.NoError(t, err)
	require.Equal(t, pb.Status_OK, resp.Status)

	require.Len(t, obv.completes, 3)
	require.Equal(t, "small", contentString(t, obv.completes[2], pb.CompressionCodec_GZIP))

	// Empty fragments don't trigger completion events (no ContentLength), so
	// completes stays at 3.
	resp, err = spool.Apply(newProposal(pb.Fragment{
		Journal:          "a/journal",
		Begin:            spool.Fragment.End,
		End:              spool.Fragment.End,
		CompressionCodec: pb.CompressionCodec_GZIP,
	}, regEmpty), true)
	require.NoError(t, err)
	require.Equal(t, pb.Status_OK, resp.Status)

	require.Len(t, obv.completes, 3)
}

func TestRejectRollBeforeCurrentEnd(t *testing.T) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	runReplicateSequence(t, &spool, pb.CompressionCodec_NONE, false)

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

	require.Equal(t, pb.ReplicateResponse{
		Status:    pb.Status_PROPOSAL_MISMATCH,
		Fragment:  &spool.Fragment.Fragment,
		Registers: &regBar,
	}, resp)
	require.NoError(t, err)

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

	require.Equal(t, pb.ReplicateResponse{Status: pb.Status_OK}, resp)
	require.NoError(t, err)
}

func TestMoreMismatchCases(t *testing.T) {
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
	require.Equal(t, pb.Status_PROPOSAL_MISMATCH, resp.Status)
	require.Equal(t, &regFoo, resp.Registers)

	// Apply 4 uncommitted bytes
	spool.MustApply(&pb.ReplicateRequest{Content: []byte("efgh")})

	// Case: Attempt a roll-back, but with incorrect registers. It's refused.
	resp, _ = spool.Apply(newProposal(spool.Fragment.Fragment, regBar), false)
	require.Equal(t, pb.Status_PROPOSAL_MISMATCH, resp.Status)

	// Case: Apply a mismatched proposal which is within current spool bounds.
	// Expect the spool doesn't roll forward.
	proposal = spool.Next()
	proposal.Begin = 1 // Cause proposal to mismatch.

	resp, _ = spool.Apply(newProposal(proposal, regBar), false)
	require.Equal(t, pb.Status_PROPOSAL_MISMATCH, resp.Status)
	require.Equal(t, int64(0), spool.Begin)
	require.Equal(t, regFoo, spool.Registers)
	require.Len(t, obv.completes, 0)

	// Case: Again, but this time the proposal is beyond current bounds.
	// Expect a MISMATCH is still returned, but the spool rolls forward
	// and takes the provided registers.
	proposal.End = 9

	resp, _ = spool.Apply(newProposal(proposal, regBar), false)
	require.Equal(t, pb.Status_PROPOSAL_MISMATCH, resp.Status)
	require.Equal(t, int64(9), spool.Begin)
	require.Equal(t, int64(9), spool.End)
	require.Equal(t, int64(0), spool.delta)
	require.Equal(t, regBar, spool.Registers)

	require.Len(t, obv.completes, 1)
	require.Equal(t, "abcd",
		contentString(t, obv.completes[0], pb.CompressionCodec_GZIP))

	// Case 3: Spool is empty, and sees a proposal within current bounds.
	// Expect it remains unchanged.
	proposal.End = 8

	resp, _ = spool.Apply(newProposal(proposal, regBaz), false)
	require.Equal(t, pb.Status_PROPOSAL_MISMATCH, resp.Status)
	require.Equal(t, int64(9), spool.End)
	require.Equal(t, regBar, spool.Registers)

	// Case 4: Spool is empty, but rolls due to a proposal beyond current bounds.
	// It's not treated as a completion, because the spool is empty.
	proposal.End = 11

	resp, _ = spool.Apply(newProposal(proposal, regBaz), false)
	require.Equal(t, pb.Status_PROPOSAL_MISMATCH, resp.Status)
	require.Equal(t, int64(11), spool.Begin)
	require.Equal(t, int64(11), spool.End)
	require.Equal(t, regBaz, spool.Registers)

	require.Len(t, obv.completes, 1) // Unchanged.
}

func TestRejectNextMismatch(t *testing.T) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)

	var resp, err = spool.Apply(&pb.ReplicateRequest{Content: []byte("foobar")}, false)
	require.Equal(t, pb.ReplicateResponse{}, resp)
	require.NoError(t, err)

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

	require.Equal(t, pb.ReplicateResponse{
		Status:    pb.Status_PROPOSAL_MISMATCH,
		Fragment:  &spool.Fragment.Fragment,
		Registers: &regEmpty,
	}, resp)
	require.NoError(t, err)

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

	require.Equal(t, pb.ReplicateResponse{
		Status:    pb.Status_PROPOSAL_MISMATCH,
		Fragment:  &spool.Fragment.Fragment,
		Registers: &regEmpty,
	}, resp)
	require.NoError(t, err)

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

	require.Equal(t, pb.ReplicateResponse{Status: pb.Status_OK}, resp)
	require.NoError(t, err)
}

func TestContentDeltaMismatch(t *testing.T) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)

	var resp, err = spool.Apply(&pb.ReplicateRequest{
		ContentDelta: 0,
		Content:      []byte("foo"),
	}, false)
	require.Equal(t, pb.ReplicateResponse{}, resp)
	require.NoError(t, err)

	_, err = spool.Apply(&pb.ReplicateRequest{
		ContentDelta: 2,
		Content:      []byte("bar"),
	}, false)
	require.EqualError(t, err, `invalid ContentDelta (2; expected 3)`)
}

func TestFileErrorRetries(t *testing.T) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	spool.CompressionCodec = pb.CompressionCodec_GZIP

	defer useShortRetryInterval()()

	injectNewSpoolFileFailure() // First open of spool.File fails.
	var _, err = spool.Apply(&pb.ReplicateRequest{Content: []byte("foo")}, true)
	require.NoError(t, err)

	var proposal = spool.Next()

	injectNewSpoolFileFailure() // First open of spool.compressedFile fails.
	_, err = spool.Apply(&pb.ReplicateRequest{Proposal: &proposal}, true)
	require.NoError(t, err)

	injectFileError(&spool.File) // Next spool WriteAt attempt fails.
	_, err = spool.Apply(&pb.ReplicateRequest{Content: []byte("bar")}, true)
	require.NoError(t, err)

	injectFileError(&spool.File)           // Next ReadAt attempt fails, forcing rebuild of compressor.
	injectFileError(&spool.compressedFile) // Next Seek-start attempt fails.

	proposal = spool.Next()
	_, err = spool.Apply(&pb.ReplicateRequest{Proposal: &proposal}, true)
	require.NoError(t, err)

	injectFileError(&spool.compressedFile) // Next Write or Seek fails, forcing rebuild of compressor.

	spool.finishCompression()
	require.Equal(t, "foobar", contentString(t, spool, pb.CompressionCodec_GZIP))
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

func contentString(t *testing.T, s Spool, codec pb.CompressionCodec) string {
	var rc io.ReadCloser
	var err error

	if s.compressedFile == nil {
		rc = io.NopCloser(io.NewSectionReader(s.File, 0, s.ContentLength()))
	} else {
		rc, err = codecs.NewCodecReader(
			io.NewSectionReader(s.compressedFile, 0, s.compressedLength), codec)
	}
	require.NoError(t, err)

	b, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	return string(b)
}

func runReplicateSequence(t *testing.T, s *Spool, codec pb.CompressionCodec, primary bool) {

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

		require.NoError(t, err)
		require.Equal(t, pb.ReplicateResponse{Status: pb.Status_OK}, resp)

		if resp.Status != pb.Status_OK {
			t.Log(resp.String())
		} else if s.ContentLength() == 0 {
			require.True(t, s.FirstAppendTime.IsZero())
		} else {
			require.True(t, s.FirstAppendTime.Equal(fixedTime))
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
)
