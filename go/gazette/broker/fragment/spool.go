package fragment

import (
	"crypto/sha1"
	"encoding"
	"fmt"
	"hash"
	"io"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/codecs"
	pb "go.gazette.dev/core/broker/protocol"
)

// Minimum size of accumulated uncompressed data before performing incremental
// compression. Useful for GZIP, which creates a new member per compression
// invocation.
var compressionBatchSize = 1024 * 1024

// Spool is a Fragment which is in the process of being created, backed by a
// local *os.File. As commits occur and the file extent is updated, the Spool
// Fragment is also updated to reflect the new committed extent. At all
// times, the Spool Fragment is a consistent, valid Fragment.
type Spool struct {
	// Fragment at time of last commit.
	Fragment
	// FirstAppendTime is the UTC Time of the first commit of the Spool Fragment.
	FirstAppendTime time.Time
	// Registers of the journal.
	Registers pb.LabelSet

	// Compressed form of the Fragment, compressed under Fragment.CompressionCodec.
	compressedFile File
	// Length of compressed content written to |compressedFile|. Set only after
	// the compressor is finalized.
	compressedLength int64
	// Spool file offset through which content has been compressed.
	compressedTo int64
	// Compressor of |compressedFile|.
	compressor codecs.Compressor

	delta    int64     // Delta offset of next byte to write, relative to Fragment.End.
	summer   hash.Hash // Running SHA1 of the Fragment.File, through |Fragment.End + delta|.
	sumState []byte    // SHA1 |summer| internal state at the last Fragment commit.

	observer SpoolObserver
}

// SpoolObserver is notified of important events in the Spool lifecycle.
type SpoolObserver interface {
	// SpoolCommit is called when the Spool Fragment is extended.
	SpoolCommit(Fragment)
	// SpoolComplete is called when the Spool has been completed.
	SpoolComplete(_ Spool, primary bool)
}

// NewSpool returns an empty Spool of |journal|.
func NewSpool(journal pb.Journal, observer SpoolObserver) Spool {
	return Spool{
		Fragment: Fragment{Fragment: pb.Fragment{
			Journal:          journal,
			CompressionCodec: pb.CompressionCodec_NONE,
		}},
		summer:   sha1.New(),
		sumState: zeroedSHA1State,
		observer: observer,
	}
}

// Apply the ReplicateRequest to the Spool, returning any encountered error.
func (s *Spool) Apply(r *pb.ReplicateRequest, primary bool) (pb.ReplicateResponse, error) {
	if r.Proposal != nil {
		return s.applyCommit(r, primary), nil
	} else {
		return pb.ReplicateResponse{}, s.applyContent(r)
	}
}

// MustApply applies the ReplicateRequest, and panics if a !OK status is returned
// or error occurs. MustApply is a convenience for cases such as rollbacks, where
// the request is derived from the Spool itself and cannot reasonably fail.
func (s *Spool) MustApply(r *pb.ReplicateRequest) {
	if resp, err := s.Apply(r, false); err != nil {
		panic(err.Error())
	} else if resp.Status != pb.Status_OK {
		panic(resp.Status.String())
	}
}

// Next returns the next Fragment which can be committed by the Spool.
func (s *Spool) Next() pb.Fragment {
	var f = s.Fragment.Fragment
	f.End += s.delta

	// Empty fragments are special-cased to have Sum of zero (as technically, SHA1('') != <zero>).
	if f.Begin == f.End {
		f.Sum = pb.SHA1Sum{}
	} else {
		f.Sum = pb.SHA1SumFromDigest(s.summer.Sum(nil))
	}
	return f
}

// String returns a debugging representation of the Spool.
func (s Spool) String() string {
	return fmt.Sprintf("Spool<Fragment: %s, Registers: %s, delta: %d>", &s.Fragment, &s.Registers, s.delta)
}

func (s *Spool) applyCommit(r *pb.ReplicateRequest, primary bool) pb.ReplicateResponse {

	// Do we need to roll the Spool forward? We do this if the proposal:
	//
	//  1) References an offset strictly larger than any we're aware of. This case
	//     happens, eg, when a new peer is introduced to a route and must "catch
	//     up" with recently written content. It can also happen on recovery from
	//     network partitions, where some replicas believe a commit occurred and
	//     others don't (note the Append RPC itself will have failed in this case,
	//     forcing the client to retry).
	//
	//  2) Begins at our exact End and has length zero. This case allows a primary
	//     pipeline to direct replicas to synchronously roll their Spools to a new
	//     and empty Fragment.
	//
	if r.Proposal.End > s.Fragment.End+s.delta ||
		(r.Proposal.End == s.Fragment.End && r.Proposal.ContentLength() == 0) {

		if s.compressor != nil || primary && s.CompressionCodec != pb.CompressionCodec_NONE {
			s.finishCompression()
		}
		if s.ContentLength() != 0 {
			spoolCompletedTotal.Inc()
			s.observer.SpoolComplete(*s, primary)
		}
		// If the proposal is strictly greater than our Fragment, take the proposal registers.
		if r.Proposal.End > s.Fragment.End {
			s.Registers.Assign(r.Registers)
		}
		*s = Spool{
			Fragment: Fragment{
				Fragment: pb.Fragment{
					Journal:          s.Fragment.Journal,
					Begin:            r.Proposal.End,
					End:              r.Proposal.End,
					CompressionCodec: r.Proposal.CompressionCodec,
				},
			},
			Registers: s.Registers,
			summer:    sha1.New(),
			sumState:  zeroedSHA1State,
			observer:  s.observer,
		}
	}

	// There are now two proposal cases which can succeed:
	//  1) A proposal of our exact fragment and registers.
	//  2) A proposal of our fragment extended by |delta| & |summer|, with updated registers.

	// Case 1? "Undo" any partial content by rolling back |delta| and |summer|.
	if s.Fragment.Fragment == *r.Proposal && s.Registers.Equal(r.Registers) {
		if s.delta != 0 {
			spoolRollbacksTotal.Inc()
			spoolRollbackBytesTotal.Add(float64(s.delta))
		}
		s.delta = 0
		s.restoreSumState()
		return pb.ReplicateResponse{Status: pb.Status_OK}
	}

	// Case 2? Apply the |delta| bytes spooled since last commit & update registers.
	if s.delta != 0 && s.Next() == *r.Proposal {
		spoolCommitsTotal.Inc()
		spoolCommitBytesTotal.Add(float64(s.delta))

		var uncompressedBytes = (r.Proposal.End - s.Fragment.Begin) - s.compressedTo
		if primary && s.CompressionCodec != pb.CompressionCodec_NONE && int(uncompressedBytes) >= compressionBatchSize {
			s.compressThrough(r.Proposal.End)
		}
		s.Fragment.Fragment = *r.Proposal
		s.observer.SpoolCommit(s.Fragment)

		if s.FirstAppendTime.IsZero() {
			s.FirstAppendTime = timeNow().UTC()
		}
		s.Registers.Assign(r.Registers)

		s.delta = 0
		s.saveSumState()
		return pb.ReplicateResponse{Status: pb.Status_OK}
	}

	// This proposal cannot apply to our Spool; return an error to the primary.
	return pb.ReplicateResponse{
		Status:    pb.Status_PROPOSAL_MISMATCH,
		Fragment:  &s.Fragment.Fragment,
		Registers: &s.Registers,
	}
}

func (s *Spool) applyContent(r *pb.ReplicateRequest) error {
	if r.ContentDelta != s.delta {
		return pb.NewValidationError("invalid ContentDelta (%d; expected %d)", r.ContentDelta, s.delta)
	}

	// Create Spool File (if it doesn't exist), and write content at the current
	// location. Retry indefinitely on filesystem errors.
	var err error
	for {
		if err != nil {
			log.WithField("err", err).Error("failed to applyContent (will retry)")
			time.Sleep(spoolRetryInterval)
		}

		if s.Fragment.File == nil {
			if s.ContentLength() != 0 {
				panic("Spool.Fragment not empty.")
			} else if s.Fragment.File, err = newSpoolFile(); err != nil {
				err = fmt.Errorf("creating spool file: %s", err)
				continue
			}
		}

		if _, err = s.Fragment.File.WriteAt(r.Content, s.ContentLength()+s.delta); err != nil {
			err = fmt.Errorf("writing spool content: %s", err)
			continue
		}

		break // Success.
	}

	if _, err := s.summer.Write(r.Content); err != nil {
		panic("SHA1.Write cannot fail: " + err.Error())
	}
	s.delta += int64(len(r.Content))

	return nil
}

func (s *Spool) compressThrough(end int64) {
	if s.CompressionCodec == pb.CompressionCodec_NONE {
		panic("expected CompressionCodec != NONE")
	}

	var err error

	var buf = bufferPool.Get().([]byte)
	defer bufferPool.Put(buf)

	// Garden path: we've already compressed all content of the current Fragment,
	// and now incrementally compress through |end|.
	if s.compressor != nil {
		var offset, delta = s.compressedTo, (end - s.Fragment.Begin) - s.compressedTo

		if _, err = io.CopyBuffer(s.compressor, io.NewSectionReader(s.File, offset, delta), buf); err == nil {
			if s.CompressionCodec == pb.CompressionCodec_GZIP {
				err = s.compressor.Close()
			}
		}
		if err == nil {
			s.compressedTo = end - s.Fragment.Begin
			return // Done.
		}
		err = fmt.Errorf("while incrementally compressing: %s", err)

		_ = s.compressor.Close()
		s.compressor = nil
	}

	// We must build or rebuild compression of the Spool.
	for {
		if err != nil {
			log.WithFields(log.Fields{"err": err, "end": end}).Error("failed to compressThrough (will retry)")

			time.Sleep(spoolRetryInterval)
		}

		if s.compressedFile == nil {
			if s.compressedFile, err = newSpoolFile(); err != nil {
				err = fmt.Errorf("creating compressed spool file: %s", err)
				continue
			}
		}
		if _, err = s.compressedFile.Seek(0, io.SeekStart); err != nil {
			err = fmt.Errorf("seeking compressedFile to start: %s", err)
			continue
		}
		if s.compressor, err = codecs.NewCodecWriter(s.compressedFile, s.CompressionCodec); err != nil {
			err = fmt.Errorf("initializing compressor: %s", err)
			continue
		}
		if _, err = io.CopyBuffer(s.compressor, io.NewSectionReader(s.File, 0, end-s.Fragment.Begin), buf); err != nil {
			err = fmt.Errorf("while compressing: %s", err)

			_ = s.compressor.Close()
			s.compressor = nil
			continue
		}
		if s.CompressionCodec == pb.CompressionCodec_GZIP {
			if err = s.compressor.Close(); err != nil {
				err = fmt.Errorf("flushing gzip batch compressor: %s", err)
				s.compressor = nil
				continue
			}
		}

		s.compressedTo = end - s.Fragment.Begin
		break // Success.
	}
}

func (s *Spool) finishCompression() {
	if s.CompressionCodec == pb.CompressionCodec_NONE {
		panic("expected CompressionCodec != NONE")
	} else if s.compressedLength != 0 {
		return // Already finalized.
	}
	var err error

	if s.compressedTo < s.Fragment.End-s.Fragment.Begin {
		s.compressThrough(s.Fragment.End)
	} else if s.compressor == nil {
		// Empty fragment.
		return
	}

	for {
		if err != nil {
			log.WithField("err", err).Error("failed to finishCompression (will retry)")

			time.Sleep(spoolRetryInterval)

			// |compressor| has been invalidated, and must be rebuilt.
			s.compressThrough(s.Fragment.End)
		}

		err = s.compressor.Close()
		s.compressor = nil

		if err != nil {
			err = fmt.Errorf("closing compressor: %s", err)
			continue
		}
		if s.compressedLength, err = s.compressedFile.Seek(0, io.SeekCurrent); err != nil {
			err = fmt.Errorf("seeking compressedFile current: %s", err)
			continue
		}
		break // Success.
	}
}

// saveSumState marshals internal state of |summer| into |sumState|.
func (s *Spool) saveSumState() {
	if state, err := s.summer.(encoding.BinaryMarshaler).MarshalBinary(); err != nil {
		panic(err.Error()) // Cannot fail.
	} else {
		s.sumState = state
	}
}

// restoreSumState unmarshals |sumState| into |summer|.
func (s *Spool) restoreSumState() {
	if err := s.summer.(encoding.BinaryUnmarshaler).UnmarshalBinary(s.sumState); err != nil {
		panic(err.Error()) // Cannot fail.
	}
}

var (
	zeroedSHA1State, _ = sha1.New().(encoding.BinaryMarshaler).MarshalBinary()
	spoolRetryInterval = time.Second * 5
	bufferPool         = sync.Pool{New: func() interface{} { return make([]byte, 32*1024) }}
)
