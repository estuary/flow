package broker

import (
	"context"
	"errors"
	"testing"

	"github.com/gazette/gazette/v2/broker/teststub"
	"github.com/gazette/gazette/v2/fragment"
	pb "github.com/gazette/gazette/v2/protocol"
	gc "github.com/go-check/check"
)

type PipelineSuite struct{}

func (s *PipelineSuite) TestBasicLifeCycle(c *gc.C) {
	var rm = newReplicationMock(c)
	defer rm.cancel()

	var pln = rm.newPipeline(rm.header(0, 100))

	var req = &pb.ReplicateRequest{Content: []byte("foobar")}
	pln.scatter(req)

	c.Check(pln.sendErr(), gc.IsNil)
	c.Check(<-rm.brokerA.ReplReqCh, gc.DeepEquals, req)
	c.Check(<-rm.brokerC.ReplReqCh, gc.DeepEquals, req)

	var proposal = pln.spool.Next()
	req = &pb.ReplicateRequest{Proposal: &proposal}
	pln.scatter(req)

	c.Check(pln.sendErr(), gc.IsNil)
	c.Check(<-rm.brokerA.ReplReqCh, gc.DeepEquals, req)
	c.Check(<-rm.brokerC.ReplReqCh, gc.DeepEquals, req)

	var waitFor1, closeAfter1 = pln.barrier()

	// Second client issues a write and close.
	pln.scatter(&pb.ReplicateRequest{Content: []byte("bazbing")})
	_, _ = <-rm.brokerA.ReplReqCh, <-rm.brokerC.ReplReqCh
	pln.closeSend()

	c.Check(pln.sendErr(), gc.IsNil)
	c.Check(<-rm.brokerA.ReplReqCh, gc.IsNil) // Expect EOF.
	c.Check(<-rm.brokerC.ReplReqCh, gc.IsNil) // Expect EOF.

	var waitFor2, closeAfter2 = pln.barrier()

	// First client reads its response.
	<-waitFor1

	rm.brokerA.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}
	rm.brokerC.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}

	pln.gatherOK()
	c.Check(pln.recvErr(), gc.IsNil)
	c.Check(pln.recvResp, gc.DeepEquals, []pb.ReplicateResponse{{}, {}, {}})

	close(closeAfter1)

	// Second client reads its response.
	<-waitFor2

	rm.brokerA.ErrCh <- nil // Send EOF.
	rm.brokerC.ErrCh <- nil // Send EOF.

	pln.gatherEOF()
	c.Check(pln.recvErr(), gc.IsNil)

	close(closeAfter2)
}

func (s *PipelineSuite) TestPeerErrorCases(c *gc.C) {
	var rm = newReplicationMock(c)
	defer rm.cancel()

	var pln = rm.newPipeline(rm.header(0, 100))

	var req = &pb.ReplicateRequest{Content: []byte("foo")}
	pln.scatter(req)

	c.Check(pln.sendErr(), gc.IsNil)
	c.Check(<-rm.brokerA.ReplReqCh, gc.DeepEquals, req)
	c.Check(<-rm.brokerC.ReplReqCh, gc.DeepEquals, req)

	// Have peer A return an error. Peer B returns a non-OK response status (where OK is expected).
	rm.brokerA.ErrCh <- errors.New("error!")
	rm.brokerC.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_FRAGMENT_MISMATCH}

	// Expect pipeline retains the first recv error for each peer.
	pln.gatherOK()
	c.Check(pln.recvErrs[0], gc.ErrorMatches, `rpc error: code = Unknown desc = error!`)
	c.Check(pln.recvErrs[1], gc.IsNil)
	c.Check(pln.recvErrs[2], gc.ErrorMatches, `unexpected !OK response: status:FRAGMENT_MISMATCH `)

	// Expect recvErr decorates the first error with peer metadata.
	c.Check(pln.recvErr(), gc.ErrorMatches, `recv from zone:"A" suffix:"1" : rpc error: .*`)

	// Scatter a ReplicateRequest to each peer.
	req = &pb.ReplicateRequest{Content: []byte("bar"), ContentDelta: 99999} // Invalid ContentDelta.
	pln.scatter(req)

	// Expect pipeline retains the first send error for each peer, including the local Spool.
	// |rm.brokerA|'s stream is already closed, and the attempted send will error with EOF
	// (non-standard, but just how gRPC does it). The request is immediately applied to the
	// local Spool during scatter(), and we expect its error is tracked on |sendErrs|. No
	// error occurs on send to|rm.brokerC| (though its |recvErr| is still set).
	c.Check(pln.sendErrs[0], gc.ErrorMatches, `EOF`)
	c.Check(pln.sendErrs[1], gc.ErrorMatches, `invalid ContentDelta \(99999; expected 3\)`)
	c.Check(pln.sendErrs[2], gc.IsNil)

	c.Check(<-rm.brokerC.ReplReqCh, gc.DeepEquals, req)

	// Expect sendErr decorates the first error with peer metadata.
	c.Check(pln.sendErr(), gc.ErrorMatches, `send to zone:"A" suffix:"1" : EOF`)

	pln.closeSend()

	// Finish shutdown by having brokerC receive and send EOF.
	c.Check(<-rm.brokerC.ReplReqCh, gc.IsNil)
	rm.brokerC.ErrCh <- nil
	pln.gatherEOF()

	// Restart a new pipeline. Immediately send an EOF, and test handling of
	// an unexpected received message prior to peer EOF.
	pln = rm.newPipeline(rm.header(0, 100))
	pln.closeSend()

	c.Check(<-rm.brokerA.ReplReqCh, gc.IsNil) // Read EOF.
	c.Check(<-rm.brokerC.ReplReqCh, gc.IsNil) // Read EOF.

	rm.brokerA.ErrCh <- nil                                                       // Send EOF.
	rm.brokerC.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_WRONG_ROUTE} // Unexpected response.
	rm.brokerC.ErrCh <- nil                                                       // Now, send EOF.

	pln.gatherEOF()
	c.Check(pln.recvErrs[0], gc.IsNil)
	c.Check(pln.recvErrs[1], gc.IsNil)
	c.Check(pln.recvErrs[2], gc.ErrorMatches, `unexpected response: status:WRONG_ROUTE `)
}

func (s *PipelineSuite) TestGatherSyncCases(c *gc.C) {
	var rm = newReplicationMock(c)
	defer rm.cancel()

	var pln = rm.newPipeline(rm.header(0, 100))

	var req = &pb.ReplicateRequest{
		Header:  rm.header(1, 100),
		Journal: "a/journal",
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            123,
			End:              123,
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Acknowledge: true,
	}
	pln.scatter(req)

	// Expect each peer sees |req| with its ID in the Header.
	req.Header = rm.header(0, 100)
	c.Check(<-rm.brokerA.ReplReqCh, gc.DeepEquals, req)
	req.Header = rm.header(2, 100)
	c.Check(<-rm.brokerC.ReplReqCh, gc.DeepEquals, req)

	// Craft a peer response Header at a later revision, with a different Route.
	var wrongRouteHdr = rm.header(0, 4567)
	wrongRouteHdr.Route.Members[0].Suffix = "other"

	rm.brokerA.ReplRespCh <- &pb.ReplicateResponse{
		Status: pb.Status_WRONG_ROUTE,
		Header: wrongRouteHdr,
	}
	rm.brokerC.ReplRespCh <- &pb.ReplicateResponse{
		Status:   pb.Status_FRAGMENT_MISMATCH,
		Fragment: &pb.Fragment{End: 800}, // End is larger than proposal.
	}

	// Expect the maximum offset and Etcd revision to read through are returned.
	var rollToOffset, readRev = pln.gatherSync(*req.Proposal)
	c.Check(rollToOffset, gc.Equals, int64(800))
	c.Check(readRev, gc.Equals, int64(4567))
	c.Check(pln.recvErr(), gc.IsNil)
	c.Check(pln.sendErr(), gc.IsNil)

	// Again. This time one peer returns a zero-length Fragment at proposal End.
	// (Note that a peer should never send such a response to a zero-length proposal,
	// but that's not under test here- only our handling of receiving such a response).
	req.Proposal = &pb.Fragment{
		Journal:          "a/journal",
		Begin:            800,
		End:              800,
		CompressionCodec: pb.CompressionCodec_NONE,
	}
	pln.scatter(req)

	_, _ = <-rm.brokerA.ReplReqCh, <-rm.brokerC.ReplReqCh
	rm.brokerA.ReplRespCh <- &pb.ReplicateResponse{
		Status:   pb.Status_FRAGMENT_MISMATCH,
		Fragment: &pb.Fragment{Begin: 790, End: 890}, // Larger than proposal End.
	}
	rm.brokerC.ReplRespCh <- &pb.ReplicateResponse{
		Status:   pb.Status_FRAGMENT_MISMATCH,
		Fragment: &pb.Fragment{Begin: 800, End: 800}, // At proposal End.
	}

	rollToOffset, readRev = pln.gatherSync(*req.Proposal)
	c.Check(rollToOffset, gc.Equals, int64(890))
	c.Check(readRev, gc.Equals, int64(0))
	c.Check(pln.recvErr(), gc.IsNil)
	c.Check(pln.sendErr(), gc.IsNil)

	// Again. This time peers return success.
	req.Proposal = &pb.Fragment{
		Journal:          "a/journal",
		Begin:            890,
		End:              890,
		CompressionCodec: pb.CompressionCodec_NONE,
	}
	pln.scatter(req)

	_, _ = <-rm.brokerA.ReplReqCh, <-rm.brokerC.ReplReqCh
	rm.brokerA.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}
	rm.brokerC.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}

	rollToOffset, readRev = pln.gatherSync(*req.Proposal)
	c.Check(rollToOffset, gc.Equals, int64(0))
	c.Check(readRev, gc.Equals, int64(0))
	c.Check(pln.recvErr(), gc.IsNil)
	c.Check(pln.sendErr(), gc.IsNil)

	// Again. This time, peers return !OK status with invalid responses.
	pln.scatter(req)

	_, _ = <-rm.brokerA.ReplReqCh, <-rm.brokerC.ReplReqCh

	rm.brokerA.ReplRespCh <- &pb.ReplicateResponse{
		Status: pb.Status_WRONG_ROUTE,
		Header: rm.header(0, 99), // Revision not greater than |pln|'s.
	}
	rm.brokerC.ReplRespCh <- &pb.ReplicateResponse{
		Status: pb.Status_FRAGMENT_MISMATCH,
		// End is at proposal, but is non-empty. This is unexpected as peer
		// should have instead rolled their Spool to End.
		Fragment: &pb.Fragment{Begin: 567, End: 890},
	}

	rollToOffset, readRev = pln.gatherSync(*req.Proposal)
	c.Check(rollToOffset, gc.Equals, int64(0))
	c.Check(readRev, gc.Equals, int64(0))
	c.Check(pln.sendErr(), gc.IsNil)
	c.Check(pln.recvErr(), gc.NotNil)

	c.Check(pln.recvErrs[0], gc.ErrorMatches, `unexpected WRONG_ROUTE: process_id:.*`)
	c.Check(pln.recvErrs[1], gc.IsNil)
	c.Check(pln.recvErrs[2], gc.ErrorMatches, `unexpected FRAGMENT_MISMATCH: begin:567 end:890 .*`)
}

func (s *PipelineSuite) TestPipelineSync(c *gc.C) {
	var rm = newReplicationMock(c)
	defer rm.cancel()

	// Tweak Spool to have a different End & Sum.
	var spool = <-rm.spoolCh
	spool.Fragment.End, spool.Fragment.Sum = 123, pb.SHA1Sum{Part1: 999}
	rm.spoolCh <- spool

	var pln = rm.newPipeline(rm.header(0, 100))

	go func() {
		// Read sync request.
		c.Check(<-rm.brokerA.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{
			Journal: "a/journal",
			Header:  rm.header(0, 100),
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				Begin:            0,
				End:              123,
				Sum:              pb.SHA1Sum{Part1: 999},
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			Acknowledge: true,
		})
		_ = <-rm.brokerC.ReplReqCh

		// Peers disagree on Fragment End.
		rm.brokerA.ReplRespCh <- &pb.ReplicateResponse{
			Status:   pb.Status_FRAGMENT_MISMATCH,
			Fragment: &pb.Fragment{Begin: 567, End: 892},
		}
		rm.brokerC.ReplRespCh <- &pb.ReplicateResponse{
			Status:   pb.Status_FRAGMENT_MISMATCH,
			Fragment: &pb.Fragment{Begin: 567, End: 890},
		}

		// Next iteration. Expect proposal is updated to reflect largest offset.
		c.Check(<-rm.brokerA.ReplReqCh, gc.DeepEquals, &pb.ReplicateRequest{
			Journal: "a/journal",
			Header:  rm.header(0, 100),
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				Begin:            892,
				End:              892,
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			Acknowledge: true,
		})
		_ = <-rm.brokerC.ReplReqCh

		// Peers agree.
		rm.brokerA.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}
		rm.brokerC.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}

		// Next round.
		_, _ = <-rm.brokerA.ReplReqCh, <-rm.brokerC.ReplReqCh

		// Peer C response with a larger Etcd revision.
		var wrongRouteHdr = rm.header(0, 4567)
		wrongRouteHdr.Route.Members[0].Suffix = "other"

		rm.brokerA.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}
		rm.brokerC.ReplRespCh <- &pb.ReplicateResponse{
			Status: pb.Status_WRONG_ROUTE,
			Header: wrongRouteHdr,
		}

		// Expect start() sends EOF.
		c.Check(<-rm.brokerA.ReplReqCh, gc.IsNil)
		c.Check(<-rm.brokerC.ReplReqCh, gc.IsNil)
		rm.brokerA.ErrCh <- nil
		rm.brokerC.ErrCh <- nil

		// Next round sends an error.
		_, _ = <-rm.brokerA.ReplReqCh, <-rm.brokerC.ReplReqCh
		rm.brokerA.ErrCh <- errors.New("an error")
		rm.brokerC.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}

		// Expect EOF.
		c.Check(<-rm.brokerA.ReplReqCh, gc.IsNil)
		c.Check(<-rm.brokerC.ReplReqCh, gc.IsNil)
		rm.brokerC.ErrCh <- nil // |brokerA| has already closed.
	}()

	c.Check(pln.synchronize(), gc.IsNil)
	c.Check(pln.readThroughRev, gc.Equals, int64(0))

	// Next round. This time, the pipeline is closed and readThroughRev is set.
	c.Check(pln.synchronize(), gc.IsNil)
	c.Check(pln.readThroughRev, gc.Equals, int64(4567))

	// Next round with new pipeline. Peer returns an error, and it's passed through.
	pln = rm.newPipeline(rm.header(0, 100))
	c.Check(pln.synchronize(), gc.ErrorMatches, `recv from zone:"A" suffix:"1" : rpc error: .*`)
}

type replicationMock struct {
	testSpoolObserver
	ctx    context.Context
	cancel context.CancelFunc

	brokerA, brokerC *teststub.Broker

	spoolCh chan fragment.Spool
}

func newReplicationMock(c *gc.C) *replicationMock {
	var ctx, cancel = context.WithCancel(context.Background())
	var brokerA, brokerC = teststub.NewBroker(c, ctx), teststub.NewBroker(c, ctx)

	var m = &replicationMock{
		ctx:     ctx,
		cancel:  cancel,
		brokerA: brokerA,
		brokerC: brokerC,
		spoolCh: make(chan fragment.Spool, 1),
	}
	m.spoolCh <- fragment.NewSpool("a/journal", m)

	return m
}

func (m *replicationMock) header(id int, rev int64) *pb.Header {
	var hdr = &pb.Header{
		Route: pb.Route{
			Primary: 1,
			Members: []pb.ProcessSpec_ID{
				{Zone: "A", Suffix: "1"},
				{Zone: "B", Suffix: "2"},
				{Zone: "C", Suffix: "3"},
			},
			Endpoints: []pb.Endpoint{
				m.brokerA.Endpoint(),
				pb.Endpoint("http://[100::]"),
				m.brokerC.Endpoint(),
			},
		},
		Etcd: pb.Header_Etcd{
			ClusterId: 12,
			MemberId:  34,
			Revision:  rev,
			RaftTerm:  78,
		},
	}
	hdr.ProcessId = hdr.Route.Members[id]
	return hdr
}

func (m *replicationMock) newPipeline(hdr *pb.Header) *pipeline {
	return newPipeline(m.ctx, *hdr, <-m.spoolCh, m.spoolCh, m.brokerA.MustClient())
}

type testSpoolObserver struct {
	commits   []fragment.Fragment
	completes []fragment.Spool
}

func (o *testSpoolObserver) SpoolCommit(f fragment.Fragment) { o.commits = append(o.commits, f) }
func (o *testSpoolObserver) SpoolComplete(s fragment.Spool, _ bool) {
	o.completes = append(o.completes, s)
}

var _ = gc.Suite(&PipelineSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
