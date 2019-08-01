package broker

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/teststub"
)

func TestPipelineBasicLifeCycle(t *testing.T) {
	var ctx, rm = context.Background(), newReplicationMock(t)
	defer rm.cleanup()

	var pln = rm.newPipeline(ctx, rm.header(0, 100))

	var req = &pb.ReplicateRequest{Content: []byte("foobar")}
	pln.scatter(req)

	assert.NoError(t, pln.sendErr())
	assert.Equal(t, req, <-rm.brokerA.ReplReqCh)
	assert.Equal(t, req, <-rm.brokerC.ReplReqCh)

	var proposal = pln.spool.Next()
	req = &pb.ReplicateRequest{Proposal: &proposal}
	pln.scatter(req)

	assert.NoError(t, pln.sendErr())
	assert.Equal(t, req, <-rm.brokerA.ReplReqCh)
	assert.Equal(t, req, <-rm.brokerC.ReplReqCh)

	var waitFor1, closeAfter1 = pln.barrier()

	// Second client issues a write and close.
	pln.scatter(&pb.ReplicateRequest{Content: []byte("bazbing")})
	_, _ = <-rm.brokerA.ReplReqCh, <-rm.brokerC.ReplReqCh
	pln.closeSend()

	assert.NoError(t, pln.sendErr())
	assert.Nil(t, <-rm.brokerA.ReplReqCh) // EOF.
	assert.Nil(t, <-rm.brokerC.ReplReqCh) // EOF.

	var waitFor2, closeAfter2 = pln.barrier()

	// First client reads its response.
	<-waitFor1

	rm.brokerA.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}
	rm.brokerC.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}

	pln.gatherOK()
	assert.NoError(t, pln.recvErr())
	assert.Equal(t, []pb.ReplicateResponse{{}, {}, {}}, pln.recvResp)

	close(closeAfter1)

	// Second client reads its response.
	<-waitFor2

	rm.brokerA.ErrCh <- nil // Send EOF.
	rm.brokerC.ErrCh <- nil // Send EOF.

	pln.gatherEOF()
	assert.NoError(t, pln.recvErr())

	close(closeAfter2)
}

func TestPipelinePeerErrorCases(t *testing.T) {
	var ctx, rm = context.Background(), newReplicationMock(t)
	defer rm.cleanup()

	var pln = rm.newPipeline(ctx, rm.header(0, 100))

	var req = &pb.ReplicateRequest{Content: []byte("foo")}
	pln.scatter(req)

	assert.NoError(t, pln.sendErr())
	assert.Equal(t, req, <-rm.brokerA.ReplReqCh)
	assert.Equal(t, req, <-rm.brokerC.ReplReqCh)

	// Have peer A return an error. Peer B returns a non-OK response status (where OK is expected).
	rm.brokerA.ErrCh <- errors.New("error!")
	rm.brokerC.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_FRAGMENT_MISMATCH}

	// Expect pipeline retains the first recv error for each peer.
	pln.gatherOK()
	assert.EqualError(t, pln.recvErrs[0], `rpc error: code = Unknown desc = error!`)
	assert.NoError(t, pln.recvErrs[1])
	assert.EqualError(t, pln.recvErrs[2], `unexpected !OK response: status:FRAGMENT_MISMATCH `)

	// Expect recvErr decorates the first error with peer metadata.
	assert.Regexp(t, `recv from zone:"A" suffix:"1" : rpc error: .*`, pln.recvErr())

	// Scatter a ReplicateRequest to each peer.
	req = &pb.ReplicateRequest{Content: []byte("bar"), ContentDelta: 99999} // Invalid ContentDelta.
	pln.scatter(req)

	// Expect pipeline retains the first send error for each peer, including the local Spool.
	// |rm.brokerA|'s stream is already closed, and the attempted send will error with EOF
	// (non-standard, but just how gRPC does it). The request is immediately applied to the
	// local Spool during scatter(), and we expect its error is tracked on |sendErrs|. No
	// error occurs on send to|rm.brokerC| (though its |recvErr| is still set).
	assert.Equal(t, io.EOF, pln.sendErrs[0])
	assert.EqualError(t, pln.sendErrs[1], `invalid ContentDelta (99999; expected 3)`)
	assert.NoError(t, pln.sendErrs[2])

	assert.Equal(t, req, <-rm.brokerC.ReplReqCh)

	// Expect sendErr decorates the first error with peer metadata.
	assert.EqualError(t, pln.sendErr(), `send to zone:"A" suffix:"1" : EOF`)

	pln.closeSend()

	// Finish shutdown by having brokerC receive and send EOF.
	assert.Nil(t, <-rm.brokerC.ReplReqCh)
	rm.brokerC.ErrCh <- nil
	pln.gatherEOF()

	// Restart a new pipeline. Immediately send an EOF, and test handling of
	// an unexpected received message prior to peer EOF.
	pln = rm.newPipeline(ctx, rm.header(0, 100))
	pln.closeSend()

	assert.Nil(t, <-rm.brokerA.ReplReqCh) // Read EOF.
	assert.Nil(t, <-rm.brokerC.ReplReqCh) // Read EOF.

	rm.brokerA.ErrCh <- nil                                                       // Send EOF.
	rm.brokerC.ReplRespCh <- &pb.ReplicateResponse{Status: pb.Status_WRONG_ROUTE} // Unexpected response.
	rm.brokerC.ErrCh <- nil                                                       // Now, send EOF.

	pln.gatherEOF()
	assert.NoError(t, pln.recvErrs[0])
	assert.NoError(t, pln.recvErrs[1])
	assert.EqualError(t, pln.recvErrs[2], `unexpected response: status:WRONG_ROUTE `)
}

func TestPipelineGatherSyncCases(t *testing.T) {
	var ctx, rm = context.Background(), newReplicationMock(t)
	defer rm.cleanup()

	var pln = rm.newPipeline(ctx, rm.header(0, 100))

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
	assert.Equal(t, req, <-rm.brokerA.ReplReqCh)
	req.Header = rm.header(2, 100)
	assert.Equal(t, req, <-rm.brokerC.ReplReqCh)

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
	var rollToOffset, readRev = pln.gatherSync()
	assert.Equal(t, int64(800), rollToOffset)
	assert.Equal(t, int64(4567), readRev)
	assert.NoError(t, pln.recvErr())
	assert.NoError(t, pln.sendErr())

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

	rollToOffset, readRev = pln.gatherSync()
	assert.Equal(t, int64(890), rollToOffset)
	assert.Equal(t, int64(0), readRev)
	assert.NoError(t, pln.recvErr())
	assert.NoError(t, pln.sendErr())

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

	rollToOffset, readRev = pln.gatherSync()
	assert.Equal(t, int64(0), rollToOffset)
	assert.Equal(t, int64(0), readRev)
	assert.NoError(t, pln.recvErr())
	assert.NoError(t, pln.sendErr())

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
	rm.brokerA.ErrCh <- nil
	rm.brokerC.ErrCh <- nil

	rollToOffset, readRev = pln.gatherSync()
	assert.Equal(t, int64(0), rollToOffset)
	assert.Equal(t, int64(0), readRev)
	assert.NoError(t, pln.sendErr())
	assert.Error(t, pln.recvErr())

	assert.Regexp(t, `unexpected WRONG_ROUTE: process_id:.*`, pln.recvErrs[0])
	assert.NoError(t, pln.recvErrs[1])
	assert.EqualError(t, pln.recvErrs[2], `unexpected FRAGMENT_MISMATCH: begin:567 end:890 sum:<> `)
}

type replicationMock struct {
	testSpoolObserver
	brokerA, brokerC *teststub.Broker
	spoolCh          chan fragment.Spool
}

func newReplicationMock(t assert.TestingT) *replicationMock {
	var brokerA, brokerC = teststub.NewBroker(t), teststub.NewBroker(t)

	var m = &replicationMock{
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

func (m *replicationMock) newPipeline(ctx context.Context, hdr *pb.Header) *pipeline {
	return newPipeline(ctx, *hdr, <-m.spoolCh, m.spoolCh, m.brokerA.Client())
}

func (m *replicationMock) cleanup() {
	m.brokerA.Cleanup()
	m.brokerC.Cleanup()
}

type testSpoolObserver struct {
	commits   []fragment.Fragment
	completes []fragment.Spool
}

func (o *testSpoolObserver) SpoolCommit(f fragment.Fragment) { o.commits = append(o.commits, f) }
func (o *testSpoolObserver) SpoolComplete(s fragment.Spool, _ bool) {
	o.completes = append(o.completes, s)
}
