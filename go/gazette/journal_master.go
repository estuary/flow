package gazette

import (
	"bytes"
	"errors"
	log "github.com/Sirupsen/logrus"
	"io"
	"net/http"
	"sync"
)

const (
	kCommitThreshold = 1 << 21 // 2 MB.
)

type Request struct {
	Request  *http.Request
	Response chan error
}

var RequestPool = sync.Pool{New: func() interface{} {
	return Request{Response: make(chan error, 1)}
}}

func (r Request) AtomicLoadToBuffer(buffer *bytes.Buffer) bool {
	buffer.Reset()

	_, err := io.Copy(buffer, r.Request.Body)
	if err != nil {
		r.Response <- err
		return false
	} else {
		r.Request.Body.Close()
		return true
	}
}

type JournalMaster struct {
	Name  string
	Index *FragmentIndex

	AppendRequests chan Request
}

func NewJournalMaster(name string, index *FragmentIndex) *JournalMaster {
	return &JournalMaster{
		Name:           name,
		Index:          index,
		AppendRequests: make(chan Request),
	}
}

func (j *JournalMaster) Serve() {
	var ok bool = true
	var request Request

	j.Index.FinishCurrentSpool()
	log.Info("finished current spool")

	for ok {
		log.Info("waiting for next transaction")
		request, ok = <-j.AppendRequests
		log.Info("popped request")

		if ok {
			j.Index.InvokeWithSpool(func(spool *Spool) {
				log.Info("got spool")
				j.masterTransaction(spool, request)
			})
		}
	}
}

// * Issue blocking read of Append or Transaction request.
//
// * Validate ring shape for the operation (done by service).
// * If master, open Transactions and get 100-continue
// * If replica, validate against known fragments and send 100-continue
//
// * Open / roll / assert a spool.
//
// * If Transaction request, stream in & commit.
// * If master, non-blocking read of Append requests.

func (j *JournalMaster) masterTransaction(spool *Spool, request Request) {

	// Open Transaction streams.
	// Wait for 100-continue.

	// Non-blocking read & stream of append requests.
	var pendingResponses []chan error
	var buffer bytes.Buffer

	var transactionBytes int

	// Pop pending requests until none remain, an error occurs,
	// or we reach a transaction threshold.
	for ok := true; ok && transactionBytes < kCommitThreshold; {
		if request.AtomicLoadToBuffer(&buffer) {
			pendingResponses = append(pendingResponses, request.Response)

			// TODO(johnny): Stream to replicas. Retain any error to check later.

			_ = spool.Write(buffer.Bytes()) // Error is checked later.
			transactionBytes += len(buffer.Bytes())
		}

		// Break if a) the channel blocks, or b) the channel closes.
		select {
		case request, ok = <-j.AppendRequests:
		default:
			ok = false
		}
	}

	var replicaSuccessCount int

	// Close replica request bodies.
	// Update index from response fragments (which can move the
	// log head even if we fail to commit).
	// Retain any error for later.

	if err := spool.Commit(); err != nil {
		log.WithField("err", err).Error("failed to write transaction")
	} else {
		replicaSuccessCount += 1
	}

	// Receive replica responses.

	// Inform clients of transaction status. Only respond with success to clients
	// if >= N succesful responses were received.
	for _, pendingResponse := range pendingResponses {
		if replicaSuccessCount >= 1 {
			pendingResponse <- nil
		} else {
			pendingResponse <- errors.New("transaction failed; retry")
		}
	}
}

//func (j *Journal) replicaTransaction(spool *Spool, replicaRequest Request) {

// Validate ring shape for the operation.
// Validate request against known fragments.
// Send 100-continue.

//}

/*
// Serialized.
func (j *Journal) servePuts(spool *Spool) {
	var requests []PutRequest
	var buffer bytes.Buffer

	// TODO(johnny): Begin replica stream requests. These requests
	// capture the journal name and the beginning byte offset. Replica
	// streams cascade from one replica to the next, and each checks:
	//  * The correctness of the ring arrangement.
	//  * That the beginning byte offset is larger than any known fragment.
	//
	// Use Expect-Continue to validate preconditions before pulling client
	// requests?
	//
	// If the ring is incorrect, the replica reports an error as such.
	// If the master attempts to use an invalid offset, the replica responds
	// with the fragment entry which invalidated the offset.
	//
	// When the request stream completes successfully, the replica attempts to
	// commit and returns the result fragment in the response.
	//
	// TODO(johnny): Concurrent 2PC or streamed through the ring?

	// Pop pending puts until none remain, an error occurs,
	// or we reach a threshold.
	for done := false; !done &&
		spool.Error == nil &&
		spool.UncommittedBytes() < kCommitThreshold; {

		select {
		case putRequest := <-j.PutRequests:
			if putRequest.AtomicLoadToBuffer(&buffer) {
				requests = append(requests, putRequest)

				// TODO(johnny): Stream to replica.

				spool.Write(buffer.Bytes())
			}
		default:
			done = true
		}
	}

	if err := spool.Commit(); err != nil {
		// Inform clients of failure.
		for _, putRequest := range requests {
			http.Error(putRequest.ResponseWriter, http.StatusInternalServerError,
				err.Error())
		}
	} else {
		// Inform clients of success.
		for _, putRequest := range requests {
			http.Error(putRequest.ResponseWriter, http.StatusNoContent, "")
		}
	}
}





*/
