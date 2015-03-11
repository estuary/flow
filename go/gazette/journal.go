package gazette

import (
	"bytes"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/pippio/api-server/logging"
	"io"
	"net/http"
)

const (
	kCommitThreshold = 1 << 21 // 2 MB.
)

type Subscriber struct {
	Name   string
	Offset int64
	Out    http.ResponseWriter
}

type Request struct {
	Request        *http.Request
	ResponseWriter http.ResponseWriter
}

func (r Request) AtomicLoadToBuffer(buffer *bytes.Buffer) bool {
	buffer.Reset()

	_, err := io.Copy(buffer, r.Request.Body)
	if err != nil {
		http.Error(r.ResponseWriter, err.Error(), http.StatusBadRequest)
		return false
	} else {
		r.Request.Body.Close()
		return true
	}
}

type Fragment struct {
	Begin, End int64
	SHA1Sum    []byte
}

func (f Fragment) FormattedName() string {
	return fmt.Sprintf("%16x-%16x-%x", f.Begin, f.End, f.SHA1Sum)
}

type Journal struct {
	Name string

	// Offset of next byte to be written in the journal.
	Head int64

	// Ordered on |End, Begin| ascending.
	//GCSFragments []Fragment
	// Ordered on |LastCommit, Begin| ascending.
	//Spools []*Spool
	Spool *Spool

	Appends   chan Request
	Replicate chan Request

	GCSContext *logging.GCSContext
}

func NewJournal(name string, context *logging.GCSContext) *Journal {
	return &Journal{
		Name:       name,
		GCSContext: context,
	}
}

func (j *Journal) obtainSpool() *Spool {
	if j.Spool == nil {
		j.Spool = NewSpool("/tmp/log-spools", j.Name, 0)
	}
	return j.Spool
}

func (j *Journal) iteration() {
	select {
	case appendRequest := <-j.Appends:
		j.masterTransaction(j.obtainSpool(), appendRequest)
	}

	// * Issue blocking read of Append or Transaction request.
	//
	// * Validate ring shape for the operation.
	// * If master, open Transactions and get 100-continue
	// * If replica, validate against known fragments and send 100-continue
	//
	// * Open / roll / assert a spool.
	//
	// * If Transaction request, stream in & commit.
	// * If master, non-blocking read of Append requests.
}

func (j *Journal) masterTransaction(spool *Spool, appendRequest Request) {

	// Validate ring shape for the operation.
	// Open Transaction streams.
	// Wait for 100-continue.

	// Non-blocking read & stream of append requests.
	var pendingResponses []Request
	var buffer bytes.Buffer

	var transactionBytes int

	// Pop pending Appends until none remain, an error occurs,
	// or we reach a transaction threshold.
	for done := false; !done && transactionBytes < kCommitThreshold; {
		if appendRequest.AtomicLoadToBuffer(&buffer) {
			pendingResponses = append(pendingResponses, appendRequest)

			// TODO(johnny): Stream to replicas. Retain any error to check later.

			_ = spool.Write(buffer.Bytes()) // Error is checked later.
			transactionBytes += len(buffer.Bytes())
		}

		select {
		case appendRequest = <-j.Appends:
		default:
			done = true
		}
	}

	var replicaSuccessCount int

	// Close replica request bodies. Retain any error for later.

	if err := spool.Commit(); err != nil {
		log.WithField("err", err).Error("failed to write log transaction")
	} else {
		replicaSuccessCount += 1
	}

	// Receive replica responses.

	// If *any* replica committed the transaction, move the log head forward.
	// This does not imply the transaction succeeded (with success reported to
	// the client).
	if replicaSuccessCount >= 1 {
		j.Head += int64(transactionBytes)
	}

	// Inform clients of transaction status. Only respond with success to clients
	// if >= N succesful responses were received.
	for _, putRequest := range pendingResponses {
		if replicaSuccessCount >= 1 {
			http.Error(putRequest.ResponseWriter, "OK", http.StatusOK)
		} else {
			http.Error(putRequest.ResponseWriter, "transaction failed; retry",
				http.StatusInternalServerError)
		}
	}
}

func (j *Journal) replicaTransaction(spool *Spool, replicaRequest Request) {

	// Validate ring shape for the operation.
	// Validate request against known fragments.
	// Send 100-continue.

}

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

func (j *Journal) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	j.Appends <- Request{r, w}
}
