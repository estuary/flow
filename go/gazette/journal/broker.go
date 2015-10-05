package journal

import (
	"errors"
	"io"

	log "github.com/Sirupsen/logrus"

	"github.com/pippio/api-server/varz"
)

const (
	kSpoolRollSize   = 1 << 30
	kCommitThreshold = 1 << 20

	AppendOpBufferSize = 100
)

// BrokerConfig is used to periodically update Broker with updated
// cluster topology and replication configuration.
type BrokerConfig struct {
	// Replica instances which should be involved in brokered transactions.
	Replicas []Replicator
	// Token representing the Broker's view of the current replication topology.
	// Sent with replication requests and verified for consensus by each remote
	// replica: for a transaction to succeed, all replicas must agree on the
	// current |RouteToken|.
	RouteToken string
	// Next offset of the next brokered write transaction. Also sent with
	// replication requests and verifed for consensus by each remote replica:
	// for a transaction to succeed, all replicas must agree on the |WriteHead|.
	WriteHead int64
	// Number of bytes written since the last spool roll.
	writtenSinceRoll int64
}

type Broker struct {
	journal Name

	appendOps chan AppendOp

	configUpdates chan BrokerConfig
	config        BrokerConfig

	commitBytes *varz.Count
	coalesce    *varz.Average

	stop chan struct{}
}

func NewBroker(journal Name) *Broker {
	b := &Broker{
		journal:       journal,
		appendOps:     make(chan AppendOp, AppendOpBufferSize),
		configUpdates: make(chan BrokerConfig, 1),
		commitBytes:   varz.ObtainCount("gazette", "commitBytes"),
		coalesce:      varz.ObtainAverage("gazette", "coalesce"),
		stop:          make(chan struct{}),
	}
	return b
}

func (b *Broker) StartServingOps(writeHead int64) *Broker {
	b.config.WriteHead = writeHead
	go b.loop()
	return b
}

func (b *Broker) Append(op AppendOp) {
	b.appendOps <- op
}

func (b *Broker) UpdateConfig(config BrokerConfig) {
	b.configUpdates <- config
}

func (b *Broker) Stop() {
	close(b.appendOps)
	close(b.configUpdates)
	<-b.stop // Blocks until loop() exits.
}

func (b *Broker) loop() {
	for b.configUpdates != nil || b.appendOps != nil {
		// Consume available config updates prior to serving appends.
		select {
		case config, ok := <-b.configUpdates:
			if ok {
				b.onConfigUpdate(config)
				continue
			}
		default:
		}
		select {
		case config, ok := <-b.configUpdates:
			if !ok {
				b.configUpdates = nil
				continue
			}
			b.onConfigUpdate(config)
			continue
		case op, ok := <-b.appendOps:
			if !ok {
				b.appendOps = nil
				continue
			}
			if b.config.writtenSinceRoll > kSpoolRollSize {
				b.config.writtenSinceRoll = 0
			}
			if writers, err := b.phaseOne(); err != nil {
				op.Result <- ErrReplicationFailed

				log.WithField("err", err).Error("transaction failed (phase one)")
			} else if err = b.phaseTwo(writers, op); err != nil {
				log.WithField("err", err).Error("transaction failed (phase two)")
			}
		}
	}
	log.WithField("journal", b.journal).Info("broker exiting")
	close(b.stop)
}

func (b *Broker) onConfigUpdate(config BrokerConfig) {
	log.WithFields(log.Fields{"config": config, "journal": b.journal}).
		Info("updated config")

	b.config.RouteToken = config.RouteToken
	b.config.Replicas = config.Replicas

	if config.WriteHead > b.config.WriteHead {
		b.config.WriteHead = config.WriteHead
	}
	// We zero writtenSinceRoll so that replicas begin new spools after
	// the route configuration changes.
	b.config.writtenSinceRoll = 0
}

// Opens a write-stream with each replica for this transaction.
func (b *Broker) phaseOne() ([]WriteCommitter, error) {
	if len(b.config.Replicas) == 0 {
		return nil, errors.New("no configured replicas")
	}
	// Scatter replication request to each replica.
	results := make(chan ReplicateResult)
	for _, r := range b.config.Replicas {
		r.Replicate(ReplicateOp{
			Journal:    b.journal,
			RouteToken: b.config.RouteToken,
			NewSpool:   b.config.writtenSinceRoll == 0,
			WriteHead:  b.config.WriteHead,
			Result:     results,
		})
	}
	// Gather responses.
	var writers []WriteCommitter
	var err error

	for _ = range b.config.Replicas {
		result := <-results

		if result.Error != nil {
			if result.ErrorWriteHead > b.config.WriteHead {
				b.config.WriteHead = result.ErrorWriteHead
			}
			err = result.Error
		} else {
			writers = append(writers, result.Writer)
		}
	}
	// Require that all replicas accept the transaction.
	if len(writers) != len(b.config.Replicas) {
		scatterCommit(writers, 0) // Tell replicas to abort.
		return nil, err
	} else {
		return writers, nil
	}
}

func (b *Broker) phaseTwo(writers []WriteCommitter, op AppendOp) error {
	var pending []AppendOp

	var commitDelta int64
	var readErr, writeErr error

	buf := make([]byte, 32*1024) // io.Copy's buffer size.

	// Consume waiting AppendOps, streaming them to writers.
	for {
		var readSize int64
		readSize, readErr, writeErr = streamToWriters(writers, op.Content, buf)

		if readErr != nil {
			op.Result <- readErr
		} else {
			// Only commit a complete read from a client.
			commitDelta += readSize
			pending = append(pending, op)
		}
		// Break if any error occurred or we've reached a commit threshold.
		if readErr != nil || writeErr != nil || commitDelta >= kCommitThreshold {
			break
		}
		// Pop another append. Break if the channel blocks or closes.
		var ok bool
		select {
		case op, ok = <-b.appendOps:
		default:
			ok = false
		}
		if !ok {
			break
		}
	}
	// If a write error occurred to any replica, roll back this transaction.
	if writeErr != nil {
		log.WithFields(log.Fields{"err": writeErr, "delta": commitDelta}).
			Warn("aborting transaction due to replica write error")
		commitDelta = 0
	}

	// Scatter / gather to close each writer in parallel.
	// Retain a replica write error, if any occur.
	var sawError error = writeErr
	var sawSuccess bool

	commitErrs := scatterCommit(writers, commitDelta)
	for _ = range writers {
		if err := <-commitErrs; err != nil {
			if sawError == nil {
				sawError = err
			}
			log.WithFields(log.Fields{"err": err, "delta": commitDelta}).
				Warn("reporting failure due to replica commit error")
		} else {
			sawSuccess = true
		}
	}
	// The write head moves forward if at least one replica committed.
	if sawSuccess {
		b.config.WriteHead += commitDelta
		b.config.writtenSinceRoll += int64(commitDelta)
		b.commitBytes.Add(commitDelta)
		b.coalesce.Add(float64(len(pending)))
	}
	if sawError == nil {
		// The transacton was fully replicated. Notify client(s) of success.
		for _, p := range pending {
			p.Result <- nil
		}
		return nil
	} else {
		// At least one replica failed. The client must retry.
		for _, p := range pending {
			p.Result <- ErrReplicationFailed
		}
		return sawError
	}
}

func streamToWriters(dst []WriteCommitter, src io.Reader,
	buf []byte) (written int64, readErr, writeErr error) {
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			for _, w := range dst {
				nw, ew := w.Write(buf[0:nr])
				if ew != nil {
					return written, er, ew
				}
				if nr != nw {
					return written, er, io.ErrShortWrite
				}
			}
			written += int64(nr)
		}
		if er == io.EOF {
			return written, nil, nil
		}
		if er != nil {
			return written, er, nil
		}
	}
}

func scatterCommit(writers []WriteCommitter, delta int64) chan error {
	// Buffer result channel to the number of writers, so goroutines
	// will exit if caller never inspects results.
	closeResults := make(chan error, len(writers))
	for _, w := range writers {
		go func(w WriteCommitter, delta int64) {
			closeResults <- w.Commit(delta)
		}(w, delta)
	}
	return closeResults
}
