package consumer

import (
	"context"
	"encoding/json"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"io/ioutil"
	"strings"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
	"google.golang.org/grpc/codes"
)

type fetchedHints struct {
	log     pb.Journal
	txnResp *clientv3.TxnResponse
	hints   []*recoverylog.FSMHints
}

// fetchHints retrieves and decodes all FSMHints for the ShardSpec.
// Nil values will be returned where hint values have not been written. It also
// returns a TxnResponse holding each of the hints values, which can be used for
// transactional updates of hints.
func fetchHints(ctx context.Context, spec *pc.ShardSpec, etcd *clientv3.Client) (out fetchedHints, err error) {
	var ops = []clientv3.Op{clientv3.OpGet(spec.HintPrimaryKey())}
	for _, hk := range spec.HintBackupKeys() {
		ops = append(ops, clientv3.OpGet(hk))
	}
	out.log = spec.RecoveryLog()

	if out.txnResp, err = etcd.Txn(ctx).If().Then(ops...).Commit(); err != nil {
		err = errors.WithMessage(err, "fetching ShardSpec.HintKeys")
		return
	}

	for i := range out.txnResp.Responses {
		var kvs = out.txnResp.Responses[i].GetResponseRange().Kvs
		if len(kvs) == 0 {
			out.hints = append(out.hints, nil) // No FSMHint at this key.
			continue
		}
		var h = new(recoverylog.FSMHints)

		// Sense whether JSON or proto encoding is used by testing for opening '{'.
		if kvs[0].Value[0] != '{' {
			if err = h.Unmarshal(kvs[0].Value); err != nil {
				err = errors.WithMessage(err, "hints.Unmarshal")
			}
		} else {
			if err = json.Unmarshal(kvs[0].Value, h); err != nil {
				err = errors.WithMessage(err, "json.Unmarshal(hints)")
			}
		}

		if err != nil {
			// Pass.
		} else if _, err = recoverylog.NewFSM(*h); err != nil {
			err = errors.WithMessage(err, "validating FSMHints")
		} else if h.Log != out.log {
			err = errors.Errorf("hints.Log %s != ShardSpec.RecoveryLog %s", h.Log, out.log)
		}

		if err != nil {
			return
		}
		out.hints = append(out.hints, h)
	}
	return
}

// pickFirstHints retrieves the first hints from |f|. If there are no primary
// hints available the most recent backup hints will be returned. If there are
// no hints available an empty set of hints is returned.
func pickFirstHints(f fetchedHints) recoverylog.FSMHints {
	for _, h := range f.hints {
		if h != nil {
			return *h
		}
	}
	return recoverylog.FSMHints{Log: f.log}
}

// storeRecordedHints writes FSMHints into the primary hint key of the spec.
func storeRecordedHints(s *shard, hints recoverylog.FSMHints) error {
	var key = s.Spec().HintPrimaryKey()
	var asn = s.Assignment()

	var val, err = json.Marshal(hints)
	if err != nil {
		return errors.WithMessage(err, "json.Marshal(hints)")
	}
	// TODO(johnny): Switch over to hints.Marshal() when proto decode support is deployed.
	/*
		var val, err = hints.Marshal()
		if val, err = hints.Marshal(); err != nil {
			return errors.WithMessage(err, "hints.Marshal")
		}
	*/

	_, err = s.svc.Etcd.Txn(s.ctx).
		// Verify our Assignment is still in effect (eg, we're still primary), then write |hints| to HintPrimaryKey.
		// Compare CreateRevision to allow for a raced ReplicaState update.
		If(clientv3.Compare(clientv3.CreateRevision(string(asn.Raw.Key)), "=", asn.Raw.CreateRevision)).
		Then(clientv3.OpPut(key, string(val))).
		Commit()

	if etcdErr, ok := err.(rpctypes.EtcdError); ok && etcdErr.Code() == codes.Unavailable {
		// Recorded hints are advisory and can generally tolerate omitted
		// updates. It's also annoying for temporary Etcd partitions to abort
		// an otherwise-fine shard primary. So, log but allow shard processing
		// to continue; we'll retry on the next hints flush interval.
		log.WithFields(log.Fields{"key": key, "err": err}).
			Warn("failed to store recorded FSMHints (will retry)")

	} else if err != nil {
		return err
	}
	return nil
}

// storeRecoveredHints writes the FSMHints into the first backup hint key of the spec,
// rotating hints previously stored under that key to the second backup hint key,
// and so on as a single transaction.
func storeRecoveredHints(s *shard, hints recoverylog.FSMHints) error {
	var (
		spec    = s.Spec()
		asn     = s.Assignment()
		backups = spec.HintBackupKeys()
	)
	var h, err = fetchHints(s.ctx, spec, s.svc.Etcd)
	if err != nil {
		return err
	}

	// |hints| is serialized and written to backups[1]. In the same txn,
	// rotate the current value at backups[1] => backups[2], and so on.
	var val []byte
	if val, err = json.Marshal(hints); err != nil {
		return errors.WithMessage(err, "json.Marshal(hints)")
	}
	// TODO(johnny): Switch over to hints.Marshal() when proto decode support is deployed.
	/*
		if val, err = hints.Marshal(); err != nil {
			return errors.WithMessage(err, "hints.Marshal")
		}
	*/

	var cmp []clientv3.Cmp
	var ops []clientv3.Op

	// The txn responses returned from fetchHints are structured such that the first response will
	// be the primary response and the subsequent responses are the backup responses, this slice
	// represents just the backup responses.
	var backupResponses = h.txnResp.Responses[1:]
	// Verify our Assignment is still in effect (eg, we're still primary).
	cmp = append(cmp, clientv3.Compare(clientv3.CreateRevision(string(asn.Raw.Key)),
		"=", asn.Raw.CreateRevision))

	for i := 0; i != len(backups) && val != nil; i++ {
		ops = append(ops, clientv3.OpPut(backups[i], string(val)))

		if kvs := backupResponses[i].GetResponseRange().Kvs; len(kvs) == 0 {
			// Verify there is still no current key/value at this hints key slot.
			cmp = append(cmp, clientv3.Compare(clientv3.ModRevision(backups[i]), "=", 0))
			val = nil
		} else {
			// Verify the key/value at this hints key slot is unchanged.
			// Retain its value to rotate into the next slot (if one exists).
			cmp = append(cmp, clientv3.Compare(clientv3.ModRevision(backups[i]), "=", kvs[0].ModRevision))
			val = kvs[0].Value
		}
	}

	var resp *clientv3.TxnResponse
	resp, err = s.svc.Etcd.Txn(s.ctx).If(cmp...).Then(ops...).Commit()

	if err == nil && !resp.Succeeded {
		err = errors.New("unexpected Etcd transaction failure")
	}
	return err
}

// beginRecovery fetches and recovers shard FSMHints into a temporary directory.
func beginRecovery(s *shard) error {
	var (
		spec    = s.Spec()
		dir     string
		h       fetchedHints
		logSpec *pb.JournalSpec
		err     error
	)

	if dir, err = ioutil.TempDir("", strings.ReplaceAll(spec.Id.String(), "/", "_")+"-"); err != nil {
		return errors.WithMessage(err, "creating shard working directory")
	} else if h, err = fetchHints(s.ctx, spec, s.svc.Etcd); err != nil {
		return errors.WithMessage(err, "fetchHints")
	} else if logSpec, err = client.GetJournal(s.ctx, s.ajc, pickFirstHints(h).Log); err != nil {
		return errors.WithMessage(err, "fetching log spec")
	} else if ct := logSpec.LabelSet.ValueOf(labels.ContentType); ct != labels.ContentType_RecoveryLog {
		return errors.Errorf("expected label %s value %s (got %v)", labels.ContentType, labels.ContentType_RecoveryLog, ct)
	}

	log.WithFields(log.Fields{
		"dir": dir,
		"log": logSpec.Name,
		"id":  spec.Id,
	}).Info("began recovering shard store from log")

	if err = s.recovery.player.Play(s.ctx, pickFirstHints(h), dir, s.ajc); err != nil {
		return errors.WithMessagef(err, "playing log %s", pickFirstHints(h).Log)
	}
	return nil
}

// completeRecovery injects a new AuthorID into the log to complete playback,
// initializes an Application Store & restores its Checkpoint, and persists
// recovered FSMHints.
func completeRecovery(s *shard) (_ pc.Checkpoint, err error) {
	var (
		recoveredHints recoverylog.FSMHints
		cp             pc.Checkpoint
	)
	if s.recovery.log != "" {

		// Instruct our player to inject a log hand-off to our generated |author|.
		var author = recoverylog.NewRandomAuthor()
		s.recovery.player.InjectHandoff(author)

		select {
		case <-s.recovery.player.Done():
			// Pass.
		case <-s.ctx.Done():
			err = s.ctx.Err()
			return
		}

		var recovered = s.recovery.player.Resolved
		if recovered.FSM == nil {
			err = errors.Errorf("completeRecovery aborting due to log playback failure")
			return
		}

		// We've completed log playback, and we're likely the most recent shard
		// primary to do so. Snapshot our recovered hints. We'll sanity-check that
		// we can open the recovered store & restore its Checkpoint, and only then
		// persist these |recoveredHints|.
		recoveredHints = recovered.FSM.BuildHints(s.recovery.log)

		// Initialize a *Recorder around the recovered file-system. Recorder
		// fences its append operations around |author| so that another process
		// completing InjectHandoff will cause appends of this Recorder to fail.
		s.recovery.recorder = recoverylog.NewRecorder(
			s.recovery.log, recovered.FSM, author, recovered.Dir, s.ajc)
	}

	if s.store, err = s.svc.App.NewStore(s, s.recovery.recorder); err != nil {
		return cp, errors.WithMessage(err, "app.NewStore")
	} else if cp, err = s.store.RestoreCheckpoint(s); err != nil {
		return cp, errors.WithMessage(err, "store.RestoreCheckpoint")
	}

	if s.recovery.log != "" {
		for i := 0; true; i++ {
			if err = storeRecoveredHints(s, recoveredHints); err == nil {
				break
			}

			// Storing recovered hints can sometimes fail, eg due to a
			// shard assignment race.
			log.WithFields(log.Fields{
				"err": err,
				"log": recoveredHints.Log,
			}).Warn("failed to store recovered hints (will retry)")

			select {
			case <-s.ctx.Done():
				return cp, s.ctx.Err()
			case <-time.After(backoff(i)):
				// Pass.
			}
		}
	}

	s.publisher = message.NewPublisher(s.ajc, &s.clock)
	s.sequencer = message.NewSequencer(pc.FlattenProducerStates(cp), messageRingSize)
	close(s.storeReadyCh) // Unblocks Resolve().

	return cp, nil
}
