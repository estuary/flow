package allocator

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.gazette.dev/core/task"
)

// Announcement manages a unique key which is "announced" to peers through Etcd,
// with an associated lease and a value which may be updated over time. It's
// useful for managing keys which simultaneously represent semantics of existence,
// configuration, and processing live-ness (such as allocator member keys).
type Announcement struct {
	Key      string
	Revision int64

	etcd *clientv3.Client
}

// Announce a key and value to etcd under the LeaseID, asserting the key doesn't
// already exist. If the key does exist, Announce will retry until it disappears
// (eg, due to a former lease timeout).
func Announce(etcd *clientv3.Client, key, value string, lease clientv3.LeaseID) *Announcement {
	for {
		var resp, err = etcd.Txn(context.Background()).
			If(clientv3.Compare(clientv3.Version(key), "=", 0)).
			Then(clientv3.OpPut(key, value, clientv3.WithLease(lease))).
			Else(clientv3.OpGet(key)).
			Commit()

		if err == nil {
			if resp.Succeeded {
				// Key was created successfully
				return &Announcement{
					Key:      key,
					Revision: resp.Header.Revision,
					etcd:     etcd,
				}
			} else {
				// Key exists, check if it's ours
				var kv = resp.Responses[0].GetResponseRange().Kvs[0]
				if clientv3.LeaseID(kv.Lease) == lease {
					// This is our key from a previous attempt that succeeded
					return &Announcement{
						Key:      key,
						Revision: kv.ModRevision,
						etcd:     etcd,
					}
				}
				// Key exists with different lease, will retry
				err = fmt.Errorf("key exists with different lease")
			}
		}

		log.WithFields(log.Fields{"err": err, "key": key}).
			Warn("failed to announce key (will retry)")

		time.Sleep(announceConflictRetryInterval)
	}
}

// Update the value of a current Announcement.
func (a *Announcement) Update(value string) error {
	var resp, err = a.etcd.Txn(context.Background()).
		If(clientv3.Compare(clientv3.ModRevision(a.Key), "=", a.Revision)).
		Then(clientv3.OpPut(a.Key, value, clientv3.WithIgnoreLease())).
		Commit()

	if err == nil && resp.Succeeded == false {
		err = fmt.Errorf("key modified or deleted externally (expected revision %d)", a.Revision)
	}
	if err == nil {
		a.Revision = resp.Header.Revision
	}
	return err
}

// SessionArgs are arguments of StartSession.
type SessionArgs struct {
	Etcd  *clientv3.Client
	Tasks *task.Group
	Spec  interface {
		Validate() error
		SetExiting()
		MarshalString() string
	}
	State    *State
	LeaseTTL time.Duration
	SignalCh <-chan os.Signal
	TestHook func(round int, isIdle bool)
}

// StartSession starts an allocator session. It:
//   - Validates the MemberSpec.
//   - Establishes an Etcd lease which conveys "liveness" of this member to its peers.
//   - Announces the MemberSpec under the lease.
//   - Loads the KeySpace as-of the announcement revision.
//   - Queues tasks to the *task.Group which:
//   - Closes the Etcd lease on task.Group cancellation.
//   - Monitors SignalCh and marks the MemberSpec as exiting on its signal.
//   - Runs the Allocate loop, cancelling the *task.Group on completion.
func StartSession(args SessionArgs) error {
	if err := args.Spec.Validate(); err != nil {
		return errors.WithMessage(err, "spec.Validate")
	}
	var lease, err = concurrency.NewSession(args.Etcd, concurrency.WithTTL(int(args.LeaseTTL.Seconds())))
	if err != nil {
		return errors.WithMessage(err, "establishing Etcd lease")
	}

	// Close |lease| when the task.Group is cancelled, or cancel the task.Group
	// if the lease fails to keep-alive within its deadline.
	args.Tasks.Queue("lease.Close", func() error {
		select {
		case <-args.Tasks.Context().Done():
			return lease.Close()
		case <-lease.Done():
			return errors.New("unable to keep member lease alive")
		}
	})

	var ann = Announce(args.Etcd, args.State.LocalKey, args.Spec.MarshalString(), lease.Lease())

	// Initialize the KeySpace at the announcement revision.
	if err = args.State.KS.Load(context.Background(), args.Etcd, ann.Revision); err != nil {
		return errors.WithMessage(err, "loading KeySpace")
	}

	// Monitor |SignalCh|, and mark member as exiting if it triggers.
	args.Tasks.Queue("mark member exiting on signal", func() error {
		select {
		case sig := <-args.SignalCh:
			log.WithField("signal", sig).Info("caught signal")
		case <-args.Tasks.Context().Done():
			return nil
		}

		// Mark as exiting in Etcd. Upon seeing this, Allocator will work to
		// discharge our assigned items based on available capacity, and
		// Allocate will exit gracefully when none remain.
		args.Spec.SetExiting()
		return ann.Update(args.Spec.MarshalString())
	})

	// Serve the allocation loop. Cancel the task.Group when it finishes.
	args.Tasks.Queue("Allocate", func() error {
		defer args.Tasks.Cancel()

		var err = Allocate(AllocateArgs{
			Context:  args.Tasks.Context(),
			Etcd:     args.Etcd,
			State:    args.State,
			TestHook: args.TestHook,
		})
		if errors.Cause(err) == context.Canceled {
			err = nil
		}
		return err
	})

	return nil
}

var announceConflictRetryInterval = time.Second * 10
