package v3_allocator

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"
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
// (eg, due to a former lease timeout) or the Context is cancelled.
func Announce(ctx context.Context, etcd *clientv3.Client, key, value string,
	lease clientv3.LeaseID) (*Announcement, error) {

	for {
		var resp, err = etcd.Txn(ctx).
			If(clientv3.Compare(clientv3.Version(key), "=", 0)).
			Then(clientv3.OpPut(key, value, clientv3.WithLease(lease))).
			Commit()

		if err == nil && resp.Succeeded == false {
			err = fmt.Errorf("key exists")
		}

		if err == nil {
			return &Announcement{
				Key:      key,
				Revision: resp.Header.Revision,
				etcd:     etcd,
			}, nil
		}

		log.WithFields(log.Fields{"err": err, "key": key}).
			Warn("failed to announce key (will retry)")

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(announceConflictRetryInterval):
			// Pass.
		}
	}
}

// Update the value of a current Announcement.
func (a *Announcement) Update(ctx context.Context, value string) error {
	var resp, err = a.etcd.Txn(ctx).
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

var announceConflictRetryInterval = time.Second * 10
