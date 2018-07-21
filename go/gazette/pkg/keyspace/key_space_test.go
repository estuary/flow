package keyspace

import (
	"context"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	epb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/integration"
	gc "github.com/go-check/check"
)

type KeySpaceSuite struct{}

func (s *KeySpaceSuite) TestLoadAndWatch(c *gc.C) {
	var client = etcdCluster.RandClient()
	var ctx, cancel = context.WithCancel(context.Background())

	var _, err = client.Delete(ctx, "", clientv3.WithPrefix())
	c.Assert(err, gc.IsNil)

	// Fix some initial keys and values.
	_, err = client.Put(ctx, "/one", "1")
	c.Assert(err, gc.IsNil)
	_, err = client.Put(ctx, "/foo", "invalid value is logged and skipped")
	c.Assert(err, gc.IsNil)
	resp, err := client.Put(ctx, "/three", "3")
	c.Assert(err, gc.IsNil)
	_, err = client.Put(ctx, "/raced", "999")
	c.Assert(err, gc.IsNil)

	var ks = NewKeySpace("/", testDecoder)

	// Install an Observer and fix |obvCh| to be signaled on each call.
	var expectObserverCallCh = make(chan struct{}, 1)
	ks.Observers = append(ks.Observers, func() { expectObserverCallCh <- struct{}{} })

	// Load a Revision which is in the past (and doesn't yet reflect "/raced").
	c.Check(ks.Load(ctx, client, resp.Header.Revision), gc.IsNil)
	verifyDecodedKeyValues(c, ks.KeyValues, map[string]int{"/one": 1, "/three": 3})
	<-expectObserverCallCh

	go func() {
		for _, op := range []clientv3.Op{
			clientv3.OpPut("/two", "2"),
			clientv3.OpPut("/bar", "invalid key/value is also logged and skipped"),
			clientv3.OpDelete("/one"),
			clientv3.OpPut("/three", "4"),
			clientv3.OpPut("/foo", "5"), // Formerly invalid key/value is now consistent.
		} {
			var _, err = client.Do(ctx, op)
			c.Check(err, gc.IsNil)

			<-expectObserverCallCh
		}
		cancel()
	}()

	c.Check(ks.Watch(ctx, client), gc.Equals, context.Canceled)

	verifyDecodedKeyValues(c, ks.KeyValues,
		map[string]int{"/two": 2, "/three": 4, "/foo": 5, "/raced": 999})
}

func (s *KeySpaceSuite) TestHeaderPatching(c *gc.C) {
	var h epb.ResponseHeader

	var other = epb.ResponseHeader{
		ClusterId: 8675309,
		MemberId:  111111,
		Revision:  123,
		RaftTerm:  232323,
	}
	c.Check(patchHeader(&h, other, true), gc.IsNil)
	c.Check(h, gc.Equals, other)

	other.MemberId = 222222
	c.Check(patchHeader(&h, other, true), gc.IsNil)
	c.Check(h, gc.Equals, other)

	// Revision must be equal.
	other.Revision = 122
	c.Check(patchHeader(&h, other, true), gc.ErrorMatches,
		`etcd Revision mismatch \(expected = 123, got 122\)`)

	other.Revision = 124
	other.MemberId = 333333
	other.RaftTerm = 3434343
	c.Check(patchHeader(&h, other, false), gc.IsNil)
	c.Check(h, gc.Equals, other)

	// Revision must be monotonically increasing.
	c.Check(patchHeader(&h, other, false), gc.ErrorMatches,
		`etcd Revision mismatch \(expected > 124, got 124\)`)

	// ClusterId cannot change.
	other.Revision = 125
	other.ClusterId = 1337
	c.Check(patchHeader(&h, other, false), gc.ErrorMatches,
		`etcd ClusterID mismatch \(expected 8675309, got 1337\)`)
}

func (s *KeySpaceSuite) TestWatchResponseApply(c *gc.C) {
	var ks = NewKeySpace("/", testDecoder)

	var resp = []clientv3.WatchResponse{{
		Header: epb.ResponseHeader{ClusterId: 9999, Revision: 10},
		Events: []*clientv3.Event{
			putEvent("/some/key", "99", 10, 10, 1),
			putEvent("/other/key", "100", 10, 10, 1),
		},
	}}

	c.Check(ks.Apply(resp...), gc.IsNil)
	verifyDecodedKeyValues(c, ks.KeyValues,
		map[string]int{"/some/key": 99, "/other/key": 100})

	// Key/value inconsistencies are logged but returned as an error.
	resp = []clientv3.WatchResponse{{
		Header: epb.ResponseHeader{ClusterId: 9999, Revision: 11},
		Events: []*clientv3.Event{
			putEvent("/some/key", "101", 10, 11, 2),
			delEvent("/not/here", 11),
		},
	}}
	c.Check(ks.Apply(resp...), gc.IsNil)
	verifyDecodedKeyValues(c, ks.KeyValues,
		map[string]int{"/some/key": 101, "/other/key": 100})

	// Header inconsistencies fail the apply.
	resp = []clientv3.WatchResponse{{
		Header: epb.ResponseHeader{ClusterId: 10000, Revision: 12},
		Events: []*clientv3.Event{
			delEvent("/not/here", 11),
		},
	}}
	c.Check(ks.Apply(resp...), gc.ErrorMatches, `etcd ClusterID mismatch .*`)

	// Multiple WatchResponses may be applied at once. Keys may be in any order,
	// and mutated multiple times within the batch apply.
	resp = []clientv3.WatchResponse{
		{
			Header: epb.ResponseHeader{ClusterId: 9999, Revision: 13},
			Events: []*clientv3.Event{
				putEvent("/aaaa", "1111", 12, 12, 1),
				putEvent("/bbbb", "2222", 12, 12, 1),
				putEvent("/cccc", "invalid", 12, 12, 1),
				putEvent("/to-delete", "0000", 12, 12, 1),
				putEvent("/bbbb", "3333", 12, 13, 2),
				putEvent("/cccc", "4444", 12, 13, 2),
			},
		},
		{
			Header: epb.ResponseHeader{ClusterId: 9999, Revision: 14},
			Events: []*clientv3.Event{
				putEvent("/aaaa", "5555", 12, 14, 2),
				delEvent("/to-delete", 14),
			},
		},
		{
			Header: epb.ResponseHeader{ClusterId: 9999, Revision: 15},
			Events: []*clientv3.Event{},
		},
		{
			Header: epb.ResponseHeader{ClusterId: 9999, Revision: 16},
			Events: []*clientv3.Event{
				putEvent("/bbbb", "6666", 12, 16, 3),
				putEvent("/eeee", "7777", 16, 16, 1),
			},
		},
	}
	c.Check(ks.Apply(resp...), gc.IsNil)

	// A ProgressNotify WatchResponse moves the Revision forward.
	c.Check(ks.Apply(clientv3.WatchResponse{
		Header: epb.ResponseHeader{ClusterId: 9999, Revision: 20},
		Events: []*clientv3.Event{},
	}), gc.IsNil)
	c.Check(ks.Header.Revision, gc.Equals, int64(20))

	// However, as a special case and unlike any other WatchResponse,
	// a ProgressNotify is not *required* to increase the revision.
	c.Check(ks.Apply(clientv3.WatchResponse{
		Header: epb.ResponseHeader{ClusterId: 9999, Revision: 20},
		Events: []*clientv3.Event{},
	}), gc.IsNil)

	// It's possible such a WatchResponse is queued & processed with
	// a mutation which follows. This works as expected.
	c.Check(ks.Apply(
		clientv3.WatchResponse{
			Header: epb.ResponseHeader{ClusterId: 9999, Revision: 20},
			Events: []*clientv3.Event{},
		},
		clientv3.WatchResponse{
			Header: epb.ResponseHeader{ClusterId: 9999, Revision: 21},
			Events: []*clientv3.Event{
				putEvent("/ffff", "8888", 21, 21, 1),
			},
		},
	), gc.IsNil)

	verifyDecodedKeyValues(c, ks.KeyValues,
		map[string]int{
			"/some/key":  101,
			"/other/key": 100,

			"/aaaa": 5555,
			"/bbbb": 6666,
			"/cccc": 4444,
			"/eeee": 7777,
			"/ffff": 8888,
		})
}

func (s *KeySpaceSuite) TestWaitForRevision(c *gc.C) {
	var ks = NewKeySpace("/", testDecoder)

	var ctx, cancel = context.WithCancel(context.Background())

	go func() {

		c.Check(ks.Apply(clientv3.WatchResponse{
			Header: epb.ResponseHeader{ClusterId: 123, Revision: 10},
			Events: []*clientv3.Event{
				putEvent("/key/10", "", 10, 10, 1),
			},
		}), gc.IsNil)

		c.Check(ks.Apply(clientv3.WatchResponse{
			Header: epb.ResponseHeader{ClusterId: 123, Revision: 99},
			Events: []*clientv3.Event{
				putEvent("/key/99", "", 99, 99, 1),
			},
		}), gc.IsNil)

		time.Sleep(time.Millisecond)

		c.Check(ks.Apply(clientv3.WatchResponse{
			Header: epb.ResponseHeader{ClusterId: 123, Revision: 100},
			Events: []*clientv3.Event{
				putEvent("/key/100", "", 100, 100, 1),
			},
		}), gc.IsNil)
	}()

	ks.Mu.RLock()
	defer ks.Mu.RUnlock()

	c.Check(ks.WaitForRevision(ctx, 100), gc.IsNil)
	c.Check(ks.Header.Revision, gc.Equals, int64(100))

	cancel()

	// Revision already met: succeeds immediately.
	c.Check(ks.WaitForRevision(ctx, 99), gc.IsNil)
	// Future revision: doesn't block as context is cancelled.
	c.Check(ks.WaitForRevision(ctx, 101), gc.Equals, context.Canceled)
}

var (
	_           = gc.Suite(&KeySpaceSuite{})
	etcdCluster *integration.ClusterV3
)

func Test(t *testing.T) {
	etcdCluster = integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	gc.TestingT(t)
	etcdCluster.Terminate(t)
}
