package consensus

import (
	"context"
	"time"

	etcd "github.com/coreos/etcd/client"
	gc "github.com/go-check/check"
)

type ModelSuite struct{}

func (s *ModelSuite) TestBlockUntilModified(c *gc.C) {
	var ctx = context.Background()
	var keys MockKeysAPI
	var watcher MockWatcher

	var modelWatcher = RetryWatcher(&keys, "foo",
		&etcd.GetOptions{}, &etcd.WatcherOptions{})

	keys.On("Get", ctx, "foo", &etcd.GetOptions{}).
		Return(&etcd.Response{
			Action: "get",
			Index:  1234,
			Node:   &etcd.Node{Key: "foo", Value: "bar", ModifiedIndex: 1234},
		}, nil).Once()
	keys.On("Watcher", "foo",
		&etcd.WatcherOptions{AfterIndex: 1234}).Return(&watcher).Once()

	watcher.On("Next", ctx).Return(&etcd.Response{Node: &etcd.Node{ModifiedIndex: 2345}}, nil).Once()

	var unblocked = make(chan struct{})
	go func() {
		BlockUntilModified(modelWatcher, uint64(1234))
		unblocked <- struct{}{}
	}()

	select {
	case <-unblocked:
	case <-time.After(10 * time.Second):
		c.Log("did not unblock")
		c.FailNow()
	}

	keys.AssertExpectations(c)
}

var _ = gc.Suite(&ModelSuite{})
