package consensus

import (
	"time"

	etcd "github.com/coreos/etcd/client"
	gc "github.com/go-check/check"
	"golang.org/x/net/context"
)

type ModelSuite struct{}

func (s *ModelSuite) TestBlockUntilModified(c *gc.C) {
	ctx := context.Background()
	var keys MockKeysAPI
	var watcher MockWatcher

	modelWatcher := RetryWatcher(&keys, "foo",
		&etcd.GetOptions{}, &etcd.WatcherOptions{})

	keys.On("Get", ctx, "foo", &etcd.GetOptions{}).
		Return(&etcd.Response{
			Action: "get",
			Index:  1234,
			Node:   &etcd.Node{Key: "foo", Value: "bar", ModifiedIndex: 1234},
		}, nil).Once()
	keys.On("Watcher", "foo",
		&etcd.WatcherOptions{AfterIndex: 1234}).Return(&watcher).Once()

	watcher.On("Next", ctx).After(5*time.Millisecond).Return(&etcd.Response{Node: &etcd.Node{ModifiedIndex: 2345}}, nil).Once()

	var reloaded bool
	go func() {
		BlockUntilModified(modelWatcher, uint64(1234))
		reloaded = true
	}()

	c.Check(reloaded, gc.Equals, false)
	time.Sleep(10 * time.Millisecond)
	c.Check(reloaded, gc.Equals, true)

	keys.AssertExpectations(c)
}

var _ = gc.Suite(&ModelSuite{})
