package consensus

import (
	"math"
	"strings"
	"sync"

	etcd "github.com/coreos/etcd/client"
	gc "github.com/go-check/check"
	"golang.org/x/net/context"
)

type AllocRunSuite struct {
	// Allocator fields.
	etcdClient etcd.Client
	pathRoot   string
	replicas   int
	fixedItems []string

	// Tracked state, for verification.
	routesMu sync.Mutex
	routes   map[string]Route

	notifyCh chan notify
}

func (s *AllocRunSuite) SetUpSuite(c *gc.C) {
	// TODO(johnny): Issue #1164 & #1148: Work out a consistent policy for test
	// endpoint expectations.
	s.etcdClient, _ = etcd.New(etcd.Config{Endpoints: []string{"http://127.0.0.1:2379"}})
	s.notifyCh = make(chan notify, 1)

	// Skip suite if Etcd is not available.
	if _, err := s.KeysAPI().Get(context.Background(), "/",
		&etcd.GetOptions{Recursive: false}); err != nil {
		c.Skip(err.Error())
	}
}

func (s *AllocRunSuite) SetUpTest(c *gc.C) {
	s.pathRoot = "/tests/" + c.TestName()
	s.replicas = 0 // Overridden by some tests.
	s.fixedItems = []string{"bar", "baz", "foo"}
	s.routes = make(map[string]Route)

	// Clear a previous test Etcd directory (if it exists).
	s.KeysAPI().Delete(context.Background(), s.pathRoot, &etcd.DeleteOptions{Recursive: true})
}

func (s *AllocRunSuite) TestSingle(c *gc.C) {
	alloc := newTestAlloc(s, "my-key")
	go alloc.run(c)

	// Stage 1: Expect that we acquired master locks on all items.
	s.wait(waitFor{idle: []string{"my-key"}})

	for _, item := range s.fixedItems {
		c.Check(s.routes[item].Entries, gc.HasLen, 1)
		c.Check(s.routes[item].Index("my-key"), gc.Equals, 0)
	}
	Cancel(alloc)

	// Stage 2: Allocator exited. Expect all item entries were released.
	s.wait(waitFor{exit: []string{"my-key"}})

	for _, item := range s.fixedItems {
		c.Check(s.routes[item].Entries, gc.HasLen, 0)
	}
}

func (s *AllocRunSuite) TestHandlingOfNestedItemDirectories(c *gc.C) {
	s.fixedItems = []string{}
	s.replicas = 1

	_, err := s.KeysAPI().Set(context.Background(), s.PathRoot()+"/items/some/nested/item", "",
		&etcd.SetOptions{Dir: true})
	c.Check(err, gc.IsNil)

	alloc := newTestAlloc(s, "my-key")
	go alloc.run(c)

	// Stage 1: Expect that we acquired a replica lock on "some" (We interpret
	// "nested" to be the master lock, due to its earlier creation index).
	// Expect that nothing else blew up.
	s.wait(waitFor{idle: []string{"my-key"}})

	c.Check(s.routes, gc.HasLen, 1)
	c.Check(s.routes["some"].Entries, gc.HasLen, 2)
	c.Check(s.routes["some"].Index("my-key"), gc.Equals, 1)

	Cancel(alloc)
	s.wait(waitFor{exit: []string{"my-key"}})
}

func (s *AllocRunSuite) TestAsyncItemCreationAndRemoval(c *gc.C) {
	s.replicas = 1

	alloc1 := newTestAlloc(s, "alloc-1")
	alloc2 := newTestAlloc(s, "alloc-2")
	go alloc1.run(c)
	go alloc2.run(c)

	// Stage 1: Expect fixed items have master & replica entries.
	s.wait(waitFor{idle: []string{"alloc-1", "alloc-2"}})

	c.Check(s.routes, gc.HasLen, 3)
	for _, route := range s.routes {
		c.Check(route.Entries, gc.HasLen, 2)
	}

	// Create a new item (by adding its directory).
	_, err := s.KeysAPI().Set(context.Background(), s.PathRoot()+"/items/a-new-item", "",
		&etcd.SetOptions{Dir: true})
	c.Check(err, gc.IsNil)

	// Stage 2: Expect new item is now assigned as well.
	s.wait(waitFor{idle: []string{"alloc-1", "alloc-2"}})

	c.Check(s.routes, gc.HasLen, 4)
	c.Check(s.routes["a-new-item"].Entries, gc.HasLen, 2)

	delete(s.routes, "a-new-item") // Expect ItemRoute("a-new-item") isn't invoked again.

	// Delete both "a-new-item" and fixed item "bar".
	_, err = s.KeysAPI().Delete(context.Background(), s.PathRoot()+"/items/a-new-item",
		&etcd.DeleteOptions{Dir: true, Recursive: true})
	c.Check(err, gc.IsNil)

	_, err = s.KeysAPI().Delete(context.Background(), s.PathRoot()+"/items/bar",
		&etcd.DeleteOptions{Dir: true, Recursive: true})
	c.Check(err, gc.IsNil)

	// Stage 3: Expect fixed item "bar" is re-created, and all are assigned.
	s.wait(waitFor{idle: []string{"alloc-1", "alloc-2"}})

	c.Check(s.routes, gc.HasLen, 3)
	c.Check(s.routes["bar"].Entries, gc.HasLen, 2)

	// Shut down both allocators.
	s.replicas = 0
	Cancel(alloc1)
	Cancel(alloc2)

	s.wait(waitFor{exit: []string{"alloc-1", "alloc-2"}})

	// Stage 3: Allocator exited. All items were released.
	c.Check(s.routes, gc.HasLen, 3)
	for _, route := range s.routes {
		c.Check(route.Entries, gc.HasLen, 0)
	}
}

func (s *AllocRunSuite) TestAllocatorHandoff(c *gc.C) {
	s.replicas = 1

	alloc1 := newTestAlloc(s, "alloc-1")
	alloc2 := newTestAlloc(s, "alloc-2")
	alloc3 := newTestAlloc(s, "alloc-3")

	go alloc1.run(c)

	// Stage 1: |alloc1| has acquired master on all items.
	s.wait(waitFor{idle: []string{"alloc-1"}})
	for _, item := range s.fixedItems {
		c.Check(s.routes[item].Index("alloc-1"), gc.Equals, 0)
	}
	go alloc2.run(c)

	// Stage 2: Mastered items are split between both allocators.
	s.wait(waitFor{idle: []string{"alloc-1", "alloc-2"}})

	c.Check(s.routes, gc.HasLen, len(s.fixedItems))
	for _, route := range s.routes {
		c.Check(route.Entries, gc.HasLen, 2)
	}
	c.Check(s.masterCounts(), gc.DeepEquals, map[string]int{
		"alloc-1": 2, // Has larger share, because it was first.
		"alloc-2": 1,
	})
	Cancel(alloc1)
	go alloc3.run(c)

	// Stage 3. First allocator has exited. Items are still split.
	s.wait(waitFor{exit: []string{"alloc-1"}, idle: []string{"alloc-2", "alloc-3"}})

	for _, route := range s.routes {
		c.Check(route.Entries, gc.HasLen, 2)
	}
	c.Check(s.masterCounts(), gc.DeepEquals, map[string]int{
		"alloc-2": 2, // Has larger share, because it was first.
		"alloc-3": 1,
	})
	Cancel(alloc2)

	// Stage 4: Second allocator has exited. Third allocator handles all items.
	s.wait(waitFor{exit: []string{"alloc-2"}, idle: []string{"alloc-3"}})

	for _, route := range s.routes {
		c.Check(route.Entries, gc.HasLen, 1)
	}
	c.Check(s.masterCounts(), gc.DeepEquals, map[string]int{"alloc-3": 3})

	s.replicas = 0 // Required for allocator to voluntarily exit.
	Cancel(alloc3)

	// Stage 5: All item locks are released. Allocators have exited.
	s.wait(waitFor{exit: []string{"alloc-3"}})

	for _, item := range s.fixedItems {
		c.Check(s.routes[item].Entries, gc.HasLen, 0)
	}
}

func (t *AllocRunSuite) masterCounts() map[string]int {
	var counts = make(map[string]int)
	for _, item := range t.fixedItems {
		// Group by allocator name, and count.
		key := t.routes[item].Entries[0].Key
		counts[key[strings.LastIndexByte(key, '/')+1:]] += 1
	}
	return counts
}

// Helper which uses testNotifier hooks in Allocate(), along with tracking
// exits of Allocate() calls from run(), to determine when a set of concurrent
// Allocate() calls are fully idle and/or exited. This is done by returning only
// after all instances have become idle (have no actions to perform) at the
// largest "acted" index returned by any instance.
func (s *AllocRunSuite) wait(desc waitFor) {
	var maxIndex uint64 = 1

	state := make(map[string]uint64)
	for _, n := range desc.idle {
		state[n] = 0
	}
	for _, n := range desc.exit {
		state[n] = math.MaxUint64 // Can never be matched. Must be deleted.
	}

	for {
		if msg := <-s.notifyCh; msg.key == "" {
			// ActedAt() notification.
			if msg.index > maxIndex {
				maxIndex = msg.index
			}
			continue
		} else if msg.exit {
			// Exit notification.
			delete(state, msg.key)
		} else if msg.index > state[msg.key] {
			// IdleAt() notification.
			state[msg.key] = msg.index
		}

		// Return iff all allocators are at |maxIndex|.
		done := true
		for _, ind := range state {
			if ind != maxIndex {
				done = false
				break
			}
		}
		if done {
			return
		}
	}
}

// Partial Allocator implementation.
func (s *AllocRunSuite) KeysAPI() etcd.KeysAPI                     { return etcd.NewKeysAPI(s.etcdClient) }
func (s *AllocRunSuite) PathRoot() string                          { return s.pathRoot }
func (s *AllocRunSuite) Replicas() int                             { return s.replicas }
func (s *AllocRunSuite) FixedItems() []string                      { return s.fixedItems }
func (s *AllocRunSuite) ItemState(item string) string              { return "ready" }
func (s *AllocRunSuite) ItemIsReadyForPromotion(state string) bool { return state == "ready" }

func (s *AllocRunSuite) ItemRoute(item string, rt Route, ind int) {
	s.routesMu.Lock()
	defer s.routesMu.Unlock()

	if s.routes[item].EtcdIndex < rt.EtcdIndex {
		s.routes[item] = rt.Copy()
	}
}

// Represents a specific Allocator in the test context.
// Also implements testNotifier interface used by Allocate().
type testAllocator struct {
	*AllocRunSuite

	instanceKey string
}

// Allocator interface.
func (t testAllocator) InstanceKey() string { return t.instanceKey }

// testNotifier interface.
func (t testAllocator) IdleAt(index uint64) {
	t.notifyCh <- notify{key: t.instanceKey, index: index}
}
func (t testAllocator) ActedAt(index uint64) {
	t.notifyCh <- notify{index: index}
}

func newTestAlloc(s *AllocRunSuite, key string) testAllocator {
	return testAllocator{
		AllocRunSuite: s,
		instanceKey:   key,
	}
}

func (t testAllocator) run(c *gc.C) {
	c.Check(Allocate(t), gc.IsNil)
	t.notifyCh <- notify{key: t.instanceKey, exit: true}
}

type waitFor struct{ idle, exit []string }

type notify struct {
	key   string
	index uint64
	exit  bool
}

var _ = gc.Suite(&AllocRunSuite{})
