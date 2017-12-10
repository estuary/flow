package consensus

import (
	"sync"
	"time"

	gc "github.com/go-check/check"
)

type RingMutexMapSuite struct{}

func (s *RingMutexMapSuite) TestValueCaching(c *gc.C) {
	var rmm = NewRingMutexMap(func(v interface{}) interface{} {
		// Re-use but zero a previous value.
		if v != nil {
			*v.(*int) = 0
			return v
		}
		return new(int)
	})
	var foo, bar interface{}
	var mu *sync.Mutex

	// Initialize a key.
	foo, mu = rmm.Lock(1111)
	*(foo.(*int)) = 1111
	mu.Unlock()

	// Expect another key recieves its own, empty value.
	bar, mu = rmm.Lock(2222)
	c.Check(bar, gc.Not(gc.DeepEquals), foo)
	mu.Unlock()

	// Expect 1111 can still be mapped.
	bar, mu = rmm.Lock(1111)
	c.Check(foo, gc.Equals, bar)
	mu.Unlock()

	// Tweak such that 1111's entry falls out of the ring.
	rmm.next = 0

	// Expect 3333 is given 1111's old entry.
	bar, mu = rmm.Lock(3333)
	var expect int = 0

	c.Check(bar, gc.Equals, foo) // |foo| was re-used, but zero'd.
	c.Check(bar, gc.DeepEquals, &expect)
	mu.Unlock()
}

func (s *RingMutexMapSuite) TestConcurrentRequestsBlock(c *gc.C) {
	var rmm = NewRingMutexMap(nil)
	var coord, result = make(chan struct{}), make(chan string, 2)

	var lazyWinner = func() {
		var _, mu = rmm.Lock(1234)
		defer mu.Unlock()

		close(coord)
		time.Sleep(time.Millisecond * 5)
		result <- "winner"
	}
	var fastLoser = func() {
		var _, mu = rmm.Lock(1234)
		defer mu.Unlock()

		result <- "loser"
	}

	go lazyWinner()
	<-coord // Wait for |lazyWinner| to obtain lock.
	fastLoser()

	c.Check(<-result, gc.Equals, "winner")
	c.Check(<-result, gc.Equals, "loser")
}

var _ = gc.Suite(&RingMutexMapSuite{})
