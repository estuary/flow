package consensus

import "sync"

// RingMutexMap is a ring buffer which maps keys to buffer spaces, each holding
// a user-defined value and Mutex. Typically a key is a hash value of some
// kind, and the MutexRingMap allows raced processes colliding on a given hash
// key to sequence their activities and to share state. RingMutexMap operates
// over a bounded ring buffer, which means that colliding keys will coordinate
// only if they complete within a single loop through the ring.
type RingMutexMap struct {
	ring [ringSize]struct {
		key uint64
		sync.Mutex
		value interface{}
	}
	init  func(interface{}) interface{}
	index map[uint64]int // Maps a key to a |ring| offset.
	next  int            // Next |ring| index to be expunged.
	mu    sync.Mutex     // Guards |index| and |next|, but not |ring|.
}

// NewRingMutexMap returns a RingMutexMap with the user-defined |init|
// function. If non-nil, |init| is invoked to initialize the value of a newly
// mapped key. It is passed a previous value stored in the same ring space
// under a different key.
func NewRingMutexMap(init func(interface{}) interface{}) *RingMutexMap {
	return &RingMutexMap{
		init:  init,
		index: make(map[uint64]int),
	}
}

// Lock finds or creates a ring entry for |key|. It returns a locked Mutex
// for coordination over |key|, and the user-defined value it maps to.
func (rm *RingMutexMap) Lock(key uint64) (interface{}, *sync.Mutex) {
	// There are three orders of held locks in this loop:
	//  * (rm.mu)
	//  * (ring[i].Mu)
	//  * (ring[i].Mu, rm.mu)
	//
	// Note we cannot ever lock (rm.mu, ring[i].Mu) as that allows the
	// possibility of deadlock.

	for {
		rm.mu.Lock()

		if i, ok := rm.index[key]; ok {
			// Switch from a lock over |index| to a lock on the entry. Note that
			// |index| and |ring| may be modified while we wait, and item |i| could
			// be expired and replaced by another entry.
			rm.mu.Unlock()
			rm.ring[i].Lock()

			if rm.ring[i].key == key {
				// Success. Aquired lock for existing cached item |key|.
				return rm.ring[i].value, &rm.ring[i].Mutex
			}

			// The entry was expired while waiting on its Mutex. Try again.
			rm.ring[i].Unlock()
			continue
		}

		// Assign ring offset |next| to |key|.
		var i = rm.next
		rm.next = (rm.next + 1) % len(rm.ring)

		// Switch from a lock over |index| to a lock on the entry.
		rm.mu.Unlock()
		rm.ring[i].Lock()

		// Briefly lock again to update |index| (swapping old key for the new one).
		rm.mu.Lock()
		delete(rm.index, rm.ring[i].key)
		rm.index[key] = i
		rm.mu.Unlock()

		// Initialize the ring entry for the new key.
		if rm.init != nil {
			rm.ring[i].value = rm.init(rm.ring[i].value)
		}
		rm.ring[i].key = key

		return rm.ring[i].value, &rm.ring[i].Mutex
	}
}

// 131072, or sufficient to avoid collisions for locks held < 30s at 4K+ QPS.
const ringSize = 1 << 17
