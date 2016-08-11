package consensus

import (
	"errors"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

const (
	MemberPrefix = "members" // Directory root for member announcements.
	ItemsPrefix  = "items"   // Directory root for allocated items.

	lockDuration          = time.Minute * 5 // Duration of held locks
	allocErrSleepInterval = time.Second * 5
)

var ErrAllocatorInstanceExists = errors.New("Allocator member key exists")

// Interface for types which perform distributed allocation of items.
type Allocator interface {
	KeysAPI() etcd.KeysAPI
	// Etcd path which roots shared state for this Context.
	PathRoot() string
	// A key uniquely identifying this Allocator within shared state.
	InstanceKey() string
	// The required number of replicas. Except in cases of failure, allocation
	// changes will not be made which would violate having at least this many
	// ready replicas of an item at all times.
	Replicas() int
	// Items which will be created if they do not exist. May be empty, and
	// additional items may be added at any time out-of-band (via creation of
	// a corresponding Etcd directory).
	FixedItems() []string

	// For |item| which is currently a local replica or master, returns a
	// representation of the local item processing state. State is shared with
	// other Allocators via this Allocator's |item| announcement in Etcd.
	ItemState(item string) string
	// For |state| of an item, which may be processed by another Allocator,
	// returns whether the item can safely be promoted at this time.
	ItemIsReadyForPromotion(item, state string) bool
	// Notifies Allocator of |route| for |item|. If |index| == -1, then Allocator
	// has no entry for |item|. Otherwise, |route.Entries[index]| is the entry
	// of this Allocator (and will have basename InstanceKey()). |tree| is given
	// as context: ItemRoute() will often wish to wish to inspect other state
	// within |tree| in response to a route change. Note that |route| or |tree|
	// must be copied if retained beyond this call
	ItemRoute(item string, route Route, index int, tree *etcd.Node)
}

// Attempts to create an Allocator member lock reflecting instance |alloc|.
// If the member lock already exists, returns ErrAllocatorInstanceExists.
// An Allocator member lock should be obtained prior to an Allocate call.
func Create(alloc Allocator) error {
	_, err := alloc.KeysAPI().Set(context.Background(), memberKey(alloc), "",
		&etcd.SetOptions{PrevExist: etcd.PrevNoExist, TTL: lockDuration})

	if err, ok := err.(etcd.Error); ok && err.Code == etcd.ErrorCodeNodeExist {
		return ErrAllocatorInstanceExists
	}
	return err
}

// Acts on behalf of |alloc| to achieve distributed allocation of items.
// This is a long-lived call, which will exit only after |alloc|'s member
// announcement is removed (eg, by Cancel(alloc)) and all allocated items have
// been safely handed off to ready replicas.
//
// Allocate acts on behalf of an existing member lock. If such a lock does not
// exist, Allocate will take no action. If it exists but is owned by another
// process, Allocate will duplicate the item allocations of that process. It is
// the caller's responsibility to obtain and verify uniqueness of the member
// lock (eg, via a preceeding Create).
func Allocate(alloc Allocator) error {
	// Channels for receiving & cancelling watched tree updates.
	var watchCh = make(chan *etcd.Response)
	var cancelWatch = make(chan struct{})
	defer func() { close(cancelWatch) }()

	var tree *etcd.Node // Watched tree rooted at alloc.PathRoot().

	watcher := RetryWatcher(alloc.KeysAPI(), alloc.PathRoot(),
		&etcd.GetOptions{Recursive: true, Sort: true},
		&etcd.WatcherOptions{Recursive: true})

	// Load initial tree. Fail-fast on any error.
	if r, err := watcher.Next(context.Background()); err != nil {
		return err
	} else {
		tree = r.Node
	}

	// Begin monitoring alloc.PathRoot() for changes.
	go func() {
		for {
			if r, err := watcher.Next(context.Background()); err != nil {
				log.WithField("err", err).Warn("allocator watch")
				select {
				case <-cancelWatch:
					return
				case <-time.Tick(allocErrSleepInterval):
				}
			} else {
				select {
				case <-cancelWatch:
					return
				case watchCh <- r:
				}
			}
		}
	}()

	// Test support hooks.
	testNotifier, _ := alloc.(interface {
		IdleAt(uint64)  // Allocate() is idle at the given |modifiedIndex|.
		ActedAt(uint64) // Allocate() acted at |modifiedIndex|.
	})

	var now time.Time        // Current timepoint.
	var modifiedIndex uint64 // Current Etcd ModifiedIndex.

	// When idle, manages deadline at which we must wake for next lock refresh.
	var deadlineTimer = time.NewTimer(0)
	var deadlineCh = deadlineTimer.C

	for {
		// Repeatedly wait for either |deadlineCh| or |watchCh| to select.
		select {
		case now = <-deadlineCh:
		case response := <-watchCh:
			for done := false; !done; {
				var err error
				if tree, err = PatchTree(tree, response); err != nil {
					log.WithFields(log.Fields{"err": err, "resp": response}).Error("patch failed")
				}

				// Process further queued watch updates without blocking.
				select {
				case response = <-watchCh:
				default:
					done = true
				}
			}

			if modifiedIndex > response.Node.ModifiedIndex {
				// We are waiting for a previous allocation action to be reflected
				// in our watched tree. Don't attempt another action until it is.
				continue
			}
			modifiedIndex = response.Node.ModifiedIndex
		}

		// Disable timer notifications until explicitly re-enabled.
		deadlineTimer.Stop()
		deadlineCh = nil

		var params = allocParams{Allocator: alloc}
		params.Input.Time = now
		params.Input.Tree = tree
		params.Input.Index = modifiedIndex

		allocExtract(&params)
		desiredMaster, desiredTotal := targetCounts(&params)

		log.WithFields(log.Fields{
			"allocParams":   params,
			"desiredMaster": desiredMaster,
			"desiredTotal":  desiredTotal,
		}).Debug("allocator params")

		if response, err := allocAction(&params, desiredMaster, desiredTotal); err != nil {
			log.WithField("err", err).Warn("failed to apply allocation action")

			// Action is implicitly retried the next iteration, which will occur
			// on the next watch update or after cool-off.
			deadlineTimer.Reset(allocErrSleepInterval)
			deadlineCh = deadlineTimer.C
			continue
		} else if response != nil {
			// Action was applied. We expect to see |response| again via our Etcd
			// watch, and defer further processing or actions until we do.
			modifiedIndex = response.Node.ModifiedIndex

			if testNotifier != nil {
				testNotifier.ActedAt(modifiedIndex)
			}
			continue
		}

		// No action is possible at this time.
		if nextDeadline := nextDeadline(&params); nextDeadline.IsZero() {
			// Termination condition: no Etcd entries remain.
			return nil
		} else {
			// Arrange to sleep until |nextDeadline|.
			deadlineTimer.Reset(nextDeadline.Sub(time.Now()))
			deadlineCh = deadlineTimer.C

			if testNotifier != nil {
				testNotifier.IdleAt(modifiedIndex)
			}
		}
	}
}

// Cancels |alloc| by deleting its member announcement. The matching Allocate()
// invocation will begin an orderly release of held items. When all items are
// released, Allocate() will exit. Note that mastered items will be released
// only once they have a sufficient number of ready replicas for hand-off.
func Cancel(alloc Allocator) error {
	_, err := alloc.KeysAPI().Delete(context.Background(), memberKey(alloc), nil)
	return err
}

// Cancels |item| by deleting its announcement. This should be undertaken only
// under exceptional circumstances, where the local Allocator is unable to
// service the allocated |item| (eg, because of an unrecoverable local error).
func CancelItem(alloc Allocator, item string) error {
	_, err := alloc.KeysAPI().Delete(context.Background(), itemKey(alloc, item), nil)
	return err
}

// Composes Create and Allocate to run an Allocator which will additionally
// use an installed signal handler to gracefully Cancel itself on a SIGTERM
// or SIGINT. Performs a polled retry of Create on ErrAllocatorInstanceExists,
// until aquired or signaled. Top-level programs implementing an Allocator will
// generally want to use this.
func CreateAndAllocateWithSignalHandling(alloc Allocator) error {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)
	shutdownCh := make(chan struct{})

	go func() {
		sig, ok := <-signalCh
		if ok {
			log.WithField("signal", sig).Info("caught signal")
			close(shutdownCh)
		}
	}()

	// Obtain Allocator lock. If it exists, retry until signalled.
	for {
		err := Create(alloc)
		if err == nil {
			break
		} else if err != ErrAllocatorInstanceExists {
			return err
		}
		log.WithField("key", alloc.InstanceKey()).
			Warn("waiting for prior Allocator to expire")

		select {
		case <-time.After(time.Second * 10):
			continue
		case <-shutdownCh:
			return err
		}
	}

	// Arrange to Cancel Allocate on signal, allowing it to gracefully tear down.
	go func() {
		<-shutdownCh

		if err := Cancel(alloc); err != nil {
			log.WithField("err", err).Error("allocator cancel failed")
		}
	}()

	return Allocate(alloc)
}

// Returns the member announcement key for |alloc|.
// Ex: /path/root/members/my-alloc-key
func memberKey(alloc Allocator) string {
	return alloc.PathRoot() + "/" + MemberPrefix + "/" + alloc.InstanceKey()
}

// Returns the item entry key for |item| held by |alloc|.
// Ex: /path/root/items/an-item/my-alloc-key
func itemKey(alloc Allocator, item string) string {
	return alloc.PathRoot() + "/" + ItemsPrefix + "/" + item + "/" + alloc.InstanceKey()
}

// Returns the item name represented by item entry |key| held by |alloc|.
// Ex: /path/root/items/an-item/my-alloc-key => an-item
func itemOfItemKey(alloc Allocator, key string) string {
	lstrip := len(alloc.PathRoot()) + 1 + len(ItemsPrefix) + 1
	rstrip := len(key) - len(alloc.InstanceKey()) - 1
	return key[lstrip:rstrip]
}

// POD type built by individual iterations of the Allocate() protocol,
// to succinctly describe global allocator state.
type allocParams struct {
	Allocator

	Input struct {
		Time  time.Time
		Tree  *etcd.Node
		Index uint64 // Current Etcd ModifiedIndex.
	}
	Item struct {
		Master       []*etcd.Node // Items for which we're master.
		Replica      []*etcd.Node // Items for which we're a replica.
		Extra        []*etcd.Node // Items for which we hold an extra lock.
		Releaseable  []*etcd.Node // Mastered items we may release.
		OpenMasters  []string     // Names of items in need of a master.
		OpenReplicas []string     // Names of items in need of a replica.
		Count        int          // Total number of items.
	}
	Member struct {
		Entry *etcd.Node // Our member entry.
		Count int        // Total number of allocator members.
	}
}

// From |p.Input|, builds |p.Item| and |p.Member| descriptions of allocParams.
func allocExtract(p *allocParams) {
	var itemsDir etcd.Node
	if d := Child(p.Input.Tree, ItemsPrefix); d != nil {
		itemsDir = *d
	} else {
		// Fabricate items directory if it doesn't exist.
		itemsDir = etcd.Node{Key: p.Input.Tree.Key + "/" + ItemsPrefix, Dir: true}
	}

	// Perform a zipped, outer-join iteration of |items| and |desiredItems|.
	var scratch [8]*etcd.Node // Re-usable buffer for building Route.Entries.
	forEachChild(&itemsDir, p.FixedItems(), func(name string, node *etcd.Node) {
		p.Item.Count += 1

		// Bypass NewRoute to avoid extra deep copies and because we know (per the
		// ItemRoute contract) that |route| will not be retained.
		var route = Route{
			EtcdIndex: p.Input.Index,
			Item:      node,
			Entries:   append(scratch[:0], node.Nodes...),
		}
		route.init()

		index := route.Index(p.InstanceKey())
		p.ItemRoute(name, route, index, p.Input.Tree)

		if index == -1 {
			// We do not hold a lock on this item.
			if len(route.Entries) == 0 {
				p.Item.OpenMasters = append(p.Item.OpenMasters, name)
			} else if len(route.Entries) < p.Replicas()+1 {
				p.Item.OpenReplicas = append(p.Item.OpenReplicas, name)
			}
		} else if index == 0 {
			// We act as item master.
			p.Item.Master = append(p.Item.Master, route.Entries[0])

			// We always require that mastered items be ready for hand-off
			// before we may release them, even if our member lock is gone.
			if route.IsReadyForHandoff(p) {
				p.Item.Releaseable = append(p.Item.Releaseable, route.Entries[0])
			}
		} else if index < p.Replicas()+1 {
			// We act as an item replica.
			p.Item.Replica = append(p.Item.Replica, route.Entries[index])
		} else {
			// We hold an extra lock (we lost a race to become a replica).
			p.Item.Extra = append(p.Item.Extra, route.Entries[index])
		}
	})

	if membersDir := Child(p.Input.Tree, MemberPrefix); membersDir != nil {
		p.Member.Entry = Child(membersDir, p.InstanceKey())
		p.Member.Count = len(membersDir.Nodes)
	}
}

// Selects and attempts an action (state transition) given the current parameters,
// as an Etcd operation. Etcd response and error code are passed through. If
// both are nil, no action was available to be attempted.
func allocAction(p *allocParams, desiredMaster, desiredTotal int) (*etcd.Response, error) {
	// Locks are refreshed when less than 1/2 of their TTL remains.
	var horizon = p.Input.Time.Add(lockDuration / 2)

	// Helper which CASs |node| to |value| with TTL.
	var compareAndSet = func(node *etcd.Node, value string) (*etcd.Response, error) {
		return p.KeysAPI().Set(context.Background(), node.Key, value,
			&etcd.SetOptions{PrevIndex: node.ModifiedIndex, TTL: lockDuration})
	}
	// Helper which CADs |node|.
	var compareAndDelete = func(node *etcd.Node) (*etcd.Response, error) {
		return p.KeysAPI().Delete(context.Background(), node.Key,
			&etcd.DeleteOptions{PrevIndex: node.ModifiedIndex})
	}
	// Helper which creates |key| with TTL.
	var create = func(key string) (*etcd.Response, error) {
		return p.KeysAPI().Set(context.Background(), key, "",
			&etcd.SetOptions{PrevExist: etcd.PrevNoExist, TTL: lockDuration})
	}

	// 1) Refresh the member lock.
	if p.Member.Entry != nil {
		if p.Member.Entry.Expiration.Before(horizon) {
			log.WithField("key", p.Member.Entry.Key).Debug("refreshing member lock")

			return compareAndSet(p.Member.Entry, "")
		}
	}

	// An item lock is updated if it's beyond |horizon|, *or* if Allocator state
	// for the item no longer matches persisted item state. For example, the item
	// may now be ready for promotion, which must be published via Etcd.

	// 2) Refresh or update a master lock
	for _, entry := range p.Item.Master {
		value := p.ItemState(itemOfItemKey(p, entry.Key))

		if entry.Expiration.Before(horizon) || value != entry.Value {
			log.WithFields(log.Fields{"key": entry.Key, "value": value}).
				Debug("refreshing allocated master lock")

			return compareAndSet(entry, value)
		}
	}
	// 3) Refresh or update a replica lock.
	for _, entry := range p.Item.Replica {
		value := p.ItemState(itemOfItemKey(p, entry.Key))

		if entry.Expiration.Before(horizon) || value != entry.Value {
			log.WithFields(log.Fields{"key": entry.Key, "value": value}).
				Debug("refreshing allocated replica lock")

			return compareAndSet(entry, value)
		}
	}
	// 4) Release a spurious lock from a lost acquisition race.
	for _, entry := range p.Item.Extra {
		log.WithField("key", entry.Key).Debug("deleting lost-race item lock")

		return compareAndDelete(entry)
	}
	// 5) Select a random master item to release. This may occur iff:
	//  * We are currently the item master.
	//  * The item has the required number of ready replicas.
	//  * We'd like to release a mastered item.
	if len(p.Item.Master) > desiredMaster && len(p.Item.Releaseable) != 0 {
		entry := p.Item.Releaseable[rand.Int()%len(p.Item.Releaseable)]
		log.WithField("key", entry.Key).Debug("releasing mastered item lock")

		return compareAndDelete(entry)
	}
	// 6) Select a random replica to release. In normal operation we never
	// release a replica we hold. However, iff we have no member lock (we're in
	// the process of shutdown), then we may release held replicas (not masters).
	if p.Member.Entry == nil && len(p.Item.Replica) != 0 {
		entry := p.Item.Replica[rand.Int()%len(p.Item.Replica)]
		log.WithField("key", entry.Key).Debug("releasing replica item lock")

		return compareAndDelete(entry)
	}
	// 7) Select a random item to master. This may occur iff:
	//  * We don't hold an entry for the item.
	//  * The item has an open master slot.
	//  * We'd like to have another master.
	if len(p.Item.Master) < desiredMaster && len(p.Item.OpenMasters) != 0 {
		name := p.Item.OpenMasters[rand.Int()%len(p.Item.OpenMasters)]
		key := itemKey(p, name)
		log.WithField("key", key).Debug("aquiring item master lock")

		return create(key)
	}
	// 8) Select a random item to replicate. This may occur iff:
	//  * We don't hold an entry for the item.
	//  * The item has an open replica slot.
	//  * We'd like to have another replica.
	if len(p.Item.Master)+len(p.Item.Replica) < desiredTotal && len(p.Item.OpenReplicas) != 0 {
		name := p.Item.OpenReplicas[rand.Int()%len(p.Item.OpenReplicas)]
		key := itemKey(p, name)
		log.WithField("key", key).Debug("aquiring item replica lock")

		return create(key)
	}
	// 9) Deadlock avoidance: Select a random master to release, iff:
	//  * We are currently the item master.
	//  * The item has the required number of ready replicas.
	//  * We hold exactly as many master slots as we'd like.
	//  * We have too many items overall.
	if len(p.Item.Master) == desiredMaster &&
		len(p.Item.Master)+len(p.Item.Replica) > desiredTotal &&
		len(p.Item.Releaseable) != 0 {

		var entry = p.Item.Releaseable[rand.Int()%len(p.Item.Releaseable)]
		log.WithField("key", entry.Key).Debug("releasing EXTRA mastered item lock")
		return compareAndDelete(entry)
	}
	// 10) Deadlock avoidance: Select a random item to replicate with delay, iff:
	//  * We don't hold an entry for the item.
	//  * The item has an open replica slot.
	//  * We have the exact right number of items overall (we'll be going over).
	//  * We are not actively seeking to exit.
	//
	// Note that this case means an allocator can potentially fail to converge.
	// We resolve this in practice by sleeping for a period of time: if there's
	// another allocator that actively seeks more replicas, we'd prefer that they
	// win. Sleeping is safe because we've already asserted that all held keys
	// have at least 1/2 of their TTL remaining.
	if len(p.Item.Master)+len(p.Item.Replica) == desiredTotal &&
		len(p.Item.Master) == desiredMaster &&
		len(p.Item.OpenReplicas) != 0 &&
		p.Member.Entry != nil {

		var name = p.Item.OpenReplicas[rand.Int()%len(p.Item.OpenReplicas)]
		var key = itemKey(p, name)

		time.Sleep(100 * time.Millisecond)
		log.WithField("key", key).Debug("aquiring EXTRA item replica lock")
		return create(key)
	}
	return nil, nil
}

// Returns the desired number of mastered and total (mastered + replica) items.
func targetCounts(p *allocParams) (desiredMaster, desiredTotal int) {
	// If we do not hold a member lock, our target is zero. Otherwise, it's
	// p.Item.Count / p.Member.Count rounded up.
	if p.Member.Entry != nil {
		desiredMaster = p.Item.Count / p.Member.Count
		if p.Item.Count%p.Member.Count != 0 {
			desiredMaster += 1
		}
		desiredTotal = desiredMaster * (p.Replicas() + 1)
	}
	return
}

// Computes the next deadline by finding the minimum Expiration of all held
// Etcd entries, and subtracting 1/2 of lockDuration. Eg, we wish to refresh
// a held entry once its remaining TTL is less than 1/2 of lockDuration.
func nextDeadline(p *allocParams) time.Time {
	var firstExpire time.Time
	if p.Member.Entry != nil {
		firstExpire = *p.Member.Entry.Expiration
	}
	for _, entry := range p.Item.Master {
		if firstExpire.IsZero() || entry.Expiration.Before(firstExpire) {
			firstExpire = *entry.Expiration
		}
	}
	for _, entry := range p.Item.Replica {
		if firstExpire.IsZero() || entry.Expiration.Before(firstExpire) {
			firstExpire = *entry.Expiration
		}
	}

	if firstExpire.IsZero() {
		return time.Time{}
	}
	return firstExpire.Add(-lockDuration / 2)
}
