package topic

import (
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/pippio/api-server/discovery"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type lockState int

const (
	BasePath      = "/gazette/consumers"
	MembersPrefix = "members/"
	OffsetsPrefix = "offsets/"
	LocksPrefix   = "locks/"

	notLocked   lockState = iota
	lockedSelf  lockState = iota
	lockedOther lockState = iota

	kLockTimeout         = 5 * time.Minute
	kConvergenceInterval = time.Minute
)

type consumerContext interface {
	Name() string
	Topic() *TopicDescription
	Etcd() discovery.EtcdService

	StartConsuming(journal string, offset int64)
	StopConsuming(journal string) int64

	ConsumingJournals() []string
	ConsumedOffset(journal string) (int64, bool)
}

type ConsumerCoordinator struct {
	context consumerContext

	kvs *discovery.KeyValueService

	localRouteKey string
	router        discovery.HRWRouter
	routerMu      sync.Mutex

	timer *time.Timer
}

func NewConsumerCoordinator(ctx consumerContext) (*ConsumerCoordinator, error) {

	kvs, err := discovery.NewKeyValueService(BasePath+"/"+ctx.Name(), ctx.Etcd(),
		consumerDecode)
	if err != nil {
		return nil, err
	}

	var localRouteKey string
	if hostname, err := os.Hostname(); err != nil {
		return nil, err
	} else {
		suffix := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(10000)
		localRouteKey = fmt.Sprintf("%s-%d", hostname, suffix)
	}

	consumer := &ConsumerCoordinator{
		context:       ctx,
		localRouteKey: localRouteKey,
		kvs:           kvs,
	}
	// Track all routed journals of the topic.
	consumer.router = discovery.NewHRWRouter(1, consumer.onRouteUpdate)
	for i := 0; i != ctx.Topic().Partitions; i++ {
		consumer.router.Track(ctx.Topic().Journal(i), []discovery.HRWRoute{})
	}

	kvs.AddObserver(MembersPrefix, consumer.onMembershipChange)
	kvs.AddObserver(LocksPrefix, consumer.onLockUpdate)

	// Announce the local route key.
	if err := ctx.Etcd().Create(consumer.basePath()+MembersPrefix+localRouteKey,
		localRouteKey, kLockTimeout); err != nil {
		return nil, err
	}

	// Schedule the first convergence interval.
	consumer.timer = time.AfterFunc(kConvergenceInterval,
		consumer.convergeConsumer)

	return consumer, nil
}

func (c *ConsumerCoordinator) Cancel() error {
	c.timer.Stop()
	return c.context.Etcd().Delete(c.basePath()+MembersPrefix+c.localRouteKey,
		false)
}

func (c *ConsumerCoordinator) basePath() string {
	return BasePath + "/" + c.context.Name() + "/"
}

func (c *ConsumerCoordinator) convergeConsumer() {
	// Update membership annoucement TTL.
	path := c.basePath() + MembersPrefix + c.localRouteKey
	if err := c.context.Etcd().Update(path, c.localRouteKey,
		kLockTimeout); err != nil {
		log.WithFields(log.Fields{"path": path, "err": err}).
			Error("failed to consumer annoucement")
	}

	journals := c.context.ConsumingJournals()
	for _, journal := range journals {
		go c.convergeJournal(journal)
	}

	// Schedule the next interval.
	c.timer = time.AfterFunc(kConvergenceInterval, c.convergeConsumer)
}

func (c *ConsumerCoordinator) convergeJournal(journal string) {
	lockState := c.lockState(journal)
	ownsJournal := c.ownsJournal(journal)
	localOffset, isConsuming := c.context.ConsumedOffset(journal)

	//log.WithFields(log.Fields{"lockState": lockState, "owns": ownsJournal,
	// "localOffset": localOffset, "isConsuming": isConsuming}).Info("Converge")

	if isConsuming {
		if !ownsJournal || lockState != lockedSelf {
			localOffset = c.context.StopConsuming(journal)
		}
	} else if ownsJournal && lockState == lockedSelf {
		c.context.StartConsuming(journal, c.persistedOffset(journal))
	}

	if lockState == lockedSelf {
		if isConsuming && localOffset > c.persistedOffset(journal) {
			// Update consumed journal offset within Etcd.
			if err := c.context.Etcd().Set(c.basePath()+OffsetsPrefix+journal,
				strconv.FormatInt(localOffset, 16), time.Duration(0)); err != nil {
				log.WithFields(log.Fields{"journal": journal, "err": err}).
					Error("failed to set journal offset")
			}
		}
		if ownsJournal {
			// Update our lock TTL.
			if err := c.context.Etcd().Update(c.basePath()+LocksPrefix+journal,
				c.localRouteKey, kLockTimeout); err != nil {
				log.WithFields(log.Fields{"journal": journal, "err": err}).
					Error("failed to update journal lock")
			}
		} else {
			// We no longer own the journal. Delete our lock.
			if err := c.context.Etcd().Delete(c.basePath()+LocksPrefix+journal,
				false); err != nil {
				log.WithFields(log.Fields{"journal": journal, "err": err}).
					Error("failed to delete journal lock")
			}
		}
	} else if ownsJournal && lockState == notLocked {
		// Not currently locked, but we own this journal. Attempt to lock.
		if err := c.context.Etcd().Create(c.basePath()+LocksPrefix+journal,
			c.localRouteKey, kLockTimeout); err != nil {
			log.WithFields(log.Fields{"journal": journal, "err": err}).
				Error("failed to create journal lock")
		}
	}
}

func (c *ConsumerCoordinator) ownsJournal(journal string) bool {
	c.routerMu.Lock()
	routes := c.router.Route(journal)
	c.routerMu.Unlock()

	return len(routes) != 0 && routes[0].Value.(string) == c.localRouteKey
}

func (c *ConsumerCoordinator) lockState(journal string) lockState {
	entry, ok := c.kvs.Get(LocksPrefix + journal)
	if !ok {
		return notLocked
	} else if entry.Value.(string) == c.localRouteKey {
		return lockedSelf
	} else {
		return lockedOther
	}
}

func (c *ConsumerCoordinator) persistedOffset(journal string) int64 {
	entry, ok := c.kvs.Get(OffsetsPrefix + journal)
	if !ok {
		return 0
	} else {
		return entry.Value.(int64)
	}
}

func (c *ConsumerCoordinator) onMembershipChange(members, old,
	new discovery.KeyValues) {
	c.routerMu.Lock()
	c.router.RebuildRoutes(members, old, new)
	c.routerMu.Unlock()
}

func (c *ConsumerCoordinator) onRouteUpdate(journal string, oldRoute,
	newRoute []discovery.HRWRoute) {
	// Called from within onMembershipChange(), so we're already locked.
	go c.convergeJournal(journal)
}

func (c *ConsumerCoordinator) onLockUpdate(all, old, new discovery.KeyValues) {
	for _, removed := range old.Difference(new) {
		go c.convergeJournal(removed.Key[len(LocksPrefix):])
	}
	for _, upsert := range new {
		go c.convergeJournal(upsert.Key[len(LocksPrefix):])
	}
}

func consumerDecode(key, value string) (interface{}, error) {
	if strings.HasPrefix(key, MembersPrefix) ||
		strings.HasPrefix(key, LocksPrefix) {
		return value, nil
	} else if strings.HasPrefix(key, OffsetsPrefix) {
		return strconv.ParseInt(value, 16, 64)
	} else {
		return nil, errors.New("unknown key: " + key)
	}
}
