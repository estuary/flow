package stores

import (
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
)

var (
	constructors = make(map[string]Constructor)
	stores       = make(map[pb.FragmentStore]*ActiveStore)
	storesMu     sync.RWMutex
)

// RegisterProviders registers store constructors for different storage schemes.
// This should be called during initialization to register all available store types.
func RegisterProviders(providers map[string]Constructor) {
	Sweep() // Mark.
	Sweep() // Sweep.

	storesMu.Lock()
	defer storesMu.Unlock()

	constructors = make(map[string]Constructor, len(providers))
	stores = make(map[pb.FragmentStore]*ActiveStore)

	for scheme, constructor := range providers {
		constructors[scheme] = constructor
	}
}

// Get returns an ActiveStore for the given FragmentStore configuration.
// It will attempt to initialize the store if not already cached.
// If initialization fails, it returns an ActiveStore with initErr set.
func Get(fs pb.FragmentStore) *ActiveStore {
	// Fast path: check if store already exists
	storesMu.RLock()
	if active, ok := stores[fs]; ok {
		storesMu.RUnlock()
		return active
	}
	storesMu.RUnlock()

	// Slow path: need to initialize
	storesMu.Lock()
	defer storesMu.Unlock()

	// Double-check after acquiring write lock
	if active, ok := stores[fs]; ok {
		return active
	}

	// Attempt to construct the store
	var (
		ep    = fs.URL()
		err   error
		store Store
	)
	if constructor, ok := constructors[ep.Scheme]; !ok {
		err = fmt.Errorf("unsupported fragment store scheme: %s", ep.Scheme)
	} else {
		store, err = constructor(ep)
	}

	var active = NewActiveStore(fs, store, err)

	if err == nil {
		stores[fs] = active
		activeStoresGauge.Set(float64(len(stores)))
		go checkLoop(stores, fs, 0) // Start health checks.
		log.WithFields(log.Fields{"store": fs, "n": len(stores)}).Info("started fragment store")
	}

	return active
}

// Sweep removes any stores that haven't been marked since the last sweep.
// Returns the number of stores removed.
func Sweep() int {
	storesMu.Lock()
	defer storesMu.Unlock()

	var removed int
	for fs, active := range stores {
		if !active.Mark.Load() {
			delete(stores, fs) // Not marked since last sweep.

			// Configure to return errLastHealthCheck repeatedly.
			active.health.mu.Lock()
			active.health.err = ErrLastHealthCheck
			close(active.health.nextCh)
			active.health.mu.Unlock()

			log.WithFields(log.Fields{"store": fs, "n": len(stores)}).Info("stopped fragment store")
			removed++
		} else {
			active.Mark.Store(false) // Clear for next sweep cycle.
		}
	}

	// Update active stores metric
	activeStoresGauge.Set(float64(len(stores)))

	return removed
}

var (
	activeStoresGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gazette_store_active",
		Help: "Number of active fragment stores",
	})

	storeOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gazette_store_operation_duration_seconds",
		Help:    "Duration of store operations in seconds",
		Buckets: prometheus.ExponentialBuckets(0.01, 4, 6), // 10ms to ~41s
	}, []string{"store", "operation", "status"})

	storeOperationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gazette_store_operation_total",
		Help: "Total number of store operations",
	}, []string{"store", "operation", "status"})

	storePutBytesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gazette_store_put_bytes_total",
		Help: "Total number of bytes put to the store",
	}, []string{"store", "encoding"})

	storeListItems = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gazette_store_list_items_count",
		Help:    "Number of items returned by list operations",
		Buckets: prometheus.ExponentialBuckets(10, 4, 6), // 10 to ~41k items
	}, []string{"store"})

	storeHealthCheckTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gazette_store_health_check_total",
		Help: "Total number of health checks performed",
	}, []string{"store", "status"})
)
