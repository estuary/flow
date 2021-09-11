package bindings

/*
#include "../../crates/bindings/flow_bindings.h"
*/
import "C"

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// A prometheus.Collector that exposes general rust metrics that are exposed by libbindings as
// prometheus metrics.
type promCollector struct{}

// Describe implements prometheus.Collector for promCollector
func (c *promCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

// Collect implements prometheus.Collector for promCollector
func (c *promCollector) Collect(ch chan<- prometheus.Metric) {
	var stats = C.get_memory_stats()
	var counter = func(desc *prometheus.Desc, value C.uint64_t) {
		ch <- prometheus.MustNewConstMetric(
			desc,
			prometheus.CounterValue,
			float64(value),
		)
	}
	counter(allocCountDesc, stats.allocations)
	counter(allocBytesDesc, stats.bytes_allocated)

	counter(deallocCountDesc, stats.deallocations)
	counter(deallocBytesDesc, stats.bytes_deallocated)

	counter(reallocCountDesc, stats.reallocations)
	counter(reallocBytesDesc, stats.bytes_reallocated)
}

var promCollectorInstance = &promCollector{}

var registration = sync.Once{}

// RegisterPrometheusCollector initializes prometheus metric collection that's pulled from the rust
// libbindings. Subsequent calls to this function will panic.
func RegisterPrometheusCollector() {
	prometheus.MustRegister(promCollectorInstance)
}

var (
	allocCountDesc = prometheus.NewDesc(
		"flow_rust_mem_alloc_count",
		"Number of memory allocations performed by Rust code",
		nil, nil,
	)
	allocBytesDesc = prometheus.NewDesc(
		"flow_rust_mem_alloc_bytes",
		"Total bytes of all allocations performed by Rust code",
		nil, nil,
	)
	deallocCountDesc = prometheus.NewDesc(
		"flow_rust_mem_dealloc_count",
		"Number of memory deallocations performed by Rust code",
		nil, nil,
	)
	deallocBytesDesc = prometheus.NewDesc(
		"flow_rust_mem_dealloc_bytes",
		"Total bytes of all deallocations performed by Rust code",
		nil, nil,
	)
	reallocCountDesc = prometheus.NewDesc(
		"flow_rust_mem_realloc_count",
		"Number of memory reallocations performed by Rust code",
		nil, nil,
	)
	reallocBytesDesc = prometheus.NewDesc(
		"flow_rust_mem_realloc_bytes",
		"Total bytes of all reallocations performed by Rust code",
		nil, nil,
	)
)
