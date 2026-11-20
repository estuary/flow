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
	var gauge = func(desc *prometheus.Desc, value C.uint64_t) {
		ch <- prometheus.MustNewConstMetric(
			desc,
			prometheus.GaugeValue,
			float64(value),
		)
	}
	gauge(activeBytesDesc, stats.active)
	gauge(allocBytesDesc, stats.allocated)
	gauge(mappedBytesDesc, stats.mapped)
	gauge(metadataBytesDesc, stats.metadata)
	gauge(residentBytesDesc, stats.resident)
	gauge(retainedBytesDesc, stats.retained)

	var counter = func(desc *prometheus.Desc, value C.uint64_t) {
		ch <- prometheus.MustNewConstMetric(
			desc,
			prometheus.CounterValue,
			float64(value),
		)
	}
	counter(allocOpsDesc, stats.alloc_ops_total)
	counter(deallocOpsDesc, stats.dealloc_ops_total)
	counter(reallocOpsDesc, stats.realloc_ops_total)
}

var promCollectorInstance = &promCollector{}

var registration = sync.Once{}

// RegisterPrometheusCollector initializes prometheus metric collection that's pulled from the rust
// libbindings. Subsequent calls to this function will panic.
func RegisterPrometheusCollector() {
	prometheus.MustRegister(promCollectorInstance)
}

var (
	activeBytesDesc = prometheus.NewDesc(
		"flow_rust_mem_active_bytes",
		"Total number of bytes in active pages allocated by the application.",
		nil, nil,
	)
	allocBytesDesc = prometheus.NewDesc(
		"flow_rust_mem_alloc_bytes",
		"Total bytes of all allocations performed by Rust code",
		nil, nil,
	)
	mappedBytesDesc = prometheus.NewDesc(
		"flow_rust_mem_mapped_bytes",
		"Total number of bytes in active extents mapped by the allocator.",
		nil, nil,
	)
	metadataBytesDesc = prometheus.NewDesc(
		"flow_rust_mem_metadata_bytes",
		"Total number of bytes dedicated to metadata, which comprise base allocations used for bootstrap-sensitive allocator metadata structures",
		nil, nil,
	)
	residentBytesDesc = prometheus.NewDesc(
		"flow_rust_mem_resident_bytes",
		"Maximum number of bytes in physically resident data pages mapped by the allocator",
		nil, nil,
	)
	retainedBytesDesc = prometheus.NewDesc(
		"flow_rust_mem_retained_bytes",
		"Total number of bytes in virtual memory mappings that were retained rather than being returned to the operating system",
		nil, nil,
	)

	allocOpsDesc = prometheus.NewDesc(
		"flow_rust_mem_alloc_ops_total",
		"Count of allocation operations performed by Rust code",
		nil, nil,
	)
	deallocOpsDesc = prometheus.NewDesc(
		"flow_rust_mem_dealloc_ops_total",
		"Count of deallocation operations performed by Rust code",
		nil, nil,
	)
	reallocOpsDesc = prometheus.NewDesc(
		"flow_rust_mem_realloc_ops_total",
		"Count of reallocation operations performed by Rust code",
		nil, nil,
	)
)
