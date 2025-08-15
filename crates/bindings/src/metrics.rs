use allocator::current_mem_stats;

/// Statistics related to memory allocations for the entire (rust portion) of the application. The
/// precise meaning of most fields are included in the [jemalloc man
/// page](http://jemalloc.net/jemalloc.3.html). The first group of fields in this struct can be
/// found in the man page prefixed by "stats.". These fields are all gauges that are in terms of
/// bytes.
///
/// The `_ops_total` fields are _not_ provided by jemalloc, but instead come from instrumenting the
/// allocator to track the number of invocations. Those represent monotonic counters of the number
/// of invocations.
#[derive(Debug)]
#[repr(C)]
pub struct GlobalMemoryStats {
    pub active: u64,
    pub allocated: u64,
    pub mapped: u64,
    pub metadata: u64,
    pub resident: u64,
    pub retained: u64,

    pub alloc_ops_total: u64,
    pub dealloc_ops_total: u64,
    pub realloc_ops_total: u64,
}

impl From<allocator::JemallocGlobalStats> for GlobalMemoryStats {
    fn from(s: allocator::JemallocGlobalStats) -> Self {
        GlobalMemoryStats {
            active: s.active,
            allocated: s.allocated,
            mapped: s.mapped,
            metadata: s.metadata,
            resident: s.resident,
            retained: s.retained,

            alloc_ops_total: s.counts.alloc_ops,
            dealloc_ops_total: s.counts.dealloc_ops,
            realloc_ops_total: s.counts.realloc_ops,
        }
    }
}

/// Returns general statistics on memory allocations performed from within libbindings.
#[unsafe(no_mangle)]
pub extern "C" fn get_memory_stats() -> GlobalMemoryStats {
    current_mem_stats().into()
}
