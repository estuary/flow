use metrics::current_mem_stats;

// TODO: this struct is re-defined here because otherwise it's not getting included in the header
// file. Figure out a way to either export the definition from the metrics crate, or else try to
// just put it all here and get rid of the metrics crate.
/// Statistics related to memory allocations for the entire (rust portion) of the application. The
/// precise meaning of each field is included in the [jemalloc man
/// page](http://jemalloc.net/jemalloc.3.html). Each field in this struct can be found in the man
/// page prefixed by "stats.".
#[derive(Debug)]
#[repr(C)]
pub struct GlobalMemoryStats {
    pub active: u64,
    pub allocated: u64,
    pub mapped: u64,
    pub metadata: u64,
    pub resident: u64,
    pub retained: u64,
}

impl From<metrics::GlobalMemoryStats> for GlobalMemoryStats {
    fn from(s: metrics::GlobalMemoryStats) -> Self {
        GlobalMemoryStats {
            active: s.active,
            allocated: s.allocated,
            mapped: s.mapped,
            metadata: s.metadata,
            resident: s.resident,
            retained: s.retained,
        }
    }
}

/// Returns general statistics on memory allocations perfomed from within libbindings.
#[no_mangle]
pub extern "C" fn get_memory_stats() -> GlobalMemoryStats {
    current_mem_stats().into()
}
