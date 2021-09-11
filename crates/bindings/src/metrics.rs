use metrics::current_mem_stats;

#[repr(C)]
#[derive(Debug)]
pub struct MemoryStats {
    /// Monotonically increasing counter of the total number of allocations performed.
    allocations: u64,
    /// Monotonically increasing counter of the total number of bytes allocated.
    bytes_allocated: u64,
    /// Monotonically increasing counter of the total number of deallocations perfomed.
    deallocations: u64,
    /// Monotonically increasing counter of the total number of bytes deallocated.
    bytes_deallocated: u64,
    /// Monotonically increasing counter of the total number of reallocations perfomed.
    reallocations: u64,
    /// Monotonically increasing counter of the total number of bytes reallocated.
    bytes_reallocated: u64,
}

impl From<metrics::MemoryStats> for MemoryStats {
    fn from(stats: metrics::MemoryStats) -> MemoryStats {
        MemoryStats {
            allocations: stats.allocations as u64,
            bytes_allocated: stats.bytes_allocated as u64,
            deallocations: stats.deallocations as u64,
            bytes_deallocated: stats.bytes_deallocated as u64,
            reallocations: stats.reallocations as u64,
            bytes_reallocated: stats.bytes_reallocated as u64,
        }
    }
}

/// Returns general statistics on memory allocations perfomed from within libbindings.
#[no_mangle]
pub extern "C" fn get_memory_stats() -> MemoryStats {
    current_mem_stats().into()
}
