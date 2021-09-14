use jemalloc_ctl::{epoch, epoch_mib, stats, thread};

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

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

/// Returns the current global (to rust) memory stats.
pub fn current_mem_stats() -> GlobalMemoryStats {
    MEM_STATS.current()
}

/// Memory allocation statistics that are scoped to a specific thread.
#[derive(Debug, Clone, Copy)]
pub struct ThreadStats {
    pub allocated: u64,
    pub deallocated: u64,
}

impl std::ops::Sub for ThreadStats {
    type Output = ThreadStats;
    fn sub(self, rhs: Self) -> Self::Output {
        ThreadStats {
            allocated: rhs.allocated - self.allocated,
            deallocated: rhs.deallocated - self.deallocated,
        }
    }
}

/// Allows reading memory allocation stats for the current thread.
pub struct ThreadStatsReader {
    allocated: thread::ThreadLocal<u64>,
    deallocated: thread::ThreadLocal<u64>,
}

impl ThreadStatsReader {
    pub fn new() -> ThreadStatsReader {
        ThreadStatsReader {
            allocated: THREAD_ALLOC_MIB.read().unwrap(),
            deallocated: THREAD_DEALLOC_MIB.read().unwrap(),
        }
    }

    /// Return the current cumulative totals for the current thread.
    pub fn current(&self) -> ThreadStats {
        ThreadStats {
            allocated: self.allocated.get(),
            deallocated: self.deallocated.get(),
        }
    }
}

/// A helper that reads allocation stats from jemalloc using the MIB API, which caches the lookups
/// of string keys to make reading the values faster.
struct GlobalStatReader {
    epoch_mib: epoch_mib,
    active_mib: stats::active_mib,
    allocated_mib: stats::allocated_mib,
    mapped_mib: stats::mapped_mib,
    metadata_mib: stats::metadata_mib,
    resident_mib: stats::resident_mib,
    retained_mib: stats::retained_mib,
}

impl GlobalStatReader {
    fn new() -> GlobalStatReader {
        GlobalStatReader {
            epoch_mib: epoch::mib().unwrap(),
            active_mib: stats::active::mib().unwrap(),
            allocated_mib: stats::allocated::mib().unwrap(),
            mapped_mib: stats::mapped::mib().unwrap(),
            metadata_mib: stats::metadata::mib().unwrap(),
            resident_mib: stats::resident::mib().unwrap(),
            retained_mib: stats::retained::mib().unwrap(),
        }
    }

    fn current(&self) -> GlobalMemoryStats {
        // The epoch needs advanced in order to updated jemalloc's internal caches of statistics.
        // Without this, values may be quite stale.
        self.epoch_mib.advance().unwrap();
        GlobalMemoryStats {
            active: self.active_mib.read().unwrap() as u64,
            allocated: self.allocated_mib.read().unwrap() as u64,
            mapped: self.mapped_mib.read().unwrap() as u64,
            metadata: self.metadata_mib.read().unwrap() as u64,
            resident: self.resident_mib.read().unwrap() as u64,
            retained: self.retained_mib.read().unwrap() as u64,
        }
    }
}

lazy_static::lazy_static! {
    static ref MEM_STATS: GlobalStatReader = GlobalStatReader::new();
    static ref THREAD_ALLOC_MIB: thread::allocatedp_mib = thread::allocatedp::mib().unwrap();
    static ref THREAD_DEALLOC_MIB: thread::deallocatedp_mib = thread::deallocatedp::mib().unwrap();
}
