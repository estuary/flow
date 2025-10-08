use jemalloc_ctl::{epoch, epoch_mib, stats, thread};
use jemallocator::Jemalloc;
use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicU64, Ordering};

#[global_allocator]
static ALLOC: FlowAllocator = FlowAllocator;

/// Statistics related to memory allocations for the entire (rust portion) of the application. The
/// precise meaning of each field is included in the [jemalloc man
/// page](http://jemalloc.net/jemalloc.3.html). Each field in this struct can be found in the man
/// page prefixed by "stats.". The TLDR is that the fields in this struct are all effectively
/// gauges of the number of bytes.
#[derive(Debug)]
pub struct JemallocGlobalStats {
    pub active: u64,
    pub allocated: u64,
    pub mapped: u64,
    pub metadata: u64,
    pub resident: u64,
    pub retained: u64,

    /// counts are not part of the stats exposed by jemalloc, so see the struct definition for the
    /// meanings.
    pub counts: AllocationCounts,
}

/// Counts are collected by instrumenting the global allocator, rather than introspecting it.
/// These are monotonic counters of allocator invocations.
#[derive(Debug)]
pub struct AllocationCounts {
    /// Total number of allocation operations performed.
    pub alloc_ops: u64,
    /// Total number of deallocation operations performed.
    pub dealloc_ops: u64,
    /// Total number of reallocation operations performed.
    pub realloc_ops: u64,
}

/// Returns the current global (to rust) memory stats.
pub fn current_mem_stats() -> JemallocGlobalStats {
    MEM_STATS.current()
}

/// Memory allocation statistics that are scoped to a specific thread.
/// These are exposed by jemalloc, and the values are in terms of bytes.
#[derive(Debug, Clone, Copy)]
pub struct ThreadStats {
    pub allocated: u64,
    pub deallocated: u64,
}

impl ThreadStats {
    pub fn total_allocated(&self) -> i64 {
        self.allocated as i64 - self.deallocated as i64
    }
}

impl std::ops::Sub for ThreadStats {
    type Output = ThreadStats;
    fn sub(self, rhs: Self) -> Self::Output {
        ThreadStats {
            allocated: self.allocated - rhs.allocated,
            deallocated: self.deallocated - rhs.deallocated,
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

    fn current(&self) -> JemallocGlobalStats {
        // The epoch needs advanced in order to updated jemalloc's internal caches of statistics.
        // Without this, values may be quite stale.
        self.epoch_mib.advance().unwrap();
        JemallocGlobalStats {
            active: self.active_mib.read().unwrap() as u64,
            allocated: self.allocated_mib.read().unwrap() as u64,
            mapped: self.mapped_mib.read().unwrap() as u64,
            metadata: self.metadata_mib.read().unwrap() as u64,
            resident: self.resident_mib.read().unwrap() as u64,
            retained: self.retained_mib.read().unwrap() as u64,
            counts: FlowAllocator::get_counts(),
        }
    }
}

lazy_static::lazy_static! {
    static ref MEM_STATS: GlobalStatReader = GlobalStatReader::new();
    static ref THREAD_ALLOC_MIB: thread::allocatedp_mib = thread::allocatedp::mib().unwrap();
    static ref THREAD_DEALLOC_MIB: thread::deallocatedp_mib = thread::deallocatedp::mib().unwrap();
}

static ALLOCS_COUNT: AtomicU64 = AtomicU64::new(0);
static DEALLOCS_COUNT: AtomicU64 = AtomicU64::new(0);
static REALLOCS_COUNT: AtomicU64 = AtomicU64::new(0);

/// This allocator exists solely to instrument invocations of Jemalloc, which is the actual
/// allocator we're using.
struct FlowAllocator;
impl FlowAllocator {
    fn get_counts() -> AllocationCounts {
        AllocationCounts {
            alloc_ops: ALLOCS_COUNT.load(Ordering::SeqCst),
            dealloc_ops: DEALLOCS_COUNT.load(Ordering::SeqCst),
            realloc_ops: REALLOCS_COUNT.load(Ordering::SeqCst),
        }
    }
}
unsafe impl GlobalAlloc for FlowAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS_COUNT.fetch_add(1, Ordering::SeqCst);
        unsafe { Jemalloc.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCS_COUNT.fetch_add(1, Ordering::SeqCst);
        unsafe { Jemalloc.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOCS_COUNT.fetch_add(1, Ordering::SeqCst);
        unsafe { Jemalloc.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        REALLOCS_COUNT.fetch_add(1, Ordering::SeqCst);
        unsafe { Jemalloc.realloc(ptr, layout, new_size) }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_thread_local_mem_stats() {
        let reader = ThreadStatsReader::new();
        let start = reader.current();

        let some_vec: Vec<u8> = std::iter::repeat(1).take(8192).collect();
        let some_sum: usize = some_vec.iter().map(|i| *i as usize).sum();

        let end = reader.current();

        assert_eq!(8192, end.total_allocated() - start.total_allocated());
        assert_eq!(8192, (end - start).total_allocated());

        std::mem::drop(some_vec);

        let end = reader.current();
        assert_eq!(0, end.total_allocated() - start.total_allocated());

        println!("{some_sum} cause some_vec to be used and not elided by dead-code analysis");
    }
}
