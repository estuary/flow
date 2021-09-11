use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
use std::alloc::System;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

pub use stats_alloc::Stats as MemoryStats;

/// Returns a `Region`, which can be used to track changes to memory allocations across two
/// or more timepoints.
pub fn track_mem_stats() -> Region<'static, System> {
    Region::new(GLOBAL)
}

/// Returns the current memory stats for this process. This operation is pretty cheap, as it only
/// loads the integer values from the instrumented allocator.
pub fn current_mem_stats() -> MemoryStats {
    INSTRUMENTED_SYSTEM.stats()
}
