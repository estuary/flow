use rand::{Rng, SeedableRng};
use serde_json::Value;
use std::time::Instant;

// This benchmark is regularly run as part of our test suite to ensure it remains functional.
// When actually developing it, you may wish to run as:
//
//   cargo test --release -p doc --test widen_perf -- --nocapture
//
// And additionally increase TOTAL_ROUNDS to a much larger value.
//
// NOTE(johnny): As of Aug 9 2023, performance of the routine in this
// benchmark is dominated by cache misses (L1-LLC) when walking the input
// document. However, when widen() is called from the combiner_perf
// benchmark, there are *substantially* more cache misses of the Shape
// structures themselves.
//
// In both cases branch miss-predictions appear low (<2.0%) so,
// aside from doing less work overall, the key to making this faster
// will be running it in a tight loop over packed, sequential ArchivedNode<>'s.

// How many total rounds to run?
const TOTAL_ROUNDS: usize = 10_000;

// Re-use github and citi-bike event data for this benchmark. See:
// * crates/json/benches/github_events.rs
// * crates/json/benches/citi_rides.rs
const GITHUB_SCRAPES: &[&[u8]] = &[
    include_bytes!("../../json/benches/testdata/github-scrape1.json"),
    include_bytes!("../../json/benches/testdata/github-scrape2.json"),
    include_bytes!("../../json/benches/testdata/github-scrape3.json"),
    include_bytes!("../../json/benches/testdata/github-scrape4.json"),
];
const CITI_RIDES: &[u8] = include_bytes!("../../json/benches/testdata/citi-rides1.json");

#[test]
pub fn widen_perf() {
    // Load all github document fixtures into Value.
    let github_docs = GITHUB_SCRAPES
        .iter()
        .copied()
        .flat_map(|s| serde_json::from_slice::<Vec<Value>>(s).unwrap())
        .collect::<Vec<Value>>();

    // Load all citi-bike document fixtures into Value.
    let ride_docs = serde_json::Deserializer::from_slice(CITI_RIDES).into_iter::<Value>();
    let ride_docs = ride_docs.collect::<Result<Vec<_>, _>>().unwrap();

    // Assemble parts for document generation and validation.
    let mut rng = rand::rngs::SmallRng::seed_from_u64(8675309);
    let mut shape = doc::Shape::nothing();

    // Begin to measure performance.
    let start_stats = allocator::current_mem_stats();
    let begin = Instant::now();

    for _round in 0..TOTAL_ROUNDS {
        shape.widen(&ride_docs[(rng.random::<f64>() * ride_docs.len() as f64) as usize]);
        shape.widen(&github_docs[(rng.random::<f64>() * github_docs.len() as f64) as usize]);
    }

    let duration = begin.elapsed();
    let peak_stats = allocator::current_mem_stats();

    std::mem::forget(shape);

    let trough_stats = allocator::current_mem_stats();

    eprintln!(
        "Rounds: {}\nElapsed: {}s\nMemory: active {}MB allocated {}MB resident {}MB retained {}MB alloc_ops {} dealloc_ops {} realloc_ops {}",
        TOTAL_ROUNDS,
        duration.as_secs_f64(),
        peak_stats.active / (1024 * 1024),
        peak_stats.allocated / (1024 * 1024),
        peak_stats.resident / (1024 * 1024),
        peak_stats.retained / (1024 * 1024),
        trough_stats.counts.alloc_ops - start_stats.counts.alloc_ops,
        trough_stats.counts.dealloc_ops - start_stats.counts.dealloc_ops,
        trough_stats.counts.realloc_ops - start_stats.counts.realloc_ops,
    );
}
