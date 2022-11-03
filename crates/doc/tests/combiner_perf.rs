use doc::{Annotation, Pointer};
use json::schema::{build::build_schema, index::IndexBuilder};
use json::validator::{SpanContext, Validator};
use rand::{distributions::Distribution, Rng, SeedableRng};
use serde_json::{json, value::RawValue, Value};
use std::io::Write;
use std::rc::Rc;
use std::time::Instant;

// This benchmark is regularly run as part of our test sweet to ensure it remains functional.
// When actually developing it, you may wish to run as:
//
//   cargo test --release -p doc-poc --test combiner_perf -- --nocapture
//
// And additionally increase TOTAL_ROUNDS to a larger value.

// How many total rounds to run?
const TOTAL_ROUNDS: usize = 10000;
// Keys are drawn from the Zipfian distribution. The choice of parameter means
// that about 55% of sampled keys are unique, and the remaining 45% are duplicates.
// Of the duplicates, key 1 is about twice as likely as key 2, which is twice as
// likely as key 3, and so on. This distribution exercises our ability to reduce
// many instances of a repeated key while simultaneously handling many documents
// which occur rarely or only once.
const ZIPF_PARAM: f64 = 1.05;
// For each round, what is the probability of sampling a citi-bike ride sub-document?
// These model combines over a medium-sized document.
const PROB_SAMPLE_CITI: f64 = 0.5;
// For each round, what is the probability of sampling a github sub-document?
// These model combines over a large document.
const PROB_SAMPLE_GH: f64 = 0.5;
// For each round, what is the probability of sampling a set add & remove?
// These model reductions which have poor performance when applied strictly linearly
// (as opposed to taking advantage of associativity during reduction).
const PROB_SAMPLE_SET: f64 = 0.5;

// Re-use github and citi-bike event data for this benchmark. See:
// * crates/json/benches/github_events.rs
// * crates/json/benches/citi_rides.rs
const GITHUB_SCHEMA: &[u8] = include_bytes!("../../json/benches/testdata/github-event.schema.json");
const GITHUB_SCRAPES: &[&[u8]] = &[
    include_bytes!("../../json/benches/testdata/github-scrape1.json"),
    include_bytes!("../../json/benches/testdata/github-scrape2.json"),
    include_bytes!("../../json/benches/testdata/github-scrape3.json"),
    include_bytes!("../../json/benches/testdata/github-scrape4.json"),
];
const CITI_RIDES_SCHEMA: &[u8] =
    include_bytes!("../../json/benches/testdata/citi-rides.schema.json");
const CITI_RIDES: &[u8] = include_bytes!("../../json/benches/testdata/citi-rides1.json");

#[test]
pub fn combiner_perf() {
    let github_schema: Value = serde_json::from_slice(GITHUB_SCHEMA).unwrap();
    let citi_schema: Value = serde_json::from_slice(CITI_RIDES_SCHEMA).unwrap();
    let set_schema: Value = json!({
        "type": "object",
        "reduce": { "strategy": "set" },
        "additionalProperties": {
            "type": "object",
            "additionalProperties": {
                "type": "number",
                "reduce": {"strategy": "sum"}
            },
            "reduce": {"strategy": "lastWriteWins" }
        }
    });

    let url = url::Url::parse("http://schema").unwrap();
    let schema = build_schema::<Annotation>(
        url,
        &json!({
            "type": "object",
            "properties": {
                "key": {"type": "integer"},
                "citi": citi_schema,
                "gh": github_schema,
                "set": set_schema,
                "cnt": {
                    "type": "integer",
                    "reduce": {"strategy": "sum"},
                }
            },
            "required": ["key"],
            "additionalProperties": false,
            "reduce": {"strategy": "merge"}
        }),
    )
    .unwrap();
    let key: Rc<[Pointer]> = vec!["/key".into()].into();

    let mut index = IndexBuilder::new();
    index.add(&schema).unwrap();
    index.verify_references().unwrap();
    let index = index.into_index();

    // Load all github document fixtures into RawValue.
    let github_docs = GITHUB_SCRAPES
        .iter()
        .copied()
        .flat_map(|s| serde_json::from_slice::<Vec<Box<RawValue>>>(s).unwrap())
        .collect::<Vec<Box<RawValue>>>();

    // Load all citi-bike document fixtures into RawValue.
    let ride_docs = serde_json::Deserializer::from_slice(CITI_RIDES).into_iter::<Box<RawValue>>();
    let ride_docs = ride_docs.collect::<Result<Vec<_>, _>>().unwrap();

    // Assemble parts for document generation and validation.
    let mut rng = rand::rngs::SmallRng::seed_from_u64(8675309);
    let key_dist = rand_distr::Zipf::new(u64::MAX, ZIPF_PARAM).unwrap();
    let mut val = Validator::<Annotation, SpanContext>::new(&index);

    // Initialize the combiner itself.
    let mut accum = doc::combine::Accumulator::new(
        key.clone(),
        schema.curi.clone(),
        tempfile::tempfile().unwrap(),
    )
    .unwrap();

    // Begin to measure performance.
    let start_stats = allocator::current_mem_stats();
    let begin = Instant::now();

    let mut buf = Vec::new();
    for _round in 0..TOTAL_ROUNDS {
        // Build up the the next document to combine.
        buf.clear();
        write!(
            &mut buf,
            "{{\"key\":{},\"cnt\":1",
            key_dist.sample(&mut rng) as u64
        )
        .unwrap();

        if rng.gen_bool(PROB_SAMPLE_CITI) {
            write!(
                &mut buf,
                ",\"citi\":{}",
                ride_docs[rng.gen::<usize>() % ride_docs.len()].get()
            )
            .unwrap();
        }
        if rng.gen_bool(PROB_SAMPLE_GH) {
            write!(
                &mut buf,
                ",\"gh\":{}",
                github_docs[rng.gen::<usize>() % github_docs.len()].clone(),
            )
            .unwrap();
        }
        if rng.gen_bool(PROB_SAMPLE_SET) {
            write!(
                &mut buf,
                ",\"set\":{{\"add\":{{\"s{}\":1}},\"remove\":{{\"s{}\":1}}}}",
                rng.gen::<u16>() % 1024,
                rng.gen::<u16>() % 1024,
            )
            .unwrap();
        }
        buf.push(b'}');

        let memtable = accum.memtable().unwrap();
        let doc = doc::HeapNode::from_serde(
            &mut serde_json::Deserializer::from_slice(&buf),
            memtable.alloc(),
        )
        .unwrap();

        memtable.combine_right(doc, &mut val).unwrap();
    }

    let peak_stats = allocator::current_mem_stats();
    let mut drained: usize = 0;

    let mut drainer = accum.into_drainer().unwrap();
    while drainer
        .drain_while(&mut val, |_entry, _reduce| {
            drained += 1;
            Ok::<_, doc::combine::Error>(true)
        })
        .unwrap()
    {}

    let duration = begin.elapsed();
    let trough_stats = allocator::current_mem_stats();

    eprintln!(
        "Rounds: {}\nDrained: {}\nElapsed: {}s\nMemory: active {}MB allocated {}MB resident {}MB retained {}MB alloc_ops {} dealloc_ops {} realloc_ops {}",
        TOTAL_ROUNDS,
        drained,
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
