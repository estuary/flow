use doc::{Extractor, Validator};
use json::schema::build::build_schema;
use rand::{Rng, SeedableRng};
use rand_distr::Distribution;
use serde_json::{Value, json, value::RawValue};
use std::io::Write;
use std::time::Instant;

// This benchmark is regularly run as part of our test sweet to ensure it remains functional.
// When actually developing it, you may wish to run as:
//
//   cargo test --release -p doc --test combiner_perf -- --nocapture
//
// And additionally increase TOTAL_ROUNDS to a larger value.

// How many total rounds to run?
const TOTAL_ROUNDS: usize = 1000;
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

    let schema = build_schema(
        &url::Url::parse("http://schema").unwrap(),
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
    let key_dist = rand_distr::Zipf::new(u64::MAX as f64, ZIPF_PARAM).unwrap();

    // Initialize the combiner itself.
    let spec = doc::combine::Spec::with_one_binding(
        true, // Full reductions.
        vec![Extractor::new("/key", &doc::SerPolicy::noop())],
        "source-name",
        Vec::new(),
        Validator::new(schema).unwrap(),
    );
    let mut accum = doc::combine::Accumulator::new(spec, tempfile::tempfile().unwrap()).unwrap();

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

        if rng.random::<f64>() < PROB_SAMPLE_CITI {
            write!(
                &mut buf,
                ",\"citi\":{}",
                ride_docs[(rng.random::<f64>() * ride_docs.len() as f64) as usize].get()
            )
            .unwrap();
        }
        if rng.random::<f64>() < PROB_SAMPLE_GH {
            write!(
                &mut buf,
                ",\"gh\":{}",
                github_docs[(rng.random::<f64>() * github_docs.len() as f64) as usize].clone(),
            )
            .unwrap();
        }
        if rng.random::<f64>() < PROB_SAMPLE_SET {
            write!(
                &mut buf,
                ",\"set\":{{\"add\":{{\"s{}\":1}},\"remove\":{{\"s{}\":1}}}}",
                rng.random::<u16>() % 1024,
                rng.random::<u16>() % 1024,
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

        memtable.add(0, doc, false).unwrap();
    }

    let peak_stats = allocator::current_mem_stats();
    let mut drained: usize = 0;
    let mut shape = doc::Shape::nothing();

    for drained_doc in accum.into_drainer().unwrap() {
        let drained_doc = drained_doc.unwrap();
        drained += 1;
        shape.widen_owned(&drained_doc.root);
    }

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

// This benchmark simulates a "history mode" combiner, which performs no
// reductions. A worst case scenario is evaluated: Each segment contains many
// documents with non-overlapping keys, but separate segments contain documents
// with that same set of keys.

// Number of document to generate in the history mode performance test. A single
// segment contains about 215k documents, and at least 10 segments should be
// generated to exercise the allocator freeing behavior. Change this to a much
// larger value if evaluating this kind of combiner performance in detail.
const HIST_ROUNDS: usize = 1000;

#[test]
pub fn combiner_perf_history_mode() {
    let schema = build_schema(
        &url::Url::parse("http://schema").unwrap(),
        &json!({
            "type": "object",
            "required": ["id", "_meta"],
            "reduce": {"strategy": "merge"},
            "properties": {
                "id": {"type": "integer"},
                "_meta": {
                    "type": "object",
                    "reduce": {"strategy": "merge"},
                    "required": ["source"],
                    "properties": {
                        "source": {
                            "type": "object",
                            "reduce": {"strategy": "lastWriteWins", "associative": false},
                            "required": ["lsn"],
                            "properties": {
                                "lsn": {"type": "string"},
                            }
                        },
                    }
                },
                // Some extra fields to increase allocation per document.
                "stringField1": {"type": "string"},
                "stringField2": {"type": "string"},
                "stringField3": {"type": "string"},
                "stringField4": {"type": "string"},
                "stringField5": {"type": "string"},
                "stringField6": {"type": "string"},
                "stringField7": {"type": "string"},
                "stringField8": {"type": "string"},
                "stringField9": {"type": "string"},
                "stringField10": {"type": "string"}
            }
        }),
    )
    .unwrap();

    let spec = doc::combine::Spec::with_one_binding(
        false,
        vec![Extractor::new("/id", &doc::SerPolicy::noop())],
        "source-name",
        Vec::new(),
        Validator::new(schema).unwrap(),
    );
    let mut accum = doc::combine::Accumulator::new(spec, tempfile::tempfile().unwrap()).unwrap();

    let begin = Instant::now();
    let mut segment_count = 1;
    let mut document_id: u64 = 0;
    let mut last_alloc_bytes: usize = 0;

    let mut buf = Vec::new();
    for round in 0..HIST_ROUNDS {
        buf.clear();
        write!(
            &mut buf,
            concat!(
                "{{",
                "\"id\":{},",
                "\"_meta\":{{\"source\":{{\"lsn\":\"lsn_{}\"}}}},",
                "\"stringField1\":\"value1\",",
                "\"stringField2\":\"value2\",",
                "\"stringField3\":\"value3\",",
                "\"stringField4\":\"value4\",",
                "\"stringField5\":\"value5\",",
                "\"stringField6\":\"value6\",",
                "\"stringField7\":\"value7\",",
                "\"stringField8\":\"value8\",",
                "\"stringField9\":\"value9\",",
                "\"stringField10\":\"value10\"",
                "}}"
            ),
            document_id, round,
        )
        .unwrap();

        let memtable = accum.memtable().unwrap();
        let alloc_bytes = memtable.alloc().allocated_bytes();

        // Detect spills - memtable() creates new memtable after spill.
        // Reset document_id so each segment has the same keys.
        if alloc_bytes < last_alloc_bytes {
            segment_count += 1;
            document_id = 0;
        }

        let doc = doc::HeapNode::from_serde(
            &mut serde_json::Deserializer::from_slice(&buf),
            memtable.alloc(),
        )
        .unwrap();

        memtable.add(0, doc, false).unwrap();
        last_alloc_bytes = memtable.alloc().allocated_bytes();
        document_id += 1;
    }

    let mut drained: usize = 0;
    let mut shape = doc::Shape::nothing();

    eprintln!("Draining {} segments...", segment_count);

    for drained_doc in accum.into_drainer().unwrap() {
        let drained_doc = drained_doc.unwrap();
        drained += 1;
        shape.widen_owned(&drained_doc.root);

        // Periodically check current memory stats, since once the drainer is
        // dropped its allocator is also dropped.
        if drained % 100_000 == 0 {
            let trough_stats = allocator::current_mem_stats();
            eprintln!(
                "Draining in progress: Drained: {} Memory: active {}MB allocated {}MB resident {}MB retained {}MB",
                drained,
                trough_stats.active / (1024 * 1024),
                trough_stats.allocated / (1024 * 1024),
                trough_stats.resident / (1024 * 1024),
                trough_stats.retained / (1024 * 1024),
            );
        }
    }

    let duration = begin.elapsed();

    eprintln!(
        "Rounds: {}\nDrained: {}\nElapsed: {}s",
        TOTAL_ROUNDS,
        drained,
        duration.as_secs_f64(),
    );
}
