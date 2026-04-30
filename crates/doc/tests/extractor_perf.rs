use doc::{ArchivedNode, Extractor, ExtractorPlan, HeapNode, SerPolicy};
use serde_json::{Map, Value, json};
use std::sync::atomic::AtomicBool;
use std::time::Instant;

// This benchmark compares `Extractor::extract_all_indicate_truncation`
// against `ExtractorPlan::extract_all_indicate_truncation` across the
// essential planner performance cases.
//
// Run in CI to keep it functional; for manual investigation:
//
//   cargo test --release -p doc --test extractor_perf -- --nocapture

// How many total rounds to run?
const TOTAL_ROUNDS: usize = 100;

#[derive(Debug)]
struct CaseReport {
    name: &'static str,
    extractor_count: usize,
    heap_extractor_ns_per_doc: f64,
    heap_plan_ns_per_doc: f64,
    archived_extractor_ns_per_doc: f64,
    archived_plan_ns_per_doc: f64,
}

#[test]
fn extractor_perf() {
    let policy = SerPolicy::noop();
    let begin = Instant::now();

    let mut reports: Vec<CaseReport> = Vec::new();
    for (name, doc, extractors) in [
        build_single_wide_block(&policy),
        build_nested_blocks_with_singles(&policy),
        build_no_blocks(&policy),
        build_sparse_block_large_parent(&policy),
        build_many_small_blocks(&policy),
        build_deeply_nested_blocks(&policy),
    ] {
        reports.push(run_case(name, &doc, &extractors));
    }

    let duration = begin.elapsed();

    eprintln!("\n=== extractor_perf ({TOTAL_ROUNDS} rounds/case) ===");
    for r in &reports {
        eprintln!(
            "\n{name}:\n  extractors={extractors}\n  HeapNode     Extractor={h_ex:.0}ns   ExtractorPlan={h_pl:.0}ns   ({speed_h:.2}x)\n  ArchivedNode Extractor={a_ex:.0}ns   ExtractorPlan={a_pl:.0}ns   ({speed_a:.2}x)",
            name = r.name,
            extractors = r.extractor_count,
            h_ex = r.heap_extractor_ns_per_doc,
            h_pl = r.heap_plan_ns_per_doc,
            speed_h = r.heap_extractor_ns_per_doc / r.heap_plan_ns_per_doc.max(1.0),
            a_ex = r.archived_extractor_ns_per_doc,
            a_pl = r.archived_plan_ns_per_doc,
            speed_a = r.archived_extractor_ns_per_doc / r.archived_plan_ns_per_doc.max(1.0),
        );
    }
    eprintln!(
        "\nRounds: {}\nCases: {}\nElapsed: {}s",
        TOTAL_ROUNDS,
        reports.len(),
        duration.as_secs_f64(),
    );
}

fn run_case(name: &'static str, doc: &Value, extractors: &[Extractor]) -> CaseReport {
    let alloc = HeapNode::new_allocator();
    let heap = HeapNode::from_serde(doc, &alloc).unwrap();
    let archive = heap.to_archive();
    let archived = ArchivedNode::from_archive(&archive);

    let plan = ExtractorPlan::new(extractors);

    let extractor_count = extractors.len();

    let heap_ex = time_extractor(extractors, &heap);
    let heap_pl = time_plan(&plan, &heap);
    let arch_ex = time_extractor(extractors, archived);
    let arch_pl = time_plan(&plan, archived);

    CaseReport {
        name,
        extractor_count,
        heap_extractor_ns_per_doc: heap_ex,
        heap_plan_ns_per_doc: heap_pl,
        archived_extractor_ns_per_doc: arch_ex,
        archived_plan_ns_per_doc: arch_pl,
    }
}

fn time_extractor<N: json::AsNode>(extractors: &[Extractor], doc: &N) -> f64 {
    let mut buf = bytes::BytesMut::with_capacity(4096);
    // Warm up.
    for _ in 0..(TOTAL_ROUNDS / 10).max(1) {
        buf.clear();
        let indicator = AtomicBool::new(false);
        Extractor::extract_all_indicate_truncation(doc, extractors, &mut buf, &indicator);
    }

    let start = Instant::now();
    for _ in 0..TOTAL_ROUNDS {
        buf.clear();
        let indicator = AtomicBool::new(false);
        Extractor::extract_all_indicate_truncation(doc, extractors, &mut buf, &indicator);
    }
    let elapsed = start.elapsed().as_nanos() as f64;
    elapsed / TOTAL_ROUNDS as f64
}

fn time_plan<N: json::AsNode>(plan: &ExtractorPlan, doc: &N) -> f64 {
    let mut buf = bytes::BytesMut::with_capacity(4096);
    for _ in 0..(TOTAL_ROUNDS / 10).max(1) {
        buf.clear();
        let indicator = AtomicBool::new(false);
        plan.extract_all_indicate_truncation(doc, &mut buf, &indicator);
    }

    let start = Instant::now();
    for _ in 0..TOTAL_ROUNDS {
        buf.clear();
        let indicator = AtomicBool::new(false);
        plan.extract_all_indicate_truncation(doc, &mut buf, &indicator);
    }
    let elapsed = start.elapsed().as_nanos() as f64;
    elapsed / TOTAL_ROUNDS as f64
}

fn build_single_wide_block(policy: &SerPolicy) -> (&'static str, Value, Vec<Extractor>) {
    // One top-level merge-join block. This is the primary happy path.
    const N: usize = 64;

    let doc = Value::Object(numbered_fields("f", N));
    let extractors = (0..N)
        .map(|i| Extractor::new(&format!("/f_{i:03}"), policy))
        .collect();

    ("single wide block", doc, extractors)
}

fn build_nested_blocks_with_singles(policy: &SerPolicy) -> (&'static str, Value, Vec<Extractor>) {
    // Realistic materialization shape: two wide nested blocks plus metadata,
    // UUID magic, and truncation indicator singles.
    const PER_SECTION: usize = 32;

    let doc = json!({
        "_meta": {
            "uuid": "85bad119-15f2-11ee-8401-43f05f562888",
            "op": "u",
            "source": {"ts_ms": 1_696_000_000_000u64, "table": "orders"},
        },
        "after": Value::Object(numbered_fields("col", PER_SECTION)),
        "before": Value::Object(numbered_fields("col", PER_SECTION)),
    });

    let mut extractors = Vec::new();
    extractors.push(Extractor::for_uuid_v1_date_time("/_meta/uuid"));
    extractors.push(Extractor::new("/_meta/op", policy));
    extractors.push(Extractor::new("/_meta/source/table", policy));
    extractors.push(Extractor::new("/_meta/source/ts_ms", policy));
    for i in 0..PER_SECTION {
        extractors.push(Extractor::new(&format!("/after/col_{i:03}"), policy));
    }
    for i in 0..PER_SECTION {
        extractors.push(Extractor::new(&format!("/before/col_{i:03}"), policy));
    }
    extractors.push(Extractor::for_truncation_indicator());

    ("cdc blocks with singles", doc, extractors)
}

fn build_no_blocks(policy: &SerPolicy) -> (&'static str, Value, Vec<Extractor>) {
    // Every consecutive pair of extractors targets a different parent, so each
    // run is a singleton and no blocks form. Measures plan-overhead parity
    // against the reference path's pure `Fields::get` walk.
    let doc = json!({
        "_meta": {
            "timestamp": "2024-06-01T12:34:56Z",
            "uuid": "85bad119-15f2-11ee-8401-43f05f562888",
            "version": "v2",
        },
        "client": {"ip": "10.0.0.5", "platform": "macos", "user_agent": "example/1.0"},
        "request": {
            "duration_ms": 37,
            "headers": {"content_type": "application/json"},
            "method": "POST",
            "path": "/api/v1/orders",
        },
        "response": {"size_bytes": 4096, "status": 200},
        "trace": {"span_id": "abc123", "trace_id": "def456"},
        "user": {"id": 42, "org_id": "acme", "role": "admin"},
    });

    let extractors = vec![
        Extractor::new("/_meta/timestamp", policy),
        Extractor::new("/client/ip", policy),
        Extractor::new("/request/duration_ms", policy),
        Extractor::new("/request/headers/content_type", policy),
        Extractor::new("/response/size_bytes", policy),
        Extractor::new("/trace/span_id", policy),
        Extractor::new("/user/id", policy),
        Extractor::for_uuid_v1_date_time("/_meta/uuid"),
        Extractor::new("/client/platform", policy),
        Extractor::new("/request/method", policy),
        Extractor::new("/response/status", policy),
        Extractor::new("/trace/trace_id", policy),
        Extractor::new("/user/org_id", policy),
        Extractor::new("/_meta/version", policy),
        Extractor::new("/client/user_agent", policy),
        Extractor::new("/request/path", policy),
        Extractor::new("/user/role", policy),
    ];

    ("no blocks", doc, extractors)
}

fn build_sparse_block_large_parent(policy: &SerPolicy) -> (&'static str, Value, Vec<Extractor>) {
    // Risk case: the block is much smaller than the parent object, so the
    // merge path scans many fields that are not projected.
    const PARENT_FIELDS: usize = 512;
    const BLOCK: usize = 32;

    let doc = Value::Object(numbered_fields("f", PARENT_FIELDS));
    let extractors = (0..BLOCK)
        .map(|i| {
            let target = (i * (PARENT_FIELDS / BLOCK) + 7) % PARENT_FIELDS;
            Extractor::new(&format!("/f_{target:03}"), policy)
        })
        .collect();

    ("sparse block, large parent", doc, extractors)
}

fn build_many_small_blocks(policy: &SerPolicy) -> (&'static str, Value, Vec<Extractor>) {
    // Per-block overhead case: many parents, each just large enough to form
    // a block.
    const SECTIONS: [&str; 8] = [
        "client", "context", "env", "request", "response", "server", "trace", "user",
    ];
    const FIELDS_PER_SECTION: usize = 10;

    let mut doc_map = Map::new();
    for section in SECTIONS {
        doc_map.insert(
            section.to_string(),
            Value::Object(numbered_fields("f", FIELDS_PER_SECTION)),
        );
    }
    let doc = Value::Object(doc_map);

    let mut extractors = Vec::with_capacity(SECTIONS.len() * FIELDS_PER_SECTION);
    for section in SECTIONS {
        for i in 0..FIELDS_PER_SECTION {
            extractors.push(Extractor::new(&format!("/{section}/f_{i:03}"), policy));
        }
    }

    ("many small blocks", doc, extractors)
}

fn build_deeply_nested_blocks(policy: &SerPolicy) -> (&'static str, Value, Vec<Extractor>) {
    // 500 fields across five nesting levels: 100 at root, plus 100 more at each
    // of /a, /a/b, /a/b/c, /a/b/c/d. Each level forms its own wide block.
    const PER_LEVEL: usize = 100;

    // Build deepest-first, wrap outward.
    let d = numbered_fields("f", PER_LEVEL);
    let mut c = numbered_fields("f", PER_LEVEL);
    c.insert("d".into(), Value::Object(d));
    let mut b = numbered_fields("f", PER_LEVEL);
    b.insert("c".into(), Value::Object(c));
    let mut a = numbered_fields("f", PER_LEVEL);
    a.insert("b".into(), Value::Object(b));
    let mut root = numbered_fields("f", PER_LEVEL);
    root.insert("a".into(), Value::Object(a));

    let doc = Value::Object(root);

    let mut extractors = Vec::with_capacity(PER_LEVEL * 5);
    for prefix in ["", "/a", "/a/b", "/a/b/c", "/a/b/c/d"] {
        for i in 0..PER_LEVEL {
            extractors.push(Extractor::new(&format!("{prefix}/f_{i:03}"), policy));
        }
    }

    ("deeply nested blocks", doc, extractors)
}

fn numbered_fields(prefix: &str, n: usize) -> Map<String, Value> {
    let mut fields = Map::new();
    for i in 0..n {
        fields.insert(format!("{prefix}_{i:03}"), json!(format!("v_{i}")));
    }
    fields
}
