use std::time::Instant;

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

const TOTAL_ROUNDS: usize = 5_000;
const CHUNK_SIZE: usize = 1 << 17; // 128K.

#[test]
pub fn parser_perf() {
    let mut fixture: Vec<u8> = Vec::new();

    let mut add_doc = |doc: serde_json::Value| {
        serde_json::to_writer(&mut fixture, &doc).unwrap();
        fixture.push(b'\n');
    };
    add_doc(serde_json::from_slice(GITHUB_SCHEMA).unwrap());

    for scrape in GITHUB_SCRAPES {
        let items: Vec<_> = serde_json::from_slice(*scrape).unwrap();
        for item in items {
            add_doc(item);
        }
    }
    add_doc(serde_json::from_slice(CITI_RIDES_SCHEMA).unwrap());
    fixture.extend_from_slice(CITI_RIDES); // Already JSON newlines.

    let chunks: Vec<_> = fixture.chunks(CHUNK_SIZE).collect();

    let mut input = Vec::new();
    let mut parser = simd_doc::Parser::new();

    let mut docs: usize = 0;
    let mut bytes: usize = 0;

    // Begin to measure performance.
    let start_stats = allocator::current_mem_stats();
    let begin = Instant::now();

    for _ in 0..TOTAL_ROUNDS {
        for chunk in &chunks {
            output.clear();
            alloc.reset();
            input.extend_from_slice(*chunk);

            () = parser
                .parse_simd(
                    &alloc,
                    unsafe { std::mem::transmute(&mut output) },
                    &mut input,
                )
                .unwrap();

            use rkyv::ser::Serializer;

            let wiz =
                rkyv::AlignedVec::with_capacity(alloc.allocated_bytes() - alloc.chunk_capacity());

            let mut serializer = rkyv::ser::serializers::AllocSerializer::<4096>::new(
                rkyv::ser::serializers::AlignedSerializer::new(wiz),
                Default::default(),
                Default::default(),
            );

            for (_offset, doc) in &output {
                let _pos = serializer.serialize_value(doc).unwrap();
            }
            let mut wiz = serializer.into_serializer().into_inner();
            wiz.shrink_to_fit();

            /*
            eprintln!(
                "docs {} alloc {} wiz {}",
                output.len(),
                alloc.allocated_bytes() - alloc.chunk_capacity(),
                wiz.len()
            );
            */

            bytes += chunk.len();
            docs += output.len();
        }
    }

    let peak_stats = allocator::current_mem_stats();
    let duration = begin.elapsed();

    eprintln!(
        "Rounds: {} of {}\nElapsed: {}s\nDocs/sec: {}\nMB/sec: {}s\nMemory: active {}MB allocated {}MB resident {}MB retained {}MB alloc_ops {} dealloc_ops {} realloc_ops {}",
        TOTAL_ROUNDS,
        chunks.len(),
        duration.as_secs_f64(),
        (docs as f64) / duration.as_secs_f64(),
        (bytes as f64) / (duration.as_secs_f64() * 1024f64 * 1024f64),
        peak_stats.active / (1024 * 1024),
        peak_stats.allocated / (1024 * 1024),
        peak_stats.resident / (1024 * 1024),
        peak_stats.retained / (1024 * 1024),
        peak_stats.counts.alloc_ops - start_stats.counts.alloc_ops,
        peak_stats.counts.dealloc_ops - start_stats.counts.dealloc_ops,
        peak_stats.counts.realloc_ops - start_stats.counts.realloc_ops,
    );
}
