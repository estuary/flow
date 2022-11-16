use criterion::{criterion_group, criterion_main, Criterion};
use json::de;
use json::schema::{build::build_schema, index::IndexBuilder, CoreAnnotation};
use json::validator::{SpanContext, Validator};
use serde_json::{json, Value};

const CITI_RIDES_SCHEMA: &[u8] = include_bytes!("testdata/citi-rides.schema.json");
const CITI_RIDES: &[u8] = include_bytes!("testdata/citi-rides1.json");

pub fn citi_rides(c: &mut Criterion) {
    let schema: Value = serde_json::from_slice(CITI_RIDES_SCHEMA).unwrap();
    let url = url::Url::parse("http://ignored").unwrap(); // Schema has $id.
    let schema = build_schema::<CoreAnnotation>(url, &schema).unwrap();

    let mut index = IndexBuilder::new();
    index.add(&schema).unwrap();
    index.verify_references().unwrap();
    let index = index.into_index();

    let rides = serde_json::Deserializer::from_slice(CITI_RIDES).into_iter::<Value>();
    let rides = rides.collect::<Result<Vec<_>, _>>().unwrap();

    let rides4x = rides
        .iter()
        .map(|r| json!({ "arr": [r, r, r, r] }))
        .collect::<Vec<_>>();

    c.bench_function("rides1x", |b| {
        let mut val = Validator::<CoreAnnotation, SpanContext>::new(&index);
        let curi = url::Url::parse("https://example/citi-rides.schema.json#/$defs/ride").unwrap();

        b.iter(|| {
            for (_n, doc) in rides.iter().enumerate() {
                val.prepare(&curi).unwrap();
                let _ = de::walk(doc, &mut val).expect("validation error");
                // println!("outcomes {}: {:?}", _n, val.outcomes());
                assert!(!val.invalid());
            }
        })
    });

    c.bench_function("rides4x", |b| {
        let mut val = Validator::<CoreAnnotation, SpanContext>::new(&index);
        let curi =
            url::Url::parse("https://example/citi-rides.schema.json#/$defs/rideArray").unwrap();

        b.iter(|| {
            for (_n, doc) in rides4x.iter().enumerate() {
                val.prepare(&curi).unwrap();
                let _ = de::walk(doc, &mut val).expect("validation error");
                // println!("outcomes {}: {:?}", _n, val.outcomes());
                assert!(!val.invalid());
            }
        })
    });
}

criterion_group!(benches, citi_rides);
criterion_main!(benches);
