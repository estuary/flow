use criterion::{Criterion, criterion_group, criterion_main};
use json::{
    Validator,
    schema::{self, index},
};
use serde_json::Value;

// Obtained as:
// $ wget https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/ghes-3.2/dereferenced/ghes-3.2.deref.json
// $ jq '.paths["/events"].get.responses["200"].content["application/json"].schema.items' ghes-3.2.deref.json
const GITHUB_SCHEMA: &[u8] = include_bytes!("testdata/github-event.schema.json");
// Obtained as: curl -H "Accept: application/vnd.github.v3+json" "https://api.github.com/events?per_page=100"
const GITHUB_SCRAPES: &[&[u8]] = &[
    include_bytes!("testdata/github-scrape1.json"),
    include_bytes!("testdata/github-scrape2.json"),
    include_bytes!("testdata/github-scrape3.json"),
    include_bytes!("testdata/github-scrape4.json"),
];

pub fn github_events(c: &mut Criterion) {
    let schema: Value = serde_json::from_slice(GITHUB_SCHEMA).unwrap();
    let url = url::Url::parse("http://bench/schema").unwrap();
    let schema = schema::build::<schema::CoreAnnotation>(&url, &schema).unwrap();

    let mut index = index::Builder::new();
    index.add(&schema).unwrap();
    index.verify_references().unwrap();
    let index = index.into_index();

    let scrapes = GITHUB_SCRAPES
        .iter()
        .copied()
        .map(serde_json::from_slice)
        .collect::<Result<Vec<Vec<Value>>, _>>()
        .unwrap();

    for (s, scrape) in scrapes.iter().enumerate() {
        c.bench_function(&format!("scrape{}", s), |b| {
            let mut val = Validator::new(&index);
            b.iter(|| {
                for (_n, doc) in scrape.iter().enumerate() {
                    let (valid, _outcomes) = val.validate(&schema, doc, |_o| None);
                    //println!("scrape {} errors {}: {:?}", s, _n, errors);
                    assert!(valid);
                }
            })
        });
    }
}

criterion_group!(benches, github_events);
criterion_main!(benches);
