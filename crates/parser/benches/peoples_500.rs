#[path="../tests/testutil.rs"] mod testutil;

use criterion::{criterion_group, criterion_main, Criterion};

use parser::ParseConfig;
use testutil::{input_for_file, run_parser};

fn peoples_500(c: &mut Criterion) {
    let path = "benches/data/people-500.csv";
    let cfg = ParseConfig {
        filename: Some(path.to_string()),
        ..Default::default()
    };

    c.bench_function("peoples_500", |b| b.iter(|| {
        let input = input_for_file(path);
        run_parser(&cfg, input, false);
    }));
}

criterion_group!(benches, peoples_500);
criterion_main!(benches);
