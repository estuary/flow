//! Reading a file via `--file` must produce the same output as piping the same
//! bytes through stdin, across every format.

mod testutil;

use parser::ParseConfig;
use testutil::{input_for_file, run_parser, run_parser_reading_file};

/// `filename` is set in the config so both paths infer the same format.
fn assert_file_matches_stream(path: &str) {
    let config = ParseConfig {
        filename: Some(path.to_string()),
        ..Default::default()
    };

    let stream_result = run_parser(&config, input_for_file(path), false);
    let file_result = run_parser_reading_file(&config, path, false);

    assert_eq!(
        stream_result.exit_code, 0,
        "stream parse of {path} should succeed"
    );
    assert_eq!(
        file_result.exit_code, 0,
        "--file parse of {path} should succeed"
    );
    assert_eq!(
        stream_result.raw_stdout, file_result.raw_stdout,
        "--file output must match stdin output for {path}"
    );
    assert!(
        !file_result.parsed.is_empty(),
        "expected at least one record from {path}"
    );
}

#[test]
fn parquet_file_input_matches_stream() {
    assert_file_matches_stream("tests/examples/iris.parquet");
}

#[test]
fn csv_file_input_matches_stream() {
    assert_file_matches_stream("tests/examples/valid-big-nums.csv");
}

#[test]
fn json_file_input_matches_stream() {
    assert_file_matches_stream("tests/examples/valid.json");
}

#[test]
fn jsonl_file_input_matches_stream() {
    assert_file_matches_stream("tests/examples/valid.jsonl");
}

#[test]
fn avro_file_input_matches_stream() {
    assert_file_matches_stream("tests/examples/valid-avro-snappy.avro");
}
