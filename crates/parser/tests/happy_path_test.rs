mod testutil;

use std::fs;
use std::path::PathBuf;

use parser::{character_separated::AdvancedCsvConfig, Format, JsonPointer, ParseConfig};
use serde_json::{json, Value};
use testutil::{input_for_file, run_test};

#[test]
fn protobuf_file_is_parsed() {
    let config = ParseConfig {
        format: Format::Protobuf(parser::protobuf::ProtobufConfig {
            proto_file_content: include_str!("examples/gtfs-realtime.proto").to_string(),
            message: "FeedMessage".to_string(),
        }),
        ..Default::default()
    };
    assert_file_is_parsed_with_config("tests/examples/vehicle-positions.pb".into(), config);
}

#[test]
fn valid_examples_are_parsed_with_default_config() {
    for result in fs::read_dir("tests/examples").unwrap() {
        let entry = result.unwrap();
        let filename = entry.file_name();
        if filename.to_str().unwrap().starts_with("valid") {
            assert_file_is_parsed(entry.path());
        }
    }
}

#[test]
fn w3c_extended_log_file_is_parsed() {
    let config = ParseConfig {
        // Explicit format is required, since there's no file extension that's associated with
        // this format.
        format: Format::W3cExtendedLog,
        ..Default::default()
    };
    let input = input_for_file("tests/examples/w3c-extended-log");
    let result = run_test(&config, input);
    result.assert_success(1);
}

#[test]
fn csv_does_not_require_explicit_quote_configuration() {
    let path = "tests/examples/valid-with-double-quotes.csv";
    let no_quote = ParseConfig {
        filename: Some(path.to_string()),
        ..Default::default()
    };

    let input = input_for_file(path);
    let output = run_test(&no_quote, input);
    output.assert_success(1);

    assert_eq!(206, output.parsed[0].as_object().unwrap().len());
    // Assert that quotes were handled correctly by checking to ensure that none of the string
    // values begin or end with a double-quote.
    for value in output.parsed[0].as_object().unwrap().values() {
        match value {
            Value::String(s) => {
                assert!(!s.starts_with("\""));
                assert!(!s.ends_with("\""));
            }
            Value::Null => { /* this is expected for some columns */ }
            other => panic!("unexpected value type: {:?}", other),
        }
    }
}

#[test]
fn csv_is_parsed_after_skipping_lines() {
    let path = "tests/examples/csv-with-extra-steps";
    let config = ParseConfig {
        filename: Some(path.to_string()),
        format: Format::Csv(AdvancedCsvConfig {
            skip_lines: 3,
            ..Default::default()
        }),
        ..Default::default()
    };

    let input = input_for_file(path);
    let result = run_test(&config, input);
    result.assert_success(3);
}

#[test]
fn csv_multi_archive_with_headers_is_parsed() {
    let path = "tests/examples/valid-csv-multi-archive-with-headers.csv.zip";
    let config = ParseConfig {
        filename: Some(path.to_string()),
        ..Default::default()
    };

    let input = input_for_file(path);
    let result = run_test(&config, input);
    // Each file has headers, so assert that headers are not parsed as data rows based on the count
    // of parsed results. There are 3 files with 10 rows each with 1 being headers, so 3 * 9 = 27.
    result.assert_success(27);
}

#[test]
fn parquet_file_is_parsed() {
    let config = ParseConfig {
        format: Format::Parquet,
        ..Default::default()
    };
    assert_file_is_parsed_with_config("tests/examples/iris.parquet".into(), config);
}

fn assert_file_is_parsed(file: PathBuf) {
    assert_file_is_parsed_with_config(file, ParseConfig::default());
}

fn assert_file_is_parsed_with_config(file: PathBuf, mut config: ParseConfig) {
    let canary_ptr = "/_meta/canary";
    let offset_ptr = "/_meta/sourceOffset";
    config
        .add_values
        .insert(JsonPointer::from(canary_ptr), json!(true));
    config
        .add_values
        .insert(JsonPointer::from("/_meta/file"), json!(file));
    config.add_record_offset = Some(JsonPointer::from(offset_ptr));
    config.filename = Some(file.display().to_string());
    let input = input_for_file(&file);

    let result = run_test(&config, input);
    assert_eq!(0, result.exit_code, "parser exited with non-0 status");
    assert!(!result.parsed.is_empty());
    for (i, doc) in result.parsed.into_iter().enumerate() {
        assert_eq!(
            Some(&json!(true)),
            doc.pointer(canary_ptr),
            "document missing canary: {}",
            doc
        );
        let offset = doc
            .pointer(offset_ptr)
            .expect("document missing offset")
            .as_u64()
            .expect("offset was not a u64");
        assert!(offset == i as u64);
    }
}
