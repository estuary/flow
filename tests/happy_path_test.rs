mod testutil;

use parser::{JsonPointer, ParseConfig};
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use testutil::{input_for_file, run_test};

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

fn assert_file_is_parsed(file: PathBuf) {
    let canary_ptr = "/_meta/canary";
    let offset_ptr = "/_meta/sourceOffset";
    let mut config = ParseConfig::default();
    config
        .add_values
        .insert(JsonPointer::from(canary_ptr), json!(true));
    config.add_record_offset = Some(JsonPointer::from(offset_ptr));
    config.filename = Some(file.display().to_string());
    let input = input_for_file(&file);

    let result = run_test(&config, input);
    assert_eq!(0, result.exit_code);
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
