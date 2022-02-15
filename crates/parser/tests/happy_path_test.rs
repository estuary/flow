mod testutil;

use std::fs;
use std::path::PathBuf;

use parser::{Format, JsonPointer, ParseConfig};
use serde_json::json;
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

#[test]
fn w3c_extended_log_file_is_parsed() {
    let config = ParseConfig {
        // Explicit format is required, since there's no file extension that's associated with
        // this format.
        format: Some(Format::W3cExtendedLog),
        ..Default::default()
    };
    let input = input_for_file("tests/examples/w3c-extended-log");
    let result = run_test(&config, input);
    result.assert_success(1);
}

/// The file `requires-explicit-quote.csv` has 206 columns. The header row does not use any quote
/// characters, and so we don't automatically determine the correct quote character because we only
/// look at the first 2KiB, which in this case is all unquoted headers. So this test asserts that
/// we fail to parse the file, but that it succeeds when the quote character is provided in the
/// config.
#[test]
fn csv_requires_explicit_quote() {
    let path = "tests/examples/requires-explicit-quote.csv";
    let no_quote = ParseConfig {
        filename: Some(path.to_string()),
        ..Default::default()
    };

    {
        let input = input_for_file(path);
        run_test(&no_quote, input).assert_failure(0);
    }

    let with_quote = ParseConfig {
        filename: Some(path.to_string()),
        csv: Some(parser::csv::CharacterSeparatedConfig {
            quote: Some(parser::csv::Char('"' as u8)),
            ..Default::default()
        }),
        ..Default::default()
    };
    let same_input = input_for_file(path);
    let output = run_test(&with_quote, same_input);
    output.assert_success(1);
    // Confirm the number of columns as a way of confirming that we're using the correct quote in
    // the parse configuration.
    assert_eq!(206, output.parsed[0].as_object().unwrap().len());
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
