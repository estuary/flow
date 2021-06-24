mod testutil;

use parser::{JsonPointer, ParseConfig};
use serde_json::json;
use testutil::{input_for_file, run_test};

#[test]
fn jsonl_content_is_parsed() {
    let input = input_for_file("tests/valid.jsonl");
    let mut config = ParseConfig::default();
    config
        .add_values
        .insert(JsonPointer::from("/_meta/canary"), json!(true));
    config.add_source_offset = Some(JsonPointer::from("/line"));
    config.filename = Some("valid.jsonl".to_string());

    let result = run_test(&config, input);
    assert_eq!(0, result.exit_code);
    let expected_data = vec![
        json!({"foo": "bar", "_meta": {"canary": true}, "line": 1}),
        json!({"one": 2, "_meta": {"canary": true}, "line": 2}),
        json!({"three": 5.0, "_meta": {"canary": true}, "line": 3}),
        json!({"wut": true, "_meta": {"canary": true}, "line": 4}),
        json!({"_meta": {"canary": true, "yea": "boiiiii"}, "line": 5}),
    ];
    assert_eq!(
        expected_data,
        result.parsed,
        "expected: {}\nactual: {}",
        serde_json::to_string(&expected_data).unwrap(),
        result.raw_stdout
    );
}
