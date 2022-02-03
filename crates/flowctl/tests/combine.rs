use assert_cmd::{assert::Assert, Command};
use tempfile::tempdir;

const FLOWCTL: &str = "flowctl-rs";

#[test]
fn combine_produces_help_message() {
    let mut cmd = Command::cargo_bin(FLOWCTL).unwrap();
    let assert = cmd.arg("combine").arg("--help").assert();
    assert.success();
}

#[test]
fn combine_using_collection_defined_in_catalog() {
    let dir = tempdir().unwrap();

    let mut cmd = Command::cargo_bin(FLOWCTL).unwrap();
    let assert = cmd
        .arg("combine")
        .arg("--directory")
        .arg(dir.path().display().to_string())
        .arg("--log.level=debug")
        .arg("--source")
        .arg("tests/fixtures/test-catalog.yaml")
        .arg("--collection")
        .arg("test/test-combine")
        .write_stdin(VALID_INPUT)
        .assert();
    assert_combine_output(assert.success());
    // The explicit close just ensures that dir doesn't get dropped before we're done with it.
    dir.close().unwrap();
}

#[test]
fn combine_using_schema_and_key() {
    let dir = tempdir().unwrap();

    let mut cmd = Command::cargo_bin(FLOWCTL).unwrap();
    let assert = cmd
        .arg("combine")
        .arg("--directory")
        .arg(dir.path().display().to_string())
        .arg("--log.level=debug")
        .arg("--schema")
        .arg("tests/fixtures/test-schema.yaml")
        .arg("--key")
        .arg("/id")
        .write_stdin(VALID_INPUT)
        .assert();
    assert_combine_output(assert.success());
    // The explicit close just ensures that dir doesn't get dropped before we're done with it.
    dir.close().unwrap();
}

#[test]
fn combine_fails_when_neither_schema_nor_source_arguments_provided() {
    let mut cmd = Command::cargo_bin(FLOWCTL).unwrap();
    let assert = cmd.arg("combine").write_stdin(VALID_INPUT).assert();
    assert.failure();
}

#[test]
fn combine_source_and_schema_are_mutually_exclusive() {
    let mut cmd = Command::cargo_bin(FLOWCTL).unwrap();
    let assert = cmd
        .arg("combine")
        .arg("--schema")
        .arg("tests/fixtures/test-schema.yaml")
        .arg("--key")
        .arg("/id")
        .arg("--source")
        .arg("tests/fixtures/test-catalog.yaml")
        .arg("--collection")
        .arg("test/test-combine")
        .write_stdin(VALID_INPUT)
        .assert();
    assert.failure();
}

const VALID_INPUT: &str = r##"{"id": 3, "a": "wut"}
{"id": 1, "b": 3}
{"id": 1, "b": 7, "a": "A"}
{"id": 2, "b": 2}
{"id": 3, "b": 4, "a": "nope"}
{"id": 3, "b": -4}
"##;

fn assert_combine_output(actual: Assert) {
    let out = &actual.get_output().stdout;

    let actual_docs: Vec<serde_json::Value> = serde_json::Deserializer::from_slice(out)
        .into_iter()
        .map(|result| result.expect("failed to deserialize output"))
        .collect();

    assert_eq!(expected_output(), actual_docs);
}

fn expected_output() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({"id": 1, "a": "A", "b": 10}),
        serde_json::json!({"id": 2, "b": 2}),
        serde_json::json!({"id": 3, "b": 0, "a": "wut"}),
    ]
}
