// This file contains format validation test cases from the official JSON Schema Test Suite for draft2019-09.
// Source: https://github.com/json-schema-org/JSON-Schema-Test-Suite
// Location: crates/json/tests/official/tests/draft2019-09/optional/format/
//
// IMPORTANT: Test functions must correspond 1:1 with test case files from the official suite.
// Tests are organized in alphabetical order to make it easy to verify all cases are covered.
//
// Maintenance instructions:
// 1. Each test function corresponds to a .json file in the format test directory
// 2. Test function names follow the pattern: test_d09_format_<filename_without_extension>
// 3. Keep tests in alphabetical order matching the sorted list of .json files
// 4. If a test fails, comment it out with an explanation rather than deleting it

mod utils;
use utils::run_draft09_format_test;

#[test]
fn test_d09_format_date_time() {
    run_draft09_format_test("date-time.json", &[]);
}

#[test]
fn test_d09_format_date() {
    run_draft09_format_test("date.json", &[]);
}

#[test]
fn test_d09_format_duration() {
    run_draft09_format_test("duration.json", &[]);
}

#[test]
fn test_d09_format_email() {
    run_draft09_format_test("email.json", &[]);
}

#[test]
fn test_d09_format_hostname() {
    run_draft09_format_test("hostname.json", &[]);
}

#[test]
fn test_d09_format_ipv4() {
    run_draft09_format_test("ipv4.json", &[]);
}

#[test]
fn test_d09_format_ipv6() {
    run_draft09_format_test("ipv6.json", &[]);
}

#[test]
fn test_d09_format_iri_reference() {
    run_draft09_format_test("iri-reference.json", &[]);
}

#[test]
fn test_d09_format_iri() {
    run_draft09_format_test("iri.json", &[]);
}

#[test]
fn test_d09_format_json_pointer() {
    run_draft09_format_test("json-pointer.json", &[]);
}

#[test]
fn test_d09_format_regex() {
    run_draft09_format_test("regex.json", &[]);
}

#[test]
fn test_d09_format_relative_json_pointer() {
    run_draft09_format_test("relative-json-pointer.json", &[]);
}

// TODO(johnny): The `time` crate doesn't support the leap seconds of this test,
// such as "23:59:60Z".
// #[test]
// fn test_d09_format_time() {
//     run_draft09_format_test("time.json", &[]);
// }

#[test]
fn test_d09_format_uri_reference() {
    run_draft09_format_test("uri-reference.json", &[]);
}

#[test]
fn test_d09_format_uri_template() {
    run_draft09_format_test("uri-template.json", &[]);
}

#[test]
fn test_d09_format_uri() {
    run_draft09_format_test("uri.json", &[]);
}

#[test]
fn test_d09_format_uuid() {
    run_draft09_format_test("uuid.json", &[]);
}
