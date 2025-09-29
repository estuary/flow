// This file contains format validation test cases from the official JSON Schema Test Suite for draft2020-12.
// Source: https://github.com/json-schema-org/JSON-Schema-Test-Suite
// Location: crates/json/tests/official/tests/draft2020-12/optional/format/
//
// IMPORTANT: Test functions must correspond 1:1 with test case files from the official suite.
// Tests are organized in alphabetical order to make it easy to verify all cases are covered.
//
// Maintenance instructions:
// 1. Each test function corresponds to a .json file in the format test directory
// 2. Test function names follow the pattern: test_d12_format_<filename_without_extension>
// 3. Keep tests in alphabetical order matching the sorted list of .json files
// 4. If a test fails, comment it out with an explanation rather than deleting it
//
// Note: In draft2020-12, format validation is annotation-only by default unless
// the implementation explicitly enables format assertion behavior.

mod utils;
use utils::run_draft12_format_test;

#[test]
fn test_d12_format_date_time() {
    run_draft12_format_test("date-time.json", &[]);
}

#[test]
fn test_d12_format_date() {
    run_draft12_format_test("date.json", &[]);
}

#[test]
fn test_d12_format_duration() {
    run_draft12_format_test("duration.json", &[]);
}

#[test]
fn test_d12_format_ecmascript_regex() {
    run_draft12_format_test(
        "ecmascript-regex.json",
        &[(
            "\\a is not an ECMA 262 control escape",
            "when used as a pattern",
            serde_json::json!("\\a"),
        )],
    );
}

#[test]
fn test_d12_format_email() {
    run_draft12_format_test(
        "email.json",
        &[(
            "validation of e-mail addresses",
            "an IPv6-address-literal after the @ is valid",
            serde_json::json!("joe.bloggs@[IPv6:::1]"),
        )],
    );
}

#[test]
fn test_d12_format_hostname() {
    run_draft12_format_test("hostname.json", &[]);
}

// IDN email format validation is not implemented (always fails)
// #[test]
// fn test_d12_format_idn_email() {
//     run_draft12_format_test("idn-email.json", &[]);
// }

// IDN hostname format validation is not implemented (always fails)
// #[test]
// fn test_d12_format_idn_hostname() {
//     run_draft12_format_test("idn-hostname.json", &[]);
// }

#[test]
fn test_d12_format_ipv4() {
    run_draft12_format_test("ipv4.json", &[]);
}

#[test]
fn test_d12_format_ipv6() {
    run_draft12_format_test("ipv6.json", &[]);
}

#[test]
fn test_d12_format_iri_reference() {
    run_draft12_format_test("iri-reference.json", &[]);
}

#[test]
fn test_d12_format_iri() {
    run_draft12_format_test("iri.json", &[]);
}

#[test]
fn test_d12_format_json_pointer() {
    run_draft12_format_test("json-pointer.json", &[]);
}

#[test]
fn test_d12_format_regex() {
    run_draft12_format_test("regex.json", &[]);
}

#[test]
fn test_d12_format_relative_json_pointer() {
    run_draft12_format_test("relative-json-pointer.json", &[]);
}

#[test]
fn test_d12_format_time() {
    run_draft12_format_test(
        "time.json",
        &[
            (
                "validation of time strings",
                "a valid time string with leap second, Zulu",
                serde_json::json!("23:59:60Z"),
            ),
            (
                "validation of time strings",
                "valid leap second, large negative time-offset",
                serde_json::json!("00:29:60-23:30"),
            ),
            (
                "validation of time strings",
                "valid leap second, large positive time-offset",
                serde_json::json!("23:29:60+23:30"),
            ),
            (
                "validation of time strings",
                "valid leap second, negative time-offset",
                serde_json::json!("15:59:60-08:00"),
            ),
            (
                "validation of time strings",
                "valid leap second, positive time-offset",
                serde_json::json!("01:29:60+01:30"),
            ),
            (
                "validation of time strings",
                "valid leap second, zero time-offset",
                serde_json::json!("23:59:60+00:00"),
            ),
        ],
    );
}

// We deliberately reject unknown formats
// #[test]
// fn test_d12_format_unknown() {
//     run_draft12_format_test("unknown.json", &[]);
// }

#[test]
fn test_d12_format_uri_reference() {
    run_draft12_format_test("uri-reference.json", &[]);
}

#[test]
fn test_d12_format_uri_template() {
    run_draft12_format_test("uri-template.json", &[]);
}

#[test]
fn test_d12_format_uri() {
    run_draft12_format_test("uri.json", &[]);
}

#[test]
fn test_d12_format_uuid() {
    run_draft12_format_test("uuid.json", &[]);
}
