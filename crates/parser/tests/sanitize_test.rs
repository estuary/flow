mod testutil;

use std::fs::File;
use std::io::Write;

use parser::ParseConfig;
use tempfile::tempdir;
use testutil::{input_for_file, run_test};

fn test_sanitize(description: &str, input: &str, expected: &str, default_offset: &str) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("sanitize-test.csv");
    let mut f = File::create(path.clone()).unwrap();
    writeln!(f, "header").unwrap();
    writeln!(f, "\"{}\"", input).unwrap();

    let cfg = ParseConfig {
        filename: Some(path.to_string_lossy().to_string()),
        default_offset: default_offset.to_string(),
        ..Default::default()
    };

    let input = input_for_file(path);
    let output = run_test(&cfg, input);
    output.assert_success(1);

    for value in output.parsed[0].as_object().unwrap().values() {
        assert_eq!(expected, value.as_str().unwrap(), "{}", description)
    }
}

#[test]
fn sanitize_datetime_to_rfc3339() {
    // With Timezone
    test_sanitize(
        "tz rfc3339 utc",
        "2020-01-01T12:34:56Z",
        "2020-01-01T12:34:56Z",
        "+00:00",
    );
    test_sanitize(
        "tz rfc3339 offset",
        "2020-01-01T12:34:56-04:00",
        "2020-01-01T12:34:56-04:00",
        "+00:00",
    );
    test_sanitize(
        "tz rfc3339 fractional",
        "2020-01-01T12:34:56.999999999Z",
        "2020-01-01T12:34:56.999999999Z",
        "+00:00",
    );
    test_sanitize(
        "tz rfc3339 fractional + offset",
        "2020-01-01T12:34:56.999999999-04:00",
        "2020-01-01T12:34:56.999999999-04:00",
        "+00:00",
    );
    test_sanitize(
        "tz spaced fractional + offset",
        "2020-01-01 12:34:56.999999999-04:00",
        "2020-01-01T12:34:56.999999999-04:00",
        "+00:00",
    );
    test_sanitize(
        "tz spaced fractional + utc",
        "2020-01-01 12:34:56.999999999Z",
        "2020-01-01T12:34:56.999999999Z",
        "+00:00",
    );
    test_sanitize(
        "tz spaced offset",
        "2020-01-01 12:34:56-04:00",
        "2020-01-01T12:34:56-04:00",
        "+00:00",
    );
    test_sanitize(
        "tz spaced utc",
        "2020-01-01 12:34:56Z",
        "2020-01-01T12:34:56Z",
        "+00:00",
    );

    // Without Timezone
    test_sanitize(
        "naive t",
        "2020-01-01T12:34:56",
        "2020-01-01T12:34:56Z",
        "+00:00",
    );
    test_sanitize(
        "naive t + fractional",
        "2020-01-01T12:34:56.999999999",
        "2020-01-01T12:34:56.999999999Z",
        "+00:00",
    );
    test_sanitize(
        "naive t + fractional 2",
        "2020-01-01T12:34:56.999999999",
        "2020-01-01T12:34:56.999999999+04:00",
        "+04:00",
    );
    test_sanitize(
        "naive space",
        "2020-01-01 12:34:56",
        "2020-01-01T12:34:56Z",
        "+00:00",
    );
    test_sanitize(
        "naive space + fractional",
        "2020-01-01 12:34:56.999999999",
        "2020-01-01T12:34:56.999999999Z",
        "+00:00",
    );
    test_sanitize(
        "naive space + fractional 2",
        "2020-01-01 12:34:56.999999999",
        "2020-01-01T12:34:56.999999999+04:00",
        "+04:00",
    );
}

#[test]
fn sanitize_datetime_to_rfc3339_nested() {
    let path = "tests/examples/datetimes-nested.json";
    let cfg = ParseConfig {
        filename: Some(path.to_string()),
        ..Default::default()
    };

    let input = input_for_file(path);
    let output = run_test(&cfg, input);
    output.assert_success(1);

    let expected = "2020-01-01T00:00:00Z";
    let out = output.parsed[0].as_object().unwrap();
    assert_eq!(
        expected,
        out.get("x").unwrap().as_array().unwrap()[0]
            .as_str()
            .unwrap()
    );
    assert_eq!(
        expected,
        out.get("y")
            .unwrap()
            .as_object()
            .unwrap()
            .get("z")
            .unwrap()
            .as_array()
            .unwrap()[0]
            .as_str()
            .unwrap()
    );
    assert_eq!(
        expected,
        out.get("y")
            .unwrap()
            .as_object()
            .unwrap()
            .get("k")
            .unwrap()
            .as_str()
            .unwrap()
    );
}
