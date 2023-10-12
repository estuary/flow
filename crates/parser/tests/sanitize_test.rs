mod testutil;

use chrono::DateTime;

use parser::ParseConfig;
use testutil::{input_for_file, run_test};

#[test]
fn sanitize_datetime_to_rfc3339() {
    let path = "tests/examples/datetimes.csv";
    let cfg = ParseConfig {
        filename: Some(path.to_string()),
        ..Default::default()
    };

    let input = input_for_file(path);
    let output = run_test(&cfg, input);
    output.assert_success(1);

    let expected_first_row = "2020-01-01T00:00:00+00:00";
    for value in output.parsed[0].as_object().unwrap().values() {
        assert_eq!(expected_first_row, DateTime::parse_from_rfc3339(value.as_str().unwrap()).unwrap().to_rfc3339())
    }
}

#[test]
fn sanitize_datetime_to_rfc3339_iana_timezone() {
    let path = "tests/examples/datetimes-naive.csv";
    let cfg = ParseConfig {
        default_timezone: "America/New_York".to_string(),
        filename: Some(path.to_string()),
        ..Default::default()
    };

    let input = input_for_file(path);
    let output = run_test(&cfg, input);
    output.assert_success(1);

    let expected_first_row = "2020-01-01T00:00:00-05:00";
    for value in output.parsed[0].as_object().unwrap().values() {
        assert_eq!(expected_first_row, DateTime::parse_from_rfc3339(value.as_str().unwrap()).unwrap().to_rfc3339())
    }
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
    assert_eq!(expected, out.get("x").unwrap().as_array().unwrap()[0].as_str().unwrap());
    assert_eq!(expected, out.get("y").unwrap().as_object().unwrap().get("z").unwrap().as_array().unwrap()[0].as_str().unwrap());
    assert_eq!(expected, out.get("y").unwrap().as_object().unwrap().get("k").unwrap().as_str().unwrap());
}
