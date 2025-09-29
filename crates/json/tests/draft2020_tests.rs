// This file contains test cases from the official JSON Schema Test Suite for draft2020-12.
// Source: https://github.com/json-schema-org/JSON-Schema-Test-Suite
// Location: crates/json/tests/official/tests/draft2020-12/
//
// IMPORTANT: Test functions must correspond 1:1 with test case files from the official suite.
// Tests are organized in alphabetical order to make it easy to verify all cases are covered.
//
// Maintenance instructions:
// 1. Each test function corresponds to a .json file in the official test suite
// 2. Test function names follow the pattern: test_d12_<filename_without_extension>
// 3. Keep tests in alphabetical order matching the sorted list of .json files
// 4. If a test fails, comment it out with an explanation rather than deleting it

mod utils;
use serde_json::json;
use utils::run_draft12_test;

#[test]
fn test_d12_additional_properties() {
    run_draft12_test("additionalProperties.json", &[]);
}

#[test]
fn test_d12_all_of() {
    run_draft12_test("allOf.json", &[]);
}

#[test]
fn test_d12_anchor() {
    run_draft12_test("anchor.json", &[]);
}

#[test]
fn test_d12_any_of() {
    run_draft12_test("anyOf.json", &[]);
}

#[test]
fn test_d12_boolean_schema() {
    run_draft12_test("boolean_schema.json", &[]);
}

#[test]
fn test_d12_const() {
    run_draft12_test("const.json", &[]);
}

#[test]
fn test_d12_contains() {
    run_draft12_test("contains.json", &[]);
}

#[test]
fn test_d12_content() {
    run_draft12_test("content.json", &[]);
}

#[test]
fn test_d12_default() {
    run_draft12_test("default.json", &[]);
}

#[test]
fn test_d12_defs() {
    run_draft12_test("defs.json", &[]);
}

#[test]
fn test_d12_dependent_required() {
    run_draft12_test("dependentRequired.json", &[]);
}

#[test]
fn test_d12_dependent_schemas() {
    run_draft12_test("dependentSchemas.json", &[]);
}

#[test]
fn test_d12_dynamic_ref() {
    run_draft12_test("dynamicRef.json", &[]);
}

#[test]
fn test_d12_enum() {
    run_draft12_test("enum.json", &[]);
}

#[test]
fn test_d12_exclusive_maximum() {
    run_draft12_test("exclusiveMaximum.json", &[]);
}

#[test]
fn test_d12_exclusive_minimum() {
    run_draft12_test("exclusiveMinimum.json", &[]);
}

// format validation in draft2020-12 should be annotation-only by default
// but our implementation validates formats
//#[test]
//fn test_d12_format() {
//    run_draft12_test("format.json", &[]);
//}

#[test]
fn test_d12_if_then_else() {
    run_draft12_test("if-then-else.json", &[]);
}

#[test]
fn test_d12_infinite_loop_detection() {
    run_draft12_test("infinite-loop-detection.json", &[]);
}

#[test]
fn test_d12_items() {
    run_draft12_test("items.json", &[]);
}

#[test]
fn test_d12_max_contains() {
    run_draft12_test("maxContains.json", &[]);
}

#[test]
fn test_d12_maximum() {
    run_draft12_test("maximum.json", &[]);
}

#[test]
fn test_d12_max_items() {
    run_draft12_test("maxItems.json", &[]);
}

#[test]
fn test_d12_max_length() {
    run_draft12_test("maxLength.json", &[]);
}

#[test]
fn test_d12_max_properties() {
    run_draft12_test("maxProperties.json", &[]);
}

#[test]
fn test_d12_min_contains() {
    run_draft12_test("minContains.json", &[]);
}

#[test]
fn test_d12_minimum() {
    run_draft12_test("minimum.json", &[]);
}

#[test]
fn test_d12_min_items() {
    run_draft12_test("minItems.json", &[]);
}

#[test]
fn test_d12_min_length() {
    run_draft12_test("minLength.json", &[]);
}

#[test]
fn test_d12_min_properties() {
    run_draft12_test("minProperties.json", &[]);
}

#[test]
fn test_d12_multiple_of() {
    run_draft12_test("multipleOf.json", &[]);
}

#[test]
fn test_d12_not() {
    run_draft12_test("not.json", &[]);
}

#[test]
fn test_d12_one_of() {
    run_draft12_test("oneOf.json", &[]);
}

#[test]
fn test_d12_pattern() {
    run_draft12_test("pattern.json", &[]);
}

#[test]
fn test_d12_pattern_properties() {
    run_draft12_test("patternProperties.json", &[]);
}

#[test]
fn test_d12_prefix_items() {
    run_draft12_test("prefixItems.json", &[]);
}

#[test]
fn test_d12_properties() {
    run_draft12_test("properties.json", &[]);
}

#[test]
fn test_d12_property_names() {
    run_draft12_test("propertyNames.json", &[]);
}

#[test]
fn test_d12_ref() {
    run_draft12_test("ref.json", &[]);
}

#[test]
fn test_d12_ref_remote() {
    run_draft12_test("refRemote.json", &[]);
}

#[test]
fn test_d12_required() {
    run_draft12_test("required.json", &[]);
}

#[test]
fn test_d12_type() {
    run_draft12_test("type.json", &[]);
}

#[test]
fn test_d12_unevaluated_items() {
    run_draft12_test("unevaluatedItems.json", &[]);
}

#[test]
fn test_d12_unevaluated_properties() {
    run_draft12_test("unevaluatedProperties.json", &[]);
}

#[test]
fn test_d12_unique_items() {
    run_draft12_test("uniqueItems.json", &[]);
}

// vocabulary.json requires custom meta-schema validation behavior
#[test]
fn test_d12_vocabulary() {
    run_draft12_test(
        "vocabulary.json",
        &[(
            "schema that uses custom metaschema with with no validation vocabulary",
            "no validation: invalid number, but it still validates",
            json!({"numberProperty": 1}),
        )],
    );
}

// Optional tests (in alphabetical order)
#[test]
fn test_d12_optional_anchor() {
    run_draft12_test("optional/anchor.json", &[]);
}

#[test]
fn test_d12_optional_bignum() {
    run_draft12_test("optional/bignum.json", &[]);
}

// Cross-draft references don't fully match historic draft behavior
#[test]
fn test_d12_optional_cross_draft() {
    run_draft12_test(
        "optional/cross-draft.json",
        &[(
            "refs to historic drafts are processed as historic drafts",
            "first item not a string is valid",
            json!([1, 2, 3]),
        )],
    );
}

// dependencies keyword is from draft4/6/7, not supported in draft2020-12
// #[test]
// fn test_d12_optional_dependencies_compatibility() {
//     run_draft12_test("optional/dependencies-compatibility.json", &[]);
// }

#[test]
fn test_d12_optional_dynamic_ref() {
    run_draft12_test("optional/dynamicRef.json", &[]);
}

// ECMAScript regex tests fail - implementation uses Rust regex crate
// #[test]
// fn test_d12_optional_ecmascript_regex() {
//     run_draft12_test("optional/ecmascript-regex.json", &[]);
// }

// Float overflow would need special handling we don't do.
#[test]
fn test_d12_optional_float_overflow() {
    run_draft12_test(
        "optional/float-overflow.json",
        &[(
            "all integers are multiples of 0.5, if overflow is handled",
            "valid if optional overflow handling is implemented",
            json!(1e308),
        )],
    );
}

#[test]
fn test_d12_optional_format_assertion() {
    run_draft12_test("optional/format-assertion.json", &[]);
}

#[test]
fn test_d12_optional_id() {
    run_draft12_test("optional/id.json", &[]);
}

#[test]
fn test_d12_optional_no_schema() {
    run_draft12_test("optional/no-schema.json", &[]);
}

#[test]
fn test_d12_optional_non_bmp_regex() {
    run_draft12_test("optional/non-bmp-regex.json", &[]);
}

// We deliberately reject unknown keywords
// #[test]
// fn test_d12_optional_ref_of_unknown_keyword() {
//    run_draft12_test("optional/refOfUnknownKeyword.json", &[]);
// }

// We deliberately reject unknown keywords
// #[test]
// fn test_d12_optional_unknown_keyword() {
//     run_draft12_test("optional/unknownKeyword.json", &[]);
// }
