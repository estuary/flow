// This file contains test cases from the official JSON Schema Test Suite for draft2019-09.
// Source: https://github.com/json-schema-org/JSON-Schema-Test-Suite
// Location: crates/json/tests/official/tests/draft2019-09/
//
// IMPORTANT: Test functions must correspond 1:1 with test case files from the official suite.
// Tests are organized in alphabetical order to make it easy to verify all cases are covered.
//
// Maintenance instructions:
// 1. Each test function corresponds to a .json file in the official test suite
// 2. Test function names follow the pattern: test_d09_<filename_without_extension>
// 3. Keep tests in alphabetical order matching the sorted list of .json files
// 4. If a test fails, comment it out with an explanation rather than deleting it

mod utils;
use utils::run_draft09_test;

#[test]
fn test_d09_additional_items() {
    run_draft09_test(
        "additionalItems.json",
        &[
            // We deviate from the spec in our treatment of "additionalItems" when "items" is absent
            // for historical reasons. See build.rs
            (
                "additionalItems as false without items",
                "items defaults to empty schema so everything is valid",
                serde_json::json!([1, 2, 3, 4, 5]),
            ),
            (
                "additionalItems does not look in applicators, valid case",
                "items defined in allOf are not examined",
                serde_json::json!([1, null]),
            ),
        ],
    );
}

#[test]
fn test_d09_additional_properties() {
    run_draft09_test("additionalProperties.json", &[]);
}

#[test]
fn test_d09_all_of() {
    run_draft09_test("allOf.json", &[]);
}

#[test]
fn test_d09_anchor() {
    run_draft09_test("anchor.json", &[]);
}

#[test]
fn test_d09_any_of() {
    run_draft09_test("anyOf.json", &[]);
}

#[test]
fn test_d09_boolean_schema() {
    run_draft09_test("boolean_schema.json", &[]);
}

#[test]
fn test_d09_const() {
    run_draft09_test("const.json", &[]);
}

#[test]
fn test_d09_contains() {
    run_draft09_test("contains.json", &[]);
}

#[test]
fn test_d09_content() {
    run_draft09_test("content.json", &[]);
}

#[test]
fn test_d09_default() {
    run_draft09_test("default.json", &[]);
}

// These tests reference the draft 2019-09 schema, which uses unsupported recursive keywords.
// #[test]
// fn test_d09_defs() {
//     run_draft09_test("defs.json", &[]);
// }

#[test]
fn test_d09_dependent_required() {
    run_draft09_test("dependentRequired.json", &[]);
}

#[test]
fn test_d09_dependent_schemas() {
    run_draft09_test("dependentSchemas.json", &[]);
}

#[test]
fn test_d09_enum() {
    run_draft09_test("enum.json", &[]);
}

#[test]
fn test_d09_exclusive_maximum() {
    run_draft09_test("exclusiveMaximum.json", &[]);
}

#[test]
fn test_d09_exclusive_minimum() {
    run_draft09_test("exclusiveMinimum.json", &[]);
}

#[test]
fn test_d09_format() {
    run_draft09_test("format.json", &[]);
}

#[test]
fn test_d09_if_then_else() {
    run_draft09_test("if-then-else.json", &[]);
}

#[test]
fn test_d09_infinite_loop_detection() {
    run_draft09_test("infinite-loop-detection.json", &[]);
}

#[test]
fn test_d09_items() {
    run_draft09_test("items.json", &[]);
}

#[test]
fn test_d09_max_contains() {
    run_draft09_test("maxContains.json", &[]);
}

#[test]
fn test_d09_maximum() {
    run_draft09_test("maximum.json", &[]);
}

#[test]
fn test_d09_max_items() {
    run_draft09_test("maxItems.json", &[]);
}

#[test]
fn test_d09_max_length() {
    run_draft09_test("maxLength.json", &[]);
}

#[test]
fn test_d09_max_properties() {
    run_draft09_test("maxProperties.json", &[]);
}

#[test]
fn test_d09_min_contains() {
    run_draft09_test("minContains.json", &[]);
}

#[test]
fn test_d09_minimum() {
    run_draft09_test("minimum.json", &[]);
}

#[test]
fn test_d09_min_items() {
    run_draft09_test("minItems.json", &[]);
}

#[test]
fn test_d09_min_length() {
    run_draft09_test("minLength.json", &[]);
}

#[test]
fn test_d09_min_properties() {
    run_draft09_test("minProperties.json", &[]);
}

#[test]
fn test_d09_multiple_of() {
    run_draft09_test("multipleOf.json", &[]);
}

#[test]
fn test_d09_not() {
    run_draft09_test("not.json", &[]);
}

#[test]
fn test_d09_one_of() {
    run_draft09_test("oneOf.json", &[]);
}

#[test]
fn test_d09_pattern() {
    run_draft09_test("pattern.json", &[]);
}

#[test]
fn test_d09_pattern_properties() {
    run_draft09_test("patternProperties.json", &[]);
}

#[test]
fn test_d09_properties() {
    run_draft09_test("properties.json", &[]);
}

#[test]
fn test_d09_property_names() {
    run_draft09_test("propertyNames.json", &[]);
}

// $recursiveRef has been replaced by $dynamicRef in draft2020-12
// The semantics are too different for us to support both.
// #[test]
// fn test_d09_recursive_ref() {
//     run_draft09_test("recursiveRef.json", &[]);
// }

#[test]
fn test_d09_ref() {
    run_draft09_test("ref.json", &[]);
}

#[test]
fn test_d09_ref_remote() {
    run_draft09_test("refRemote.json", &[]);
}

#[test]
fn test_d09_required() {
    run_draft09_test("required.json", &[]);
}

#[test]
fn test_d09_type() {
    run_draft09_test("type.json", &[]);
}

#[test]
fn test_d09_unevaluated_items() {
    run_draft09_test(
        "unevaluatedItems.json",
        &[
            // We ignore $recursiveRef.
            (
                "unevaluatedItems with $recursiveRef",
                "with no unevaluated items",
                serde_json::json!([1, [2, [], "b"], "a"]),
            ),
            // We deviate from the spec in our treatment of "additionalItems" when "items" is absent.
            (
                "unevaluatedItems with ignored additionalItems",
                "all valid under unevaluatedItems",
                serde_json::json!(["foo", "bar", "baz"]),
            ),
            (
                "unevaluatedItems with ignored applicator additionalItems",
                "all valid under unevaluatedItems",
                serde_json::json!(["foo", "bar", "baz"]),
            ),
        ],
    );
}

#[test]
fn test_d09_unevaluated_properties() {
    run_draft09_test(
        "unevaluatedProperties.json",
        &[(
            "unevaluatedProperties with $recursiveRef",
            "with no unevaluated properties",
            serde_json::json!({"branches": {"name": "b", "node": 2}, "name": "a", "node": 1}),
        )],
    );
}

#[test]
fn test_d09_unique_items() {
    run_draft09_test("uniqueItems.json", &[]);
}

// vocabulary.json is not supported: it uses a custom meta-schema
// that requires we must be able to disable validation. No thanks.
// #[test]
// fn test_d09_vocabulary() {
//     run_draft09_test("vocabulary.json", &[]);
// }

// Optional tests (in alphabetical order)
#[test]
fn test_d09_optional_anchor() {
    run_draft09_test("optional/anchor.json", &[]);
}

#[test]
fn test_d09_optional_bignum() {
    run_draft09_test("optional/bignum.json", &[]);
}

// Cross-draft references require loading schemas from other draft versions
// #[test]
// fn test_d09_optional_cross_draft() {
//     run_draft09_test("optional/cross-draft.json", &[]);
// }

// dependencies keyword is from draft4/6/7, not supported in draft2019-09
// #[test]
// fn test_d09_optional_dependencies_compatibility() {
//     run_draft09_test("optional/dependencies-compatibility.json", &[]);
// }

// ECMAScript regex tests fail - implementation uses Rust regex crate
// #[test]
// fn test_d09_optional_ecmascript_regex() {
//     run_draft09_test("optional/ecmascript-regex.json", &[]);
// }

// Float overflow tests fail - needs special handling of very large numbers
// #[test]
// fn test_d09_optional_float_overflow() {
//     run_draft09_test("optional/float-overflow.json", &[]);
// }

#[test]
fn test_d09_optional_id() {
    run_draft09_test("optional/id.json", &[]);
}

#[test]
fn test_d09_optional_no_schema() {
    run_draft09_test("optional/no-schema.json", &[]);
}

#[test]
fn test_d09_optional_non_bmp_regex() {
    run_draft09_test("optional/non-bmp-regex.json", &[]);
}

// We deliberately reject unknown keywords
// #[test]
// fn test_d09_optional_ref_of_unknown_keyword() {
//     run_draft09_test("optional/refOfUnknownKeyword.json", &[]);
// }

// We deliberately reject unknown keywords
// #[test]
// fn test_d09_optional_unknown_keyword() {
//     run_draft09_test("optional/unknownKeyword.json", &[]);
// }
