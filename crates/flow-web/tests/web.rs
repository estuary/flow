//! Test suite for the Web and headless browsers.

#![cfg(target_arch = "wasm32")]

extern crate wasm_bindgen_test;
use flow_web::{infer, AnalyzedSchema, Property};
use serde_json::json;
use serde_wasm_bindgen::{from_value as from_js_value, Serializer};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
fn test_end_to_end_schema_inferrence() {
    let schema: JsValue = to_js_value(&json!({
        "type": "object",
        "reduce": { "strategy": "merge" },
        "properties": {
            "a": { "title": "A", "type": "integer", "reduce": { "strategy": "sum"} },
            "b": { "description": "the b", "type": "string", "format": "date-time"},
            "c": { "type": ["string", "integer"] },
            "d": { "type": "boolean"},
            "e": {
                "type": "object",
                "patternProperties": {
                    "e.*": {"type": "number"}
                }
            },
            "f": { "const": "F"}
        },
        "required": ["a", "e"]
    }));
    let result = infer(schema).expect("failed to infer");
    let inferred: serde_json::Value =
        from_js_value(result).expect("failed to deserialize analyzed schema");

    let expected = json!({
      "properties": [
        {
          "description": null,
          "enum_vals": [],
          "exists": "must",
          "is_pattern_property": false,
          "name": null,
          "pointer": "",
          "reduction": "merge",
          "string_format": null,
          "title": null,
          "types": [ "object" ]
        },
        {
          "description": null,
          "enum_vals": [],
          "exists": "must",
          "is_pattern_property": false,
          "name": "a",
          "pointer": "/a",
          "reduction": "sum",
          "string_format": null,
          "title": "A",
          "types": [ "integer" ]
        },
        {
          "description": "the b",
          "enum_vals": [],
          "exists": "may",
          "is_pattern_property": false,
          "name": "b",
          "pointer": "/b",
          "reduction": "unset",
          "string_format": "date-time",
          "title": null,
          "types": [ "string" ]
        },
        {
          "description": null,
          "enum_vals": [],
          "exists": "may",
          "is_pattern_property": false,
          "name": "c",
          "pointer": "/c",
          "reduction": "unset",
          "string_format": null,
          "title": null,
          "types": [ "integer", "string" ]
        },
        {
          "description": null,
          "enum_vals": [],
          "exists": "may",
          "is_pattern_property": false,
          "name": "d",
          "pointer": "/d",
          "reduction": "unset",
          "string_format": null,
          "title": null,
          "types": [ "boolean" ]
        },
        {
          "description": null,
          "enum_vals": [],
          "exists": "must",
          "is_pattern_property": false,
          "name": "e",
          "pointer": "/e",
          "reduction": "unset",
          "string_format": null,
          "title": null,
          "types": [ "object" ]
        },
        {
          "description": null,
          "enum_vals": [],
          "exists": "may",
          "is_pattern_property": true,
          "name": null,
          "pointer": "/e/e.*",
          "reduction": "unset",
          "string_format": null,
          "title": null,
          "types": [ "number" ]
        },
        {
          "description": null,
          "enum_vals": [ "F" ],
          "exists": "may",
          "is_pattern_property": false,
          "name": "f",
          "pointer": "/f",
          "reduction": "unset",
          "string_format": null,
          "title": null,
          "types": [ "string" ]
        }
      ]
    });
    assert_eq!(expected, inferred);
}

fn to_js_value(val: &serde_json::Value) -> JsValue {
    use serde::Serialize;
    // We need to use the json compatible serializer because the default
    // serializer will use a `Map` instead of an `Object`, which doesn't
    // directly work with JSON. Note that we may just want to deal with that
    // in our deserialization code, somehow, if library callers want to use
    // `Map`s for some reason. But it didn't seem worth the effort for now.
    val.serialize(&Serializer::json_compatible()).unwrap()
}
