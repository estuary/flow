//! Test suite for the Web and headless browsers.

#![cfg(target_arch = "wasm32")]

extern crate wasm_bindgen_test;
use serde_json::json;
use serde_wasm_bindgen::{from_value as from_js_value, Serializer};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
fn test_end_to_end_schema_inference() {
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
    let result = flow_web::infer(schema).expect("failed to infer");
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

#[wasm_bindgen_test]
fn test_end_to_end_extend_read_bundle() {
    let input: JsValue = to_js_value(&json!({
      "read": {
        "$defs": {
            "existing://def": {"type": "array"},
        },
        "maxProperties": 10,
        "allOf": [
            {"$ref": "flow://inferred-schema"},
            {"$ref": "flow://write-schema"},
        ]
      },
      "write": {
        "$id": "old://value",
        "required": ["a_key"],
      },
      "inferred": {
        "$id": "old://value",
        "minProperties": 5,
      }
    }));

    let output = flow_web::extend_read_bundle(input).expect("extend first bundle");
    let output: serde_json::Value = from_js_value(output).expect("failed to deserialize output");

    assert_eq!(
        output,
        json!({
          "$defs": {
              "existing://def": {"type": "array"}, // Left alone.
              "flow://write-schema": { "$id": "flow://write-schema", "required": ["a_key"] },
              "flow://inferred-schema": { "$id": "flow://inferred-schema", "minProperties": 5 },
          },
          "maxProperties": 10,
          "allOf": [
              {"$ref": "flow://inferred-schema"},
              {"$ref": "flow://write-schema"},
          ]
        })
    );

    let input: JsValue = to_js_value(&json!({
      "read": {
        "maxProperties": 10,
      },
      "write": {
        "$id": "old://value",
        "required": ["a_key"],
      },
      "inferred": null,
    }));

    let output = flow_web::extend_read_bundle(input).expect("extend second bundle");
    let output: serde_json::Value = from_js_value(output).expect("failed to deserialize output");

    assert_eq!(
        output,
        json!({
          "$defs": {},
          "maxProperties": 10,
        })
    );
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
