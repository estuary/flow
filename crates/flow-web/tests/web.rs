//! Test suite for the Web and headless browsers.

#![cfg(target_arch = "wasm32")]

extern crate wasm_bindgen_test;
use proto_flow::flow;
use serde_json::{json, to_string};
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

    assert_eq!(output, json!({"maxProperties": 10}));

    let input: JsValue = to_js_value(&json!({
      "write": {
        "required": ["a_key"]
      },
      "read": {
        "allOf": [
            {"$ref": "flow://inferred-schema"},
            {"$ref": "flow://write-schema"},
        ]
      },
      "inferred": {
        "$id": "flow://inferred-schema",
        "x-canary-annotation": true
      }
    }));

    let output = flow_web::extend_read_bundle(input).expect("extend first bundle");
    let output: serde_json::Value = from_js_value(output).expect("failed to deserialize output");
    assert_eq!(
        output,
        json!({
            "$defs": {
                "flow://inferred-schema": {
                    "$id": "flow://inferred-schema",
                    "x-canary-annotation": true
                },
                "flow://write-schema": {
                    "$id": "flow://write-schema",
                    "required": ["a_key"]
                }
            },
            "allOf": [
                {"$ref": "flow://inferred-schema"},
                {"$ref": "flow://write-schema"},
            ]
        })
    );
}

#[wasm_bindgen_test]
fn test_update_materialization_resource_spec() {
    let arguments: JsValue = to_js_value(&json!({
      "resourceSpecPointers": {
        "x_collection_name": "/a",
        "x_schema_name": "/b/c",
        "x_delta_updates": "/d/e/f",
      },
      "collectionName": "acme/collectionPreface/fakeNameHere",
      "resourceSpec": {},
      "sourceCapture": {
        "capture": "acme/capture/source-fake",
        "deltaUpdates": true,
        "targetSchema": "fromSourceName"
      },
    }));
    let result =
        flow_web::update_materialization_resource_spec(arguments).expect("failed to infer");

    assert_eq!(
        result,
        to_string(&json!({
          "a": "fakeNameHere",
          "b": {
            "c": "collectionPreface"
          },
          "d": {
            "e": {
              "f": true
            }
          }
        }))
        .unwrap()
    );
}

#[wasm_bindgen_test]
fn test_skim_projections_basic() {
    let input: JsValue = to_js_value(&json!({
        "collection": "test/collection",
        "model": {
            "schema": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"}
                },
                "required": ["id"]
            },
            "key": ["/id"],
            "projections": {
                "Id": "/id",
                "Name": "/name"
            }
        }
    }));
    let result = flow_web::skim_collection_projections(input).unwrap();
    let result: flow_web::collection::CollectionProjectionsResult =
        serde_wasm_bindgen::from_value(result).unwrap();

    assert_eq!(
        result.projections[0],
        flow::Projection {
            ptr: "/id".to_string(),
            field: "Id".to_string(),
            explicit: true,
            is_primary_key: true,
            is_partition_key: false,
            inference: Some(flow::Inference {
                types: vec!["integer".to_string()],
                numeric: Some(Default::default()),
                exists: flow::inference::Exists::Must as i32,
                ..Default::default()
            }),
        }
    );
    assert!(result.errors.is_empty());
}

#[wasm_bindgen_test]
fn test_field_selection_basic() {
    let input: JsValue = to_js_value(&json!({
        "collectionKey": ["/id"],
        "collectionProjections": [
            {
                "ptr": "/id",
                "field": "id",
                "inference": {
                    "types": ["integer"],
                    "exists": "MUST"
                },
            }
        ],
        "liveSpec": null,
        "model": {
            "source":"test/collection",
            "resource": {"table": "foo"},
            "fields": {
                "require": {
                  "id": {"my": "config"}
                },
                "recommended": 1
            }
        },
        "validated": {
            "resourcePath": ["test_table"],
            "constraints": {
                "id": {
                    "type": "FIELD_OPTIONAL",
                    "reason": "Available field"
                }
            },
            "deltaUpdates": false
        }
    }));
    let result = flow_web::evaluate_field_selection(input).unwrap();
    let result: serde_json::Value = from_js_value(result).unwrap();

    assert_eq!(
        result,
        json!({
          "outcomes":[
            {
              "field":"id",
              "select":{
                "reason": "GroupByKey",
                "detail":"field is part of the materialization group-by key"
              }
            }
          ],
          "selection":{
              "keys":["id"],
              "fieldConfig": {"id": {"my": "config"}}
          },
          "hasConflicts":false
        })
    );
}

#[wasm_bindgen_test]
fn test_field_selection_with_reject() {
    let input: JsValue = to_js_value(&json!({
        "collectionKey": ["/id"],
        "collectionProjections": [
            {
                "ptr": "/id",
                "field": "id",
                "inference": {
                    "types": ["integer"],
                    "exists": "MUST"
                },
            },
            {
                "ptr": "/name",
                "field": "name",
                "inference": {
                    "types": ["string"],
                    "exists": "MAY"
                },
            }
        ],
        "liveSpec": null,
        "model": {
            "source":"test/collection",
            "resource": {"table": "foo"},
            "fields": {
                "exclude": ["name"],
                "recommended": 1
            }
        },
        "validated": {
            "resourcePath": ["test_table"],
            "constraints": {
                "id": {
                    "type": "FIELD_OPTIONAL",
                    "reason": "Available field"
                },
                "name": {
                    "type": "FIELD_OPTIONAL",
                    "reason": "Available field"
                }
            },
            "deltaUpdates": false
        }
    }));
    let result = flow_web::evaluate_field_selection(input).unwrap();
    let result: serde_json::Value = from_js_value(result).unwrap();

    // Should have both select and reject outcomes
    let outcomes = &result["outcomes"];
    assert_eq!(outcomes.as_array().unwrap().len(), 2);
    
    // ID field should be selected (group-by key)
    let id_outcome = &outcomes[0];
    assert_eq!(id_outcome["field"], "id");
    assert!(id_outcome["select"]["reason"] == "GroupByKey");
    assert!(id_outcome["reject"].is_null());
    
    // Name field should be rejected (user excluded)
    let name_outcome = &outcomes[1];
    assert_eq!(name_outcome["field"], "name");
    assert!(name_outcome["select"].is_null());
    assert_eq!(name_outcome["reject"]["reason"], "UserExcludes");
    assert_eq!(name_outcome["reject"]["detail"], "field is excluded by the user's field selection");
    
    assert_eq!(result["hasConflicts"], false);
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
