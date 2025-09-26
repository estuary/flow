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
fn test_skim_projections() {
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
            is_primary_key: false,
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
fn test_field_selection() {
    let input: JsValue = to_js_value(&json!({
        "collection": {
            "name": "test/collection",
            "model": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "value": {"type": "string"},
                        "bad": {"type": "object"}
                    },
                    "required": ["id"]
                },
                "key": ["/id"]
            }
        },
        "binding": {
            "live": null,
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
                    },
                    "flow_published_at": {
                        "type": "FIELD_OPTIONAL",
                        "reason": "Available field"
                    },
                    "value": {
                        "type": "FIELD_OPTIONAL",
                        "reason": "Available field"
                    },
                    "bad": {
                        "type": "FIELD_FORBIDDEN",
                        "reason": "Not today, pal."
                    }
                },
                "deltaUpdates": false
            }
        }
    }));
    let result = flow_web::evaluate_field_selection(input).unwrap();
    let result: serde_json::Value = from_js_value(result).unwrap();

    assert_eq!(
        result,
        json!({
            "hasConflicts": false,
            "outcomes": [
                {
                    "field": "_meta/flow_truncated",
                    "reject": {
                        "detail": "connector didn't return a constraint for this field",
                        "reason": {
                            "type": "ConnectorOmits"
                        }
                    }
                },
                {
                    "field": "bad",
                    "reject": {
                        "detail": "field is forbidden by the connector (Not today, pal.)",
                        "reason": {
                            "reason": "Not today, pal.",
                            "type": "ConnectorForbids"
                        }
                    }
                },
                {
                    "field": "flow_document",
                    "reject": {
                        "detail": "connector didn't return a constraint for this field",
                        "reason": {
                            "type": "ConnectorOmits"
                        }
                    }
                },
                {
                    "field": "flow_published_at",
                    "select": {
                        "detail": "field is important metadata which is typically selected",
                        "reason": {
                            "type": "CoreMetadata"
                        }
                    }
                },
                {
                    "field": "id",
                    "select": {
                        "detail": "field is part of the materialization group-by key",
                        "reason": {
                            "type": "GroupByKey"
                        }
                    }
                },
                {
                    "field": "value",
                    "select": {
                        "detail": "field is within the desired depth",
                        "reason": {
                            "type": "DesiredDepth"
                        }
                    }
                }
            ],
            "selection": {
                "fieldConfig": {
                    "id": {"my": "config"}
                },
                "keys": ["id"],
                "values": ["flow_published_at", "value"]
            }
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
