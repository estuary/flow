//! Test suite for the Web and headless browsers.

#![cfg(target_arch = "wasm32")]

extern crate wasm_bindgen_test;
use proto_flow::flow;
use serde_json::{json, to_string};
use serde_wasm_bindgen::{from_value as from_js_value, Serializer};
use wasm_bindgen::prelude::wasm_bindgen;
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
fn test_skim_projections_write_schema_redaction() {
    // A redact annotation on the write schema of a collection with a
    // standalone read schema (no $ref to flow://write-schema). Redaction runs
    // at write time against the write schema, so the annotation must surface
    // on the projection's inference.
    let input: JsValue = to_js_value(&json!({
        "collection": "acmeCo/test",
        "model": {
            "key": ["/id"],
            "writeSchema": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "secret": {"type": "string", "redact": {"strategy": "block"}}
                },
                "required": ["id"]
            },
            "readSchema": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "secret": {"type": "string"}
                },
                "required": ["id"]
            }
        }
    }));
    let result = flow_web::skim_collection_projections(input).unwrap();
    let result: flow_web::collection::CollectionProjectionsResult =
        serde_wasm_bindgen::from_value(result).unwrap();

    let secret = result
        .projections
        .iter()
        .find(|p| p.ptr == "/secret")
        .expect("a /secret projection is skimmed");
    assert_eq!(
        secret.inference.as_ref().unwrap().redact(),
        flow::inference::Redact::Block,
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
                "projectionConstraints": [
                    {
                        "field": "id",
                        "constraint": {
                            "type": "FIELD_OPTIONAL",
                            "reason": "Available field"
                        }
                    },
                    {
                        "field": "flow_published_at",
                        "constraint": {
                            "type": "FIELD_OPTIONAL",
                            "reason": "Available field"
                        }
                    },
                    {
                        "field": "value",
                        "constraint": {
                            "type": "FIELD_OPTIONAL",
                            "reason": "Available field"
                        }
                    },
                    {
                        "field": "bad",
                        "constraint": {
                            "type": "FIELD_FORBIDDEN",
                            "reason": "Not today, pal."
                        }
                    }
                ],
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

#[wasm_bindgen_test]
fn test_strip_and_restore_trigger_hmac_excluded_fields() {
    let original = json!({
        "config": [{
            "url": "https://example.com/webhook",
            "method": "POST",
            "headers": {"Authorization": "Bearer secret"},
            "payloadTemplate": "my template",
            "timeout": "45s",
            "maxAttempts": 5
        }]
    });

    let stripped_result =
        flow_web::strip_trigger_hmac_excluded_fields(&to_string(&original).unwrap())
            .expect("strip should succeed");

    #[derive(serde::Deserialize)]
    struct StripOutput {
        stripped: String,
        originals: serde_json::Value,
    }
    let StripOutput {
        stripped,
        originals,
    } = serde_wasm_bindgen::from_value(stripped_result).unwrap();

    let stripped_value: serde_json::Value = serde_json::from_str(&stripped).unwrap();

    // HMAC-excluded fields replaced with placeholders; secret header untouched.
    assert_eq!(stripped_value["config"][0]["payloadTemplate"], "");
    assert_eq!(stripped_value["config"][0]["timeout"], "0s");
    assert_eq!(stripped_value["config"][0]["maxAttempts"], 0);
    assert_eq!(
        stripped_value["config"][0]["headers"]["Authorization"],
        "Bearer secret"
    );

    // Restore the originals into the stripped JSON (simulating post-encryption restore).
    let restored =
        flow_web::restore_trigger_hmac_excluded_fields(&stripped, to_js_value(&originals))
            .expect("restore should succeed");

    let restored_str: String = serde_wasm_bindgen::from_value(restored).unwrap();
    let restored_value: serde_json::Value = serde_json::from_str(&restored_str).unwrap();

    assert_eq!(restored_value, original);
}

#[wasm_bindgen_test]
fn test_resolve_built_collections_capture() {
    let one = built_collection("acmeCo/one", "/id");
    let two = built_collection("acmeCo/two", "/id");

    let inline = json!({
        "name": "acmeCo/capture",
        "connectorType": "IMAGE",
        "config": {"image": "an/image", "config": {"a": "config"}},
        "bindings": [
            built_binding("one-a", json!({"collection": one})),
            built_binding("two-a", json!({"collection": two})),
            built_binding("one-b", json!({"collection": one})),
            // A binding without a collection doesn't resolve.
            built_binding("none", json!({})),
        ],
    });
    let indirect = json!({
        "name": "acmeCo/capture",
        "connectorType": "IMAGE",
        "config": {"image": "an/image", "config": {"a": "config"}},
        "bindings": [
            // A `collectionIndex` of zero is omitted.
            built_binding("one-a", json!({})),
            built_binding("two-a", json!({"collectionIndex": 1})),
            built_binding("one-b", json!({})),
            // An out-of-range index doesn't resolve.
            built_binding("none", json!({"collectionIndex": 7})),
        ],
        "linkedCollections": [one, two],
    });

    let expected = json!({
        "collections": [one, two],
        "bindings": [0, 1, 0, null],
    });
    assert_eq!(resolve_built_collections("capture", inline), expected);
    assert_eq!(resolve_built_collections("capture", indirect), expected);
}

#[wasm_bindgen_test]
fn test_resolve_built_collections_materialization() {
    // Two bindings of one collection whose `groupBy` differs have distinct
    // keys, and are distinct entries which share a name.
    let by_id = built_collection("acmeCo/one", "/id");
    let by_val = built_collection("acmeCo/one", "/val");

    let inline = json!({
        "name": "acmeCo/materialization",
        "connectorType": "IMAGE",
        "config": {"image": "other/image", "config": {"a": "config"}},
        "bindings": [
            built_binding("by-id", json!({"collection": by_id})),
            built_binding("by-val", json!({"collection": by_val})),
            built_binding("also-by-id", json!({"collection": by_id})),
        ],
    });
    let indirect = json!({
        "name": "acmeCo/materialization",
        "connectorType": "IMAGE",
        "config": {"image": "other/image", "config": {"a": "config"}},
        "bindings": [
            built_binding("by-id", json!({})),
            built_binding("by-val", json!({"collectionIndex": 1})),
            built_binding("also-by-id", json!({})),
        ],
        "linkedCollections": [by_id, by_val],
    });

    let expected = json!({
        "collections": [by_id, by_val],
        "bindings": [0, 1, 0],
    });
    assert_eq!(
        resolve_built_collections("materialization", inline),
        expected
    );
    assert_eq!(
        resolve_built_collections("materialization", indirect),
        expected
    );
}

// Call `resolve_built_collections` and return its result as JSON. The result
// passes through JSON.stringify, which renders a `Map` as {} and throws on a
// `BigInt`, so that comparisons also assert it's made of plain JSON values.
fn resolve_built_collections(spec_type: &str, built_spec: serde_json::Value) -> serde_json::Value {
    let input = to_js_value(&json!({
        "specType": spec_type,
        "builtSpec": built_spec,
    }));
    let result = json_stringify(&flow_web::resolve_built_collections(input).unwrap());

    // serde_json parses a RawValue wrapper object back into the value it
    // wraps, so check for one before parsing.
    assert!(
        !result.contains("$serde_json::private::RawValue"),
        "{result}"
    );
    serde_json::from_str(&result).unwrap()
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = JSON, js_name = stringify)]
    fn json_stringify(value: &JsValue) -> String;
}

// A built collection in the JSON form of `live_specs.built_spec`, which
// omits default values.
fn built_collection(name: &str, key: &str) -> serde_json::Value {
    json!({
        "name": name,
        "writeSchema": {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "val": {"type": "integer"},
            },
            "required": ["id", "val"],
        },
        "key": [key],
        "uuidPtr": "/_meta/uuid",
        "projections": [
            {"ptr": key, "field": &key[1..], "isPrimaryKey": true},
        ],
        "partitionTemplate": {
            "name": name,
            "replication": 3,
            "fragment": {"length": "536870912", "flushInterval": "86400s"},
        },
    })
}

// A built binding of resource `name`, with additional `fields` which name its
// collection.
fn built_binding(name: &str, fields: serde_json::Value) -> serde_json::Value {
    let mut binding = json!({
        "resourceConfig": {"name": name},
        "resourcePath": [name],
        "stateKey": name,
    });
    binding
        .as_object_mut()
        .unwrap()
        .extend(fields.as_object().unwrap().clone());
    binding
}

/// The browser checks egress hosts with the data plane's helper, so its
/// address parsing and Public Suffix List must read the same under WASM.
#[wasm_bindgen_test]
fn test_egress_hosts() {
    let cases: &[(&str, Result<&str, &str>)] = &[
        ("PyPI.ORG.", Ok("pypi.org")),
        ("*.Mirror.AcmeCo.example.", Ok("*.mirror.acmeco.example")),
        ("127.1", Err("the IPv4 address 127.0.0.1")),
        ("0177.0.0.1", Err("the IPv4 address 127.0.0.1")),
        (
            "*.github.io",
            Err("the Public Suffix List's private section"),
        ),
        ("*.corp", Err("the Public Suffix List's default rule")),
        ("*.acmeco.ck", Err("the Public Suffix List's ICANN section")),
    ];

    for (name, expected) in cases {
        let outcome = models::egress::hosts("egress.hosts", &[name.to_string()])
            .map(|names| names[0].to_string())
            .map_err(|error| format!("{error:#}"));

        match (expected, &outcome) {
            (Ok(expected), Ok(outcome)) => assert_eq!(outcome, expected, "{name}"),
            (Err(fragment), Err(error)) => assert!(error.contains(fragment), "{name}: {error}"),
            _ => panic!("{name}: expected {expected:?}, got {outcome:?}"),
        }
    }
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
