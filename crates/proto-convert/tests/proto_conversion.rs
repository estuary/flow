use proto_convert::{FromMessage, IntoMessages};
use proto_flow::{
    capture::{self, pull_request},
    flow::{self, inference},
    materialize::{self, transaction_request},
};
use serde_json::{json, Value};
use std::io::Write;
use tuple::TuplePack;

#[test]
fn test_proto_request_to_json() {
    let collection_one = flow::CollectionSpec {
        collection: "collection/one".to_string(),
        key_ptrs: vec!["/key/one".to_string(), "/two/3".to_string()],
        partition_fields: vec!["part-field".to_string()],
        projections: vec![flow::Projection {
            explicit: false,
            is_partition_key: true,
            is_primary_key: false,
            field: "a-field".to_string(),
            ptr: "/json/ptr".to_string(),
            inference: Some(flow::Inference {
                default_json: json!({"def": "ault"}).to_string(),
                description: "desc".to_string(),
                title: "title".to_string(),
                exists: inference::Exists::Must as i32,
                secret: false,
                string: Some(inference::String {
                    content_encoding: "enc".to_string(),
                    content_type: "typ".to_string(),
                    format: "date".to_string(),
                    is_base64: false,
                    max_length: 12345,
                }),
                types: vec!["integer".to_string(), "string".to_string()],
            }),
        }],
        write_schema_json: json!({"common": "schema"}).to_string(),
        ..Default::default()
    };
    let collection_two = flow::CollectionSpec {
        collection: "collection/two".to_string(),
        key_ptrs: vec!["/a/key".to_string()],
        write_schema_json: json!({"write": "schema"}).to_string(),
        read_schema_json: json!({"read": "schema"}).to_string(),
        ..Default::default()
    };

    let capture = flow::CaptureSpec {
        capture: "some/capture".to_string(),
        endpoint_spec_json: json!({"endpoint": "config"}).to_string(),
        interval_seconds: 300,
        bindings: vec![
            flow::capture_spec::Binding {
                collection: Some(collection_one.clone()),
                resource_spec_json: json!({"first": "resource"}).to_string(),
                resource_path: vec!["table_one".to_string()],
            },
            flow::capture_spec::Binding {
                collection: Some(collection_two.clone()),
                resource_spec_json: json!({"resource": 2}).to_string(),
                resource_path: vec!["other_schema".to_string(), "table_two".to_string()],
            },
        ],
        ..Default::default()
    };

    let materialization = flow::MaterializationSpec {
        materialization: "some/materialization".to_string(),
        endpoint_spec_json: json!({"endpoint": "config"}).to_string(),
        bindings: vec![
            flow::materialization_spec::Binding {
                collection: Some(collection_one.clone()),
                resource_spec_json: json!({"first": "resource"}).to_string(),
                resource_path: vec!["table_one".to_string()],
                field_selection: Some(flow::FieldSelection {
                    keys: vec!["key/one".to_string()],
                    values: vec!["val1".to_string(), "val2".to_string()],
                    document: "flow_document".to_string(),
                    field_config_json: [
                        ("val1".to_string(), json!({"a": "setting"}).to_string()),
                        ("val2".to_string(), json!({}).to_string()),
                    ]
                    .into_iter()
                    .collect(),
                }),
                delta_updates: false,
                shuffle: None,
            },
            flow::materialization_spec::Binding {
                collection: Some(collection_two.clone()),
                resource_spec_json: json!({"resource": 2}).to_string(),
                resource_path: vec!["other_schema".to_string(), "table_two".to_string()],
                field_selection: Some(flow::FieldSelection {
                    keys: vec!["k1".to_string(), "k2".to_string()],
                    values: vec!["value_field".to_string()],
                    document: String::new(),
                    field_config_json: [].into_iter().collect(),
                }),
                delta_updates: true,
                shuffle: None,
            },
        ],
        ..Default::default()
    };

    let mut output = Vec::new();

    test_case(
        "Capture Spec",
        capture::SpecRequest {
            endpoint_type: flow::EndpointType::FlowSink as i32,
            endpoint_spec_json: json!({
                "foo": "capture",
                "forty": 2,
            })
            .to_string(),
        },
        &mut output,
    );

    test_case(
        "Materialize Spec",
        materialize::SpecRequest {
            endpoint_type: flow::EndpointType::FlowSink as i32,
            endpoint_spec_json: json!({
                "foo": "materialize",
                "forty": 2,
            })
            .to_string(),
        },
        &mut output,
    );

    test_case(
        "Capture Discover",
        capture::DiscoverRequest {
            endpoint_type: flow::EndpointType::FlowSink as i32,
            endpoint_spec_json: json!({
                "foo": "bar",
                "forty": 2,
            })
            .to_string(),
        },
        &mut output,
    );

    test_case(
        "Capture Validate",
        capture::ValidateRequest {
            endpoint_type: flow::EndpointType::FlowSink as i32,
            endpoint_spec_json: json!({
                "foo": "capture",
                "forty": 2,
            })
            .to_string(),
            capture: "name/of/capture".to_string(),
            bindings: vec![
                capture::validate_request::Binding {
                    collection: Some(collection_one.clone()),
                    resource_spec_json: json!({"first": "resource"}).to_string(),
                },
                capture::validate_request::Binding {
                    collection: Some(collection_two.clone()),
                    resource_spec_json: json!({"resource": 2}).to_string(),
                },
            ],
        },
        &mut output,
    );

    test_case(
        "Materialize Validate",
        materialize::ValidateRequest {
            endpoint_type: flow::EndpointType::FlowSink as i32,
            endpoint_spec_json: json!({
                "foo": "materialize",
                "forty": 2,
            })
            .to_string(),
            materialization: "name/of/materialization".to_string(),
            bindings: vec![
                materialize::validate_request::Binding {
                    collection: Some(collection_one.clone()),
                    resource_spec_json: json!({"first": "resource"}).to_string(),
                    field_config_json: [
                        ("field-one".to_string(), json!({"one": 1}).to_string()),
                        ("two".to_string(), json!({"two": 2}).to_string()),
                    ]
                    .into_iter()
                    .collect(),
                },
                materialize::validate_request::Binding {
                    collection: Some(collection_two.clone()),
                    resource_spec_json: json!({"resource": 2}).to_string(),
                    field_config_json: [].into_iter().collect(),
                },
            ],
        },
        &mut output,
    );

    test_case(
        "Capture Apply",
        capture::ApplyRequest {
            capture: Some(capture.clone()),
            dry_run: true,
            version: "aabbccddee".to_string(),
        },
        &mut output,
    );

    test_case(
        "Materialize Apply",
        materialize::ApplyRequest {
            materialization: Some(materialization.clone()),
            dry_run: true,
            version: "aabbccddee".to_string(),
        },
        &mut output,
    );

    test_case(
        "Capture Open",
        capture::PullRequest {
            open: Some(pull_request::Open {
                capture: Some(capture.clone()),
                key_begin: 12345,
                key_end: 678910,
                version: "aabbccddee".to_string(),
                driver_checkpoint_json: serde_json::to_vec(&json!({"driver": "checkpoint"}))
                    .unwrap(),
            }),
            ..Default::default()
        },
        &mut output,
    );

    test_case(
        "Capture Acknowledge",
        capture::PullRequest {
            acknowledge: Some(capture::Acknowledge {}),
            ..Default::default()
        },
        &mut output,
    );

    test_case(
        "Materialize Open",
        materialize::TransactionRequest {
            open: Some(transaction_request::Open {
                materialization: Some(materialization.clone()),
                key_begin: 12345,
                key_end: 678910,
                version: "aabbccddee".to_string(),
                driver_checkpoint_json: serde_json::to_vec(&json!({"driver": "checkpoint"}))
                    .unwrap(),
            }),
            ..Default::default()
        },
        &mut output,
    );

    // Build an arena fixture with keys, values, and documents.
    // We'll use portions of this arena in multiple request fixtures.
    let mut arena = vec![0x0, 0x1, 0x2, 0x3]; // Extra unused bytes.
    let pivot = arena.len();

    vec![json!("key"), json!("one")]
        .pack_root(&mut arena)
        .unwrap();
    let (key1, pivot) = (pivot..arena.len(), arena.len());

    vec![json!(1), json!({"two": "three"}), json!("four")]
        .pack_root(&mut arena)
        .unwrap();
    let (values1, pivot) = (pivot..arena.len(), arena.len());

    serde_json::to_writer(&mut arena, &json!({"doc": "one"})).unwrap();
    let (doc1, pivot) = (pivot..arena.len(), arena.len());

    vec![json!("key"), json!(2)].pack_root(&mut arena).unwrap();
    let (key2, pivot) = (pivot..arena.len(), arena.len());

    vec![json!(1), json!({"two": "three"}), json!("four")]
        .pack_root(&mut arena)
        .unwrap();
    let (values2, pivot) = (pivot..arena.len(), arena.len());

    serde_json::to_writer(&mut arena, &json!({"doc": 2})).unwrap();
    let (doc2, _pivot) = (pivot..arena.len(), arena.len());

    test_case(
        "Materialize Load",
        materialize::TransactionRequest {
            load: Some(transaction_request::Load {
                arena: arena.clone(),
                binding: 2,
                packed_keys: vec![
                    flow::Slice {
                        begin: key1.start as u32,
                        end: key1.end as u32,
                    },
                    flow::Slice {
                        begin: key2.start as u32,
                        end: key2.end as u32,
                    },
                ],
            }),
            ..Default::default()
        },
        &mut output,
    );

    test_case(
        "Materialize Flush",
        materialize::TransactionRequest {
            flush: Some(transaction_request::Flush {}),
            ..Default::default()
        },
        &mut output,
    );

    test_case(
        "Materialize Store",
        materialize::TransactionRequest {
            store: Some(transaction_request::Store {
                arena: arena.clone(),
                binding: 2,
                packed_keys: vec![
                    flow::Slice {
                        begin: key1.start as u32,
                        end: key1.end as u32,
                    },
                    flow::Slice {
                        begin: key2.start as u32,
                        end: key2.end as u32,
                    },
                ],
                packed_values: vec![
                    flow::Slice {
                        begin: values1.start as u32,
                        end: values1.end as u32,
                    },
                    flow::Slice {
                        begin: values2.start as u32,
                        end: values2.end as u32,
                    },
                ],
                docs_json: vec![
                    flow::Slice {
                        begin: doc1.start as u32,
                        end: doc1.end as u32,
                    },
                    flow::Slice {
                        begin: doc2.start as u32,
                        end: doc2.end as u32,
                    },
                ],
                exists: vec![true, false],
            }),
            ..Default::default()
        },
        &mut output,
    );

    test_case(
        "Materialize Start Commit",
        materialize::TransactionRequest {
            start_commit: Some(transaction_request::StartCommit {
                runtime_checkpoint: vec![0x1, 0x2, 0x32, 0x1, 0x2, 0x32, 0x1, 0x2, 0x32, 0x99],
            }),
            ..Default::default()
        },
        &mut output,
    );

    test_case(
        "Materialize Acknowledge",
        materialize::TransactionRequest {
            acknowledge: Some(transaction_request::Acknowledge {}),
            ..Default::default()
        },
        &mut output,
    );

    insta::assert_display_snapshot!(String::from_utf8_lossy(&output));
}

#[test]
fn test_json_responses_to_proto() {
    let spec = json!({
        "spec": {
            "documentationUrl": "https://docs.example.com",
            "configSchema": {
                "$schema": "https://config-schema.example.com",
                "type": "object",
            },
            "resourceConfigSchema": {
                "$schema": "https://resource-schema.example.com",
                "type": "object",
            },
            "oauth2": {
                "provider": "facebook",
                "authUrlTemplate": "https://www.facebook.com/v15.0/dialog/oauth?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&state={{#urlencode}}{{{  state }}}{{/urlencode}}&scope=ads_management,ads_read,read_insights,business_management",
                "accessTokenResponseMap": {
                    "access_token": "/access_token"
                },
                "accessTokenUrlTemplate": "https://graph.facebook.com/v15.0/oauth/access_token?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}&code={{#urlencode}}{{{ code }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
            }
        }
    });

    insta::assert_debug_snapshot!(parse_responses::<capture::SpecResponse>(vec![spec.clone()]));
    insta::assert_debug_snapshot!(parse_responses::<materialize::SpecResponse>(vec![
        spec.clone()
    ]));

    insta::assert_debug_snapshot!(parse_responses::<capture::DiscoverResponse>(vec![json!({
        "discovered": {
            "bindings": [
                {
                    "recommendedName": "thing_one",
                    "resourceConfig": {
                        "table": "one",
                        "foo": "bar"
                    },
                    "documentSchema": {
                        "type": "object",
                        "const": "document"
                    },
                    "key": ["/key/one", "/two"],
                },
                {
                    "recommendedName": "thing_2",
                    "resourceConfig": {
                        "table": 2,
                    },
                    "documentSchema": {
                        "type": "object",
                        "const": "document-two"
                    },
                    "key": ["/key/key"],
                },
            ]
        }
    })]));

    insta::assert_debug_snapshot!(parse_responses::<capture::ValidateResponse>(vec![json!({
        "validated": {
            "bindings": [
                { "resourcePath": ["thing_one"] },
                { "resourcePath": ["schema", "thing_2"] },
            ]
        }
    })]));
    insta::assert_debug_snapshot!(parse_responses::<materialize::ValidateResponse>(vec![
        json!({
            "validated": {
                // Use many bindings, because the protobuf type uses a hash map
                // and iterates in random order which breaks the debug fixtures.
                "bindings": [
                    { "resourcePath": ["one"], "constraints": { "field_required": {"type": "FieldRequired", "reason": "doesn't fit"} }, "deltaUpdates": true},
                    { "resourcePath": ["two"], "constraints": { "loc_required": {"type": "LocationRequired", "reason": "gaak" } }, "deltaUpdates": false},
                    { "resourcePath": ["three"], "constraints": { "loc_recommended": {"type": "LocationRecommended", "reason": "cool"} }, "deltaUpdates": true},
                    { "resourcePath": ["four"], "constraints": { "field_optional": {"type": "FieldOptional", "reason": "don't like the cut of it's jib"} }, "deltaUpdates": false},
                    { "resourcePath": ["five"], "constraints": { "field_forbidden": {"type": "FieldForbidden", "reason": "bad bad"} }, "deltaUpdates": true},
                    { "resourcePath": ["six"], "constraints": { "super-bad": {"type": "Unsatisfiable", "reason": "whoops"} }, "deltaUpdates": false},
                ]
            }
        })
    ]));

    let applied = json!({
        "applied": {
            "actionDescription": "something something"
        }
    });

    insta::assert_debug_snapshot!(parse_responses::<capture::ApplyResponse>(vec![
        applied.clone()
    ]));
    insta::assert_debug_snapshot!(parse_responses::<materialize::ApplyResponse>(vec![
        applied.clone()
    ]));

    insta::assert_debug_snapshot!(parse_responses::<capture::PullResponse>(vec![
        json!({"opened": { "explicitAcknowledgements": true } }),
        json!({ "document": {"binding": 1, "doc": {"p": 1}} }),
        json!({ "document": {"binding": 1, "doc": {"p": 2}} }),
        json!({ "document": {"binding": 1, "doc": {"p": 3}} }),
        json!({ "document": {"binding": 2, "doc": {"p": 4}} }),
        json!({ "checkpoint": {"driverCheckpoint": {"check": "point"}, "mergePatch": true} }),
    ]));

    insta::assert_debug_snapshot!(parse_responses::<materialize::TransactionResponse>(vec![
        json!({ "opened": { "runtimeCheckpoint": "aGVsbG8=" } }),
        json!({ "loaded": {"binding": 1, "doc": {"p": 1}} }),
        json!({ "loaded": {"binding": 1, "doc": {"p": 2}} }),
        json!({ "loaded": {"binding": 1, "doc": {"p": 3}} }),
        json!({ "loaded": {"binding": 2, "doc": {"p": 4}} }),
        json!({ "flushed": {} }),
        json!({ "startedCommit": {"driverCheckpoint": {"check": "point"}, "mergePatch": true} }),
        json!({ "acknowledged": {} }),
    ]));
}

fn test_case<M>(name: &str, m: M, w: &mut Vec<u8>)
where
    M: IntoMessages,
    M::Message: for<'de> serde::Deserialize<'de>,
{
    write!(w, "\n// {name}:\n").unwrap();
    for m in m.into_messages() {
        let v = serde_json::to_vec_pretty(&m).unwrap();

        let _: M::Message =
            serde_json::from_slice(&v).expect("we can round-trip to parse message encodings");

        w.write(&v).unwrap();
        write!(w, "\n").unwrap();
    }
}

fn parse_responses<M>(input: Vec<Value>) -> Vec<M>
where
    M: FromMessage,
    M::Message: serde::Serialize,
{
    let mut out = Vec::new();
    for value in input {
        let msg = serde_json::from_value(value).unwrap();
        let _ = serde_json::to_vec(&msg).unwrap(); // Ensure we can serialize without error.
        M::from_message(msg, &mut out).unwrap();
    }
    out
}
