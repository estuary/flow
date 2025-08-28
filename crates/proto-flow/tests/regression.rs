use prost::Message;
use proto_flow::flow::SerPolicy;
use proto_flow::{capture, derive, flow, flow::inference, materialize, ops};
use proto_gazette::{broker, consumer};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Duration;

fn ex_projections() -> Vec<flow::Projection> {
    vec![flow::Projection {
        explicit: false,
        is_partition_key: true,
        is_primary_key: false,
        field: "a-field".to_string(),
        ptr: "/json/ptr".to_string(),
        inference: Some(flow::Inference {
            default_json: json!({"def": "ault"}).to_string().into(),
            description: "desc".to_string(),
            title: "title".to_string(),
            exists: inference::Exists::Must as i32,
            secret: false,
            string: Some(inference::String {
                content_encoding: "enc".to_string(),
                content_type: "typ".to_string(),
                format: "date".to_string(),
                max_length: 12345,
            }),
            types: vec!["integer".to_string(), "string".to_string()],
            numeric: Some(inference::Numeric {
                has_minimum: true,
                minimum: -1000.0,
                has_maximum: false,
                maximum: 0.0,
            }),
            array: Some(inference::Array {
                min_items: 10,
                has_max_items: true,
                max_items: 20,
                item_types: vec!["null".to_string(), "integer".to_string()],
            }),
            enum_json_vec: vec![
                json!("const-one").to_string().into(),
                json!("const-two").to_string().into(),
            ],
            redact: flow::inference::Redact::Sha256 as i32,
            reduce: flow::inference::Reduce::Merge as i32,
        }),
    }]
}

fn ex_oauth2() -> flow::OAuth2 {
    flow::OAuth2 {
        provider: "oauth-provider".to_string(),
        auth_url_template: "https://auth-url".to_string(),
        access_token_url_template: "https://access-token".to_string(),
        access_token_method: "POST".to_string(),
        access_token_body: "foo".to_string(),
        access_token_headers_json_map: [
            (
                "hdr-one".to_string(),
                json!({"hello": "hdr"}).to_string().into(),
            ),
            ("hdr-two".to_string(), json!(42.5).to_string().into()),
        ]
        .into(),
        access_token_response_json_map: [
            ("key".to_string(), json!("value").to_string().into()),
            ("foo".to_string(), json!(true).to_string().into()),
        ]
        .into(),
        refresh_token_url_template: "https://refresh-token".to_string(),
        refresh_token_method: "POST".to_string(),
        refresh_token_body: "refresh!".to_string(),
        refresh_token_headers_json_map: [(
            "hdr-three".to_string(),
            json!({"refresh": "hdr"}).to_string().into(),
        )]
        .into(),
        refresh_token_response_json_map: [("access".to_string(), json!("here").to_string().into())]
            .into(),
    }
}

fn ex_label_set() -> broker::LabelSet {
    broker::LabelSet {
        labels: vec![
            broker::Label {
                name: "estuary.dev/foo".to_string(),
                value: "label-value".to_string(),
                prefix: false,
            },
            broker::Label {
                name: "estuary.dev/bar".to_string(),
                value: "other-value".to_string(),
                prefix: false,
            },
        ],
    }
}
fn ex_label_selector() -> broker::LabelSelector {
    broker::LabelSelector {
        include: Some(ex_label_set()),
        exclude: Some(broker::LabelSet {
            labels: vec![broker::Label {
                name: "my-label".to_string(),
                value: "prefix/".to_string(),
                prefix: true,
            }],
        }),
    }
}

fn ex_network_ports() -> Vec<flow::NetworkPort> {
    [
        flow::NetworkPort {
            number: 8080,
            protocol: "https".to_string(),
            public: true,
        },
        flow::NetworkPort {
            number: 9000,
            protocol: String::new(),
            public: false,
        },
    ]
    .into()
}

fn ex_partition_template() -> broker::JournalSpec {
    broker::JournalSpec {
        name: "partition/template".to_string(),
        labels: Some(ex_label_set()),
        flags: broker::journal_spec::Flag::ORdwr as u32,
        max_append_rate: 4020303,
        replication: 3,
        fragment: Some(broker::journal_spec::Fragment {
            compression_codec: broker::CompressionCodec::Zstandard as i32,
            flush_interval: Some(Duration::from_secs_f32(62.25).into()),
            length: 112233,
            path_postfix_template: "Path{{Postfix.Template}}".to_string(),
            refresh_interval: Some(Duration::from_secs_f32(300.0).into()),
            stores: vec!["s3://bucket/prefix".to_string()],
            retention: None,
        }),
        suspend: None,
    }
}
fn ex_recovery_template() -> broker::JournalSpec {
    broker::JournalSpec {
        name: "recovery/template".to_string(),
        labels: None,
        flags: 0,
        max_append_rate: 0,
        replication: 3,
        fragment: Some(broker::journal_spec::Fragment {
            compression_codec: broker::CompressionCodec::Snappy as i32,
            length: 1 << 20,
            stores: vec!["s3://bucket/recovery".to_string()],
            retention: None,
            flush_interval: None,
            path_postfix_template: String::new(),
            refresh_interval: Some(Duration::from_secs_f32(300.0).into()),
        }),
        suspend: None,
    }
}
fn ex_shard_template() -> consumer::ShardSpec {
    consumer::ShardSpec {
        id: "shard/template".to_string(),
        disable: false,
        disable_wait_for_ack: false,
        hint_backups: 3,
        hint_prefix: "hint/prefix".to_string(),
        min_txn_duration: None,
        max_txn_duration: Some(Duration::from_secs_f32(60.0).into()),
        hot_standbys: 1,
        read_channel_size: 112233,
        ring_buffer_size: 44556,
        recovery_log_prefix: "recovery/prefix".to_string(),
        sources: Vec::new(),
        labels: Some(ex_label_set()),
    }
}

fn ex_collection_spec() -> flow::CollectionSpec {
    flow::CollectionSpec {
        name: "acmeCo/collection".to_string(),
        write_schema_json: json!({"write": "schema"}).to_string().into(),
        read_schema_json: json!({"read":"schema"}).to_string().into(),
        key: vec!["/key/one".to_string(), "/key/two".to_string()],
        uuid_ptr: "/_meta/uuid".to_string(),
        projections: ex_projections(),
        partition_fields: vec!["type".to_string(), "region".to_string()],
        ack_template_json: json!({"ack":"true"}).to_string().into(),
        partition_template: Some(ex_partition_template()),
        derivation: None,
    }
}

fn ex_capture_spec() -> flow::CaptureSpec {
    flow::CaptureSpec {
        name: "acmeCo/capture".to_string(),
        config_json: json!({"capture": {"config": 42}}).to_string().into(),
        connector_type: flow::capture_spec::ConnectorType::Image as i32,
        interval_seconds: 300,
        recovery_log_template: Some(ex_recovery_template()),
        shard_template: Some(ex_shard_template()),
        bindings: vec![flow::capture_spec::Binding {
            resource_config_json: json!({"resource": "config"}).to_string().into(),
            resource_path: vec!["some".to_string(), "path".to_string()],
            collection: Some(ex_collection_spec()),
            backfill: 3,
            state_key: "a%2Fcdc%2Ftable+baz.v3".to_string(),
        }],
        network_ports: ex_network_ports(),
        inactive_bindings: Vec::new(),
        redact_salt: b"test-capture-salt".to_vec().into(),
    }
}

fn ex_derivation_spec() -> flow::CollectionSpec {
    let mut spec = ex_collection_spec();

    spec.derivation = Some(flow::collection_spec::Derivation {
        config_json: json!({"derivation": {"config": 42}}).to_string().into(),
        connector_type: flow::collection_spec::derivation::ConnectorType::Sqlite as i32,
        recovery_log_template: Some(ex_recovery_template()),
        shard_template: Some(ex_shard_template()),
        transforms: vec![flow::collection_spec::derivation::Transform {
            name: "transform_name".to_string(),
            collection: Some(ex_collection_spec()),
            lambda_config_json: json!({"lambda": "config"}).to_string().into(),
            partition_selector: Some(ex_label_selector()),
            priority: 1,
            read_delay_seconds: 14,
            read_only: true,
            shuffle_key: vec!["/shuffle".to_string(), "/key".to_string()],
            shuffle_lambda_config_json: json!("SELECT $shuffle, $key;").to_string().into(),
            journal_read_suffix: "derive/a/collection/transform_name.v2".to_string(),
            not_before: Some(pbjson_types::Timestamp {
                seconds: 1691722827,
                nanos: 0,
            }),
            not_after: Some(pbjson_types::Timestamp {
                seconds: 1680000000,
                nanos: 0,
            }),
            backfill: 2,
        }],
        shuffle_key_types: vec![
            flow::collection_spec::derivation::ShuffleType::String as i32,
            flow::collection_spec::derivation::ShuffleType::Integer as i32,
        ],
        network_ports: ex_network_ports(),
        inactive_transforms: Vec::new(),
        redact_salt: b"test-derivation-salt".to_vec().into(),
    });

    spec
}

fn ex_field_config() -> BTreeMap<String, bytes::Bytes> {
    [
        (
            "a_field".to_string(),
            json!({"field": "config"}).to_string().into(),
        ),
        ("other/field".to_string(), json!(42.5).to_string().into()),
    ]
    .into()
}

fn ex_materialization_spec() -> flow::MaterializationSpec {
    flow::MaterializationSpec {
        name: "acmeCo/materialization".to_string(),
        config_json: json!({"materialize": {"config": 42}}).to_string().into(),
        connector_type: flow::materialization_spec::ConnectorType::Image as i32,
        recovery_log_template: Some(ex_recovery_template()),
        shard_template: Some(ex_shard_template()),
        bindings: vec![flow::materialization_spec::Binding {
            resource_config_json: json!({"resource": "config"}).to_string().into(),
            resource_path: vec!["some".to_string(), "path".to_string()],
            collection: Some(ex_collection_spec()),
            partition_selector: Some(ex_label_selector()),
            priority: 3,
            field_selection: Some(flow::FieldSelection {
                document: "flow_document".to_string(),
                field_config_json_map: ex_field_config(),
                keys: vec!["key/one".to_string()],
                values: vec!["val/two".to_string()],
            }),
            delta_updates: false,
            deprecated_shuffle: None,
            journal_read_suffix: "materialize/acmeCo/materialization/some%20path.v1".to_string(),
            not_before: Some(pbjson_types::Timestamp {
                seconds: 1691722827,
                nanos: 0,
            }),
            not_after: Some(pbjson_types::Timestamp {
                seconds: 1680000000,
                nanos: 0,
            }),
            backfill: 1,
            state_key: "some%20path.v1".to_string(),
            ser_policy: Some(SerPolicy {
                str_truncate_after: 1 << 16,
                nested_obj_truncate_after: 1000,
                array_truncate_after: 1000,
            }),
        }],
        network_ports: ex_network_ports(),
        inactive_bindings: Vec::new(),
    }
}

fn ex_test_spec() -> flow::TestSpec {
    flow::TestSpec {
        name: "acmeCo/test".to_string(),
        steps: vec![
            flow::test_spec::Step {
                collection: "ingest/collection".to_string(),
                description: "ingest step".to_string(),
                step_scope: "scope://ingest".to_string(),
                partitions: None,
                step_index: 0,
                step_type: flow::test_spec::step::Type::Ingest as i32,
                docs_json_vec: vec![
                    json!({"doc": "one"}).to_string().into(),
                    json!({"doc": 2}).to_string().into(),
                ],
            },
            flow::test_spec::Step {
                collection: "verify/collection".to_string(),
                description: "verify step".to_string(),
                step_scope: "scope://verify".to_string(),
                partitions: Some(ex_label_selector()),
                step_index: 1,
                step_type: flow::test_spec::step::Type::Verify as i32,
                docs_json_vec: vec![
                    json!({"verify": "one"}).to_string().into(),
                    json!({"verify": 2}).to_string().into(),
                ],
            },
        ],
    }
}

fn ex_connector_state() -> flow::ConnectorState {
    flow::ConnectorState {
        updated_json: json!({"state":"update"}).to_string().into(),
        merge_patch: true,
    }
}

fn ex_range() -> flow::RangeSpec {
    flow::RangeSpec {
        key_begin: 0x00112233,
        key_end: 0x44556677,
        r_clock_begin: 0x8899aabb,
        r_clock_end: 0xccddeeff,
    }
}

fn ex_internal() -> bytes::Bytes {
    flow::Projection {
        field: "Hi".to_string(),
        explicit: true,
        ..Default::default()
    }
    .encode_to_vec()
    .into()
}

fn ex_consumer_checkpoint() -> consumer::Checkpoint {
    consumer::Checkpoint {
        sources: [(
            "a/read/journal;suffix".to_string(),
            consumer::checkpoint::Source {
                read_through: 12345,
                producers: vec![
                    consumer::checkpoint::source::ProducerEntry {
                        id: vec![3, 9, 8, 5, 7].into(),
                        state: Some(consumer::checkpoint::ProducerState {
                            begin: 1111,
                            last_ack: 8675,
                        }),
                    },
                    consumer::checkpoint::source::ProducerEntry {
                        id: vec![7, 12, 102, 43, 29].into(),
                        state: Some(consumer::checkpoint::ProducerState {
                            begin: 2222,
                            last_ack: 309,
                        }),
                    },
                ],
            },
        )]
        .into(),
        ack_intents: [("an/ack/journal".to_string(), vec![3, 4, 2, 5].into())].into(),
    }
}

fn ex_capture_request() -> capture::Request {
    capture::Request {
        spec: Some(capture::request::Spec {
            connector_type: flow::capture_spec::ConnectorType::Image as i32,
            config_json: json!({"spec":"config"}).to_string().into(),
        }),
        discover: Some(capture::request::Discover {
            name: "discover/capture".to_string(),
            connector_type: flow::capture_spec::ConnectorType::Image as i32,
            config_json: json!({"discover":"config"}).to_string().into(),
        }),
        validate: Some(capture::request::Validate {
            name: "validate/capture".to_string(),
            connector_type: flow::capture_spec::ConnectorType::Image as i32,
            config_json: json!({"validate":"config"}).to_string().into(),
            bindings: vec![capture::request::validate::Binding {
                collection: Some(ex_collection_spec()),
                resource_config_json: json!({"resource":"config"}).to_string().into(),
                backfill: 1,
            }],
            last_capture: None,
            last_version: "11:22:33:44".to_string(),
        }),
        apply: Some(capture::request::Apply {
            capture: Some(ex_capture_spec()),
            version: "11:22:33:44".to_string(),
            last_capture: None,
            last_version: "00:11:22:33".to_string(),
        }),
        open: Some(capture::request::Open {
            capture: Some(ex_capture_spec()),
            version: "11:22:33:44".to_string(),
            range: Some(ex_range()),
            state_json: json!({"connector": {"state": 42}}).to_string().into(),
        }),
        acknowledge: Some(capture::request::Acknowledge { checkpoints: 32 }),
        internal: ex_internal(),
    }
}

fn ex_capture_response() -> capture::Response {
    capture::Response {
        spec: Some(capture::response::Spec {
            protocol: 3032023,
            config_schema_json: json!({"config": "schema"}).to_string().into(),
            resource_config_schema_json: json!({"resource": "schema"}).to_string().into(),
            documentation_url: "https://example/docs".to_string(),
            oauth2: Some(ex_oauth2()),
            resource_path_pointers: vec!["/stream".to_string()],
        }),
        discovered: Some(capture::response::Discovered {
            bindings: vec![capture::response::discovered::Binding {
                document_schema_json: json!({"doc":"schema"}).to_string().into(),
                recommended_name: "recommended name".to_string(),
                disable: true,
                resource_config_json: json!({"resource": 1234}).to_string().into(),
                key: vec!["/key/ptr".to_string()],
                resource_path: vec!["1234".to_string()],
                is_fallback_key: false,
            }],
        }),
        validated: Some(capture::response::Validated {
            bindings: vec![capture::response::validated::Binding {
                resource_path: vec!["some".to_string(), "path".to_string()],
            }],
        }),
        applied: Some(capture::response::Applied {
            action_description: "I did some stuff".to_string(),
        }),
        opened: Some(capture::response::Opened {
            explicit_acknowledgements: true,
        }),
        captured: Some(capture::response::Captured {
            binding: 2,
            doc_json: json!({"captured":"doc"}).to_string().into(),
        }),
        sourced_schema: Some(capture::response::SourcedSchema {
            binding: 3,
            schema_json: json!({"type": "string", "format": "date-time"})
                .to_string()
                .into(),
        }),
        checkpoint: Some(capture::response::Checkpoint {
            state: Some(ex_connector_state()),
        }),
        internal: ex_internal(),
    }
}

fn ex_derive_request() -> derive::Request {
    derive::Request {
        spec: Some(derive::request::Spec {
            connector_type: flow::collection_spec::derivation::ConnectorType::Sqlite as i32,
            config_json: json!({"spec":"config"}).to_string().into(),
        }),
        validate: Some(derive::request::Validate {
            connector_type: flow::collection_spec::derivation::ConnectorType::Sqlite as i32,
            config_json: json!({"validate":"config"}).to_string().into(),
            collection: Some(ex_collection_spec()),
            transforms: vec![derive::request::validate::Transform {
                name: "stable_name".to_string(),
                collection: Some(ex_collection_spec()),
                lambda_config_json: json!({"lambda": "config"}).to_string().into(),
                shuffle_lambda_config_json: json!({"shuffle": "config"}).to_string().into(),
                backfill: 2,
            }],
            shuffle_key_types: vec![
                flow::collection_spec::derivation::ShuffleType::Boolean as i32,
                flow::collection_spec::derivation::ShuffleType::Integer as i32,
            ],
            project_root: "file:///project/root".to_string(),
            import_map: [(
                "/using/typescript/module".to_string(),
                "file:///path/to/import".to_string(),
            )]
            .into(),
            last_collection: None,
            last_version: "00:11:22:33".to_string(),
        }),
        open: Some(derive::request::Open {
            collection: Some(ex_collection_spec()),
            version: "11:22:33:44".to_string(),
            range: Some(ex_range()),
            state_json: json!({"connector": {"state": 42}}).to_string().into(),
        }),
        read: Some(derive::request::Read {
            transform: 2,
            uuid: Some(flow::UuidParts {
                node: 1234,
                clock: 5678,
            }),
            shuffle: Some(derive::request::read::Shuffle {
                key_json: json!([true, 32]).to_string().into(),
                packed: vec![86, 75, 30, 9].into(),
                hash: 44556677,
            }),
            doc_json: json!({"read": "doc"}).to_string().into(),
        }),
        flush: Some(derive::request::Flush {}),
        start_commit: Some(derive::request::StartCommit {
            runtime_checkpoint: Some(ex_consumer_checkpoint()),
        }),
        reset: Some(derive::request::Reset {}),
        internal: ex_internal(),
    }
}

fn ex_derive_response() -> derive::Response {
    derive::Response {
        spec: Some(derive::response::Spec {
            protocol: 3032023,
            config_schema_json: json!({"config": "schema"}).to_string().into(),
            resource_config_schema_json: json!({"lambda": "schema"}).to_string().into(),
            documentation_url: "https://example/docs".to_string(),
            oauth2: Some(ex_oauth2()),
        }),
        validated: Some(derive::response::Validated {
            transforms: vec![
                derive::response::validated::Transform { read_only: true },
                derive::response::validated::Transform { read_only: false },
            ],
            generated_files: [(
                "file:///project/root/deno.json".to_string(),
                "content".to_string(),
            )]
            .into(),
        }),
        opened: Some(derive::response::Opened {}),
        published: Some(derive::response::Published {
            doc_json: json!({"published": "doc"}).to_string().into(),
        }),
        flushed: Some(derive::response::Flushed {}),
        started_commit: Some(derive::response::StartedCommit {
            state: Some(ex_connector_state()),
        }),
        internal: ex_internal(),
    }
}

fn ex_materialize_request() -> materialize::Request {
    materialize::Request {
        spec: Some(materialize::request::Spec {
            connector_type: flow::materialization_spec::ConnectorType::Image as i32,
            config_json: json!({"spec":"config"}).to_string().into(),
        }),
        validate: Some(materialize::request::Validate {
            name: "validate/materialization".to_string(),
            connector_type: flow::materialization_spec::ConnectorType::Image as i32,
            config_json: json!({"validate":"config"}).to_string().into(),
            bindings: vec![materialize::request::validate::Binding {
                collection: Some(ex_collection_spec()),
                resource_config_json: json!({"resource":"config"}).to_string().into(),
                field_config_json_map: ex_field_config(),
                backfill: 3,
                group_by: vec!["key/one".to_string()],
            }],
            last_materialization: None,
            last_version: "00:11:22:33".to_string(),
        }),
        apply: Some(materialize::request::Apply {
            materialization: Some(ex_materialization_spec()),
            version: "11:22:33:44".to_string(),
            last_materialization: None,
            last_version: "00:11:22:33".to_string(),
            state_json: json!({"connector":"state"}).to_string().into(),
        }),
        open: Some(materialize::request::Open {
            materialization: Some(ex_materialization_spec()),
            version: "11:22:33:44".to_string(),
            range: Some(ex_range()),
            state_json: json!({"connector": {"state": 42}}).to_string().into(),
        }),
        acknowledge: Some(materialize::request::Acknowledge {}),
        load: Some(materialize::request::Load {
            binding: 12,
            key_packed: vec![86, 75, 30, 9].into(),
            key_json: json!([42, "hi"]).to_string().into(),
        }),
        flush: Some(materialize::request::Flush {}),
        store: Some(materialize::request::Store {
            binding: 3,
            key_packed: vec![90, 21, 0].into(),
            key_json: json!([true, null]).to_string().into(),
            values_packed: vec![60, 91].into(),
            values_json: json!([3.14159, "field!"]).to_string().into(),
            doc_json: json!({"full": "document"}).to_string().into(),
            exists: true,
            delete: true,
        }),
        start_commit: Some(materialize::request::StartCommit {
            runtime_checkpoint: Some(ex_consumer_checkpoint()),
        }),
        internal: ex_internal(),
    }
}

fn ex_materialize_response() -> materialize::Response {
    materialize::Response {
        spec: Some(materialize::response::Spec {
            protocol: 3032023,
            config_schema_json: json!({"config": "schema"}).to_string().into(),
            resource_config_schema_json: json!({"resource": "schema"}).to_string().into(),
            documentation_url: "https://example/docs".to_string(),
            oauth2: Some(ex_oauth2()),
        }),
        validated: Some(materialize::response::Validated {
            bindings: vec![materialize::response::validated::Binding {
                resource_path: vec!["some".to_string(), "path".to_string()],
                case_insensitive_fields: true,
                constraints: [
                    (
                        "req_field".to_string(),
                        materialize::response::validated::Constraint {
                            r#type:
                                materialize::response::validated::constraint::Type::FieldRequired
                                    as i32,
                            reason: "is required".to_string(),
                            folded_field: "REQ_FIELD".to_string(),
                        },
                    ),
                    (
                        "opt_field".to_string(),
                        materialize::response::validated::Constraint {
                            r#type:
                                materialize::response::validated::constraint::Type::FieldOptional
                                    as i32,
                            reason: "is optional".to_string(),
                            folded_field: String::new(),
                        },
                    ),
                ]
                .into(),
                delta_updates: true,
                ser_policy: Some(SerPolicy {
                    str_truncate_after: 1 << 16,
                    nested_obj_truncate_after: 1000,
                    array_truncate_after: 1000,
                }),
            }],
        }),
        applied: Some(materialize::response::Applied {
            action_description: "I did some stuff".to_string(),
            state: Some(ex_connector_state()),
        }),
        opened: Some(materialize::response::Opened {
            runtime_checkpoint: Some(ex_consumer_checkpoint()),
            disable_load_optimization: true,
        }),
        acknowledged: Some(materialize::response::Acknowledged {
            state: Some(ex_connector_state()),
        }),
        loaded: Some(materialize::response::Loaded {
            binding: 4,
            doc_json: json!({"loaded": "doc"}).to_string().into(),
        }),
        flushed: Some(materialize::response::Flushed {
            state: Some(ex_connector_state()),
        }),
        started_commit: Some(materialize::response::StartedCommit {
            state: Some(ex_connector_state()),
        }),
        internal: ex_internal(),
    }
}

fn ex_shard_labeling() -> ops::ShardLabeling {
    ops::ShardLabeling {
        build: "a-build-id".to_string(),
        hostname: "a-hostname".to_string(),
        log_level: ops::log::Level::Info as i32,
        range: Some(ex_range()),
        split_source: "split/source/shard".to_string(),
        split_target: String::new(),
        task_name: "the/task/name".to_string(),
        task_type: ops::TaskType::Derivation as i32,
        logs_journal: "ops/logs/one=capture/two=the%2Ftask%2Fname".to_string(),
        stats_journal: "ops/stats/one=capture/two=the%2Ftask%2Fname".to_string(),
    }
}

fn ex_log() -> ops::Log {
    ops::Log {
        meta: Some(ops::Meta {
            uuid: "c13a3412-903a-40f2-8bca-0a2f4d9260be".to_string(),
        }),
        shard: Some(ops::ShardRef {
            name: "my/cool/task".to_string(),
            kind: ops::TaskType::Derivation as i32,
            key_begin: "00112233".to_string(),
            r_clock_begin: "aabbccdd".to_string(),
            build: "0011223344556677".to_string(),
        }),
        timestamp: Some(proto_flow::as_timestamp(std::time::SystemTime::UNIX_EPOCH)),
        level: ops::log::Level::Info as i32,
        message: "my log message".to_string(),
        fields_json_map: [
            (
                "structured".to_string(),
                json!({"log": "fields"}).to_string().into(),
            ),
            ("a".to_string(), json!(42).to_string().into()),
        ]
        .into(),
        spans: vec![ops::Log {
            message: "some parent span".to_string(),
            fields_json_map: [(
                "more".to_string(),
                json!(["structured", "stuff", true]).to_string().into(),
            )]
            .into(),
            ..Default::default()
        }],
    }
}

fn ex_stats() -> ops::Stats {
    ops::Stats {
        meta: Some(ops::Meta {
            uuid: "c13a3412-903a-40f2-8bca-0a2f4d9260be".to_string(),
        }),
        shard: Some(ops::ShardRef {
            name: "my/cool/task".to_string(),
            kind: ops::TaskType::Derivation as i32,
            key_begin: "00112233".to_string(),
            r_clock_begin: "aabbccdd".to_string(),
            build: "0011223344556677".to_string(),
        }),
        timestamp: Some(proto_flow::as_timestamp(std::time::SystemTime::UNIX_EPOCH)),
        open_seconds_total: 3.14159,
        txn_count: 15,
        capture: [(
            "captured/collection".to_string(),
            ops::stats::Binding {
                last_source_published_at: Some(proto_flow::Timestamp {
                    seconds: 6,
                    nanos: 7,
                }),
                left: None,
                right: Some(ops::stats::DocsAndBytes {
                    docs_total: 2,
                    bytes_total: 200,
                }),
                out: Some(ops::stats::DocsAndBytes {
                    docs_total: 1,
                    bytes_total: 100,
                }),
            },
        )]
        .into(),
        derive: Some(ops::stats::Derive {
            transforms: [
                (
                    "my-transform".to_string(),
                    ops::stats::derive::Transform {
                        last_source_published_at: Some(proto_flow::Timestamp {
                            seconds: 6,
                            nanos: 7,
                        }),
                        source: "the/source/collection".to_string(),
                        input: Some(ops::stats::DocsAndBytes {
                            docs_total: 12,
                            bytes_total: 369,
                        }),
                    },
                ),
                (
                    "otherTransform".to_string(),
                    ops::stats::derive::Transform {
                        last_source_published_at: Some(proto_flow::Timestamp {
                            seconds: 6,
                            nanos: 7,
                        }),
                        source: "other/collection".to_string(),
                        input: Some(ops::stats::DocsAndBytes {
                            docs_total: 52,
                            bytes_total: 2389,
                        }),
                    },
                ),
            ]
            .into(),
            published: Some(ops::stats::DocsAndBytes {
                docs_total: 69,
                bytes_total: 1269,
            }),
            out: Some(ops::stats::DocsAndBytes {
                docs_total: 3,
                bytes_total: 102,
            }),
        }),
        materialize: [(
            "materialized/collection".to_string(),
            ops::stats::Binding {
                last_source_published_at: Some(proto_flow::Timestamp {
                    seconds: 6,
                    nanos: 7,
                }),
                left: Some(ops::stats::DocsAndBytes {
                    docs_total: 1,
                    bytes_total: 100,
                }),
                right: Some(ops::stats::DocsAndBytes {
                    docs_total: 2,
                    bytes_total: 200,
                }),
                out: Some(ops::stats::DocsAndBytes {
                    docs_total: 3,
                    bytes_total: 300,
                }),
            },
        )]
        .into(),
        interval: Some(ops::stats::Interval {
            uptime_seconds: 300,
            usage_rate: 1.5,
        }),
    }
}

fn json_test<
    M: serde::Serialize + for<'de> serde::Deserialize<'de> + PartialEq + std::fmt::Debug,
>(
    msg: M,
) -> String {
    let encoded = serde_json::to_string_pretty(&msg).unwrap();

    // Deserialize from borrowed.
    let recovered: M = serde_json::from_str(&encoded).unwrap();
    assert_eq!(msg, recovered);

    // Deserialize from owned.
    let mut reader = encoded.as_bytes();
    let recovered: M = serde_json::from_reader(&mut reader).unwrap();
    assert_eq!(msg, recovered);

    encoded
}

fn proto_test<M: prost::Message + PartialEq + std::fmt::Debug + Default>(msg: M) -> String {
    let encoded = msg.encode_to_vec();
    let recovered = M::decode(encoded.as_slice()).unwrap();
    assert_eq!(msg, recovered);

    hexdump::hexdump_iter(&encoded)
        .map(|line| format!("{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn test_collection_spec_json() {
    let msg = ex_collection_spec();
    insta::assert_snapshot!(json_test(msg));
}

#[test]
fn test_collection_spec_proto() {
    let msg = ex_collection_spec();
    insta::assert_snapshot!(proto_test(msg));
}

#[test]
fn test_capture_spec_json() {
    let msg = ex_capture_spec();
    insta::assert_snapshot!(json_test(msg));
}

#[test]
fn test_capture_spec_proto() {
    let msg = ex_capture_spec();
    insta::assert_snapshot!(proto_test(msg));
}

#[test]
fn test_derivation_spec_json() {
    let msg = ex_derivation_spec();
    insta::assert_snapshot!(json_test(msg));
}

#[test]
fn test_derivation_spec_proto() {
    let msg = ex_derivation_spec();
    insta::assert_snapshot!(proto_test(msg));
}

#[test]
fn test_materialization_spec_json() {
    let msg = ex_materialization_spec();
    insta::assert_snapshot!(json_test(msg));
}

#[test]
fn test_materialization_spec_proto() {
    let msg = ex_materialization_spec();
    insta::assert_snapshot!(proto_test(msg));
}

#[test]
fn test_test_spec_json() {
    let msg = ex_test_spec();
    insta::assert_snapshot!(json_test(msg));
}

#[test]
fn test_test_spec_proto() {
    let msg = ex_test_spec();
    insta::assert_snapshot!(proto_test(msg));
}

#[test]
fn test_oauth2_json() {
    let msg = ex_oauth2();
    insta::assert_snapshot!(json_test(msg));
}

#[test]
fn test_oauth2_proto() {
    let msg = ex_oauth2();
    insta::assert_snapshot!(proto_test(msg));
}

#[test]
fn test_shard_labeling_json() {
    let msg = ex_shard_labeling();
    insta::assert_snapshot!(json_test(msg));
}

#[test]
fn test_shard_labeling_proto() {
    let msg = ex_shard_labeling();
    insta::assert_snapshot!(proto_test(msg));
}

#[test]
fn test_log_json() {
    let msg = ex_log();
    insta::assert_snapshot!(json_test(msg));
}

#[test]
fn test_log_proto() {
    let msg = ex_log();
    insta::assert_snapshot!(proto_test(msg));
}

#[test]
fn test_stats_json() {
    let msg = ex_stats();
    insta::assert_snapshot!(json_test(msg));
}

#[test]
fn test_stats_proto() {
    let msg = ex_stats();
    insta::assert_snapshot!(proto_test(msg));
}

#[test]
fn test_capture_request_json() {
    let msg = ex_capture_request();
    insta::assert_snapshot!(json_test(msg));
}

#[test]
fn test_capture_request_proto() {
    let msg = ex_capture_request();
    insta::assert_snapshot!(proto_test(msg));
}

#[test]
fn test_capture_response_json() {
    let msg = ex_capture_response();
    insta::assert_snapshot!(json_test(msg));
}

#[test]
fn test_capture_response_proto() {
    let msg = ex_capture_response();
    insta::assert_snapshot!(proto_test(msg));
}

#[test]
fn test_derive_request_json() {
    let msg = ex_derive_request();
    insta::assert_snapshot!(json_test(msg));
}

#[test]
fn test_derive_request_proto() {
    let msg = ex_derive_request();
    insta::assert_snapshot!(proto_test(msg));
}

#[test]
fn test_derive_response_json() {
    let msg = ex_derive_response();
    insta::assert_snapshot!(json_test(msg));
}

#[test]
fn test_derive_response_proto() {
    let msg = ex_derive_response();
    insta::assert_snapshot!(proto_test(msg));
}

#[test]
fn test_materialize_request_json() {
    let msg = ex_materialize_request();
    insta::assert_snapshot!(json_test(msg));
}

#[test]
fn test_materialize_request_proto() {
    let msg = ex_materialize_request();
    insta::assert_snapshot!(proto_test(msg));
}

#[test]
fn test_materialize_response_json() {
    let msg = ex_materialize_response();
    insta::assert_snapshot!(json_test(msg));
}

#[test]
fn test_materialize_response_proto() {
    let msg = ex_materialize_response();
    insta::assert_snapshot!(proto_test(msg));
}
