use doc::inference::{Exists, Shape};
use json::schema::types;
use proto_flow::flow;
use proto_gazette::{broker, consumer};
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Duration;

mod bundle;
pub use bundle::bundled_schema;

mod npm;
pub use npm::{generate_npm_package, write_npm_package};

mod ops;
pub use ops::generate_ops_collections;

pub fn inference(shape: &Shape, exists: Exists) -> flow::Inference {
    let default_json = shape
        .default
        .as_ref()
        .map(|v| v.to_string())
        .unwrap_or_default();

    let is_base64 = shape
        .string
        .content_encoding
        .as_ref()
        .map(|v| v.to_ascii_lowercase() == "base64")
        .unwrap_or_default();

    let exists = match exists {
        Exists::Must => flow::inference::Exists::Must,
        Exists::May => flow::inference::Exists::May,
        Exists::Implicit => flow::inference::Exists::Implicit,
        Exists::Cannot => flow::inference::Exists::Cannot,
    };

    flow::Inference {
        types: shape.type_.to_vec(),
        exists: exists as i32,
        title: shape.title.clone().unwrap_or_default(),
        description: shape.description.clone().unwrap_or_default(),
        default_json,
        secret: shape.secret.unwrap_or_default(),
        string: if shape.type_.overlaps(types::STRING) {
            Some(flow::inference::String {
                content_type: shape.string.content_type.clone().unwrap_or_default(),
                format: shape
                    .string
                    .format
                    .map(|f| f.to_string())
                    .unwrap_or_default(),
                content_encoding: shape.string.content_encoding.clone().unwrap_or_default(),
                is_base64,
                max_length: shape.string.max_length.unwrap_or_default() as u32,
            })
        } else {
            None
        },
    }
}

// partition_template returns a template JournalSpec for creating
// or updating data partitions of the collection.
pub fn partition_template(
    build_config: &flow::build_api::Config,
    collection: &models::Collection,
    journals: &models::JournalTemplate,
    stores: &[models::Store],
) -> broker::JournalSpec {
    let models::JournalTemplate {
        fragments:
            models::FragmentTemplate {
                compression_codec: codec,
                flush_interval,
                length,
                retention,
            },
    } = journals.clone();

    // Until there's a good reason otherwise, we hard-code that partition journals are replicated 3x.
    let replication = 3;

    // Use a supplied compression codec. Or, if none, then default to gzip.
    let compression_codec = compression_codec(codec.unwrap_or(models::CompressionCodec::Gzip));

    // If an explicit flush interval isn't provided, then don't set one.
    let flush_interval = flush_interval.map(Into::into);

    // If a fragment length isn't set, default to 512MB.
    let length = length.unwrap_or(1 << 29) as i64;

    // Until there's a good reason otherwise, we hard-code that fragments include the UTC date
    // and hour they were created as components of their path. This makes it easy to filter
    // collections on time when making ad-hoc queries using the Hive partitioning scheme.
    let path_postfix_template = r#"utc_date={{.Spool.FirstAppendTime.Format "2006-01-02"}}/utc_hour={{.Spool.FirstAppendTime.Format "15"}}"#.to_string();

    // Until there's a good reason otherwise, we hard-code that fragments are refreshed every five minutes.
    let refresh_interval = Some(Duration::from_secs(5 * 60).into());

    // If an explicit retention interval isn't provided, then don't set one.
    let retention = retention.map(Into::into);

    // Partition journals are readable and writable.
    // We could get fancier here by disabling writes to a journal which has no captures.
    let flags = broker::journal_spec::Flag::ORdwr as u32;

    // We hard-code max_append_rate to 4MB/s, which back-pressures captures
    // and derivations that produce lots of documents. They'll perform more
    // aggregation per-transaction, and may stall until there's quota.
    let max_append_rate = 1 << 22; // 4MB.

    // Labels must be in alphabetical order.
    let labels = vec![
        broker::Label {
            name: labels::MANAGED_BY.to_string(),
            value: labels::MANAGED_BY_FLOW.to_string(),
        },
        broker::Label {
            name: labels::CONTENT_TYPE.to_string(),
            value: labels::CONTENT_TYPE_JSON_LINES.to_string(),
        },
        broker::Label {
            name: labels::BUILD.to_string(),
            value: build_config.build_id.clone(),
        },
        broker::Label {
            name: labels::COLLECTION.to_string(),
            value: collection.to_string(),
        },
    ];

    broker::JournalSpec {
        name: collection.to_string(),
        replication,
        fragment: Some(broker::journal_spec::Fragment {
            compression_codec: compression_codec as i32,
            flush_interval,
            length,
            path_postfix_template,
            refresh_interval,
            retention,
            stores: stores.iter().map(|s| s.to_url().into()).collect(),
        }),
        flags,
        labels: Some(broker::LabelSet { labels }),
        max_append_rate,
    }
}

// recovery_log_template returns a template JournalSpec for creating
// or updating recovery logs of task shards.
pub fn recovery_log_template(
    build_config: &flow::build_api::Config,
    task_name: &str,
    task_type: &str,
    stores: &[models::Store],
) -> broker::JournalSpec {
    // Until there's a good reason otherwise, we hard-code that recovery logs are replicated 3x.
    let replication = 3;

    // Use Snappy compression. Note that lower levels of an LSM tree
    // typically apply their own compression, but the rocks WAL is
    // uncompressed. Snappy has good support for passing-through content
    // that's already compressed.
    // TODO(johnny): Switch gazette to https://github.com/klauspost/compress/tree/master/s2
    let compression_codec = compression_codec(models::CompressionCodec::Snappy);

    // Never set a flush interval for recovery logs.
    let flush_interval = None;

    // We hard-code a 256MB fragment size, which matches the typical RocksDB SST size.
    let length = 1 << 28;

    // Recovery logs don't use postfix templates.
    let path_postfix_template = String::new();

    // Until there's a good reason otherwise, we hard-code that fragments
    // are refreshed every five minutes.
    let refresh_interval = Some(Duration::from_secs(5 * 60).into());

    // Never set a retention. Recovery logs are pruned using a separate mechanism.
    let retention = None;

    // Recovery logs are readable and writable.
    let flags = broker::journal_spec::Flag::ORdwr as u32;

    // We hard-code max_append_rate to 4MB/s, which back-pressures derivations
    // that produce lots of register updates. They'll perform more
    // aggregation per-transaction, and may stall until there's quota.
    let max_append_rate = 1 << 22; // 4MB.

    // Labels must be in alphabetical order.
    let labels = vec![
        broker::Label {
            name: labels::MANAGED_BY.to_string(),
            value: labels::MANAGED_BY_FLOW.to_string(),
        },
        broker::Label {
            name: labels::CONTENT_TYPE.to_string(),
            value: labels::CONTENT_TYPE_RECOVERY_LOG.to_string(),
        },
        broker::Label {
            name: labels::BUILD.to_string(),
            value: build_config.build_id.clone(),
        },
        broker::Label {
            name: labels::TASK_NAME.to_string(),
            value: task_name.to_string(),
        },
        broker::Label {
            name: labels::TASK_TYPE.to_string(),
            value: task_type.to_string(),
        },
    ];

    broker::JournalSpec {
        name: format!("recovery/{}", shard_id_base(task_name, task_type)),
        replication,
        fragment: Some(broker::journal_spec::Fragment {
            compression_codec: compression_codec as i32,
            flush_interval,
            length,
            path_postfix_template,
            refresh_interval,
            retention,
            stores: stores.iter().map(|s| s.to_url().into()).collect(),
        }),
        flags,
        labels: Some(broker::LabelSet { labels }),
        max_append_rate,
    }
}

// shard_id_base returns the base Gazette Shard ID for the task name and type.
// At runtime, this base ID is then '/'-joined with a hex-encoded
// "{KeyBegin}-{RClockBegin}" suffix of the specific splits of the task,
// to form complete shard IDs.
// See also ShardSuffix in go/labels/partitions.go
pub fn shard_id_base(task_name: &str, task_type: &str) -> String {
    let task_type = match task_type {
        labels::TASK_TYPE_CAPTURE => "capture",
        labels::TASK_TYPE_DERIVATION => "derivation",
        labels::TASK_TYPE_MATERIALIZATION => "materialize",
        _ => panic!("invalid task type {}", task_type),
    };

    format!("{}/{}", task_type, task_name)
}

// shard_template returns a template ShardSpec for creating or updating
// shards of the task.
pub fn shard_template(
    build_config: &flow::build_api::Config,
    task_name: &str,
    task_type: &str,
    shard: &models::ShardTemplate,
    disable_wait_for_ack: bool,
) -> consumer::ShardSpec {
    let models::ShardTemplate {
        disable,
        hot_standbys,
        max_txn_duration,
        min_txn_duration,
        read_channel_size,
        ring_buffer_size,
        log_level,
    } = shard;

    // We hard-code that recovery logs always have prefix "recovery".
    let recovery_log_prefix = "recovery".to_string();
    // We hard-code that hints are stored under this Etcd prefix.
    let hint_prefix = "/estuary/flow/hints".to_string();
    // We hard-code two hint backups per shard.
    let hint_backups = 2;

    // If not set, the maximum transaction duration is one second and the minimum is zero.
    let max_txn_duration = max_txn_duration
        .or(Some(Duration::from_secs(1)))
        .map(Into::into);
    let min_txn_duration = min_txn_duration.or(Some(Duration::ZERO)).map(Into::into);
    // If not set, no hot standbys are used.
    let hot_standbys = hot_standbys.unwrap_or(0);

    // If not set, the default ring buffer size is 64k.
    let ring_buffer_size = ring_buffer_size.unwrap_or(1 << 16);
    // If not set, the default read channel size is 128k.
    let read_channel_size = read_channel_size.unwrap_or(1 << 17);

    // Labels must be in alphabetical order.
    let labels = vec![
        broker::Label {
            name: labels::MANAGED_BY.to_string(),
            value: labels::MANAGED_BY_FLOW.to_string(),
        },
        broker::Label {
            name: labels::BUILD.to_string(),
            value: build_config.build_id.clone(),
        },
        broker::Label {
            name: labels::LOG_LEVEL.to_string(),
            value: log_level.clone().unwrap_or_else(|| "info".to_string()),
        },
        broker::Label {
            name: labels::TASK_NAME.to_string(),
            value: task_name.to_string(),
        },
        broker::Label {
            name: labels::TASK_TYPE.to_string(),
            value: task_type.to_string(),
        },
    ];

    consumer::ShardSpec {
        id: shard_id_base(task_name, task_type),
        disable: *disable,
        disable_wait_for_ack,
        hint_backups,
        hint_prefix,
        hot_standbys,
        labels: Some(broker::LabelSet { labels }),
        max_txn_duration,
        min_txn_duration,
        read_channel_size,
        recovery_log_prefix,
        ring_buffer_size,
        sources: Vec::new(),
    }
}

pub fn collection_spec(
    build_config: &flow::build_api::Config,
    collection: &tables::Collection,
    projections: Vec<flow::Projection>,
    schema_bundle: &Value,
    stores: &[models::Store],
) -> flow::CollectionSpec {
    let tables::Collection {
        collection: name,
        scope: _,
        schema,
        spec: models::CollectionDef { key, journals, .. },
    } = collection;

    let partition_fields = projections
        .iter()
        .filter_map(|p| {
            if p.is_partition_key {
                Some(p.field.clone())
            } else {
                None
            }
        })
        .collect();

    // For the forseeable future, we don't allow customizing this.
    let uuid_ptr = "/_meta/uuid".to_string();

    flow::CollectionSpec {
        collection: name.to_string(),
        schema_uri: schema.to_string(),
        schema_json: schema_bundle.to_string(),
        key_ptrs: key.iter().map(|p| p.to_string()).collect(),
        projections,
        partition_fields,
        uuid_ptr,
        ack_json_template: serde_json::json!({
                "_meta": {"uuid": "DocUUIDPlaceholder-329Bb50aa48EAa9ef",
                "ack": true,
            } })
        .to_string(),
        partition_template: Some(partition_template(build_config, name, journals, stores)),
    }
}

pub fn journal_selector(
    collection: &models::Collection,
    selector: Option<&models::PartitionSelector>,
) -> broker::LabelSelector {
    let mut include = vec![broker::Label {
        name: labels::COLLECTION.to_string(),
        value: collection.to_string(),
    }];
    let mut exclude = Vec::new();

    if let Some(selector) = selector {
        push_partitions(&selector.include, &mut include);
        push_partitions(&selector.exclude, &mut exclude);
    }

    // LabelSets must be in sorted order.
    include.sort_by(|l, r| (&l.name, &l.value).cmp(&(&r.name, &r.value)));
    exclude.sort_by(|l, r| (&l.name, &l.value).cmp(&(&r.name, &r.value)));

    broker::LabelSelector {
        include: Some(broker::LabelSet { labels: include }),
        exclude: Some(broker::LabelSet { labels: exclude }),
    }
}

/// The set of characters that must be percent-encoded when used as a URL path segment. This set
/// matches the set of characters that must be percent encoded according to [RFC 3986 Section
/// 3.3](https://datatracker.ietf.org/doc/html/rfc3986#section-3.3) This also matches the rules
/// that are used in Go to encode partition fields.
const PATH_SEGMENT_SET: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'$')
    .remove(b'&')
    .remove(b'+')
    .remove(b':')
    .remove(b'=')
    .remove(b'@');

/// Percent-encodes string values so that they can be used in Gazette label values.
pub fn percent_encode_partition_value(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, PATH_SEGMENT_SET).to_string()
}

// Flatten partition selector fields into a Vec<Label>.
// JSON strings are percent-encoded but un-quoted.
// Other JSON types map to their literal JSON strings.
// *** This MUST match the Go-side behavior! ***
fn push_partitions(fields: &BTreeMap<String, Vec<Value>>, out: &mut Vec<broker::Label>) {
    for (field, value) in fields {
        for value in value {
            let value = match value {
                Value::String(s) => percent_encode_partition_value(s),
                _ => serde_json::to_string(value).unwrap(),
            };
            out.push(broker::Label {
                name: format!("estuary.dev/field/{}", field),
                value,
            });
        }
    }
}

fn lambda_spec(
    lambda: &models::Lambda,
    transform: &tables::Transform,
    suffix: &str,
) -> flow::LambdaSpec {
    match lambda {
        models::Lambda::Typescript => flow::LambdaSpec {
            typescript: format!("/{}/{}", transform_group_name(transform), suffix),
            ..Default::default()
        },
        models::Lambda::Remote(addr) => flow::LambdaSpec {
            remote: addr.clone(),
            ..Default::default()
        },
    }
}

// Group name of this transform, used to group shards & shuffled reads
// which collectively process the transformation.
pub fn transform_group_name(table: &tables::Transform) -> String {
    format!(
        "derive/{}/{}",
        table.derivation.as_str(),
        table.transform.as_str()
    )
}

pub fn transform_spec(
    transform: &tables::Transform,
    source: &flow::CollectionSpec,
    validate_schema_bundle: &serde_json::Value,
) -> flow::TransformSpec {
    let tables::Transform {
        scope: _,
        derivation,
        transform: name,
        spec:
            models::TransformDef {
                priority,
                source:
                    models::TransformSource {
                        name: source_collection,
                        partitions: source_partitions,
                        schema: _,
                    },
                publish,
                update,
                read_delay,
                shuffle,
            },
        source_schema,
    } = &transform;

    let (uses_source_key, shuffle_key_ptrs, shuffle_lambda) = match shuffle {
        Some(models::Shuffle::Key(key)) => {
            (
                false,
                key.iter().map(|k| k.to_string()).collect(), // CompositeKey => Vec<String>.
                None,
            )
        }
        Some(models::Shuffle::Lambda(lambda)) => (
            false,
            Vec::new(),
            Some(lambda_spec(&lambda, transform, "Shuffle")),
        ),
        None => (true, source.key_ptrs.clone(), None),
    };

    let shuffle = flow::Shuffle {
        group_name: transform_group_name(transform),
        source_collection: source.collection.clone(),
        source_partitions: Some(journal_selector(
            source_collection,
            source_partitions.as_ref(),
        )),
        source_uuid_ptr: source.uuid_ptr.clone(),
        shuffle_key_ptrs,
        uses_source_key,
        shuffle_lambda,
        source_schema_uri: source_schema
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| source.schema_uri.clone()),
        uses_source_schema: source_schema.is_none(),
        deprecated_validate_schema_at_read: true,
        filter_r_clocks: update.is_none(),
        read_delay_seconds: read_delay.map(|d| d.as_secs() as u32).unwrap_or(0),
        priority: *priority,
        validate_schema_json: validate_schema_bundle.to_string(),
    };

    flow::TransformSpec {
        derivation: derivation.to_string(),
        transform: name.to_string(),
        shuffle: Some(shuffle),
        update_lambda: update
            .as_ref()
            .map(|update| lambda_spec(&update.lambda, transform, "Update")),
        publish_lambda: publish
            .as_ref()
            .map(|publish| lambda_spec(&publish.lambda, transform, "Publish")),
    }
}

pub fn derivation_spec(
    build_config: &flow::build_api::Config,
    derivation: &tables::Derivation,
    collection: &tables::BuiltCollection,
    mut transforms: Vec<flow::TransformSpec>,
    recovery_stores: &[models::Store],
    register_schema_bundle: &serde_json::Value,
) -> flow::DerivationSpec {
    let tables::Derivation {
        scope: _,
        derivation: name,
        spec:
            models::Derivation {
                register:
                    models::Register {
                        initial: register_initial,
                        schema: _,
                    },
                transform: _,
                typescript: _,
                shards,
            },
        register_schema,
        typescript_module: _,
    } = derivation;

    transforms.sort_by(|l, r| l.transform.cmp(&r.transform));

    // We should disable waiting for acknowledgements only if
    // the derivation reads from itself.
    let disable_wait_for_ack = transforms
        .iter()
        .map(|t| &t.shuffle.as_ref().unwrap().source_collection)
        .any(|n| n == name.as_str());

    flow::DerivationSpec {
        collection: Some(collection.spec.clone()),
        transforms,
        register_schema_uri: register_schema.to_string(),
        register_schema_json: register_schema_bundle.to_string(),
        register_initial_json: register_initial.to_string(),
        recovery_log_template: Some(recovery_log_template(
            build_config,
            name,
            labels::TASK_TYPE_DERIVATION,
            recovery_stores,
        )),
        shard_template: Some(shard_template(
            build_config,
            name,
            labels::TASK_TYPE_DERIVATION,
            shards,
            disable_wait_for_ack,
        )),
    }
}

/// `encode_resource_path` encodes path components into a string which is
/// suitable for use within a Gazette path, such as a Journal name or suffix, or
/// a Shard ID.
///
/// Paths are restricted to unicode letters and numbers, plus the symbols
/// `-_+/.=%`.  All other runes are percent-encoded.
///
/// See Gazette for more details:
/// - Path Tokens: broker/protocol/validator.go
/// - Path Validation Rules: broker/protocol/journal_spec_extensions.go
pub fn encode_resource_path(resource_path: &[impl AsRef<str>]) -> String {
    let mut parts = Vec::new();
    parts.extend(resource_path.iter().map(AsRef::as_ref));

    let mut name = String::new();

    for c in parts.join("/").chars() {
        match c {
            // This *must* conform the set of path validation rules in the
            // Gazette. Notably, a Path allows `/` characters, but only in
            // certain positions, no repeats, etc. As a resource_path
            // potentially contains arbitrary user input, we percent encode any
            // `/` characters here to avoid duplicating that validation logic.
            '-' | '_' | '+' | '.' | '=' => name.push(c),
            _ if c.is_alphanumeric() => name.push(c),
            c => name.extend(percent_encoding::utf8_percent_encode(
                &c.to_string(),
                percent_encoding::NON_ALPHANUMERIC,
            )),
        }
    }

    name
}

pub fn materialization_shuffle(
    binding: &tables::MaterializationBinding,
    source: &flow::CollectionSpec,
    resource_path: &[impl AsRef<str>],
) -> flow::Shuffle {
    let tables::MaterializationBinding {
        materialization,
        spec:
            models::MaterializationBinding {
                source: collection,
                partitions: source_partitions,
                ..
            },
        ..
    } = binding;

    flow::Shuffle {
        group_name: format!(
            "materialize/{}/{}",
            materialization.as_str(),
            encode_resource_path(resource_path),
        ),
        source_collection: collection.to_string(),
        source_partitions: Some(journal_selector(collection, source_partitions.as_ref())),
        source_uuid_ptr: source.uuid_ptr.clone(),
        // Materializations always group by the collection's key.
        shuffle_key_ptrs: source.key_ptrs.clone(),
        uses_source_key: true,
        shuffle_lambda: None,
        source_schema_uri: source.schema_uri.clone(),
        uses_source_schema: true,
        deprecated_validate_schema_at_read: false,
        // At all times, a given collection key must be exclusively owned by
        // a single materialization shard. Therefore we only subdivide
        // materialization shards on key, never on r-clock.
        filter_r_clocks: false,
        // Never delay materializations.
        read_delay_seconds: 0,
        // Priority has no meaning since there's just one shuffle
        // (we're not joining across collections as transforms do).
        priority: 0,
        // Schemas are validated when combined over by the materialization runtime.
        validate_schema_json: String::new(),
    }
}

pub fn test_step_spec(
    test_step: &tables::TestStep,
    documents: &Vec<Value>,
) -> flow::test_spec::Step {
    let tables::TestStep {
        scope,
        test: _,
        step_index,
        spec,
        documents: _,
    } = test_step;

    let (step_type, collection, description, selector) = match spec {
        models::TestStep::Ingest(ingest) => (
            flow::test_spec::step::Type::Ingest,
            &ingest.collection,
            &ingest.description,
            None,
        ),
        models::TestStep::Verify(verify) => (
            flow::test_spec::step::Type::Verify,
            &verify.collection,
            &verify.description,
            verify.partitions.as_ref(),
        ),
    };

    flow::test_spec::Step {
        step_type: step_type as i32,
        step_index: *step_index,
        step_scope: scope.to_string(),
        collection: collection.to_string(),
        docs_json_lines: documents
            .iter()
            .map(|d| serde_json::to_string(d).expect("object cannot fail to serialize"))
            .collect::<Vec<_>>()
            .join("\n"),
        partitions: Some(journal_selector(collection, selector)),
        description: description.clone(),
    }
}

pub fn capture_endpoint_type(t: &models::CaptureEndpoint) -> flow::EndpointType {
    match t {
        models::CaptureEndpoint::Connector(_) => flow::EndpointType::AirbyteSource,
        models::CaptureEndpoint::Ingest(_) => flow::EndpointType::Ingest,
    }
}

pub fn materialization_endpoint_type(t: &models::MaterializationEndpoint) -> flow::EndpointType {
    match t {
        models::MaterializationEndpoint::Connector(_) => flow::EndpointType::FlowSink,
        models::MaterializationEndpoint::Sqlite(_) => flow::EndpointType::Sqlite,
    }
}

pub fn compression_codec(t: models::CompressionCodec) -> broker::CompressionCodec {
    match t {
        models::CompressionCodec::None => broker::CompressionCodec::None,
        models::CompressionCodec::Gzip => broker::CompressionCodec::Gzip,
        models::CompressionCodec::Zstandard => broker::CompressionCodec::Zstandard,
        models::CompressionCodec::Snappy => broker::CompressionCodec::Snappy,
        models::CompressionCodec::GzipOffloadDecompression => {
            broker::CompressionCodec::GzipOffloadDecompression
        }
    }
}

pub fn label(t: models::Label) -> broker::Label {
    let models::Label { name, value } = t;
    broker::Label { name, value }
}

pub fn label_set(t: models::LabelSet) -> broker::LabelSet {
    let models::LabelSet { mut labels } = t;

    // broker::LabelSet requires that labels be ordered on (name, value).
    // Establish this invariant.
    labels.sort_by(|lhs, rhs| (&lhs.name, &lhs.value).cmp(&(&rhs.name, &rhs.value)));

    broker::LabelSet {
        labels: labels.into_iter().map(label).collect(),
    }
}

pub fn label_selector(t: models::LabelSelector) -> broker::LabelSelector {
    let models::LabelSelector { include, exclude } = t;

    let include = if include.labels.is_empty() {
        None
    } else {
        Some(include)
    };

    let exclude = if exclude.labels.is_empty() {
        None
    } else {
        Some(exclude)
    };

    broker::LabelSelector {
        include: include.map(label_set),
        exclude: exclude.map(label_set),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use doc::inference::StringShape;
    use serde_json::json;

    #[test]
    fn test_inference() {
        let mut shape = Shape {
            type_: types::STRING | types::BOOLEAN,
            default: Some(json!({"hello": "world"})),
            description: Some("the description".to_string()),
            title: Some("the title".to_owned()),
            secret: Some(true),
            string: StringShape {
                content_encoding: Some("BaSE64".to_owned()),
                format: Some(json::schema::formats::Format::DateTime),
                content_type: Some("a/type".to_string()),
                min_length: 10,
                max_length: Some(123),
            },
            ..Default::default()
        };

        let out1 = inference(&shape, Exists::Must);
        shape.type_ = types::BOOLEAN;
        let out2 = inference(&shape, Exists::May);

        insta::assert_debug_snapshot!(&[out1, out2]);
    }

    #[test]
    fn test_name_escapes() {
        let out = encode_resource_path(&vec![
            "he!lo৬".to_string(),
            "a/part%".to_string(),
            "_¾the-=res+.".to_string(),
        ]);
        assert_eq!(&out, "he%21lo৬%2Fa%2Fpart%25%2F_¾the-=res+.");

        let gross_url =
            "http://user:password@foo.bar.example.com:9000/hooks///baz?type=critical&test=true";
        let out = encode_resource_path(&vec!["prefix".to_string(), gross_url.to_string()]);
        assert_eq!(&out, "prefix%2Fhttp%3A%2F%2Fuser%3Apassword%40foo.bar.example.com%3A9000%2Fhooks%2F%2F%2Fbaz%3Ftype=critical%26test=true");
    }

    #[test]
    fn journal_selector_percent_encodes_values() {
        let mut include = BTreeMap::new();
        let mut exclude = BTreeMap::new();
        include.insert(
            String::from("foo"),
            vec!["some/val-ue".into(), "a_whole:nother".into()],
        );
        include.insert(
            String::from("bar"),
            vec![Value::from(123), Value::from(true)],
        );
        exclude.insert(String::from("foo"), vec!["no&no@no$yes();".into()]);

        let selector = models::PartitionSelector { include, exclude };
        let collection = models::Collection::new("the/collection");
        let labels = journal_selector(&collection, Some(&selector));
        insta::assert_debug_snapshot!(labels);
    }
}
