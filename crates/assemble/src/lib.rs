use doc::shape::{location::Exists, Shape};
use json::schema::{formats, types};
use proto_flow::flow;
use proto_gazette::{broker, consumer};
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Duration;

mod ops;
pub use ops::generate_ops_collections;

pub fn inference(shape: &Shape, exists: Exists) -> flow::Inference {
    let default_json = shape
        .default
        .as_ref()
        .map(|v| v.0.to_string())
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
        title: shape.title.clone().map(Into::into).unwrap_or_default(),
        description: shape
            .description
            .clone()
            .map(Into::into)
            .unwrap_or_default(),
        default_json,
        secret: shape.secret.unwrap_or_default(),
        string: if shape.type_.overlaps(types::STRING) {
            Some(flow::inference::String {
                content_type: shape
                    .string
                    .content_type
                    .clone()
                    .map(Into::into)
                    .unwrap_or_default(),
                format: shape
                    .string
                    .format
                    .map(|f| f.to_string())
                    .unwrap_or_default(),
                content_encoding: shape
                    .string
                    .content_encoding
                    .clone()
                    .map(Into::into)
                    .unwrap_or_default(),
                max_length: shape.string.max_length.unwrap_or_default() as u32,
            })
        } else {
            None
        },
    }
}

// inference_uuid_timestamp is a special-case flow::Inference
// for the timestamp embedded within the Flow document UUID.
pub fn inference_uuid_v1_date_time() -> flow::Inference {
    flow::Inference {
        types: vec!["string".to_string()],
        string: Some(flow::inference::String {
            format: formats::Format::DateTime.to_string(),
            content_encoding: "uuid".to_string(),
            ..Default::default()
        }),
        title: "Flow Publication Time".to_string(),
        description: "Flow publication date-time of this document".to_string(),
        exists: flow::inference::Exists::Must as i32,
        ..Default::default()
    }
}

// partition_template returns a template JournalSpec for creating
// or updating data partitions of the collection.
pub fn partition_template(
    build_id: &str,
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
            value: build_id.to_string(),
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
            stores: stores
                .iter()
                .map(|s| s.to_url(&collection).into())
                .collect(),
        }),
        flags,
        labels: Some(broker::LabelSet { labels }),
        max_append_rate,
    }
}

// recovery_log_template returns a template JournalSpec for creating
// or updating recovery logs of task shards.
pub fn recovery_log_template(
    build_id: &str,
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
            value: build_id.to_string(),
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
            stores: stores.iter().map(|s| s.to_url(task_name).into()).collect(),
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
    build_id: &str,
    task_name: &str,
    task_type: &str,
    shard: &models::ShardTemplate,
    disable_wait_for_ack: bool,
    ports: &[flow::NetworkPort],
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

    // If not set, the maximum transaction duration is five minutes
    // for materializations and one second for captures and derivations.
    let mut max_txn_duration = if let Some(max_txn_duration) = max_txn_duration {
        *max_txn_duration
    } else if task_type == labels::TASK_TYPE_MATERIALIZATION {
        Duration::from_secs(5 * 60)
    } else {
        Duration::from_secs(1)
    };
    // By default, there is no minimum duration.
    let min_txn_duration = min_txn_duration.unwrap_or(Duration::ZERO);

    if min_txn_duration > max_txn_duration {
        max_txn_duration = min_txn_duration;
    }

    // If not set, no hot standbys are used.
    let hot_standbys = hot_standbys.unwrap_or(0);

    // If not set, the default ring buffer size is 64k.
    let ring_buffer_size = ring_buffer_size.unwrap_or(1 << 16);
    // If not set, the default read channel size is 4,096.
    let read_channel_size = read_channel_size.unwrap_or(1 << 12);

    let mut labels = vec![
        broker::Label {
            name: labels::MANAGED_BY.to_string(),
            value: labels::MANAGED_BY_FLOW.to_string(),
        },
        broker::Label {
            name: labels::BUILD.to_string(),
            value: build_id.to_string(),
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

    // Only add a hostname if the task actually exposes any ports.
    if !ports.is_empty() {
        labels.push(broker::Label {
            name: labels::HOSTNAME.to_string(),
            value: shard_hostname_label(task_name),
        });
    }
    for flow::NetworkPort {
        number,
        protocol,
        public,
    } in ports
    {
        // labels are a multiset, so we use the same label for all exposed port numbers.
        labels.push(broker::Label {
            name: labels::EXPOSE_PORT.to_string(),
            value: number.to_string(),
        });

        // Only add these labels if they differ from the defaults
        if *public {
            labels.push(broker::Label {
                name: format!("{}{number}", labels::PORT_PUBLIC_PREFIX),
                value: "true".to_string(),
            });
        }
        if !protocol.is_empty() {
            labels.push(broker::Label {
                name: format!("{}{number}", labels::PORT_PROTO_PREFIX),
                value: protocol.clone(),
            });
        }
    }
    // Labels must be in lexicographic order.
    labels.sort_by(|l, r| l.name.cmp(&r.name));

    consumer::ShardSpec {
        id: shard_id_base(task_name, task_type),
        disable: *disable,
        disable_wait_for_ack,
        hint_backups,
        hint_prefix,
        hot_standbys,
        labels: Some(broker::LabelSet { labels }),
        max_txn_duration: Some(max_txn_duration.into()),
        min_txn_duration: Some(min_txn_duration.into()),
        read_channel_size,
        recovery_log_prefix,
        ring_buffer_size,
        sources: Vec::new(),
    }
}

/// This function supplies a domain name label that identifies _all_ shards for a given task.
/// To do this, we just hash the task name and convert it to a hexidecimal string.
/// It's a bit janky, but the only idea I've liked better is pet-names, which we
/// don't have yet. This also has the property of being pretty short (16 chars),
/// which is nice because it leaves a little more headroom for other labels in the
/// the full hostname.
fn shard_hostname_label(task_name: &str) -> String {
    let hash = fxhash::hash64(task_name);
    format!("{:x}", hash)
}

pub fn collection_spec(
    build_id: &str,
    collection: &tables::Collection,
    projections: Vec<flow::Projection>,
    stores: &[models::Store],
    uuid_ptr: &str,
) -> flow::CollectionSpec {
    let tables::Collection {
        scope: _,
        collection: name,
        spec:
            models::CollectionDef {
                schema,
                read_schema,
                write_schema,
                key,
                projections: _,
                journals,
                derivation: _,
                derive: _,
                ..
            },
    } = collection;

    // Projections must be ascending and unique on field.
    // We expect they already are.
    assert!(projections.windows(2).all(|p| p[0].field < p[1].field));

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

    let (write_schema_json, read_schema_json) = match (schema, write_schema, read_schema) {
        (Some(schema), None, None) => (schema.to_string(), String::new()),
        (None, Some(write_schema), Some(read_schema)) => {
            (write_schema.to_string(), read_schema.to_string())
        }
        _ => (String::new(), String::new()),
    };

    flow::CollectionSpec {
        name: name.to_string(),
        write_schema_json,
        read_schema_json,
        key: key.iter().map(|p| p.to_string()).collect(),
        projections,
        partition_fields,
        uuid_ptr: uuid_ptr.to_string(),
        ack_template_json: serde_json::json!({
                "_meta": {"uuid": "DocUUIDPlaceholder-329Bb50aa48EAa9ef",
                "ack": true,
            } })
        .to_string(),
        partition_template: Some(partition_template(build_id, name, journals, stores)),
        derivation: None,
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

/// Percent-encodes string values so that they can be used in Gazette label values.
pub fn percent_encode_partition_value(s: &str) -> String {
    // The set of characters that must be percent-encoded when used in partition
    // values. It's nearly everything, aside from a few special cases.
    const SET: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.');
    percent_encoding::utf8_percent_encode(s, SET).to_string()
}

// Flatten partition selector fields into a Vec<Label>.
// JSON strings are percent-encoded but un-quoted.
// Other JSON types map to their literal JSON strings prefixed with `%_`,
// which is a production that percent-encoding will never produce.
// *** This MUST match the Go-side behavior! ***
fn push_partitions(fields: &BTreeMap<String, Vec<Value>>, out: &mut Vec<broker::Label>) {
    for (field, value) in fields {
        for value in value {
            let value = match value {
                Value::String(s) => percent_encode_partition_value(s),
                _ => format!("%_{}", value),
            };
            out.push(broker::Label {
                name: format!("{}{}", labels::FIELD_PREFIX, field),
                value,
            });
        }
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

pub fn pb_datetime(t: &time::OffsetDateTime) -> pbjson_types::Timestamp {
    pbjson_types::Timestamp {
        seconds: t.unix_timestamp() as i64,
        nanos: 0, // Deliberately truncated.
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use doc::shape::StringShape;
    use serde_json::json;

    #[test]
    fn test_inference() {
        let mut shape = Shape {
            type_: types::STRING | types::BOOLEAN,
            default: Some(Box::new((json!({"hello": "world"}), None))),
            description: Some("the description".into()),
            title: Some("the title".into()),
            secret: Some(true),
            string: StringShape {
                content_encoding: Some("BaSE64".into()),
                format: Some(json::schema::formats::Format::DateTime),
                content_type: Some("a/type".into()),
                min_length: 10,
                max_length: Some(123),
            },
            ..Shape::anything()
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

        include.insert("null".to_string(), vec![Value::Null]);
        include.insert(
            "bool".to_string(),
            vec![Value::Bool(true), Value::Bool(false)],
        );
        include.insert(
            "integers".to_string(),
            vec![
                Value::from(123),
                Value::from(i64::MIN),
                Value::from(i64::MAX),
                Value::from(u64::MAX),
            ],
        );
        include.insert(
            String::from("strings"),
            vec![
                "simple".into(),
                "hello, world!".into(),
                "Baz!@\"Bing\"".into(),
                "no.no&no-no@no$yes_yes();".into(),
                "http://example/path?q1=v1&q2=v2;ex%20tra".into(),
            ],
        );
        exclude.insert(
            "naughty-strings".to_string(),
            vec![
                "null".into(),
                "%_null".into(),
                "123".into(),
                "-456".into(),
                "true".into(),
                "false".into(),
            ],
        );

        let selector = models::PartitionSelector { include, exclude };
        let collection = models::Collection::new("the/collection");
        let labels = journal_selector(&collection, Some(&selector));
        insta::assert_debug_snapshot!(labels);
    }
}
