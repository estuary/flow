use doc::shape::{self, Shape, location::Exists};
use doc::{redact, reduce};
use json::schema::{formats, types};
use proto_flow::flow;
use proto_gazette::{broker, consumer};
use std::time::Duration;

pub fn inference(shape: &Shape, exists: Exists) -> flow::Inference {
    let default_json: bytes::Bytes = shape
        .default
        .as_ref()
        .map(|v| v.0.to_string().into())
        .unwrap_or_default();

    let enum_json_vec: Vec<bytes::Bytes> = shape
        .enum_
        .iter()
        .flatten()
        .map(|v| v.to_string().into())
        .collect();

    let exists = match exists {
        Exists::Must => flow::inference::Exists::Must,
        Exists::May => flow::inference::Exists::May,
        Exists::Implicit => flow::inference::Exists::Implicit,
        Exists::Cannot => flow::inference::Exists::Cannot,
    };
    let reduce = match shape.reduce {
        shape::Reduce::Multiple => flow::inference::Reduce::Multiple,
        shape::Reduce::Strategy(reduce::Strategy::Append) => flow::inference::Reduce::Append,
        shape::Reduce::Strategy(reduce::Strategy::FirstWriteWins(..)) => {
            flow::inference::Reduce::FirstWriteWins
        }
        shape::Reduce::Strategy(reduce::Strategy::LastWriteWins(..)) => {
            flow::inference::Reduce::LastWriteWins
        }
        shape::Reduce::Strategy(reduce::Strategy::Maximize(..)) => {
            flow::inference::Reduce::Maximize
        }
        shape::Reduce::Strategy(reduce::Strategy::Merge(..)) => flow::inference::Reduce::Merge,
        shape::Reduce::Strategy(reduce::Strategy::Minimize(..)) => {
            flow::inference::Reduce::Minimize
        }
        shape::Reduce::Strategy(reduce::Strategy::Set(..)) => flow::inference::Reduce::Set,
        shape::Reduce::Strategy(reduce::Strategy::Sum) => flow::inference::Reduce::Sum,
        shape::Reduce::Strategy(reduce::Strategy::JsonSchemaMerge) => {
            flow::inference::Reduce::JsonSchemaMerge
        }
        shape::Reduce::Unset => flow::inference::Reduce::Unset,
    };
    let redact = match shape.redact {
        shape::Redact::Multiple => flow::inference::Redact::Multiple,
        shape::Redact::Strategy(redact::Strategy::Block) => flow::inference::Redact::Block,
        shape::Redact::Strategy(redact::Strategy::Sha256) => flow::inference::Redact::Sha256,
        shape::Redact::Unset => flow::inference::Redact::Unset,
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
        numeric: if shape.type_.overlaps(types::INT_OR_FRAC) {
            Some(flow::inference::Numeric {
                has_minimum: shape.numeric.minimum.is_some(),
                minimum: shape
                    .numeric
                    .minimum
                    .map(|n| match n {
                        json::Number::Float(f) => f,
                        json::Number::NegInt(i) => i as f64,
                        json::Number::PosInt(u) => u as f64,
                    })
                    .unwrap_or_default(),
                has_maximum: shape.numeric.maximum.is_some(),
                maximum: shape
                    .numeric
                    .maximum
                    .map(|n| match n {
                        json::Number::Float(f) => f,
                        json::Number::NegInt(i) => i as f64,
                        json::Number::PosInt(u) => u as f64,
                    })
                    .unwrap_or_default(),
            })
        } else {
            None
        },
        array: if shape.type_.overlaps(types::ARRAY) {
            Some(flow::inference::Array {
                min_items: shape.array.min_items,
                has_max_items: shape.array.max_items.is_some(),
                max_items: shape.array.max_items.unwrap_or_default(),
                item_types: shape
                    .array
                    .tuple
                    .iter()
                    .chain(shape.array.additional_items.as_deref())
                    .fold(types::INVALID, |acc, item| acc | item.type_)
                    .to_vec(),
            })
        } else {
            None
        },
        enum_json_vec,
        reduce: reduce as i32,
        redact: redact as i32,
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

pub fn inference_truncation_indicator() -> flow::Inference {
    flow::Inference {
        types: vec!["boolean".to_string()],
        string: None,
        title: "Flow truncation indicator".to_string(),
        description: "Indicates whether any of the materialized values for this row have been truncated to make them fit inside the limitations of the destination system.".to_string(),
        exists: flow::inference::Exists::Must as i32,
        ..Default::default()
    }
}

// partition_template returns a template JournalSpec for creating
// or updating data partitions of the collection.
pub fn partition_template(
    build_id: models::Id,
    collection: &models::Collection,
    journal_name_prefix: String,
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

    // If an explicit flush interval isn't provided, default to 24 hours.
    let flush_interval = flush_interval
        .unwrap_or(std::time::Duration::from_secs(24 * 3600))
        .into();

    // If a fragment length isn't set, default and then map MB to bytes.
    let length = (length.unwrap_or(512) as i64) << 20;

    // Until there's a good reason otherwise, we hard-code that fragments include the UTC date
    // and hour they were created as components of their path. This makes it easy to filter
    // collections on time when making ad-hoc queries using the Hive partitioning scheme.
    let path_postfix_template = r#"utc_date={{.Spool.FirstAppendTime.Format "2006-01-02"}}/utc_hour={{.Spool.FirstAppendTime.Format "15"}}"#.to_string();

    // Until there's a good reason otherwise, we hard-code that fragments are refreshed every five minutes.
    let refresh_interval = Some(Duration::from_secs(5 * 60).into());

    // If an explicit retention interval isn't provided, then don't set one.
    let retention = retention.map(Into::into);

    // Partition journals are readable and writable.
    let flags = broker::journal_spec::Flag::ORdwr as u32;

    // We hard-code max_append_rate to 4MB/s, which back-pressures captures
    // and derivations that produce lots of documents. They'll perform more
    // aggregation per-transaction, and may stall until there's quota.
    let max_append_rate = 1 << 22; // 4MB.

    let labels = labels::build_set([
        (labels::BUILD, build_id.to_string().as_str()),
        (labels::COLLECTION, &collection),
        (labels::CONTENT_TYPE, labels::CONTENT_TYPE_JSON_LINES),
        (labels::MANAGED_BY, labels::MANAGED_BY_FLOW),
    ]);

    broker::JournalSpec {
        name: journal_name_prefix,
        replication,
        fragment: Some(broker::journal_spec::Fragment {
            compression_codec: compression_codec as i32,
            flush_interval: Some(flush_interval),
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
        labels: Some(labels),
        max_append_rate,
        suspend: None,
    }
}

// recovery_log_template returns a template JournalSpec for creating
// or updating recovery logs of task shards.
pub fn recovery_log_template(
    build_id: models::Id,
    task_name: &str,
    task_type: &str,
    shard_id_prefix: &str,
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

    // Flush recovery logs at least once every 48 hours.
    let flush_interval = Some(std::time::Duration::from_secs(48 * 3600).into());

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

    let labels = labels::build_set([
        (labels::BUILD, build_id.to_string().as_str()),
        (labels::CONTENT_TYPE, labels::CONTENT_TYPE_RECOVERY_LOG),
        (labels::MANAGED_BY, labels::MANAGED_BY_FLOW),
        (labels::TASK_NAME, task_name),
        (labels::TASK_TYPE, &task_type.to_string()),
    ]);

    broker::JournalSpec {
        name: format!("recovery/{shard_id_prefix}"),
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
        labels: Some(labels),
        max_append_rate,
        suspend: None,
    }
}

// partition_prefix returns the base Gazette Journal name for logical and
// physical partitions of the given collection name.
// At runtime, this base ID is then '/'-joined with logical partition values,
// represented as directory-like /key=value/ components, and a final hex-encoded
// "pivot={KeyBegin}" suffix of the specific physical splits of the
// logical partition, to form complete journal names.
// See also PartitionSuffix in go/labels/partitions.go
pub fn partition_prefix(generation_id: models::Id, collection: &models::Collection) -> String {
    // Semi-colons are disallowed in Gazette journal names.
    let generation_id = generation_id.to_string().replace(":", "");
    format!("{collection}/{generation_id}")
}

// shard_id_prefix returns the base Gazette Shard ID for the task name and type.
// At runtime, this base ID is then '/'-joined with a hex-encoded
// "{KeyBegin}-{RClockBegin}" suffix of the specific splits of the task,
// to form complete shard IDs.
// See also ShardSuffix in go/labels/partitions.go
pub fn shard_id_prefix(generation_id: models::Id, task_name: &str, task_type: &str) -> String {
    let task_type = match task_type {
        labels::TASK_TYPE_CAPTURE => "capture",
        labels::TASK_TYPE_DERIVATION => "derivation",
        labels::TASK_TYPE_MATERIALIZATION => "materialize",
        _ => panic!("invalid task type {}", task_type),
    };

    format!("{task_type}/{task_name}/{generation_id}")
}

// extract_generation_id returns the generation ID which was embedded in the
// suffix of a template name returned by `partition_prefix` or `shard_id_prefix`.
// If the collection or shard is legacy, then it won't have a generation ID:
// and the last path component will fail to parse as one, and models::Id::zero()
// is returned instead.
pub fn extract_generation_id_suffix(journal_or_shard_prefix: &str) -> models::Id {
    if let Some((_, last)) = journal_or_shard_prefix.rsplit_once('/') {
        if let Ok(id) = models::Id::from_hex(last) {
            return id;
        }
    }
    models::Id::zero()
}

// shard_template returns a template ShardSpec for creating or updating
// shards of the task.
pub fn shard_template(
    build_id: models::Id,
    task_name: &str,
    task_type: &str,
    shard: &models::ShardTemplate,
    shard_id_prefix: &str,
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

    // If not set, the maximum transaction duration is twenty minutes
    // for materializations and one second for captures and derivations.
    let mut max_txn_duration = if let Some(max_txn_duration) = max_txn_duration {
        *max_txn_duration
    } else if task_type == labels::TASK_TYPE_MATERIALIZATION {
        Duration::from_secs(20 * 60)
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

    let mut labels = labels::build_set([
        (labels::BUILD, build_id.to_string().as_str()),
        (
            labels::LOG_LEVEL,
            log_level.as_ref().map(String::as_str).unwrap_or("info"),
        ),
        (labels::MANAGED_BY, labels::MANAGED_BY_FLOW),
        (labels::TASK_NAME, task_name),
        (labels::TASK_TYPE, &task_type.to_string()),
    ]);

    // Only add a hostname if the task actually exposes any ports.
    if !ports.is_empty() {
        labels = labels::add_value(labels, labels::HOSTNAME, &shard_hostname_label(task_name));
    }
    for flow::NetworkPort {
        number,
        protocol,
        public,
    } in ports
    {
        // labels are a multiset, so we use the same label for all exposed port numbers.
        labels = labels::add_value(labels, labels::EXPOSE_PORT, &number.to_string());

        // Only add these labels if they differ from the defaults
        if *public {
            labels = labels::add_value(
                labels,
                &format!("{}{number}", labels::PORT_PUBLIC_PREFIX),
                "true",
            );
        }
        if !protocol.is_empty() {
            labels = labels::add_value(
                labels,
                &format!("{}{number}", labels::PORT_PROTO_PREFIX),
                &protocol,
            );
        }
    }

    consumer::ShardSpec {
        id: shard_id_prefix.to_string(),
        disable: *disable,
        disable_wait_for_ack,
        hint_backups,
        hint_prefix,
        hot_standbys,
        labels: Some(labels),
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

pub fn journal_selector(
    collection: &flow::CollectionSpec,
    selector: Option<&models::PartitionSelector>,
) -> broker::LabelSelector {
    let mut include = labels::build_set([
        (labels::COLLECTION, collection.name.as_ref()),
        (
            "name:prefix",
            format!("{}/", collection.partition_template.as_ref().unwrap().name).as_ref(),
        ),
    ]);
    let mut exclude = broker::LabelSet::default();

    if let Some(selector) = selector {
        for (field, values) in &selector.include {
            for value in values {
                include =
                    labels::partition::add_value(include, field, value).expect("value is valid");
            }
        }
        for (field, values) in &selector.exclude {
            for value in values {
                exclude =
                    labels::partition::add_value(exclude, field, value).expect("value is valid");
            }
        }
    }

    broker::LabelSelector {
        include: Some(include),
        exclude: Some(exclude),
    }
}

/// `encode_state_key` encodes resource path components and a backfill counter
/// into a stable string value which is suited for indexing within a persistent
/// binding state, such as a Flow runtime checkpoint or a connector state.
///
/// State keys have a restricted set of allowed characters, due to the way
/// they're represented within Flow runtime checkpoints and, internal to those
/// checkpoints, as suffixes attached to Gazette Journal names.
///
/// State keys are restricted to unicode letters and numbers, plus the symbols
/// `-_+.=`.  All other runes are percent-encoded.
///
/// See Gazette for more details:
/// - Path Tokens: broker/protocol/validator.go
/// - Path Validation Rules: broker/protocol/journal_spec_extensions.go
pub fn encode_state_key(resource_path: &[impl AsRef<str>], backfill: u32) -> String {
    let mut parts = Vec::new();
    parts.extend(resource_path.iter().map(AsRef::as_ref));

    let mut key = String::new();

    for c in parts.join("/").chars() {
        match c {
            // This *must* conform the set of path validation rules in the
            // Gazette. Notably, a Path allows `/` characters, but only in
            // certain positions, no repeats, etc. As a resource_path
            // potentially contains arbitrary user input, we percent encode any
            // `/` characters here to avoid duplicating that validation logic.
            '-' | '_' | '+' | '.' | '=' => key.push(c),
            _ if c.is_alphanumeric() => key.push(c),
            c => key.extend(percent_encoding::utf8_percent_encode(
                &c.to_string(),
                percent_encoding::NON_ALPHANUMERIC,
            )),
        }
    }

    if backfill != 0 {
        key.extend(format!(".v{backfill}").chars());
    }

    key
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

pub fn pb_datetime(t: &time::OffsetDateTime) -> pbjson_types::Timestamp {
    pbjson_types::Timestamp {
        seconds: t.unix_timestamp() as i64,
        nanos: 0, // Deliberately truncated.
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use doc::shape::{ArrayShape, NumericShape, StringShape};
    use serde_json::{Value, json};
    use std::collections::BTreeMap;

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
            numeric: NumericShape {
                minimum: None,
                maximum: Some(json::Number::PosInt(1000)),
            },
            array: ArrayShape {
                additional_items: Some(Box::new(Shape {
                    type_: types::STRING,
                    ..Shape::anything()
                })),
                min_items: 10,
                max_items: Some(20),
                tuple: vec![
                    Shape {
                        type_: types::STRING,
                        ..Shape::anything()
                    },
                    Shape {
                        type_: types::BOOLEAN,
                        ..Shape::anything()
                    },
                    Shape {
                        type_: types::OBJECT,
                        ..Shape::anything()
                    },
                ],
            },
            enum_: Some(vec![json!("hello"), json!(123), json!(true)]),
            reduce: doc::shape::Reduce::Strategy(doc::reduce::Strategy::Sum),
            redact: doc::shape::Redact::Strategy(doc::redact::Strategy::Sha256),
            ..Shape::anything()
        };

        let out1 = inference(&shape, Exists::Must);
        shape.type_ = types::BOOLEAN;
        let out2 = inference(&shape, Exists::May);
        shape.type_ = types::INTEGER | types::STRING;
        let out3 = inference(&shape, Exists::May);
        shape.type_ = types::ARRAY;
        let out4 = inference(&shape, Exists::May);

        insta::assert_debug_snapshot!(&[out1, out2, out3, out4]);
    }

    #[test]
    fn test_state_key_escapes() {
        let out = encode_state_key(&["table"], 0);
        assert_eq!(&out, "table");
        let out = encode_state_key(&["public", "table"], 0);
        assert_eq!(&out, "public%2Ftable");
        let out = encode_state_key(&["public", "table"], 1);
        assert_eq!(&out, "public%2Ftable.v1");

        let out = encode_state_key(
            &vec![
                "he!lo৬".to_string(),
                "a/part%".to_string(),
                "_¾the-=res+.".to_string(),
            ],
            3,
        );
        assert_eq!(&out, "he%21lo৬%2Fa%2Fpart%25%2F_¾the-=res+..v3");

        let gross_url =
            "http://user:password@foo.bar.example.com:9000/hooks///baz?type=critical&test=true";
        let out = encode_state_key(&vec!["prefix".to_string(), gross_url.to_string()], 42);
        assert_eq!(
            &out,
            "prefix%2Fhttp%3A%2F%2Fuser%3Apassword%40foo.bar.example.com%3A9000%2Fhooks%2F%2F%2Fbaz%3Ftype=critical%26test=true.v42"
        );
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
        let collection = flow::CollectionSpec {
            name: "the/collection".to_string(),
            partition_template: Some(broker::JournalSpec {
                name: "data-plane/the/collection/xyz".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let labels = journal_selector(&collection, Some(&selector));
        insta::assert_debug_snapshot!(labels);
    }

    #[test]
    fn journal_and_shard_template_prefixes() {
        let id = models::Id::new([1, 2, 3, 4, 1, 2, 3, 4]);

        let prefix = super::partition_prefix(id, &models::Collection::new("acmeCo/some/anvils"));
        assert_eq!(prefix, "acmeCo/some/anvils/0102030401020304");
        assert_eq!(super::extract_generation_id_suffix(&prefix), id);

        let prefix = super::shard_id_prefix(id, "some/task/name", "capture");
        assert_eq!(prefix, "capture/some/task/name/0102030401020304");
        assert_eq!(super::extract_generation_id_suffix(&prefix), id);

        // Legacy prefixes without generation IDs map to zeros.
        assert_eq!(
            super::extract_generation_id_suffix("acmeCo/some/anvils"),
            models::Id::zero()
        );
        assert_eq!(
            super::extract_generation_id_suffix("capture/some/task/name"),
            models::Id::zero()
        );
    }
}
