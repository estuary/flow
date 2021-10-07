use crate::{names, tables};
use doc::inference::{Exists, Shape};
use json::schema::types;
use protocol::{consumer, flow, labels, protocol as broker};
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Duration;

mod bundle;
pub use bundle::bundled_schema;

pub fn inference(shape: &Shape, exists: Exists) -> flow::Inference {
    flow::Inference {
        types: shape.type_.to_vec(),
        must_exist: exists.must(),
        title: shape.title.clone().unwrap_or_default(),
        description: shape.description.clone().unwrap_or_default(),
        string: if shape.type_.overlaps(types::STRING) {
            Some(flow::inference::String {
                content_type: shape.string.content_type.clone().unwrap_or_default(),
                format: shape.string.format.clone().unwrap_or_default(),
                is_base64: shape.string.is_base64.unwrap_or_default(),
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
    collection: &names::Collection,
    journals: &names::JournalTemplate,
    stores: &[names::Store],
) -> broker::JournalSpec {
    let names::JournalTemplate {
        fragments:
            names::FragmentTemplate {
                compression_codec,
                flush_interval,
                length,
                retention,
            },
    } = journals.clone();

    use names::CompressionCodec;

    // Until there's a good reason otherwise, we hard-code that partition journals are replicated 3x.
    let replication = 3;

    // Use a supplied compression codec. Or, if none, then default to gzip.
    let compression_codec = compression_codec
        .unwrap_or(CompressionCodec::Gzip)
        .into_proto();

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

    // We hard-code max_append_rate to 4MB/s, which back-pressures derivations
    // that produce lots of register updates. They'll hopefully perform more
    // aggregation per-transaction, and eventually stall until there's quota.
    // TODO(johnny): I sized this to be over the steady-state of our current
    // use cases. We'll want to revisit this value when we have better tooling
    // for splitting journals, and are able to study its cumulative effects
    // with varietes of journals and use cases.
    let max_append_rate = 1 << 22; // 4MB.

    // Map stores into their URL forms.
    let stores = stores.iter().map(|s| s.to_url().into()).collect();

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
            name: labels::COLLECTION.to_string(),
            value: collection.to_string(),
        },
    ];

    broker::JournalSpec {
        name: collection.to_string(),
        replication,
        fragment: Some(broker::journal_spec::Fragment {
            compression_codec,
            flush_interval,
            length,
            path_postfix_template,
            refresh_interval,
            retention,
            stores,
        }),
        flags,
        labels: Some(broker::LabelSet { labels }),
        max_append_rate,
    }
}

// recovery_log_template returns a template JournalSpec for creating
// or updating recovery logs of task shards.
pub fn recovery_log_template(
    task_name: &str,
    task_type: &str,
    stores: &[names::Store],
) -> broker::JournalSpec {
    use names::CompressionCodec;

    // Until there's a good reason otherwise, we hard-code that recovery logs are replicated 3x.
    let replication = 3;

    // Use Snappy compression. Note that lower levels of an LSM tree
    // typically apply their own compression, but the rocks WAL is
    // uncompressed. Snappy has good support for passing-through content
    // that's already compressed.
    // TODO(johnny): Switch gazette to https://github.com/klauspost/compress/tree/master/s2
    let compression_codec = CompressionCodec::Snappy.into_proto();

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
    // that produce lots of register updates. They'll hopefully perform more
    // aggregation per-transaction, and eventually stall until there's quota.
    // TODO(johnny): We'll want to revisit this value when we can better
    // study its cumulative effects with a varietes of journals and use cases.
    let max_append_rate = 1 << 22; // 4MB.

    // Map stores into their URL forms.
    let stores = stores.iter().map(|s| s.to_url().into()).collect();

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
            compression_codec,
            flush_interval,
            length,
            path_postfix_template,
            refresh_interval,
            retention,
            stores,
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
    task_name: &str,
    task_type: &str,
    shard: &names::ShardTemplate,
    disable_wait_for_ack: bool,
) -> consumer::ShardSpec {
    let names::ShardTemplate {
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
    collection: &tables::Collection,
    projections: Vec<flow::Projection>,
    schema_bundle: &Value,
    stores: &[names::Store],
) -> flow::CollectionSpec {
    let tables::Collection {
        collection: name,
        scope: _,
        schema,
        key,
        journals,
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

    flow::CollectionSpec {
        collection: name.to_string(),
        schema_uri: schema.to_string(),
        schema_json: schema_bundle.to_string(),
        key_ptrs: key.iter().map(|p| p.to_string()).collect(),
        projections,
        partition_fields,
        uuid_ptr: collection.uuid_ptr(),
        ack_json_template: serde_json::json!({
                "_meta": {"uuid": "DocUUIDPlaceholder-329Bb50aa48EAa9ef",
                "ack": true,
            } })
        .to_string()
        .into(),
        partition_template: Some(partition_template(name, journals, stores)),
    }
}

pub fn journal_selector(
    collection: &names::Collection,
    selector: &Option<names::PartitionSelector>,
) -> broker::LabelSelector {
    let mut include = vec![broker::Label {
        name: labels::COLLECTION.to_string(),
        value: collection.to_string(),
    }];
    let mut exclude = Vec::new();

    if let Some(selector) = &selector {
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

// Flatten partition selector fields into a Vec<Label>.
// JSON strings are percent-encoded but un-quoted.
// Other JSON types map to their literal JSON strings.
// *** This MUST match the Go-side behavior! ***
fn push_partitions(fields: &BTreeMap<String, Vec<Value>>, out: &mut Vec<broker::Label>) {
    for (field, value) in fields {
        for value in value {
            let value = match value {
                Value::String(s) => {
                    percent_encoding::utf8_percent_encode(s, PATH_SEGMENT_SET).to_string()
                }
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
    lambda: &names::Lambda,
    transform: &tables::Transform,
    suffix: &str,
) -> flow::LambdaSpec {
    match lambda {
        names::Lambda::Typescript => flow::LambdaSpec {
            typescript: format!("/{}/{}", transform.group_name(), suffix),
            ..Default::default()
        },
        names::Lambda::Remote(addr) => flow::LambdaSpec {
            remote: addr.clone(),
            ..Default::default()
        },
    }
}

pub fn transform_spec(
    transform: &tables::Transform,
    source: &tables::Collection,
) -> flow::TransformSpec {
    let tables::Transform {
        scope: _,
        derivation,
        priority,
        publish_lambda,
        read_delay_seconds,
        shuffle_key,
        shuffle_lambda,
        source_collection: _,
        source_partitions,
        source_schema,
        transform: name,
        update_lambda,
    } = &transform;

    let shuffle = flow::Shuffle {
        group_name: transform.group_name(),
        source_collection: source.collection.to_string(),
        source_partitions: Some(journal_selector(&source.collection, source_partitions)),
        source_uuid_ptr: source.uuid_ptr(),
        shuffle_key_ptr: shuffle_key
            .as_ref()
            .unwrap_or(&source.key)
            .iter()
            .map(|p| p.to_string())
            .collect(),
        uses_source_key: shuffle_key.is_none(),
        shuffle_lambda: shuffle_lambda
            .as_ref()
            .map(|l| lambda_spec(&l, transform, "Shuffle")),
        source_schema_uri: source_schema
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| source.schema.to_string()),
        uses_source_schema: source_schema.is_none(),
        validate_schema_at_read: true,
        filter_r_clocks: update_lambda.is_none(),
        read_delay_seconds: read_delay_seconds.unwrap_or(0),
        priority: *priority,
    };

    flow::TransformSpec {
        derivation: derivation.to_string(),
        transform: name.to_string(),
        shuffle: Some(shuffle),
        update_lambda: update_lambda
            .as_ref()
            .map(|l| lambda_spec(l, transform, "Update")),
        publish_lambda: publish_lambda
            .as_ref()
            .map(|l| lambda_spec(l, transform, "Publish")),
    }
}

pub fn derivation_spec(
    derivation: &tables::Derivation,
    collection: &tables::BuiltCollection,
    mut transforms: Vec<flow::TransformSpec>,
    recovery_stores: &[names::Store],
) -> flow::DerivationSpec {
    let tables::Derivation {
        scope: _,
        derivation: name,
        register_schema,
        register_initial,
        shards,
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
        register_initial_json: register_initial.to_string(),
        recovery_log_template: Some(recovery_log_template(
            name,
            labels::TASK_TYPE_DERIVATION,
            recovery_stores,
        )),
        shard_template: Some(shard_template(
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
    flow::Shuffle {
        group_name: format!(
            "materialize/{}/{}",
            binding.materialization.as_str(),
            encode_resource_path(resource_path),
        ),
        source_collection: binding.collection.to_string(),
        source_partitions: Some(journal_selector(
            &binding.collection,
            &binding.source_partitions,
        )),
        source_uuid_ptr: source.uuid_ptr.clone(),
        // Materializations always group by the collection's key.
        shuffle_key_ptr: source.key_ptrs.clone(),
        uses_source_key: true,
        shuffle_lambda: None,
        source_schema_uri: source.schema_uri.clone(),
        uses_source_schema: true,
        validate_schema_at_read: false,
        // At all times, a given collection key must be exclusively owned by
        // a single materialization shard. Therefore we only subdivide
        // materialization shards on key, never on r-clock.
        filter_r_clocks: false,
        // Never delay materializations.
        read_delay_seconds: 0,
        // Priority has no meaning since there's just one shuffle
        // (we're not joining across collections as transforms do).
        priority: 0,
    }
}

pub fn test_step_spec(
    test_step: &tables::TestStep,
    collection: &tables::Collection,
) -> flow::test_spec::Step {
    let tables::TestStep {
        scope,
        collection: _,
        documents,
        partitions,
        step_index,
        step_type,
        test: _,
    } = test_step;

    flow::test_spec::Step {
        step_type: *step_type as i32,
        step_index: *step_index,
        step_scope: scope.to_string(),
        collection: collection.collection.to_string(),
        collection_schema_uri: collection.schema.to_string(),
        collection_key_ptr: collection.key.iter().map(|p| p.to_string()).collect(),
        collection_uuid_ptr: collection.uuid_ptr(),
        docs_json_lines: documents
            .iter()
            .map(|d| d.to_string())
            .collect::<Vec<_>>()
            .join("\n"),
        partitions: Some(journal_selector(&collection.collection, partitions)),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_name_escapes() {
        let out = encode_resource_path(&vec![
            "he!lo৬".to_string(),
            "a/part%".to_string(),
            "_¾the-=res+.".to_string(),
        ]);
        assert_eq!(&out, "he%21lo৬%2Fa%2Fpart%25%2F_¾the-=res+.");
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

        let selector = names::PartitionSelector { include, exclude };
        let collection = names::Collection::new("the/collection");
        let labels = journal_selector(&collection, &Some(selector));
        insta::assert_debug_snapshot!(labels);
    }

    #[test]
    fn test_arbitrary_webhook_urls() {
        let url =
            "http://user:password@foo.bar.example.com:9000/hooks///baz?type=critical&test=true";
        let out = encode_resource_path(&vec![url.to_string()]);
        assert_eq!(&out, "http%3A%2F%2Fuser%3Apassword%40foo.bar.example.com%3A9000%2Fhooks%2F%2F%2Fbaz%3Ftype=critical%26test=true");
    }
}
