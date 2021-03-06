use crate::{names, tables};
use doc::inference::{Exists, Shape};
use json::schema::types;
use protocol::{flow, protocol as broker};
use serde_json::Value;
use std::collections::BTreeMap;

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

pub fn collection_spec(
    collection: &tables::Collection,
    projections: Vec<flow::Projection>,
    schema_bundle: &Value,
) -> flow::CollectionSpec {
    let tables::Collection {
        collection: name,
        scope: _,
        schema,
        key,
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
    }
}

pub fn journal_selector(
    collection: &names::Collection,
    selector: &Option<names::PartitionSelector>,
) -> broker::LabelSelector {
    let mut include = vec![broker::Label {
        name: "estuary.dev/collection".to_owned(),
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

// Flatten partition selector fields into a Vec<Label>.
// JSON strings are percent-encoded but un-quoted.
// Other JSON types map to their literal JSON strings.
// *** This MUST match the Go-side behavior! ***
fn push_partitions(fields: &BTreeMap<String, Vec<Value>>, out: &mut Vec<broker::Label>) {
    for (field, value) in fields {
        for value in value {
            let value = match value {
                Value::String(s) => {
                    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC)
                        .to_string()
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
) -> flow::DerivationSpec {
    let tables::Derivation {
        scope: _,
        derivation: _,
        register_schema,
        register_initial,
    } = derivation;

    transforms.sort_by(|l, r| l.transform.cmp(&r.transform));

    flow::DerivationSpec {
        collection: Some(collection.spec.clone()),
        transforms,
        register_schema_uri: register_schema.to_string(),
        register_initial_json: register_initial.to_string(),
    }
}

// encode_resource_path encodes path components into a string which
// is suitable for use within a Gazette token, such as a Journal
// name or suffix, or a Shard ID. Tokens are restricted to
// unicode letters and numbers, plus the symbols `-_+/.=%`.
// All other runes are percent-encoded.
pub fn encode_resource_path(resource_path: &[impl AsRef<str>]) -> String {
    let mut parts = Vec::new();
    parts.extend(resource_path.iter().map(AsRef::as_ref));

    let mut name = String::new();

    for c in parts.join("/").chars() {
        match c {
            // Note that '%' is not included (it must be escaped).
            '-' | '_' | '+' | '/' | '.' | '=' => name.push(c),
            _ if c.is_alphanumeric() => name.push(c),
            c => name.extend(percent_encoding::utf8_percent_encode(
                &c.to_string(),
                percent_encoding::NON_ALPHANUMERIC,
            )),
        }
    }

    name
}

#[cfg(test)]
mod test {
    use super::encode_resource_path;

    #[test]
    fn test_name_escapes() {
        let out = encode_resource_path(&vec![
            "he!lo৬".to_string(),
            "a/part%".to_string(),
            "_¾the-=res+.".to_string(),
        ]);
        assert_eq!(&out, "he%21lo৬/a/part%25/_¾the-=res+.");
    }
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
