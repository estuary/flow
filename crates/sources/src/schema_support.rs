use super::proto_serde::*;
use super::specs::*;
use models::names;
use schemars::schema;
use serde_json::{from_value, json, Value};
use std::collections::BTreeMap;

impl Catalog {
    pub fn default_node_dependencies() -> BTreeMap<String, String> {
        from_value(json!({"a-npm-package": "^1.2.3"})).unwrap()
    }
    pub fn default_test() -> Value {
        json!({"Test that fob quips ipsum": []})
    }
    pub fn example_import() -> Vec<RelativeUrl> {
        vec![
            RelativeUrl::example_relative(),
            RelativeUrl::example_absolute(),
        ]
    }
    pub fn example_collections() -> BTreeMap<names::Collection, CollectionDef> {
        vec![(names::Collection::example(), CollectionDef::example())]
            .into_iter()
            .collect()
    }
    pub fn example_test() -> Value {
        json!({
            "Test that fob quips ipsum": [
                TestStep::example_ingest(),
                TestStep::example_verify(),
            ]
        })
    }
}

impl CollectionDef {
    pub fn example() -> Self {
        from_value(json!({
            "schema": RelativeUrl::example_relative(),
            "key": names::CompositeKey::example(),
        }))
        .unwrap()
    }
}

impl Projections {
    pub fn example() -> Self {
        from_value(json!({
            "a_field": names::JsonPointer::example(),
            "a_partition": {
                "location": names::JsonPointer::example(),
                "partition": true,
            }
        }))
        .unwrap()
    }
}

impl Derivation {
    pub fn example() -> Self {
        from_value(json!({
            "transform": {
                "nameOfTransform": Transform::example(),
            }
        }))
        .unwrap()
    }
    pub fn default_transform() -> Value {
        json!({"nameOfTransform": {"source": {"name": "a/source/collection"}}})
    }
}

impl Transform {
    pub fn example() -> Self {
        from_value(json!({
            "source": TransformSource::example(),
            "publish": Publish::example(),
            "update": null,
        }))
        .unwrap()
    }

    pub fn read_delay_schema(g: &mut schemars::gen::SchemaGenerator) -> schema::Schema {
        duration_schema(g)
    }
}

impl Update {
    pub fn example() -> Self {
        from_value(json!({
            "lambda": names::Lambda::example_typescript(),
        }))
        .unwrap()
    }
}

impl Publish {
    pub fn example() -> Self {
        from_value(json!({
            "lambda": names::Lambda::example_typescript(),
        }))
        .unwrap()
    }
}

impl TransformSource {
    pub fn example() -> Self {
        Self {
            name: names::Collection::new("source/collection"),
            schema: None,
            partitions: None,
        }
    }
}

impl Schema {
    pub fn example_absolute() -> Self {
        from_value(json!("http://example/schema#/$defs/subPath")).unwrap()
    }
    pub fn example_relative() -> Self {
        from_value(json!("../path/to/schema#/$defs/subPath")).unwrap()
    }
    pub fn example_inline_basic() -> Self {
        from_value(json!({
            "type": "object",
            "properties": {
                "foo": { "type": "integer" },
                "bar": { "const": 42 }
            }
        }))
        .unwrap()
    }
    pub fn example_inline_counter() -> Self {
        from_value(json!({
            "type": "object",
            "reduce": {"strategy": "merge"},
            "properties": {
                "foo_count": {
                    "type": "integer",
                    "reduce": {"strategy": "sum"},
                }
            }
        }))
        .unwrap()
    }
}

impl EndpointRef {
    pub fn example() -> Self {
        Self {
            name: names::Endpoint::example(),
            config: vec![("table".to_string(), json!("a_sql_table"))]
                .into_iter()
                .collect(),
        }
    }
}

impl MaterializationSource {
    pub fn example() -> Self {
        Self {
            name: names::Collection::new("source/collection"),
            partitions: None,
        }
    }
}

impl MaterializationFields {
    pub fn example() -> Self {
        MaterializationFields {
            include: vec![("added".to_string(), names::Object::new())]
                .into_iter()
                .collect(),
            exclude: vec!["removed".to_string()],
            recommended: true,
        }
    }
}

impl CaptureTarget {
    pub fn example() -> Self {
        Self {
            name: names::Collection::new("target/collection"),
        }
    }
}

impl RelativeUrl {
    pub fn example_relative() -> Self {
        Self("../path/to/local.yaml".to_owned())
    }
    pub fn example_absolute() -> Self {
        Self("https://example/resource".to_owned())
    }
}

fn duration_schema(_: &mut schemars::gen::SchemaGenerator) -> schema::Schema {
    from_value(json!({
        "type": ["string", "null"],
        "pattern": "^\\d+(s|m|h)$"
    }))
    .unwrap()
}

impl Label {
    pub fn example() -> Self {
        Self {
            name: "a/label".to_owned(),
            value: "value".to_owned(),
        }
    }
}

impl LabelSet {
    pub fn example() -> Self {
        Self {
            labels: vec![Label::example()],
        }
    }
}

impl LabelSelector {
    pub fn example() -> Self {
        Self {
            include: LabelSet::example(),
            exclude: LabelSet::example(),
        }
    }
}
impl JournalSpec {
    pub fn example() -> Self {
        from_value(json!({
            "fragment": JournalSpecFragment::example(),
        }))
        .unwrap()
    }
}

impl JournalSpecFragment {
    pub fn example() -> Self {
        from_value(json!({
            "stores": ["s3://bucket/and/path"],
        }))
        .unwrap()
    }
    pub fn duration_schema(g: &mut schemars::gen::SchemaGenerator) -> schema::Schema {
        duration_schema(g)
    }
}

impl CompressionCodec {
    pub fn example() -> Self {
        CompressionCodec::GzipOffloadDecompression
    }
}

impl JournalRule {
    pub fn example() -> Self {
        from_value(json!({
            "selector": {
                "include": {
                    "labels": [
                        {"name": "estuary.dev/collection", "value": "a/collection"},
                    ],
                },
            },
            "template": {
                "fragment": {
                    "stores": ["s3://my-bucket/path"],
                }
            },
        }))
        .unwrap()
    }
}

impl TestStep {
    pub fn example_ingest() -> Self {
        TestStep::Ingest(TestStepIngest::example())
    }
    pub fn example_verify() -> Self {
        TestStep::Verify(TestStepVerify::example())
    }
}

impl TestStepIngest {
    pub fn example() -> Self {
        from_value(json!({
            "collection": names::Collection::example(),
            "documents": [
                {"example": "document"},
                {"another": "document"},
            ]
        }))
        .unwrap()
    }
}

impl TestStepVerify {
    pub fn example() -> Self {
        from_value(json!({
            "collection": names::Collection::example(),
            "documents": [
                {"expected": "document"},
            ],
        }))
        .unwrap()
    }
}
