use std::time::Duration;

use super::names::*;

use schemars::schema;
use serde_json::{from_value, json};

impl Collection {
    pub fn example() -> Self {
        Self::new("acmeCo/collection")
    }
    pub fn schema(_: &mut schemars::gen::SchemaGenerator) -> schema::Schema {
        from_value(json!({
            "type": "string",
            "pattern": "^[^ \t\n\\!@#$%^&*()+=\\<\\>?;:'\"\\[\\]\\|~`]+$",
        }))
        .unwrap()
    }
}

impl Transform {
    pub fn example() -> Self {
        Self::new("my transform")
    }
}

impl Capture {
    pub fn example() -> Self {
        Self::new("acmeCo/capture")
    }
}

impl Materialization {
    pub fn example() -> Self {
        Self::new("acmeCo/materialization")
    }
}

impl Test {
    pub fn example() -> Self {
        Self::new("my test")
    }
}

impl Rule {
    pub fn example() -> Self {
        Self::new("00: Rule")
    }
}

impl JsonPointer {
    pub fn example() -> Self {
        Self::new("/json/ptr")
    }
    pub fn schema(_: &mut schemars::gen::SchemaGenerator) -> schema::Schema {
        from_value(json!({
            "type": "string",
            "pattern": "^(/[^/]+)*$",
        }))
        .unwrap()
    }
}

impl Lambda {
    pub fn example_typescript() -> Self {
        Self::Typescript
    }
    pub fn example_remote() -> Self {
        Self::Remote("http://example/api".to_string())
    }
}

impl PartitionSelector {
    pub fn example() -> Self {
        from_value(json!({
            "include": {
                "a_partition": ["A", "B"],
            },
            "exclude": {
                "other_partition": [32, 64],
            }
        }))
        .unwrap()
    }
}

impl CompressionCodec {
    pub fn example() -> Self {
        CompressionCodec::GzipOffloadDecompression
    }
}

impl BucketType {
    pub fn example() -> Self {
        BucketType::S3
    }
}

impl Store {
    pub fn example() -> Self {
        Self {
            provider: BucketType::S3,
            bucket: "my-bucket".to_string(),
            prefix: None,
        }
    }
}

impl StorageMapping {
    pub fn example() -> Self {
        Self {
            prefix: "acmeCo/widgets".to_string(),
            stores: vec![Store::example()],
        }
    }
}

impl FragmentTemplate {
    pub fn example() -> Self {
        Self {
            compression_codec: Some(CompressionCodec::Zstandard),
            flush_interval: Some(Duration::from_secs(3600)),
            ..Default::default()
        }
    }
    pub fn duration_schema(g: &mut schemars::gen::SchemaGenerator) -> schema::Schema {
        duration_schema(g)
    }
}

impl JournalTemplate {
    pub fn example() -> Self {
        Self {
            fragments: FragmentTemplate::example(),
        }
    }
}

impl ShardTemplate {
    pub fn example() -> Self {
        Self {
            max_txn_duration: Some(Duration::from_secs(30)),
            hot_standbys: Some(1),
            ..Default::default()
        }
    }
}

fn duration_schema(_: &mut schemars::gen::SchemaGenerator) -> schema::Schema {
    from_value(json!({
        "type": ["string", "null"],
        "pattern": "^\\d+(s|m|h)$"
    }))
    .unwrap()
}
