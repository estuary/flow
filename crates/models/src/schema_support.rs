use super::names::*;

use schemars::schema;
use serde_json::{from_value, json};

impl Collection {
    pub fn example() -> Self {
        Self::new("a/collection")
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
        Self::new("a transform")
    }
}

impl Endpoint {
    pub fn example() -> Self {
        Self::new("an endpoint")
    }
}

impl Test {
    pub fn example() -> Self {
        Self::new("a test")
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
            "pattern": "^(/[^/]+)*",
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
