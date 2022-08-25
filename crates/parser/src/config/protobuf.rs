use schemars::{
    gen::SchemaGenerator,
    schema::{InstanceType, Schema, SchemaObject},
    JsonSchema,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProtobufConfig {
    /// The contents of the .proto that defines the message type to deserialize.
    #[serde(rename = "protoFile")]
    #[schemars(title = "Your .proto file", schema_with = "proto_file_schema")]
    pub proto_file_content: String,

    /// The name of the protobuf Message to deserialize as. Must be defined within the given proto
    /// file.
    pub message: String,
}

fn proto_file_schema(_gen: &mut SchemaGenerator) -> Schema {
    let mut extra = schemars::Map::new();
    extra.insert("multiline".to_string(), serde_json::Value::Bool(true));
    let obj = SchemaObject {
        instance_type: Some(InstanceType::String.into()),
        extensions: extra,
        ..Default::default()
    };

    Schema::Object(obj)
}
