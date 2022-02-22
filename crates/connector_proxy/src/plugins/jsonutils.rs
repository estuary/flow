use schemars::{schema::RootSchema, JsonSchema};
use serde_json::Value;

// Create the RootSchema given datatype T.
pub fn create_root_schema<T: JsonSchema>() -> RootSchema {
    let mut settings = schemars::gen::SchemaSettings::draft07();
    settings.inline_subschemas = true;
    let generator = schemars::gen::SchemaGenerator::new(settings);
    return generator.into_root_schema_for::<T>();
}

// Extract the sub object keyed at `key` from v if v is an object, and returns both the remainder and the removed value.
pub fn remove_subobject(mut v: Value, key: &str) -> (Option<Value>, Value) {
    let mut sub_object = None;

    if let Value::Object(ref mut m) = v {
        sub_object = m.remove(key)
    }

    (sub_object, v)
}
