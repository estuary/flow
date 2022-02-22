use schemars::{schema::{RootSchema}, JsonSchema};
use serde_json::Value;

pub fn create_root_schema<T: JsonSchema>() -> RootSchema {
    let mut settings = schemars::gen::SchemaSettings::draft07();
    settings.inline_subschemas = true;
    let generator = schemars::gen::SchemaGenerator::new(settings);
    return generator.into_root_schema_for::<T>();
}

pub fn extract_subobject(mut v: Value, key: &str) -> (Option<Value>, Value) {
    let mut sub_object = None;

    if let Value::Object(ref mut m) = v {
        sub_object = m.remove(key)
    }

    (sub_object, v)
}