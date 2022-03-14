use doc::ptr::{Pointer, Token};
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

pub fn tokenize_jsonpointer(ptr: &str) -> Vec<String> {
    Pointer::from_str(&ptr)
        .iter()
        .map(|t| match t {
            // Keep the index and next index for now. Could adjust based on usecases.
            Token::Index(ind) => ind.to_string(),
            Token::Property(prop) => prop.to_string(),
            Token::NextIndex => "-".to_string(),
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_tokenize_jsonpointer() {
        let expected: Vec<String> = vec!["p1", "p2", "56", "p3", "-"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        assert!(expected == tokenize_jsonpointer("/p1/p2/56/p3/-"));
    }
}
