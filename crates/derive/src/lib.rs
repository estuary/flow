use serde::Serialize;

pub mod extract_api;

pub use extract_api::extract_uuid_parts;

/// DebugJson is a new-type wrapper around any Serialize implementation
/// that wishes to support the Debug trait via JSON encoding itself.
pub struct DebugJson<S: Serialize>(pub S);

impl<S: Serialize> std::fmt::Debug for DebugJson<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&serde_json::to_string(&self.0).unwrap())
    }
}

pub fn new_validator(schema: &[u8]) -> Result<doc::Validator, anyhow::Error> {
    let schema = doc::validation::build_bundle(schema)?;
    let validator = doc::Validator::new(schema)?;
    Ok(validator)
}

/// Common test utilities used by sub-modules.
#[cfg(test)]
pub mod test {
    // Build a test schema fixture. Use gross Box::leak to coerce a 'static lifetime.
    pub fn build_min_max_sum_schema() -> String {
        let schema = serde_json::json!({
            "properties": {
                "min": {
                    "type": "integer",
                    "reduce": {"strategy": "minimize"}
                },
                "max": {
                    "type": "number",
                    "reduce": {"strategy": "maximize"}
                },
                "sum": {
                    "type": "number",
                    "reduce": {"strategy": "sum"},
                },
            },
            "reduce": {"strategy": "merge"},

            // If "positive" property is present, then "sum" must be >= 0.
            "dependentSchemas": {
                "positive": {
                    "properties": {
                        "sum": { "minimum": 0 }
                    }
                }
            }
        });
        schema.to_string()
    }
}
