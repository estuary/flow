use anyhow::Context;
use proto_flow::flow::DocsAndBytes;
use serde::Serialize;

pub mod combine_api;
pub mod derive_api;
pub mod extract_api;

mod pipeline;
mod registers;

pub use extract_api::extract_uuid_parts;

/// A type that can accumulate statistics that can be periodically drained.
/// This trait exists primarily to help with readability and consistency. It doesn't get used as a
/// type parameter or turned into a trait object, but instead serves to identify things that
/// accumulate stats and have some ability to return them and reset.
trait StatsAccumulator: Default {
    /// The type of the stats returned by this accumulator.
    type Stats;

    /// Returns the accumulated stats and resets all of the internal accumulations to zero.
    fn drain(&mut self) -> Self::Stats;

    /// Consumes the accumulator and returns the final Stats. This is just a wrapper around `drain`
    /// to help things read a little more nicely.
    fn into_stats(mut self) -> Self::Stats {
        self.drain()
    }
}

/// A `StatsAccumulator` that corresponds to the `DocsAndBytes` type from the Flow protocol.
/// This is composed in with many other accumulators.
#[derive(Debug, Default, PartialEq)]
pub struct DocCounter(DocsAndBytes);

impl DocCounter {
    pub fn new(docs: u32, bytes: u32) -> DocCounter {
        DocCounter(DocsAndBytes { docs, bytes })
    }

    pub fn add(&mut self, other: &DocCounter) {
        self.0.docs += other.0.docs;
        self.0.bytes += other.0.bytes;
    }

    /// Add a single document of the given size, incrementing `docs` by 1 and `bytes` by
    /// `doc_byte_len`.
    pub fn increment(&mut self, doc_byte_len: u32) {
        self.0.docs += 1;
        self.0.bytes += doc_byte_len;
    }
}

impl StatsAccumulator for DocCounter {
    type Stats = DocsAndBytes;

    fn drain(&mut self) -> DocsAndBytes {
        std::mem::replace(&mut self.0, DocsAndBytes::default())
    }
}

/// DebugJson is a new-type wrapper around any Serialize implementation
/// that wishes to support the Debug trait via JSON encoding itself.
pub struct DebugJson<S: Serialize>(pub S);

impl<S: Serialize> std::fmt::Debug for DebugJson<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&serde_json::to_string(&self.0).unwrap())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("JSON error in document: {doc}")]
pub struct JsonError {
    pub doc: String,
    #[source]
    pub err: serde_json::Error,
}

impl JsonError {
    pub fn new(data: impl AsRef<[u8]>, err: serde_json::Error) -> JsonError {
        let doc = String::from_utf8_lossy(data.as_ref()).into_owned();
        JsonError { doc, err }
    }
}

pub fn new_validator(schema: &str) -> Result<doc::Validator, anyhow::Error> {
    let schema = json::schema::build::build_schema(
        // Bundled schemas carry their own $id so this isn't used in practice.
        url::Url::parse("https://example").unwrap(),
        &serde_json::from_str(&schema).context("parsing bundled JSON schema")?,
    )
    .context("building bundled JSON schema")?;

    Ok(doc::Validator::new(schema).context("preparing schema validator")?)
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
