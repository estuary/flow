use anyhow::Context;
use doc_poc as doc;
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

#[derive(Debug, Serialize, thiserror::Error)]
#[error("JSON error in document: {doc}")]
pub struct JsonError {
    pub doc: String,
    #[serde(serialize_with = "crate::serialize_as_display")]
    pub err: serde_json::Error,
}

impl JsonError {
    pub fn new(data: impl AsRef<[u8]>, err: serde_json::Error) -> JsonError {
        let doc = String::from_utf8_lossy(data.as_ref()).into_owned();
        JsonError { doc, err }
    }
}

fn serialize_as_display<T, S>(thing: T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: std::fmt::Display,
    S: serde::ser::Serializer,
{
    let s = thing.to_string();
    serializer.serialize_str(&s)
}

// ValidatorGuard encapsulates the compilation and indexing of a JSON schema,
// tied to the lifetime of a Validator which references it. It allows the
// Validator and Index to flexibly reference the built schema while making it
// difficult to misuse, since the Validator lifetime is tied to the Index
// and Schema.
struct ValidatorGuard {
    schema: Box<doc::Schema>,
    _index: Box<doc::SchemaIndex<'static>>,
    validator: doc::Validator<'static>,
}

impl ValidatorGuard {
    fn new(schema: &str) -> Result<Self, anyhow::Error> {
        // Bundled schemas carry their own $id so this isn't used in practice.
        let curi = url::Url::parse("https://example").unwrap();
        let schema: serde_json::Value =
            serde_json::from_str(&schema).context("decoding JSON-schema")?;
        let schema: doc::Schema =
            json::schema::build::build_schema(curi, &schema).context("building schema")?;

        let schema = Box::new(schema);
        let schema_static =
            unsafe { std::mem::transmute::<&'_ doc::Schema, &'static doc::Schema>(&schema) };

        let mut index = doc::SchemaIndexBuilder::new();
        index.add(schema_static).context("adding schema to index")?;
        index
            .verify_references()
            .context("verifying schema index references")?;

        let index = Box::new(index.into_index());
        let index_static = unsafe {
            std::mem::transmute::<&'_ doc::SchemaIndex, &'static doc::SchemaIndex>(&index)
        };

        let validator = doc::Validator::new(index_static);

        Ok(Self {
            schema,
            _index: index,
            validator,
        })
    }
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
