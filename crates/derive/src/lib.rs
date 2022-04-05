pub mod combine_api;
pub mod derive_api;
pub mod extract_api;
pub mod schema_api;

pub mod combiner;
mod pipeline;
mod registers;

pub use extract_api::extract_uuid_parts;
use protocol::flow::DocsAndBytes;
use serde::Serialize;

/// A type that can accumulate statistics that can be periodically drained.
/// This trait exists primarily to help with readability and consistency. It doesn't get used as a
/// type parameter or turned into a trait object, but instead serves to identify things that
/// accumultate stats and have some ability to return them and reset.
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

/// Common test utilities used by sub-modules.
#[cfg(test)]
pub mod test {
    use doc;
    use serde_json::json;
    use url::Url;

    // Build a test schema fixture. Use gross Box::leak to coerce a 'static lifetime.
    pub fn build_min_max_sum_schema() -> (&'static doc::SchemaIndex<'static>, Url) {
        let schema = json!({
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

        let uri = Url::parse("https://example/schema").unwrap();
        let scm: doc::Schema = json::schema::build::build_schema(uri.clone(), &schema).unwrap();
        let scm = Box::leak(Box::new(scm));

        let mut idx = doc::SchemaIndexBuilder::new();
        idx.add(scm).unwrap();
        idx.verify_references().unwrap();

        let idx = Box::leak(Box::new(idx.into_index()));
        (idx, uri)
    }
}
