use anyhow::Context;
use doc::{ptr::Token, AsNode, Node, Pointer};
use proto_flow::flow::{self, DocsAndBytes, UuidParts};
use serde::Serialize;
use time::{
    format_description::well_known::Rfc3339,
    macros::{date, time},
    Duration, PrimitiveDateTime,
};

pub mod combine_api;
pub mod extract_api;

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

/// Extract a UUID at the given location within the document, returning its UuidParts,
/// or None if the Pointer does not resolve to a valid v1 UUID.
pub fn extract_uuid_parts<'n, N: AsNode>(v: &'n N, ptr: &doc::Pointer) -> Option<flow::UuidParts> {
    let Some(v_uuid) = ptr.query(v) else {
        return None
    };

    match v_uuid.as_node() {
        Node::String(uuid_str) => uuid::Uuid::parse_str(uuid_str).ok().and_then(|u| {
            if u.get_version_num() != 1 {
                return None;
            }
            let (c_low, c_mid, c_high, seq_node_id) = u.as_fields();

            Some(flow::UuidParts {
                clock: (c_low as u64) << 4          // Clock low bits.
            | (c_mid as u64) << 36                  // Clock middle bits.
            | (c_high as u64) << 52                 // Clock high bits.
            | ((seq_node_id[0] as u64) >> 2) & 0xf, // High 4 bits of sequence number.

                node: (seq_node_id[2] as u64) << 56 // 6 bytes of big-endian node ID.
            | (seq_node_id[3] as u64) << 48
            | (seq_node_id[4] as u64) << 40
            | (seq_node_id[5] as u64) << 32
            | (seq_node_id[6] as u64) << 24
            | (seq_node_id[7] as u64) << 16
            | ((seq_node_id[0] as u64) & 0x3) << 8 // High 2 bits of flags.
            | (seq_node_id[1] as u64), // Low 8 bits of flags.
            })
        }),
        _ => None,
    }
}

// According to RFC4122: https://www.rfc-editor.org/rfc/rfc4122#section-4.1.4
// The calendar starts on 00:00:00.00, 15 October 1582
const GREG_START: PrimitiveDateTime = PrimitiveDateTime::new(date!(1582 - 10 - 15), time!(0:00));

pub fn uuid_parts_to_timestamp(parts: &UuidParts) -> PrimitiveDateTime {
    // UUID timestamps count from the gregorian calendar start, not the unix epoch.
    // Clock values count in increments of 100ns

    // shift off the lowest 4 bits, which represent the sequence counter
    // and convert from 100ns increments to microseconds (10 ns increments)
    let ts_greg_micros = ((parts.clock >> 4) / 10) as i64;

    // Now we just need to add our microseconds value, and we've got a timestamp cooking!
    GREG_START.saturating_add(Duration::microseconds(ts_greg_micros))
}

pub trait PointerExt {
    // Extend the behavior of `Pointer::query` to handle virtual fields like UUID timestamp extraction
    fn query_and_resolve_virtuals<'n, N: AsNode, CB, CBReturn>(
        &self,
        uuid_ptr: Option<Pointer>,
        node: &'n N,
        cb: CB,
    ) -> CBReturn
    where
        CB: for<'a> FnMut(Option<&Node<'a, N>>) -> CBReturn;
}

impl PointerExt for Pointer {
    fn query_and_resolve_virtuals<'n, N: AsNode, CB, CBReturn>(
        &self,
        uuid_ptr: Option<Pointer>,
        node: &'n N,
        mut cb: CB,
    ) -> CBReturn
    where
        CB: for<'a> FnMut(Option<&Node<'a, N>>) -> CBReturn,
    {
        if let Some(uuid_ptr) = uuid_ptr {
            if self.starts_with(&uuid_ptr)
                && self.0.len() == (uuid_ptr.0.len() + 1)
                && self
                    .0
                    .last()
                    .eq(&Some(&Token::Property("timestamp".to_string())))
            {
                let uuid_parts = extract_uuid_parts(node, &uuid_ptr);
                return match uuid_parts {
                    Some(parts) => {
                        // Then let's extract the timestamp from the UUID, and send that as an
                        // RFC3339 string, which is what the schema for the projection is expecting
                        let timestamp_str = uuid_parts_to_timestamp(&parts)
                            .assume_utc()
                            .format(&Rfc3339)
                            .expect("failed to format ts");

                        cb(Some(&Node::String(timestamp_str.as_str())))
                    }
                    None => cb(None),
                };
            }
        }
        cb(self.query(node).map(AsNode::as_node).as_ref())
    }
}
