mod actor;
mod connector;
mod drain;
mod handler;
mod scan;
mod startup;
mod task;

pub(crate) use handler::serve;

#[derive(Debug)]
struct Binding {
    collection_name: String,             // Source collection.
    delta_updates: bool,                 // Delta updates, or standard?
    key_extractors: Vec<doc::Extractor>, // Key extractors for this collection.
    read_schema_json: bytes::Bytes,      // Read JSON-Schema of collection documents.
    ser_policy: doc::SerPolicy,          // Serialization policy for this source.
    state_key: String,                   // State key for this binding.
    store_document: bool, // Are we storing the root document (often `flow_document`)?
    value_plan: doc::ExtractorPlan,
}

// Set of observed keys, used to de-duplicate sent C:Load requests.
type LoadKeys = std::collections::HashSet<u128, std::hash::BuildHasherDefault<IdentHasher>>;

/// Identity hasher over the 16-byte xxh3_128 key digests we insert.
/// We trust the digest to be well-distributed and avoid re-hashing it.
#[derive(Default)]
struct IdentHasher(u64);

impl std::hash::Hasher for IdentHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0 = u64::from_ne_bytes(bytes[..8].try_into().unwrap());
    }

    fn finish(&self) -> u64 {
        self.0
    }
}
