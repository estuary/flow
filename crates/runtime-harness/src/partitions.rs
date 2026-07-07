//! Logical-partition routing: map a document to the [`CollectionStore`] journal
//! of its logical partition, and to that partition's label set.
//!
//! This is the harness stand-in for V1's `flow.NewMapper`: given a collection's
//! partition fields, each document routes to a journal named
//! `{collection}/{field=value}/.../pivot=00` — one physical partition per
//! logical partition, covering the full key range. An unpartitioned collection
//! routes every document to `{collection}/pivot=00`
//! ([`default_partition_journal`](crate::store::default_partition_journal)).
//!
//! The label set carries the partition's `estuary.dev/field/*` labels, which
//! Verify matches against a step's partition selector (see [`crate::steps`]).

use crate::store::CollectionStore;
use anyhow::Context;
use proto_flow::flow::CollectionSpec;
use proto_gazette::broker::LabelSet;

/// Partition-routing state for one collection: its sorted partition fields and
/// their document extractors. Built once from a [`CollectionSpec`] and reused
/// across all its documents.
pub struct Partitioning {
    collection: String,
    fields: Vec<String>,
    extractors: Vec<doc::Extractor>,
}

impl Partitioning {
    /// Build routing for `collection` from its partition fields and projections.
    pub fn for_collection(collection: &CollectionSpec) -> anyhow::Result<Self> {
        let extractors = extractors::for_fields(
            &collection.partition_fields,
            &collection.projections,
            &doc::SerPolicy::noop(),
        )
        .context("building partition-field extractors")?;

        Ok(Self {
            collection: collection.name.clone(),
            fields: collection.partition_fields.clone(),
            extractors,
        })
    }

    /// The store journal name and logical-partition label set for `doc`.
    /// The label set is empty when the collection has no partition fields.
    pub fn route<N: json::AsNode>(&self, doc: &N) -> anyhow::Result<(String, LabelSet)> {
        if self.fields.is_empty() {
            return Ok((
                crate::store::default_partition_journal(&self.collection),
                LabelSet::default(),
            ));
        }

        let mut name = format!("{}/", self.collection);
        name = labels::partition::append_extracted_fields_name_suffix(
            name,
            &self.fields,
            &self.extractors,
            doc,
        )
        .context("encoding partition fields into journal name")?;
        name = labels::partition::append_key_range_name_suffix(name, u32::MIN);

        let set = labels::partition::encode_extracted_fields_labels(
            LabelSet::default(),
            &self.fields,
            &self.extractors,
            doc,
        )
        .context("encoding partition-field labels")?;

        Ok((name, set))
    }
}

/// Route `doc` (already serialized to `body`) into `store` under its logical
/// partition, registering the partition's labels and stamping the publication
/// `clock`. Returns the journal it landed in.
pub fn append_routed(
    store: &mut CollectionStore,
    routing: &Partitioning,
    doc: &serde_json::Value,
    body: Vec<u8>,
    clock: u64,
) -> anyhow::Result<String> {
    let (journal, labels) = routing.route(doc)?;
    store.register_partition(&journal, labels);
    store.append(&journal, body, clock);
    Ok(journal)
}
