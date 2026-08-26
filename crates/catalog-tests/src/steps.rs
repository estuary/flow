//! The INGEST and VERIFY test steps.
//!
//! Both combine documents by the collection key through [`doc::combine`], as the
//! runtime itself does: Ingest with associative reductions under the collection's
//! *write* schema, matching the cardinality a real capture would publish, and
//! Verify with full reductions under its *read* schema, to one document per key.

use crate::clock::Clock;
use crate::diff::{self, Mismatch};
use crate::partitions::{self, Partitioning};
use crate::store::CollectionStore;
use anyhow::Context;
use proto_flow::flow::{CollectionSpec, test_spec::Step};
use proto_gazette::broker::LabelSelector;
use serde_json::Value;
use std::sync::{Arc, Mutex};

/// Combine `docs` by the collection key (write schema, associative reductions)
/// and append each combined document to its logical-partition journal. Returns
/// the collection's resulting write clock (per-partition-journal document
/// counts) — the ingest step's `write_at`.
pub fn ingest(
    store: &Arc<Mutex<CollectionStore>>,
    collection: &CollectionSpec,
    docs: &[bytes::Bytes],
) -> anyhow::Result<Clock> {
    let routing = Partitioning::for_collection(collection)?;
    let mut acc = build_accumulator(collection, &collection.write_schema_json, false)?;

    for raw in docs {
        let value: Value =
            serde_json::from_slice(raw).context("parsing ingest fixture document")?;
        let memtable = acc.memtable().context("acquiring combiner memtable")?;
        let node = doc::HeapNode::from_node(&value, memtable.alloc());
        memtable
            .add(0, node, false)
            .context("adding ingest document to combiner")?;
    }

    let mut drainer = acc.into_drainer().context("draining ingest combiner")?;
    let mut store = store.lock().unwrap();
    while let Some(drained) = drainer.drain_next().context("combining ingest documents")? {
        let value = serde_json::to_value(doc::SerPolicy::noop().on_owned(&drained.root))
            .context("serializing combined ingest document")?;
        let body = serde_json::to_vec(&value).expect("serializing a serde_json::Value cannot fail");
        partitions::append_routed(&mut store, &routing, &value, body)?;
    }

    Ok(store.write_clock(partitions::template_name(collection)?))
}

/// Verify a collection's documents written in the `(from, to]` window against the
/// step's expected documents, returning the comparison failures (empty on a
/// pass). Only partitions matching the step's selector are read; documents are
/// combined by key under the read schema and compared with [`crate::diff`].
pub fn verify(
    store: &Arc<Mutex<CollectionStore>>,
    collection: &CollectionSpec,
    step: &Step,
    from: &Clock,
    to: &Clock,
) -> anyhow::Result<Vec<Mismatch>> {
    // Documents are read under the collection's read schema, or its write schema
    // if it doesn't declare a distinct one.
    let read_schema_json = if collection.read_schema_json.is_empty() {
        &collection.write_schema_json
    } else {
        &collection.read_schema_json
    };
    let mut acc = build_accumulator(collection, read_schema_json, true)?;

    let fetched: Vec<Vec<u8>> = {
        let store = store.lock().unwrap();
        let journals = matching_journals(
            &store,
            partitions::template_name(collection)?,
            step.partitions.as_ref(),
        )?;
        store
            .read_collection_window(&journals, from, to)
            .into_iter()
            .cloned()
            .collect()
    };
    for raw in &fetched {
        let value: Value =
            serde_json::from_slice(raw).context("parsing stored document for verify")?;
        let memtable = acc.memtable().context("acquiring combiner memtable")?;
        let node = doc::HeapNode::from_node(&value, memtable.alloc());
        memtable
            .add(0, node, false)
            .context("adding stored document to combiner")?;
    }

    let mut drainer = acc.into_drainer().context("draining verify combiner")?;
    let mut actuals = Vec::new();
    while let Some(drained) = drainer.drain_next().context("combining verify documents")? {
        actuals.push(
            serde_json::to_value(doc::SerPolicy::noop().on_owned(&drained.root))
                .context("serializing combined verify document")?,
        );
    }

    let expected: Vec<Value> = step
        .docs_json_vec
        .iter()
        .map(|d| serde_json::from_slice(d).context("parsing expected verify document"))
        .collect::<anyhow::Result<_>>()?;

    Ok(diff::compare_documents(&actuals, &expected))
}

/// Build a single-binding combine [`Accumulator`](doc::combine::Accumulator) for
/// `collection` over `schema_json`. `full` selects full (one-document-per-key)
/// vs. associative reductions.
fn build_accumulator(
    collection: &CollectionSpec,
    schema_json: &[u8],
    full: bool,
) -> anyhow::Result<doc::combine::Accumulator> {
    let schema = doc::validation::build_bundle(schema_json)
        .with_context(|| format!("collection {} schema is not a JSON schema", collection.name))?;
    let validator = doc::Validator::new(schema)
        .with_context(|| format!("could not index collection {} schema", collection.name))?;

    let key = extractors::for_key(
        &collection.key,
        &collection.projections,
        &doc::SerPolicy::noop(),
    )
    .with_context(|| {
        format!(
            "could not build collection {} key extractors",
            collection.name
        )
    })?;

    let spec = doc::combine::Spec::with_one_binding(
        full,
        key,
        collection.name.clone(),
        Vec::new(), // Redaction salt is deliberately empty (see crate README).
        validator,
    );

    doc::combine::Accumulator::new(spec, tempfile::tempfile().context("combine spill file")?)
        .context("building combine accumulator")
}

/// The store journals of the collection with partition template `template_name`
/// whose labels match `selector`.
fn matching_journals(
    store: &CollectionStore,
    template_name: &str,
    selector: Option<&LabelSelector>,
) -> anyhow::Result<Vec<String>> {
    let mut out = Vec::new();
    for journal in store.journals_of(template_name) {
        let matched = match selector {
            None => true,
            Some(selector) => labels::matches(selector, store.partition_labels_of(&journal))
                .with_context(|| format!("matching partition selector against {journal}"))?,
        };
        if matched {
            out.push(journal);
        }
    }
    Ok(out)
}
