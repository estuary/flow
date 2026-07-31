//! The INGEST and VERIFY test steps, ported from `go/runtime/testing.go`
//! (`FlowTesting.Ingest`) and `go/testing/driver.go` (`ClusterDriver.Verify`).
//!
//! Both combine documents by the collection key through [`doc::combine`], exactly
//! as V1 drives its runtime combiner RPC:
//!
//! - **Ingest** combines the fixture documents under the collection's *write*
//!   schema with associative (`full = false`) reductions — matching input
//!   cardinality to what a real capture would publish — then routes each combined
//!   document to its logical-partition journal in the [`CollectionStore`].
//! - **Verify** fetches the documents a collection's matching partitions grew by
//!   during the current test case (the `(from, to]` window), combines them under
//!   the *read* schema with full (`full = true`) reductions to one document per
//!   key, masks their UUIDs, and compares against the step's expected documents
//!   with the [`crate::diff`] comparator.
//!
//! Combined documents drain in key order, and build-time validation guarantees
//! the expected documents are in key order too, so the comparator walks both in
//! lock-step.

use crate::clock::Clock;
use crate::diff::{self, Mismatch};
use crate::partitions::{self, Partitioning};
use crate::store::CollectionStore;
use anyhow::Context;
use proto_flow::flow::{CollectionSpec, test_spec::Step};
use proto_gazette::broker::{LabelSelector, LabelSet};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Combine `docs` by the collection key (write schema, associative reductions)
/// and append each combined document to its logical-partition journal, stamped
/// with a monotonic publication `clock`. Returns the collection's resulting write
/// clock (per-partition-journal document counts) — the ingest step's `write_at`.
pub fn ingest(
    store: &Arc<Mutex<CollectionStore>>,
    clock: &AtomicU64,
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
        let at = clock.fetch_add(1, Ordering::Relaxed);
        partitions::append_routed(&mut store, &routing, &value, body, at)?;
    }

    Ok(store.write_clock(&collection.name))
}

/// The outcome of a Verify step: the actual documents observed (combined, in key
/// order, with UUIDs masked) and any comparison failures. An empty `failures`
/// is a pass; `actuals` is retained regardless for snapshot-on-failure parity
/// with V1's `--snapshot`.
pub struct VerifyOutcome {
    pub actuals: Vec<Value>,
    pub failures: Vec<Mismatch>,
}

/// Verify a collection's documents written in the `(from, to]` window against the
/// step's expected documents. Only partitions matching the step's selector are
/// read; documents are combined by key under the read schema, UUID-masked, and
/// compared with the [`crate::diff`] comparator.
pub fn verify(
    store: &Arc<Mutex<CollectionStore>>,
    collection: &CollectionSpec,
    step: &Step,
    from: &Clock,
    to: &Clock,
) -> anyhow::Result<VerifyOutcome> {
    // Verify uses the read schema, falling back to the write schema (as the
    // runtime's combiner does when a collection has no distinct read schema).
    let schema = if collection.read_schema_json.is_empty() {
        &collection.write_schema_json
    } else {
        &collection.read_schema_json
    };
    let mut acc = build_accumulator(collection, schema, true)?;

    // Select the collection's partitions matching the step's selector, then feed
    // their windowed documents into the combiner.
    let fetched: Vec<Vec<u8>> = {
        let store = store.lock().unwrap();
        let journals = matching_journals(&store, &collection.name, step.partitions.as_ref())?;
        store
            .read_collection_window(&journals, from, to)
            .into_iter()
            .map(|d| d.doc.clone())
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
        let mut value = serde_json::to_value(doc::SerPolicy::noop().on_owned(&drained.root))
            .context("serializing combined verify document")?;
        diff::mask_uuid(&mut value, &collection.uuid_ptr);
        actuals.push(value);
    }

    let expected: Vec<Value> = step
        .docs_json_vec
        .iter()
        .map(|d| serde_json::from_slice(d).context("parsing expected verify document"))
        .collect::<anyhow::Result<_>>()?;

    let failures = diff::compare_documents(&actuals, &expected);
    Ok(VerifyOutcome { actuals, failures })
}

/// Build a single-binding combine [`Accumulator`](doc::combine::Accumulator) for
/// `collection` over `schema_json`, keyed by the collection key. `full` selects
/// full (one-document-per-key) vs. associative reductions.
fn build_accumulator(
    collection: &CollectionSpec,
    schema_json: &[u8],
    full: bool,
) -> anyhow::Result<doc::combine::Accumulator> {
    let schema =
        doc::validation::build_bundle(schema_json).context("parsing collection schema bundle")?;
    let validator = doc::Validator::new(schema).context("indexing collection schema bundle")?;
    let key = extractors::for_key(
        &collection.key,
        &collection.projections,
        &doc::SerPolicy::noop(),
    )
    .context("building collection key extractors")?;

    let spec = doc::combine::Spec::with_one_binding(
        full,
        key,
        collection.name.clone(),
        Vec::new(),
        validator,
    );
    doc::combine::Accumulator::new(spec, tempfile::tempfile().context("combine spill file")?)
        .context("building combine accumulator")
}

/// The collection's store journals whose logical partition matches `selector`.
/// The step's selector also carries `estuary.dev/collection` and `name:prefix`
/// labels, but every journal here already belongs to `collection`, so only its
/// `estuary.dev/field/*` labels are considered (see [`field_only_selector`]).
fn matching_journals(
    store: &CollectionStore,
    collection: &str,
    selector: Option<&LabelSelector>,
) -> anyhow::Result<Vec<String>> {
    let reduced = selector.map(field_only_selector);

    let mut out = Vec::new();
    for journal in store.journals_of(collection) {
        let matched = match &reduced {
            None => true,
            Some(selector) => {
                let set = store.partition_labels_of(&journal);
                labels::matches(selector, &set)
                    .with_context(|| format!("matching partition selector against {journal}"))?
            }
        };
        if matched {
            out.push(journal);
        }
    }
    Ok(out)
}

/// Reduce a partition [`LabelSelector`] to only its `estuary.dev/field/*` labels.
/// Field labels retain their (already-sorted) order, which [`labels::matches`]
/// requires.
fn field_only_selector(selector: &LabelSelector) -> LabelSelector {
    let field_only = |set: Option<&LabelSet>| -> Option<LabelSet> {
        set.map(|set| LabelSet {
            labels: set
                .labels
                .iter()
                .filter(|l| l.name.starts_with(labels::FIELD_PREFIX))
                .cloned()
                .collect(),
        })
    };
    LabelSelector {
        include: field_only(selector.include.as_ref()),
        exclude: field_only(selector.exclude.as_ref()),
    }
}
