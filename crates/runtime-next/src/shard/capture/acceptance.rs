//! Acceptance gates for capture de-duplication, over one synthetic fan-in
//! capture: N bindings spread evenly across M target collections.
//!
//! Two claims, deliberately of different kinds:
//!
//! - **Structural** — the task builds M of each collection-derived structure
//!   rather than N. The counts *are* the win: memory and startup CPU follow from
//!   them arithmetically, where asserting bytes or timings would instead measure
//!   the JSON Schema parser's allocation behavior (or CI's load average).
//! - **Equivalence** — the same fixture driven through `Actor::serve` in indirect
//!   and inline form publishes byte-identical documents, stats, and inference
//!   events. Indirect form shares a validator and a publisher target across the
//!   bindings of a collection; inline form shares nothing. Their outputs
//!   agreeing is the direct test of the failure mode that matters, which is
//!   cross-binding contamination from either sharing.
//!
//! Inference is the one structure which does *not* differ between the forms: it
//! keys on `partition_template_name`, which both forms carry, so it collapses to
//! M slots either way. That is decision 3's deliberate break from "an unflagged
//! task sees no change", and it is asserted here rather than left implicit.

use crate::leader::capture::fsm;
use crate::leader::capture::task::{Task, fixture};
use crate::proto;
use proto_flow::capture::{Request, Response, response};
use proto_flow::flow;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};

/// Distinct target collections of the fixture.
const M: usize = 3;
/// Bindings of the fixture, fanning evenly onto the M collections.
const N: usize = 12;
/// Documents the connector script captures under one key, per collection.
/// Above the memtable's compaction threshold, so the documents genuinely
/// combine rather than passing through un-reduced.
const RUN: i64 = 64;

/// `binding_collections[i]` is the collection which binding `i` writes: a
/// round-robin, so every collection is written by `N / M` bindings.
fn binding_collections() -> Vec<usize> {
    (0..N).map(|index| index % M).collect()
}

fn collection_name(index: usize) -> String {
    format!("acmeCo/collection-{index}")
}

/// The counts which decide whether derived state is per-collection or
/// per-binding, plus the binding -> publisher-target remap and the collection
/// each binding's target actually names.
struct Counts {
    collection_slots: usize,
    combiner_bindings: usize,
    inference_slots: usize,
    validator_slots: usize,
    targets: usize,
    binding_targets: Vec<u32>,
    target_collections: Vec<String>,
}

fn counts_of(spec: &flow::CaptureSpec) -> Counts {
    let (open, opened) = fixture::open(spec.clone());
    let task = Task::new(&open, &opened, 0).unwrap();
    let combine_spec = task.combine_spec().unwrap();

    let (collection_specs, binding_targets) =
        super::handler::publisher_targets(spec, &task).unwrap();

    Counts {
        collection_slots: task.collection_slots.len(),
        combiner_bindings: combine_spec.binding_count(),
        inference_slots: task.inference_slots.len(),
        validator_slots: combine_spec.validator_count(),
        targets: collection_specs.len(),
        target_collections: binding_targets
            .iter()
            .map(|&target| collection_specs[target as usize].name.clone())
            .collect(),
        binding_targets,
    }
}

/// An indirect-form task builds one validator, one publisher target, and one
/// inference slot per *collection*; the inline form of the same fixture builds
/// one of the first two per *binding*, and is bit-for-bit what it was before
/// de-duplication. Both forms send each binding to its own collection.
#[test]
fn derived_state_collapses_onto_collections() {
    let indirect = fixture::capture_spec(M, &binding_collections());
    let mut inline = indirect.clone();
    fixture::into_inline(&mut inline);

    // Whichever form and however the state is grouped, binding `i` writes the
    // collection the spec named -- the invariant a mis-mapped target breaks.
    let expected: Vec<String> = binding_collections()
        .iter()
        .map(|&index| collection_name(index))
        .collect();

    let counts = counts_of(&indirect);
    assert_eq!(counts.collection_slots, M);
    assert_eq!(counts.validator_slots, M + 1); // Plus the connector-state slot.
    assert_eq!(counts.targets, M);
    assert_eq!(counts.binding_targets, [0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2]);
    assert_eq!(counts.target_collections, expected);
    // Combiner bindings stay per-binding: sharing a validator never merges the
    // documents of two bindings.
    assert_eq!(counts.combiner_bindings, N + 1);

    let counts = counts_of(&inline);
    assert_eq!(counts.collection_slots, N);
    assert_eq!(counts.validator_slots, N + 1);
    assert_eq!(counts.targets, N);
    assert_eq!(
        counts.binding_targets,
        (0..N as u32).collect::<Vec<_>>(),
        "an inline-form binding is its own target",
    );
    assert_eq!(counts.target_collections, expected);
    assert_eq!(counts.combiner_bindings, N + 1);

    // Inference is the exception: it keys on the collection's journal identity,
    // which both forms carry, so it is per-collection unconditionally.
    assert_eq!(counts_of(&indirect).inference_slots, M);
    assert_eq!(counts_of(&inline).inference_slots, M);
}

/// A [`crate::Publisher`] recording what a real one would write: each captured
/// document with the task binding it was published for, and each stats document.
/// Handles are `Arc`-shared so the test reads them back after the actor -- which
/// owns the publisher for its session -- has stopped.
#[derive(Clone, Default)]
struct RecordingPublisher {
    docs: Arc<Mutex<Vec<(usize, String)>>>,
    stats: Arc<Mutex<Vec<ops::proto::Stats>>>,
}

impl crate::Publisher for RecordingPublisher {
    fn update_clock(&mut self) {}

    async fn publish_stats(&mut self, stats: ops::proto::Stats) -> tonic::Result<()> {
        self.stats.lock().unwrap().push(stats);
        Ok(())
    }

    async fn publish_doc(
        &mut self,
        binding_index: usize,
        doc: doc::OwnedNode,
        _uuid_ptr: &json::Pointer,
    ) -> tonic::Result<usize> {
        let doc = serde_json::to_string(&doc::SerPolicy::noop().on_owned(&doc)).unwrap();
        let bytes = doc.len();
        self.docs.lock().unwrap().push((binding_index, doc));
        Ok(bytes)
    }

    async fn flush(&mut self) -> tonic::Result<()> {
        Ok(())
    }

    async fn marker_commit(
        &mut self,
        _binding_index: usize,
    ) -> tonic::Result<
        Option<(
            proto_gazette::uuid::Producer,
            proto_gazette::uuid::Clock,
            Vec<String>,
        )>,
    > {
        Ok(None)
    }

    async fn apply_truncated_at_labels(
        &mut self,
        _active_backfills: &BTreeMap<u32, u64>,
    ) -> tonic::Result<()> {
        Ok(())
    }

    fn commit_intents(
        &mut self,
    ) -> Option<(
        proto_gazette::uuid::Producer,
        proto_gazette::uuid::Clock,
        Vec<String>,
    )> {
        None
    }

    async fn write_intents(
        &mut self,
        _journal_intents: BTreeMap<String, bytes::Bytes>,
    ) -> tonic::Result<()> {
        Ok(())
    }

    fn take_throttle_samples(&mut self) -> Vec<publisher::ThrottleSample<'_>> {
        Vec::new()
    }

    fn split_partition(
        &self,
        _journal: &str,
    ) -> Option<futures::future::BoxFuture<'static, tonic::Result<publisher::SplitOutcome>>> {
        None
    }
}

/// A [`crate::Logger`] intercepting inference events structurally, and dropping
/// the rest of the task's log stream.
#[derive(Clone, Default)]
struct RecordingLogger(Arc<Mutex<Vec<(String, Option<usize>, String)>>>);

impl crate::Logger for RecordingLogger {
    fn log(&self, _log: &ops::Log) {}

    fn event(&self, event: crate::LogEvent<'_>) {
        if let crate::LogEvent::InferredSchema {
            collection_name,
            binding,
            schema,
            ..
        } = event
        {
            self.0.lock().unwrap().push((
                collection_name.to_string(),
                binding,
                serde_json::to_string(schema).unwrap(),
            ));
        }
    }
}

/// Everything the two runs must agree on, rendered as strings so a mismatch
/// reads as a diff rather than as two proto Debug dumps.
struct Recording {
    docs: Vec<(usize, String)>,
    stats: Vec<String>,
    inferences: Vec<(String, Option<usize>, String)>,
}

/// Drive one capture session over `spec` with the shared connector script, and
/// return what it published and inferred.
///
/// The script exercises both hazards of sharing: several bindings of one
/// collection publish documents under the *same* key (so a shared validator or
/// a mis-mapped target would cross-contaminate rather than merely miscount),
/// repeats within one binding reduce, and a `SourcedSchema` folds into the
/// collection's inference alongside the documents' widening.
async fn run(spec: flow::CaptureSpec) -> Recording {
    let (open, opened) = fixture::open(spec);
    let task = std::sync::Arc::new(Task::new(&open, &opened, 0).unwrap());

    let (connector_tx, mut actor_to_conn_rx) = mpsc::channel::<Request>(crate::CHANNEL_BUFFER);
    let (conn_resp_tx, conn_resp_rx) =
        mpsc::channel::<tonic::Result<Response>>(crate::CHANNEL_BUFFER);
    let (controller_tx, controller_rx) = mpsc::unbounded_channel::<tonic::Result<proto::Capture>>();

    let publisher = RecordingPublisher::default();
    let logger = RecordingLogger::default();
    let (docs, stats, inferences) = (
        publisher.docs.clone(),
        publisher.stats.clone(),
        logger.0.clone(),
    );

    let shapes = task.shapes_by_slot(Default::default());
    let actor = super::actor::Actor::new(
        BTreeMap::new(),
        (0..N).map(|index| format!("table-{index}")).collect(),
        connector_tx,
        crate::shard::RocksDB::open(None).await.unwrap(),
        true,
        super::Metrics::new("test/shard"),
        logger,
        publisher,
        shapes,
        task,
        None,
    );

    let mut serve = Some(tokio::spawn(async move {
        let mut controller_rx = UnboundedReceiverStream::new(controller_rx);
        actor
            .serve(
                ReceiverStream::new(conn_resp_rx),
                &mut controller_rx,
                fsm::Head::Idle(fsm::HeadIdle::default()),
                fsm::Tail::Recover(fsm::TailRecover {
                    checkpoints: 0,
                    ack_intents: BTreeMap::new(),
                }),
            )
            .await
    }));

    // An actor which errors drops its channels, so the test would otherwise see
    // a closed-channel error in place of the failure that actually happened.
    // Join it and re-raise instead: mis-slotted derived state surfaces here as a
    // validation error, and it should say so.
    macro_rules! actor_died {
        () => {{
            match serve.take().unwrap().await.unwrap() {
                Err(err) => panic!("actor failed: {err:?}"),
                Ok(_) => panic!("actor stopped early without an error"),
            }
        }};
    }

    // Every document uses key "shared", so each collection's documents collide
    // with the others' on key as well as within a binding.
    let captured = |binding: usize, value: i64| {
        let collection = binding % M;
        Ok(Response {
            captured: Some(response::Captured {
                binding: binding as u32,
                doc_json: bytes::Bytes::from(format!(
                    r#"{{"id":"shared","from_collection_{collection}":true,"value":{value}}}"#
                )),
            }),
            ..Default::default()
        })
    };

    // One `RUN` per collection, through bindings 0, 1 and 2, so each shared
    // validator is exercised for its *reduce* annotations and not only for
    // validation. Then single documents on bindings 3, 6 and 9 -- collection-0
    // again -- whose outputs must stay separate from binding 0's however much
    // machinery the four of them share.
    for response in (0..RUN)
        .flat_map(|_| [captured(0, 1), captured(1, 2), captured(2, 3)])
        .chain([captured(3, 10), captured(6, 100), captured(9, 1000)])
    {
        if conn_resp_tx.send(response).await.is_err() {
            actor_died!()
        }
    }
    for response in [
        Ok(Response {
            sourced_schema: Some(response::SourcedSchema {
                binding: 5, // Collection-2, which binding 2 also writes.
                schema_json: bytes::Bytes::from_static(
                    br#"{"type":"object","additionalProperties":false,"properties":{"id":{"type":"string"},"from_collection_2":{"const":true},"sourced_only":{"type":"boolean"}},"required":["id","from_collection_2"]}"#,
                ),
            }),
            ..Default::default()
        }),
        Ok(Response {
            checkpoint: Some(response::Checkpoint {
                state: Some(flow::ConnectorState {
                    updated_json: bytes::Bytes::from_static(br#"{"cursor":"lsn-1"}"#),
                    merge_patch: true,
                }),
            }),
            ..Default::default()
        }),
    ] {
        if conn_resp_tx.send(response).await.is_err() {
            actor_died!()
        }
    }

    // The Acknowledge follows Drain -> WriteStats -> Persist, so its receipt
    // proves the transaction committed and the recordings are complete.
    match actor_to_conn_rx.recv().await {
        Some(request) => assert!(request.acknowledge.is_some()),
        None => actor_died!(),
    }

    controller_tx
        .send(Ok(proto::Capture {
            stop: Some(proto::Stop {}),
            ..Default::default()
        }))
        .unwrap();
    _ = serve.take().unwrap().await.unwrap().unwrap();

    let docs = std::mem::take(&mut *docs.lock().unwrap());
    let inferences = std::mem::take(&mut *inferences.lock().unwrap());
    let stats = std::mem::take(&mut *stats.lock().unwrap())
        .into_iter()
        .map(normalize_stats)
        .collect();

    Recording {
        docs,
        stats,
        inferences,
    }
}

/// Render `stats` for comparison, dropping the fields which are a function of
/// when the transaction ran rather than of what it did.
fn normalize_stats(mut stats: ops::proto::Stats) -> String {
    stats.meta = None; // UUID is stamped by the publisher.
    stats.timestamp = None;
    stats.open_seconds_total = 0.0;

    for binding in stats.capture.values_mut() {
        binding.last_published_at = None;
    }
    serde_json::to_string_pretty(&stats).unwrap()
}

/// The indirect and inline forms of one capture are indistinguishable from
/// outside: same documents on the same bindings, same stats, same inference.
///
/// This is the strongest statement the de-duplication can make. Indirect form runs
/// four bindings of each collection through one shared validator and one shared
/// publisher target; inline form gives each binding its own. If sharing leaked
/// -- a document reduced against the wrong binding's accumulation, a target
/// pointed at the wrong collection, a widened shape attributed to the wrong slot
/// -- the two recordings would diverge.
#[tokio::test]
async fn indirect_and_inline_forms_publish_identically() {
    let indirect = fixture::capture_spec(M, &binding_collections());
    let mut inline = indirect.clone();
    fixture::into_inline(&mut inline);

    let indirect = run(indirect).await;
    let inline = run(inline).await;

    assert_eq!(indirect.docs, inline.docs);
    assert_eq!(indirect.stats, inline.stats);
    assert_eq!(indirect.inferences, inline.inferences);

    // Guard against the recordings agreeing because both are empty, and pin what
    // each binding actually published. Every binding's documents combined onto
    // its own key -- fewer documents out than in -- and summed to exactly what
    // that binding captured and nothing its peers did, so sharing a validator
    // slot never merged one binding's accumulation into another's.
    let mut published = BTreeMap::<usize, (usize, i64)>::new();

    for (binding, doc) in &indirect.docs {
        let doc: serde_json::Value = serde_json::from_str(doc).unwrap();
        let entry = published.entry(*binding).or_default();

        entry.0 += 1;
        entry.1 += doc["value"].as_i64().unwrap();

        assert_eq!(
            doc[format!("from_collection_{}", binding % M)],
            serde_json::json!(true),
            "binding {binding} published a document of another collection: {doc}",
        );
    }
    // Bindings 0, 1 and 2 each captured `RUN` documents of one key, valued 1, 2
    // and 3 -- so each binding's total is its own value times the run.
    for (binding, value) in [(0, 1), (1, 2), (2, 3)] {
        let (count, sum) = published[&binding];
        assert_eq!(sum, value * RUN, "binding {binding} published {sum}");
        assert!(
            (count as i64) < RUN,
            "binding {binding} combined its key: {count} docs",
        );
    }
    assert_eq!(published[&3], (1, 10));
    assert_eq!(published[&6], (1, 100));
    assert_eq!(published[&9], (1, 1000));
    assert_eq!(published.len(), 6);
    assert_eq!(indirect.stats.len(), 1);

    // One inference event per collection, each naming the last binding to
    // update it -- including collection-2, whose shape merges binding 5's
    // sourced schema with binding 2's document, applied in that order.
    assert_eq!(
        indirect
            .inferences
            .iter()
            .map(|(collection, binding, _schema)| (collection.as_str(), *binding))
            .collect::<Vec<_>>(),
        [
            ("acmeCo/collection-0", Some(9)),
            ("acmeCo/collection-1", Some(1)),
            ("acmeCo/collection-2", Some(2)),
        ],
    );
    assert!(
        indirect.inferences[2].2.contains("sourced_only"),
        "collection-2's inference merged the sourced schema: {}",
        indirect.inferences[2].2,
    );
}
