use crate::leader::close_policy;
use anyhow::Context;
use proto_flow::capture::{Request, Response, request, response};
use proto_flow::flow;
use proto_gazette::{consumer, uuid};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Task {
    /// Bindings of this Task.
    pub bindings: Vec<Binding>,
    /// Representative binding of each collection slot, in binding-scan order.
    /// See [`Binding::collection_slot`].
    pub collection_slots: Vec<u32>,
    /// Schema-inference slots of this Task, one per distinct target collection.
    pub inference_slots: Vec<InferenceSlot>,
    /// Policy for how transactions close.
    pub close_policy: close_policy::Policy,
    /// Does the capture connector want explicit acknowledgements?
    pub explicit_acknowledgements: bool,
    /// Transactions to complete before stopping, or zero for unbounded.
    /// Set only by the preview / test harness; production leaves it zero.
    pub max_transactions: u32,
    /// Salt used for redacting sensitive fields.
    pub redact_salt: bytes::Bytes,
    /// Clock at which this Task is eligible for restart.
    pub restart: uuid::Clock,
    /// Bound on the number of captured document bytes in a single connector
    /// checkpoint sequence. Injects a synthetic checkpoint upon breach.
    pub sequence_bytes_limit: u64,
    /// ShardRef of this task.
    pub shard_ref: ops::ShardRef,
}

#[derive(Debug, Clone)]
pub struct Binding {
    // Target collection.
    pub collection_name: String,
    // Generation id of the collection, which must be output as part of updating inferred schemas.
    pub collection_generation_id: models::Id,
    // Index of this binding's collection slot: the group of bindings which write
    // an identical `CollectionSpec` *value*, and can therefore share the derived
    // state built from it -- a combiner validator and a publisher target.
    //
    // Slots key on the indirect encoding's `collection_index`, which is a proof of
    // full value equality established by the spec interner, and not on collection
    // name: a materialization's `group_by` can rewrite two bindings of one named
    // collection to differing values, and a name-keyed slot would silently share
    // derived state between them. An inline-form spec carries no index, so each
    // of its bindings is its own slot and its behavior is unchanged.
    //
    // Distinct from `inference_slot`, which keys on *journal* identity because
    // inferred shapes outlive the build that produced an index. The two provably
    // coincide for captures; see `assert_slot_groupings`.
    pub collection_slot: u32,
    // JSON pointer at which document UUIDs are added.
    pub document_uuid_ptr: json::Pointer,
    // Index of this binding's inference slot within `Task::inference_slots`.
    pub inference_slot: u32,
    // Do other active bindings of this task also write this binding's journals?
    // When true, the binding's truncation effects are suppressed: a `TRUNCATE`
    // of one source table doesn't mean the logical collection should be
    // truncated, and one binding doesn't get to make a decision that's
    // load-bearing for its peers. Keyed on `partition_template_name` because
    // the hazard is about journals, and journals come from the partition prefix.
    pub fan_in: bool,
    // Key components which are extracted from written documents.
    pub key_extractors: Vec<doc::Extractor>,
    // Partition template name for journals of the target collection.
    pub partition_template_name: String,
    // Encoded resource path + backfill state key of this binding.
    pub state_key: String,
    // Write schema of the target collection.
    pub write_schema_json: bytes::Bytes,
}

/// Long-lived schema inference of one target collection.
///
/// Inference is about the collection, not the binding: several bindings of a
/// fan-in capture describe one collection, and the ops rollup merges their
/// inferences by collection name anyway. A slot is that merge, done once and up
/// front, so a task with many bindings over few collections holds and logs
/// per-collection state rather than per-binding state.
///
/// Slots key on `partition_template_name` -- collection name plus generation --
/// and *not* on the `collection_index` that other derived state groups on,
/// because shapes outlive the build which produced an index: the interner sorts
/// collections by name, so adding or removing one shifts every index after it,
/// and an index-keyed stow would reattach collection A's shape to collection B
/// after an unrelated edit. Generation stays embedded in the key, so a
/// collection reset starts inference fresh for free.
#[derive(Debug, Clone)]
pub struct InferenceSlot {
    // Target collection.
    pub collection_name: String,
    // Generation id of the collection, output as part of updating inferred schemas.
    pub collection_generation_id: models::Id,
    // Partition template name for journals of the target collection,
    // which is also this slot's cross-session identity.
    pub partition_template_name: String,
    // Inferred Shape of the collection's write schema. Built once per slot and
    // deep-cloned only on the rare `apply_sourced_schemas` path.
    pub write_shape: doc::Shape,
}

impl Task {
    pub fn new(open: &Request, opened: &Response, max_transactions: u32) -> anyhow::Result<Self> {
        let request::Open {
            capture: spec,
            range,
            state_json: _,
            sealed_config_json: _,
            version,
        } = open.clone().open.context("expected Open")?;

        let response::Opened {
            explicit_acknowledgements,
        } = opened.clone().opened.context("expected Opened")?;

        let spec = spec.as_ref().context("missing capture")?;

        let flow::CaptureSpec {
            bindings: _,
            config_json: _,
            connector_type: _,
            interval_seconds,
            name,
            network_ports: _,
            recovery_log_template: _,
            shard_template,
            inactive_bindings: _,
            redact_salt,
            created_at: _,
            linked_collections: _,
        } = spec;
        let range = range.context("missing range")?;

        if range.r_clock_begin != 0 || range.r_clock_end != u32::MAX {
            anyhow::bail!("captures cannot split on r-clock: {range:?}");
        }

        // Min/max transaction duration come from the shard template.
        let consumer::ShardSpec {
            min_txn_duration,
            max_txn_duration,
            ..
        } = shard_template.as_ref().context("missing shard template")?;

        let min_txn_duration = min_txn_duration
            .context("missing min_txn_duration")?
            .try_into()?;
        let max_txn_duration = max_txn_duration
            .context("missing max_txn_duration")?
            .try_into()?;

        // `doc::combine` packs its binding index into a u16, and the
        // connector-state pseudo-binding sits at index `bindings.len()`, so the
        // length itself must also fit. This guards the *format* limit and
        // deliberately shares no constant with `validation::MAX_BINDINGS`, which
        // gates published tasks far below it: tripping this means an unvalidated
        // spec reached the runtime.
        if spec.bindings.len() > u16::MAX as usize {
            anyhow::bail!(
                "capture has {} bindings, which exceeds the combiner limit of {}",
                spec.bindings.len(),
                u16::MAX,
            );
        }

        let ser_policy = doc::SerPolicy::noop();
        let mut identities = Vec::with_capacity(spec.bindings.len());

        let mut bindings = spec
            .resolved_bindings()
            .enumerate()
            .map(|(index, (binding, resolved))| {
                let (collection, identity) =
                    resolved.context("missing collection").context(index)?;
                identities.push(identity);
                Binding::new(binding, collection, ser_policy.clone()).context(index)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let collection_slots = assign_collection_slots(&mut bindings, &identities)?;
        let inference_slots = assign_inference_slots(&mut bindings)?;

        #[cfg(debug_assertions)]
        assert_slot_groupings(&bindings, &identities);

        // A Clock one poll `interval` from now, on the same wall-clock base as
        // the actor's monotonic `now`, so the HeadFSM computes the restart wait
        // directly. `tokens::now()` is tokio test-time aware.
        let now = tokens::now();
        let restart = uuid::Clock::from_unix(
            now.timestamp() as u64 + *interval_seconds as u64,
            now.timestamp_subsec_nanos(),
        );

        let shard_ref = ops::ShardRef {
            kind: ops::TaskType::Capture as i32,
            name: name.clone(),
            key_begin: format!("{:08x}", range.key_begin),
            r_clock_begin: format!("{:08x}", range.r_clock_begin),
            build: version.clone(),
        };

        let mut close_policy = close_policy::Policy::new(min_txn_duration, max_txn_duration);
        // Cap combiner usage at 64MB to favor small transactions.
        close_policy.combiner_usage_bytes = 0..(64 * 1024 * 1024);

        Ok(Self {
            bindings,
            collection_slots,
            inference_slots,
            close_policy,
            explicit_acknowledgements,
            max_transactions,
            redact_salt: redact_salt.clone(),
            restart,
            sequence_bytes_limit: 512 * 1024 * 1024, // 512 MB.
            shard_ref,
        })
    }

    /// Restore stowed inferred shapes into this session's slot layout, seeding
    /// annotations for any slot which is net-new.
    pub fn shapes_by_slot(&self, mut by_key: BTreeMap<String, doc::Shape>) -> Vec<doc::Shape> {
        let mut by_slot = Vec::new();
        by_slot.resize_with(self.inference_slots.len(), doc::shape::Shape::nothing);

        for (index, slot) in self.inference_slots.iter().enumerate() {
            // Seed inference annotations only when the slot is net-new.
            // In particular, carry forward an x-complexity-limit that may have
            // been elevated by the presence of a SourcedSchema.
            if let Some(shape) = by_key.remove(&slot.partition_template_name) {
                by_slot[index] = shape;
            } else {
                let annotations = &mut by_slot[index].annotations;
                annotations.insert(
                    crate::X_GENERATION_ID.to_string(),
                    serde_json::Value::String(slot.collection_generation_id.to_string()),
                );
                annotations.insert(
                    doc::shape::X_COMPLEXITY_LIMIT.to_string(),
                    serde_json::json!(doc::shape::limits::DEFAULT_SCHEMA_COMPLEXITY_LIMIT),
                );
            }
        }
        by_slot
    }

    /// Invert [`Self::shapes_by_slot`], re-keying inferred shapes by their
    /// stable `partition_template_name` identity.
    ///
    /// Inferred shapes are held only in memory and accumulate across the many
    /// connector sessions of a shard's lifetime (a capture re-Opens every poll
    /// `interval`). Slot *indices* are not stable across spec updates but the
    /// key is, so the shard stows shapes by key between sessions and
    /// `shapes_by_slot` restores them into the next session's layout.
    pub fn shapes_by_key(&self, by_slot: Vec<doc::Shape>) -> BTreeMap<String, doc::Shape> {
        assert_eq!(
            by_slot.len(),
            self.inference_slots.len(),
            "shapes are the session's own slot layout",
        );
        self.inference_slots
            .iter()
            .map(|slot| slot.partition_template_name.clone())
            .zip(by_slot)
            .collect()
    }

    /// Build the combiner Spec of this Task: one validator slot per distinct
    /// target collection, plus a final slot for the connector-state
    /// pseudo-binding which rides the same combiner at index `bindings.len()`.
    ///
    /// Keys and full-reduction flags stay per-binding, so combined output is
    /// separated by binding however many bindings share a validator.
    pub fn combine_spec(&self) -> anyhow::Result<doc::combine::Spec> {
        let state_schema = doc::reduce::merge_patch_schema().to_string();
        let state_schema = doc::validation::build_bundle(state_schema.as_bytes()).unwrap();
        let state_validator = doc::Validator::new(state_schema).unwrap();

        let validators = self
            .collection_slots
            .iter()
            .map(|&binding| {
                let binding = &self.bindings[binding as usize];

                (
                    format!("captured collection {}", binding.collection_name),
                    // Safe to unwrap() because `assign_collection_slots` already
                    // built a validator over this slot's write schema.
                    build_write_validator(&binding.write_schema_json).unwrap(),
                )
            })
            .chain(std::iter::once((
                "connector state".to_string(),
                state_validator,
            )));

        let state_slot = self.collection_slots.len() as u32;
        let bindings = self
            .bindings
            .iter()
            .map(|binding| {
                (
                    false,
                    binding.key_extractors.clone(),
                    binding.collection_slot,
                )
            })
            .chain(std::iter::once((false, Vec::new(), state_slot)));

        Ok(doc::combine::Spec::with_bindings(
            bindings,
            validators,
            self.redact_salt.to_vec(),
        ))
    }
}

impl Binding {
    fn new(
        spec: &flow::capture_spec::Binding,
        collection: &flow::CollectionSpec,
        ser_policy: doc::SerPolicy,
    ) -> anyhow::Result<Self> {
        let flow::capture_spec::Binding {
            backfill: _,
            collection: _,
            collection_index: _,
            resource_config_json: _,
            resource_path: _,
            state_key,
        } = spec;

        let flow::CollectionSpec {
            ack_template_json: _,
            derivation: _,
            key,
            name,
            partition_fields: _,
            partition_template,
            projections,
            read_schema_json: _,
            uuid_ptr,
            write_schema_json,
        } = collection;

        let partition_template = partition_template
            .as_ref()
            .context("missing partition template")?;

        let collection_generation_id =
            assemble::extract_generation_id_suffix(&partition_template.name);

        let document_uuid_ptr = json::Pointer::from(uuid_ptr);
        let key_extractors = extractors::for_key(key, projections, &ser_policy)?;

        Ok(Self {
            collection_name: name.clone(),
            collection_generation_id,
            collection_slot: 0, // Stamped by Task::new, which groups bindings into slots.
            document_uuid_ptr,
            fan_in: false,     // Stamped by Task::new, which sees the binding's peers.
            inference_slot: 0, // Stamped by Task::new, which groups bindings into slots.
            key_extractors,
            partition_template_name: partition_template.name.clone(),
            state_key: state_key.clone(),
            write_schema_json: write_schema_json.clone(),
        })
    }
}

impl InferenceSlot {
    /// Build the slot which `binding` is the first of, inferring the write-schema
    /// Shape once for the collection rather than once per binding.
    fn new(binding: &Binding) -> anyhow::Result<Self> {
        let validator = build_write_validator(&binding.write_schema_json)?;

        Ok(Self {
            collection_name: binding.collection_name.clone(),
            collection_generation_id: binding.collection_generation_id,
            partition_template_name: binding.partition_template_name.clone(),
            write_shape: doc::Shape::infer(validator.schema(), validator.schema_index()),
        })
    }
}

/// Group `bindings` into collection slots -- one per distinct `collection_index`
/// identity, in binding-scan order -- stamping each binding's `collection_slot`
/// and returning the representative binding of each slot.
///
/// A binding of an inline-form spec has no identity and is its own slot, so an
/// unflagged task keeps one slot per binding. See [`Binding::collection_slot`].
///
/// Each slot's write schema is built here -- and dropped -- so that a bad schema
/// is an error at Task::new rather than mid-session, and so [`Task::combine_spec`]
/// can unwrap its rebuild.
fn assign_collection_slots(
    bindings: &mut [Binding],
    identities: &[Option<u32>],
) -> anyhow::Result<Vec<u32>> {
    let mut slots = Vec::new();
    let mut slots_by_identity = BTreeMap::<u32, u32>::new();

    for (index, (binding, identity)) in bindings.iter_mut().zip(identities).enumerate() {
        binding.collection_slot = match identity.and_then(|i| slots_by_identity.get(&i)) {
            Some(slot) => *slot,
            None => {
                _ = build_write_validator(&binding.write_schema_json).context(index)?;

                slots.push(index as u32);
                let slot = slots.len() as u32 - 1;

                if let Some(identity) = identity {
                    slots_by_identity.insert(*identity, slot);
                }
                slot
            }
        };
    }
    Ok(slots)
}

/// Group `bindings` into inference slots -- one per distinct
/// `partition_template_name`, in binding-scan order -- stamping each binding's
/// `inference_slot` and its `fan_in`.
///
/// Both fall out of the same grouping because both are about journals: inference
/// describes the collection its bindings write, and a binding sharing journals
/// with a peer must not truncate them. See [`InferenceSlot`] and [`Binding::fan_in`].
fn assign_inference_slots(bindings: &mut [Binding]) -> anyhow::Result<Vec<InferenceSlot>> {
    let mut slots = Vec::<InferenceSlot>::new();
    // Slot of each binding, and count of bindings in each slot.
    let mut binding_slots = Vec::with_capacity(bindings.len());
    let mut slot_bindings = Vec::<usize>::new();
    // Borrows `bindings`, so it must go out of use before the stamping pass.
    let mut slots_by_name = BTreeMap::<&str, u32>::new();

    for (index, binding) in bindings.iter().enumerate() {
        let slot = match slots_by_name.get(binding.partition_template_name.as_str()) {
            Some(slot) => *slot,
            None => {
                slots.push(InferenceSlot::new(binding).context(index)?);
                slot_bindings.push(0);
                let slot = slots.len() as u32 - 1;
                slots_by_name.insert(binding.partition_template_name.as_str(), slot);
                slot
            }
        };
        binding_slots.push(slot);
        slot_bindings[slot as usize] += 1;
    }

    for (binding, slot) in bindings.iter_mut().zip(binding_slots) {
        binding.inference_slot = slot;
        binding.fan_in = slot_bindings[slot as usize] > 1;
    }
    Ok(slots)
}

/// Assign both slot groupings over hand-built test `bindings`, exactly as
/// `Task::new` does for an inline-form spec: no binding carries a
/// `collection_index`, so each is its own collection slot, while inference slots
/// still group by `partition_template_name`.
#[cfg(test)]
pub(crate) fn assign_inline_slots(bindings: &mut [Binding]) -> (Vec<u32>, Vec<InferenceSlot>) {
    let identities = vec![None; bindings.len()];

    let collection_slots = assign_collection_slots(bindings, &identities).unwrap();
    let inference_slots = assign_inference_slots(bindings).unwrap();

    #[cfg(debug_assertions)]
    assert_slot_groupings(bindings, &identities);

    (collection_slots, inference_slots)
}

fn build_write_validator(write_schema_json: &[u8]) -> anyhow::Result<doc::Validator> {
    let built_schema = doc::validation::build_bundle(write_schema_json)
        .context("collection write_schema_json is not a JSON schema")?;

    doc::Validator::new(built_schema).context("could not build a schema validator")
}

/// Check the assumptions which let inference slots be shared, in whichever spec
/// form the task arrived in. Debug-only: each is guaranteed by construction for
/// any validation-built spec, so this catches hand-built specs and future
/// encoder bugs rather than defending a live invariant.
#[cfg(debug_assertions)]
fn assert_slot_groupings(bindings: &[Binding], identities: &[Option<u32>]) {
    // The form is a property of the message as a whole, so one binding decides.
    if identities.first().is_some_and(Option::is_some) {
        // Indirect form: journal identity and value identity must coincide. They do
        // for captures -- nothing rewrites a capture binding's collection the way
        // materialize's `group_by` does -- but collection slots group on
        // `collection_index` while inference groups on the journal name, so the
        // coincidence is checked rather than assumed.
        let mut inference_of = BTreeMap::new();
        let mut collection_of = BTreeMap::new();

        for binding in bindings {
            debug_assert_eq!(
                *inference_of
                    .entry(binding.collection_slot)
                    .or_insert(binding.inference_slot),
                binding.inference_slot,
                "collection slot {} spans two inference slots",
                binding.collection_slot,
            );
            debug_assert_eq!(
                *collection_of
                    .entry(binding.inference_slot)
                    .or_insert(binding.collection_slot),
                binding.collection_slot,
                "inference slot {} spans two collection slots",
                binding.inference_slot,
            );
        }
        return;
    }

    // Inline form: bindings of one slot share a write-schema, so they can share
    // a write-shape. A validation-built spec guarantees this; a hand-built one
    // which reuses a collection name at differing values gets caught here rather
    // than silently inferring against the wrong schema.
    let mut schema_of = BTreeMap::new();

    for binding in bindings {
        debug_assert_eq!(
            *schema_of
                .entry(binding.inference_slot)
                .or_insert(&binding.write_schema_json),
            &binding.write_schema_json,
            "collection {} has two write schemas across its bindings",
            binding.collection_name,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_collection(index: usize) -> flow::CollectionSpec {
        flow::CollectionSpec {
            name: format!("acmeCo/collection-{index}"),
            key: vec!["/id".to_string()],
            partition_template: Some(proto_gazette::broker::JournalSpec {
                name: format!("acmeCo/collection-{index}/00112233445566{index:02x}"),
                ..Default::default()
            }),
            projections: serde_json::from_value(serde_json::json!([
                {"field": "id", "ptr": "/id", "inference": {"types": ["string"]}},
            ]))
            .unwrap(),
            uuid_ptr: "/_meta/uuid".to_string(),
            write_schema_json: r#"{"type":"object"}"#.into(),
            ..Default::default()
        }
    }

    /// An indirect-form capture Open / Opened whose `binding_collections[i]` is
    /// the linked collection which binding `i` targets. Repeats fan several
    /// bindings into one collection.
    fn mk_open_over(collections: usize, binding_collections: &[usize]) -> (Request, Response) {
        let spec = flow::CaptureSpec {
            name: "acmeCo/capture".to_string(),
            bindings: binding_collections
                .iter()
                .enumerate()
                .map(|(index, collection)| flow::capture_spec::Binding {
                    collection_index: *collection as u32,
                    resource_path: vec![format!("table-{index}")],
                    state_key: format!("table-{index}"),
                    ..Default::default()
                })
                .collect(),
            linked_collections: (0..collections).map(mk_collection).collect(),
            shard_template: Some(consumer::ShardSpec {
                min_txn_duration: Some(std::time::Duration::ZERO.into()),
                max_txn_duration: Some(std::time::Duration::from_secs(1).into()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let open = Request {
            open: Some(request::Open {
                capture: Some(spec),
                range: Some(flow::RangeSpec {
                    key_begin: 0,
                    key_end: u32::MAX,
                    r_clock_begin: 0,
                    r_clock_end: u32::MAX,
                }),
                version: "aabbccdd".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let opened = Response {
            opened: Some(response::Opened {
                explicit_acknowledgements: true,
            }),
            ..Default::default()
        };
        (open, opened)
    }

    fn mk_task(collections: usize, binding_collections: &[usize]) -> Task {
        let (open, opened) = mk_open_over(collections, binding_collections);
        Task::new(&open, &opened, 0).unwrap()
    }

    /// The same task in inline form: each binding carries its own copy of its
    /// collection and the linked table is dropped, so no binding has an identity.
    fn mk_inline_task(collections: usize, binding_collections: &[usize]) -> Task {
        let (mut open, opened) = mk_open_over(collections, binding_collections);

        let spec = open.open.as_mut().unwrap().capture.as_mut().unwrap();
        let table = std::mem::take(&mut spec.linked_collections);

        for binding in &mut spec.bindings {
            binding.collection = Some(table[binding.collection_index as usize].clone());
            binding.collection_index = 0;
        }
        Task::new(&open, &opened, 0).unwrap()
    }

    /// Bindings collapse into one inference slot per distinct target collection,
    /// and `fan_in` -- which shares the grouping, because both are about journals
    /// -- marks every binding of a collection written by more than one.
    #[test]
    fn task_groups_bindings_into_inference_slots() {
        let slots_and_fan_in = |collections, binding_collections: &[usize]| {
            let task = mk_task(collections, binding_collections);
            (
                task.inference_slots.len(),
                task.bindings
                    .iter()
                    .map(|binding| (binding.inference_slot, binding.fan_in))
                    .collect::<Vec<_>>(),
            )
        };

        // Sole binding of its collection.
        assert_eq!(slots_and_fan_in(1, &[0]), (1, vec![(0, false)]));
        // Distinct collections don't collapse, and neither is fan-in.
        assert_eq!(
            slots_and_fan_in(2, &[0, 1]),
            (2, vec![(0, false), (1, false)]),
        );
        // Four bindings over two collections collapse to two slots, all fan-in.
        assert_eq!(
            slots_and_fan_in(2, &[0, 1, 0, 1]),
            (2, vec![(0, true), (1, true), (0, true), (1, true)]),
        );
        // Slots are ordered by binding scan, not by collection index, and a
        // collection with one binding is not fan-in even beside one that is.
        assert_eq!(
            slots_and_fan_in(3, &[2, 1, 2]),
            (2, vec![(0, true), (1, false), (0, true)]),
        );

        // Each slot names its collection, and holds one inferred write-shape.
        let task = mk_task(2, &[1, 0, 1]);
        assert_eq!(
            task.inference_slots
                .iter()
                .map(|slot| slot.collection_name.as_str())
                .collect::<Vec<_>>(),
            ["acmeCo/collection-1", "acmeCo/collection-0"],
        );
    }

    /// Bindings which write one collection share a combiner validator slot: a
    /// parsed schema, its index, and its validation scratch are built once per
    /// collection rather than once per binding. Keys stay per-binding, as does
    /// the connector-state pseudo-binding riding the same combiner.
    #[test]
    fn combine_spec_groups_validators_by_collection() {
        // Five bindings over two collections, plus connector state.
        let task = mk_task(2, &[0, 1, 0, 1, 0]);
        assert_eq!(task.collection_slots, [0, 1]);
        assert_eq!(
            task.bindings
                .iter()
                .map(|binding| binding.collection_slot)
                .collect::<Vec<_>>(),
            [0, 1, 0, 1, 0],
        );

        let spec = task.combine_spec().unwrap();
        assert_eq!(spec.binding_count(), 6);
        assert_eq!(spec.validator_count(), 3);

        // Slots follow binding-scan order, not collection index.
        let task = mk_task(3, &[2, 1, 2]);
        assert_eq!(task.collection_slots, [0, 1]);
        assert_eq!(task.combine_spec().unwrap().validator_count(), 3);

        // An inline-form task carries no identity, so each binding is its own
        // slot and its combiner is bit-for-bit what it was before de-duplication.
        let task = mk_inline_task(2, &[0, 1, 0, 1, 0]);
        assert_eq!(task.collection_slots, [0, 1, 2, 3, 4]);

        let spec = task.combine_spec().unwrap();
        assert_eq!(spec.binding_count(), 6);
        assert_eq!(spec.validator_count(), 6);
    }

    /// Slot indices move when a spec update reorders bindings, so shapes are
    /// stowed under the collection's stable `partition_template_name` and restored
    /// into whichever slot that collection now occupies.
    #[test]
    fn shapes_round_trip_across_a_binding_reorder() {
        let marker = |shape: &doc::Shape| shape.annotations.get("marker").cloned();

        let task = mk_task(2, &[0, 1]);
        let mut shapes = task.shapes_by_slot(Default::default());

        // Seeded annotations come from the slot's collection.
        assert_eq!(
            shapes[0].annotations[crate::X_GENERATION_ID],
            serde_json::json!(task.inference_slots[0].collection_generation_id.to_string()),
        );
        shapes[0]
            .annotations
            .insert("marker".to_string(), serde_json::json!("collection-0"));
        let by_key = task.shapes_by_key(shapes);

        // A spec update reverses the bindings, so collection-0 is now slot 1.
        let task = mk_task(2, &[1, 0]);
        assert_eq!(
            task.inference_slots[1].collection_name,
            "acmeCo/collection-0"
        );

        let shapes = task.shapes_by_slot(by_key);
        assert_eq!(marker(&shapes[1]), Some(serde_json::json!("collection-0")));
        assert_eq!(marker(&shapes[0]), None); // Net-new slot, freshly seeded.
        assert!(shapes[0].annotations.contains_key(crate::X_GENERATION_ID));
    }

    /// The runtime guards `doc::combine`'s u16 binding index independently of
    /// `validation::MAX_BINDINGS`: a spec at the format limit builds, and one
    /// past it is a hard error rather than a wrapped pseudo-binding.
    #[test]
    fn task_guards_the_combiner_format_limit() {
        // All bindings target one collection, so this is also the cheap proof
        // that per-collection work doesn't scale with binding count.
        let bindings = vec![0; u16::MAX as usize + 1];

        let task = mk_task(1, &bindings[..u16::MAX as usize]);
        assert_eq!(task.bindings.len(), u16::MAX as usize);
        assert_eq!(task.inference_slots.len(), 1);

        let (open, opened) = mk_open_over(1, &bindings);
        let err = Task::new(&open, &opened, 0).unwrap_err();
        assert!(
            err.to_string()
                .contains("65536 bindings, which exceeds the combiner limit of 65535"),
            "unexpected error: {err}",
        );
    }
}
