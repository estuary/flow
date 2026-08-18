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
    /// Target collections written by this Task's bindings.
    pub targets: Vec<Target>,
    /// Policy for how transactions close.
    pub close_policy: close_policy::Policy,
    /// Does the capture connector want explicit acknowledgements?
    pub explicit_acknowledgements: bool,
    /// Transactions to complete before stopping, or zero for unbounded.
    /// Set only by the preview / test harness; production leaves it zero.
    /// A non-zero bound also stops the session at connector EOF, where
    /// production holds it through the poll interval (`restart`).
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

/// One capture binding: a connector resource captured into a target collection.
#[derive(Debug, Clone)]
pub struct Binding {
    // Index of this binding's target collection within [`Task::targets`].
    pub target: u32,
    // Encoded resource path + backfill state key of this binding.
    pub state_key: String,
}

/// A target collection, written by one or more bindings of the Task.
///
/// Targets group bindings by `partition_template_name` -- the identity of the
/// journals they write -- so every structure derived from the collection (a
/// combiner validator, a publisher target, schema inference) is built once per
/// collection rather than once per binding.
#[derive(Debug, Clone)]
pub struct Target {
    // Name of the target collection.
    pub collection_name: String,
    // Generation id of the collection, output as part of updating inferred schemas.
    pub collection_generation_id: models::Id,
    // Partition template name for journals of the collection, which is also the
    // stable cross-session identity of its inferred shape.
    pub partition_template_name: String,
    // JSON pointer at which document UUIDs are added.
    pub document_uuid_ptr: json::Pointer,
    // Key components extracted from written documents. One copy per target,
    // cloned into the combiner's per-binding entries.
    pub key_extractors: Vec<doc::Extractor>,
    // Write schema of the target collection.
    pub write_schema_json: bytes::Bytes,
    // Inferred Shape of the write schema. Built once per target and deep-cloned
    // only on the rare `apply_sourced_schemas` path.
    pub write_shape: doc::Shape,
    // First binding which writes this target: a representative for call sites
    // which resolve a target back to its binding's position in the built spec.
    pub first_binding: u32,
    // Do multiple active bindings of the task write this target? Such a
    // binding's backfill must not truncate journals its peers also write.
    pub fan_in: bool,
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
            secrets: _,
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

        let mut bindings = Vec::with_capacity(spec.bindings.len());
        let mut targets = Vec::<Target>::new();
        // Linked-table identity and CollectionSpec of each target's first
        // binding, for the value-equality requirement below.
        let mut reps: Vec<(Option<u32>, &flow::CollectionSpec)> = Vec::new();
        let mut targets_by_name = BTreeMap::<&str, u32>::new();

        for (index, (binding, resolved)) in spec.resolved_bindings().enumerate() {
            let (collection, identity) = resolved.context("missing collection").context(index)?;
            let template_name = collection
                .partition_template
                .as_ref()
                .context("missing partition template")
                .context(index)?
                .name
                .as_str();

            let target = match targets_by_name.get(template_name) {
                Some(&target) => {
                    // Bindings of one target share all of its derived state,
                    // which is sound only if their `CollectionSpec` values are
                    // equal. A validation-built spec guarantees this in either
                    // form; an unvalidated spec which reuses a collection name
                    // across differing values is a hard error here, rather than
                    // silent cross-binding contamination downstream.
                    let (first_identity, first_collection) = reps[target as usize];
                    match (identity, first_identity) {
                        // Equal linked-table indices prove equal values.
                        (Some(a), Some(b)) if a == b => {}
                        _ => anyhow::ensure!(
                            collection == first_collection,
                            "bindings {} and {index} write collection {} with unequal collection specs",
                            targets[target as usize].first_binding,
                            collection.name,
                        ),
                    }
                    targets[target as usize].fan_in = true;
                    target
                }
                None => {
                    let target = targets.len() as u32;
                    targets.push(
                        Target::new(collection, index as u32, ser_policy.clone()).context(index)?,
                    );
                    reps.push((identity, collection));
                    targets_by_name.insert(template_name, target);
                    target
                }
            };

            bindings.push(Binding {
                target,
                state_key: binding.state_key.clone(),
            });
        }

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
            targets,
            close_policy,
            explicit_acknowledgements,
            max_transactions,
            redact_salt: redact_salt.clone(),
            restart,
            sequence_bytes_limit: 512 * 1024 * 1024, // 512 MB.
            shard_ref,
        })
    }

    /// Restore stowed inferred shapes into this session's target layout, seeding
    /// annotations for any target which is net-new.
    pub fn shapes_by_target(&self, mut by_key: BTreeMap<String, doc::Shape>) -> Vec<doc::Shape> {
        let mut by_target = Vec::new();
        by_target.resize_with(self.targets.len(), doc::shape::Shape::nothing);

        for (index, target) in self.targets.iter().enumerate() {
            // Seed inference annotations only when the target is net-new.
            // In particular, carry forward an x-complexity-limit that may have
            // been elevated by the presence of a SourcedSchema.
            if let Some(shape) = by_key.remove(&target.partition_template_name) {
                by_target[index] = shape;
            } else {
                let annotations = &mut by_target[index].annotations;
                annotations.insert(
                    crate::X_GENERATION_ID.to_string(),
                    serde_json::Value::String(target.collection_generation_id.to_string()),
                );
                annotations.insert(
                    doc::shape::X_COMPLEXITY_LIMIT.to_string(),
                    serde_json::json!(doc::shape::limits::DEFAULT_SCHEMA_COMPLEXITY_LIMIT),
                );
            }
        }
        by_target
    }

    /// Invert [`Self::shapes_by_target`], re-keying inferred shapes by their
    /// stable `partition_template_name` identity.
    ///
    /// Inferred shapes are held only in memory and accumulate across the many
    /// connector sessions of a shard's lifetime (a capture re-Opens every poll
    /// `interval`). Target *indices* are not stable across spec updates but the
    /// key is, so the shard stows shapes by key between sessions and
    /// `shapes_by_target` restores them into the next session's layout.
    pub fn shapes_by_key(&self, by_target: Vec<doc::Shape>) -> BTreeMap<String, doc::Shape> {
        assert_eq!(
            by_target.len(),
            self.targets.len(),
            "shapes are the session's own target layout",
        );
        self.targets
            .iter()
            .map(|target| target.partition_template_name.clone())
            .zip(by_target)
            .collect()
    }

    /// Build the combiner Spec of this Task: one validator per target
    /// collection, plus a final validator for the connector-state
    /// pseudo-binding which rides the same combiner at index `bindings.len()`.
    pub fn combine_spec(&self) -> anyhow::Result<doc::combine::Spec> {
        let state_schema = doc::reduce::merge_patch_schema().to_string();
        let state_schema = doc::validation::build_bundle(state_schema.as_bytes()).unwrap();
        let state_validator = doc::Validator::new(state_schema).unwrap();

        let mut validators = Vec::with_capacity(self.targets.len() + 1);

        for target in self.targets.iter() {
            validators.push((
                format!("captured collection {}", target.collection_name),
                build_write_validator(&target.write_schema_json)
                    .with_context(|| format!("collection {}", target.collection_name))?,
            ));
        }
        validators.push(("connector state".to_string(), state_validator));

        let state_validator_index = self.targets.len() as u32;
        let bindings = self
            .bindings
            .iter()
            .map(|binding| {
                (
                    false,
                    self.targets[binding.target as usize].key_extractors.clone(),
                    binding.target,
                )
            })
            .chain(std::iter::once((false, Vec::new(), state_validator_index)));

        Ok(doc::combine::Spec::with_bindings(
            bindings,
            validators,
            self.redact_salt.to_vec(),
        ))
    }
}

impl Target {
    fn new(
        collection: &flow::CollectionSpec,
        first_binding: u32,
        ser_policy: doc::SerPolicy,
    ) -> anyhow::Result<Self> {
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

        let key_extractors = extractors::for_key(key, projections, &ser_policy)?;
        let validator = build_write_validator(write_schema_json)?;

        Ok(Self {
            collection_name: name.clone(),
            collection_generation_id,
            partition_template_name: partition_template.name.clone(),
            document_uuid_ptr: json::Pointer::from(uuid_ptr),
            key_extractors,
            write_schema_json: write_schema_json.clone(),
            write_shape: doc::Shape::infer(validator.schema(), validator.schema_index()),
            first_binding,
            fan_in: false,
        })
    }
}

fn build_write_validator(write_schema_json: &[u8]) -> anyhow::Result<doc::Validator> {
    let built_schema = doc::validation::build_bundle(write_schema_json)
        .context("collection write_schema_json is not a JSON schema")?;

    doc::Validator::new(built_schema).context("could not build a schema validator")
}

/// Synthetic capture specs which fan several bindings into one collection,
/// shared by this module's unit tests and the shard capture actor's tests.
#[cfg(test)]
pub(crate) mod fixture {
    use super::*;

    /// Group hand-built test bindings into their Targets by collection name, as
    /// `Task::new` does over a built spec. Each entry is a binding's
    /// `(collection_name, state_key, uuid_ptr)`; a collection's `uuid_ptr` is taken
    /// from its first binding, and all share `write_schema_json`.
    pub(crate) fn bindings(
        entries: &[(&str, &str, &str)],
        write_schema_json: &'static [u8],
    ) -> (Vec<Binding>, Vec<Target>) {
        let mut bindings = Vec::new();
        let mut targets = Vec::<Target>::new();

        for (index, (collection_name, state_key, uuid_ptr)) in entries.iter().enumerate() {
            let target = match targets
                .iter()
                .position(|t| t.collection_name == *collection_name)
            {
                Some(target) => {
                    targets[target].fan_in = true;
                    target as u32
                }
                None => {
                    let validator = build_write_validator(write_schema_json).unwrap();
                    targets.push(Target {
                        collection_name: collection_name.to_string(),
                        collection_generation_id: models::Id::zero(),
                        partition_template_name: collection_name.to_string(),
                        document_uuid_ptr: json::Pointer::from(*uuid_ptr),
                        key_extractors: Vec::new(),
                        write_schema_json: bytes::Bytes::from_static(write_schema_json),
                        write_shape: doc::Shape::infer(
                            validator.schema(),
                            validator.schema_index(),
                        ),
                        first_binding: index as u32,
                        fan_in: false,
                    });
                    targets.len() as u32 - 1
                }
            };
            bindings.push(Binding {
                target,
                state_key: state_key.to_string(),
            });
        }
        (bindings, targets)
    }

    /// A [`Task`] over hand-built `entries`, as the shard tests want one: wide
    /// close thresholds, so a transaction closes as soon as the connector idles
    /// (its checkpoint sequence completes and no further input is ready), free
    /// of policy-driven close timing.
    pub(crate) fn task(
        entries: &[(&str, &str, &str)],
        write_schema_json: &'static [u8],
        explicit_acknowledgements: bool,
    ) -> Task {
        let (bindings, targets) = self::bindings(entries, write_schema_json);

        Task {
            bindings,
            targets,
            close_policy: close_policy::Policy::new(
                std::time::Duration::ZERO,
                std::time::Duration::MAX,
            ),
            explicit_acknowledgements,
            max_transactions: 0,
            redact_salt: bytes::Bytes::new(),
            restart: uuid::Clock::zero(),
            sequence_bytes_limit: 1 << 20,
            shard_ref: ops::ShardRef::default(),
        }
    }

    /// Collection `index` of a synthetic capture: keyed on `/id`, with reduction
    /// annotations so that documents of one key genuinely combine -- exercising
    /// the validator a fan-in collection's bindings share, rather than only
    /// counting it.
    ///
    /// Every collection *requires* a property naming itself, so a binding which
    /// resolved to another collection's validator fails validation outright
    /// rather than quietly accepting the document.
    pub(crate) fn collection(index: usize) -> flow::CollectionSpec {
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
            write_schema_json: format!(
                r#"{{
                    "type": "object",
                    "properties": {{
                        "id": {{"type": "string"}},
                        "value": {{"type": "integer", "reduce": {{"strategy": "sum"}}}},
                        "from_collection_{index}": {{"const": true}}
                    }},
                    "required": ["id", "from_collection_{index}"],
                    "reduce": {{"strategy": "merge"}}
                }}"#
            )
            .into(),
            ..Default::default()
        }
    }

    /// An indirect-form `CaptureSpec` over `collections` linked collections, where
    /// `binding_collections[i]` is the collection which binding `i` writes.
    /// Repeats fan several bindings into one collection.
    pub(crate) fn capture_spec(
        collections: usize,
        binding_collections: &[usize],
    ) -> flow::CaptureSpec {
        flow::CaptureSpec {
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
            linked_collections: (0..collections).map(collection).collect(),
            shard_template: Some(consumer::ShardSpec {
                min_txn_duration: Some(std::time::Duration::ZERO.into()),
                max_txn_duration: Some(std::time::Duration::from_secs(1).into()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Inline `spec`: each binding carries its own copy of its collection and the
    /// linked table is dropped, so no binding has an identity.
    pub(crate) fn into_inline(spec: &mut flow::CaptureSpec) {
        let table = std::mem::take(&mut spec.linked_collections);

        for binding in &mut spec.bindings {
            binding.collection = Some(table[binding.collection_index as usize].clone());
            binding.collection_index = 0;
        }
    }

    /// The Open / Opened pair carrying `spec`, as `Task::new` consumes them.
    pub(crate) fn open(spec: flow::CaptureSpec) -> (Request, Response) {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_open_over(collections: usize, binding_collections: &[usize]) -> (Request, Response) {
        fixture::open(fixture::capture_spec(collections, binding_collections))
    }

    fn mk_task(collections: usize, binding_collections: &[usize]) -> Task {
        let (open, opened) = mk_open_over(collections, binding_collections);
        Task::new(&open, &opened, 0).unwrap()
    }

    /// The same task in inline form: each binding carries its own copy of its
    /// collection and the linked table is dropped, so no binding has an identity.
    fn mk_inline_task(collections: usize, binding_collections: &[usize]) -> Task {
        let mut spec = fixture::capture_spec(collections, binding_collections);
        fixture::into_inline(&mut spec);

        let (open, opened) = fixture::open(spec);
        Task::new(&open, &opened, 0).unwrap()
    }

    /// Bindings collapse into one Target per distinct collection -- in either
    /// spec form -- and a Target written by multiple bindings is fan-in.
    #[test]
    fn task_groups_bindings_into_targets() {
        let targets_and_fan_in = |task: Task| {
            (
                task.targets.len(),
                task.bindings
                    .iter()
                    .map(|binding| (binding.target, task.targets[binding.target as usize].fan_in))
                    .collect::<Vec<_>>(),
            )
        };

        // Sole binding of its collection.
        assert_eq!(targets_and_fan_in(mk_task(1, &[0])), (1, vec![(0, false)]),);
        // Distinct collections don't collapse, and neither is fan-in.
        assert_eq!(
            targets_and_fan_in(mk_task(2, &[0, 1])),
            (2, vec![(0, false), (1, false)]),
        );
        // Four bindings over two collections collapse to two targets, all fan-in.
        assert_eq!(
            targets_and_fan_in(mk_task(2, &[0, 1, 0, 1])),
            (2, vec![(0, true), (1, true), (0, true), (1, true)]),
        );
        // Targets are ordered by binding scan, not by collection index, and a
        // collection with one binding is not fan-in even beside one that is.
        assert_eq!(
            targets_and_fan_in(mk_task(3, &[2, 1, 2])),
            (2, vec![(0, true), (1, false), (0, true)]),
        );
        // The inline form groups identically: Targets key on the collection's
        // journal identity, which both forms carry.
        assert_eq!(
            targets_and_fan_in(mk_inline_task(2, &[0, 1, 0, 1])),
            (2, vec![(0, true), (1, true), (0, true), (1, true)]),
        );

        // Each target names its collection, its first binding, and holds one
        // inferred write-shape.
        let task = mk_task(2, &[1, 0, 1]);
        assert_eq!(
            task.targets
                .iter()
                .map(|target| (target.collection_name.as_str(), target.first_binding))
                .collect::<Vec<_>>(),
            [("acmeCo/collection-1", 0), ("acmeCo/collection-0", 1)],
        );
    }

    /// Bindings which write one collection share a combiner validator: a parsed
    /// schema, its index, and its validation scratch are built once per
    /// collection rather than once per binding. Keys stay per-binding, as does
    /// the connector-state pseudo-binding riding the same combiner.
    #[test]
    fn combine_spec_groups_validators_by_collection() {
        // Five bindings over two collections, plus connector state.
        let task = mk_task(2, &[0, 1, 0, 1, 0]);
        assert_eq!(
            task.bindings
                .iter()
                .map(|binding| binding.target)
                .collect::<Vec<_>>(),
            [0, 1, 0, 1, 0],
        );

        let spec = task.combine_spec().unwrap();
        assert_eq!(spec.binding_count(), 6);
        assert_eq!(spec.validator_count(), 3);

        // Targets follow binding-scan order, not collection index.
        let task = mk_task(3, &[2, 1, 2]);
        assert_eq!(task.targets.len(), 2);
        assert_eq!(task.combine_spec().unwrap().validator_count(), 3);

        // The inline form of the same task builds the same combiner.
        let task = mk_inline_task(2, &[0, 1, 0, 1, 0]);
        let spec = task.combine_spec().unwrap();
        assert_eq!(spec.binding_count(), 6);
        assert_eq!(spec.validator_count(), 3);
    }

    /// Bindings of one collection must carry equal `CollectionSpec` values:
    /// a hand-built spec which reuses a collection name across differing values
    /// is an error rather than silently-shared derived state.
    #[test]
    fn task_requires_equal_specs_within_a_collection() {
        // Inline form: the second binding's copy of collection-0 is tampered.
        let mut spec = fixture::capture_spec(1, &[0, 0]);
        fixture::into_inline(&mut spec);
        spec.bindings[1].collection.as_mut().unwrap().uuid_ptr = "/_meta/other".to_string();

        let (open, opened) = fixture::open(spec);
        let err = Task::new(&open, &opened, 0).unwrap_err();
        assert!(
            err.to_string().contains(
                "bindings 0 and 1 write collection acmeCo/collection-0 with unequal collection specs"
            ),
            "unexpected error: {err}",
        );

        // Indirect form: two linked-table entries share a name at differing
        // values, and each is referenced by a binding.
        let mut spec = fixture::capture_spec(1, &[0, 0]);
        let mut tampered = spec.linked_collections[0].clone();
        tampered.uuid_ptr = "/_meta/other".to_string();
        spec.linked_collections.push(tampered);
        spec.bindings[1].collection_index = 1;

        let (open, opened) = fixture::open(spec);
        let err = Task::new(&open, &opened, 0).unwrap_err();
        assert!(
            err.to_string().contains("with unequal collection specs"),
            "unexpected error: {err}",
        );
    }

    /// Target indices move when a spec update reorders bindings, so shapes are
    /// stowed under the collection's stable `partition_template_name` and
    /// restored into whichever target that collection now occupies.
    #[test]
    fn shapes_round_trip_across_a_binding_reorder() {
        let marker = |shape: &doc::Shape| shape.annotations.get("marker").cloned();

        let task = mk_task(2, &[0, 1]);
        let mut shapes = task.shapes_by_target(Default::default());

        // Seeded annotations come from the target's collection.
        assert_eq!(
            shapes[0].annotations[crate::X_GENERATION_ID],
            serde_json::json!(task.targets[0].collection_generation_id.to_string()),
        );
        shapes[0]
            .annotations
            .insert("marker".to_string(), serde_json::json!("collection-0"));
        let by_key = task.shapes_by_key(shapes);

        // A spec update reverses the bindings, so collection-0 is now target 1.
        let task = mk_task(2, &[1, 0]);
        assert_eq!(task.targets[1].collection_name, "acmeCo/collection-0");

        let shapes = task.shapes_by_target(by_key);
        assert_eq!(marker(&shapes[1]), Some(serde_json::json!("collection-0")));
        assert_eq!(marker(&shapes[0]), None); // Net-new target, freshly seeded.
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
        assert_eq!(task.targets.len(), 1);

        let (open, opened) = mk_open_over(1, &bindings);
        let err = Task::new(&open, &opened, 0).unwrap_err();
        assert!(
            err.to_string()
                .contains("65536 bindings, which exceeds the combiner limit of 65535"),
            "unexpected error: {err}",
        );
    }
}
