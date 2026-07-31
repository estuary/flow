//! A deliberately-breakable reference materialization.
//!
//! The suite has to prove itself. A consistency suite that passes vacuously is
//! worse than no suite, because it manufactures confidence that agents and
//! reviewers will act on — so every scenario is paired with a defect here that it
//! must demonstrably catch, and both runs happen in the same test.
//!
//! It also implements each connector class independently of any real connector,
//! so the harness cannot quietly bake in one vendor's assumptions, and so the
//! document-counter class is executable before any production connector adopts
//! it.
//!
//! The classes follow the four patterns from the scale-out strategies discussion:
//!
//! | Class | Commits during | Authority | Fenced by |
//! | --- | --- | --- | --- |
//! | `remoteAuthoritative` | `StartCommit` | destination checkpoint | nonce table |
//! | `postCommitApply` | `Acknowledge`, from durable staging | recovery log | — |
//! | `documentCounter` | `Store`, appending to a fenced channel | destination count | nonce table |
//! | `atLeastOnce` | `Store` | recovery log | — |

pub mod store;

use anyhow::Context;
use proto_flow::{flow, materialize};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use store::{Row, Store, Table};

/// How the connector divides responsibility for durability with the runtime.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum Class {
    /// The destination holds the authoritative checkpoint and is committed to
    /// during `StartCommit`, behind a fence.
    #[default]
    RemoteAuthoritative,
    /// Documents are staged durably during `Store` and applied during
    /// `Acknowledge`; the recovery log is authoritative.
    PostCommitApply,
    /// Documents are appended during `Store` to a channel that counts what it has
    /// accepted; recovery skips whatever the destination already holds. Rows
    /// become visible before the Flow transaction commits — a real, declared
    /// deviation rather than a defect.
    DocumentCounter,
    /// Commits during `Store` with no deduplication. Never loses data, and makes
    /// no claim about duplicates.
    AtLeastOnce,
}

/// A switchable defect. Each one exists to be caught by a specific scenario; a
/// scenario without a paired defect is not finished.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Defect {
    /// Apply staged work without claiming the transaction, so a replayed
    /// `Acknowledge` applies it twice.
    NonIdempotentAcknowledge,
    /// Commit each `Store` immediately instead of at `StartCommit`, so a crash
    /// before `StartCommit` leaves data that the replay applies again.
    CommitDuringStore,
    /// Fence and stage under a fixed range rather than the session's own, so two
    /// shards of a split task collide.
    IgnoreKeyRange,
    /// Commit without checking the fence, so a zombie's stale commit lands.
    SkipFenceCheck,
    /// Never skip on recovery, so a replayed transaction re-appends what the
    /// destination already holds.
    DropDocumentCounter,
    /// Zero the destination's count on `Open` and guess at the resulting
    /// impossible state instead of refusing it.
    ResetCounterOnOpen,
    /// Silently skip an occasional document, as a mishandled batch boundary would.
    ///
    /// This is the paired defect for every scenario whose claim is "loses nothing".
    /// It applies to all four classes, which the class-specific defects do not: a
    /// class that already commits during `Store` cannot be made to commit during
    /// `Store`, so at-least-once would otherwise have no defect to be held against.
    DropDocuments,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EndpointConfig {
    /// Absolute path of the SQLite file backing the destination. Absolute
    /// because the reactor spawns `local:` connectors from `$HOME`.
    pub path: String,
    #[serde(default)]
    pub class: Class,
    #[serde(default)]
    pub defects: Vec<Defect>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ResourceConfig {
    pub table: String,
    #[serde(default)]
    pub delta: bool,
}

/// Connector state, keyed by each shard's whole key range so that a task's shards do
/// not clobber one another *and* a shard can tell an ancestor from a sibling.
///
/// The runtime concatenates every shard's state patch and consolidates them into
/// shard zero, so a shard writing whole-state would erase its peers'. Keying it and
/// emitting a merge patch is also what makes the state split-safe: a child inherits
/// the parent's entries and its own is simply absent.
///
/// The key is `begin-end`, not `begin` alone, and that is load-bearing. After a
/// two-way split the low child shares its `key_begin` with the departed parent, so a
/// `begin`-keyed entry cannot say whether it describes an ancestor or a live sibling
/// — and acting on it would mean discarding a sibling's in-flight staging. With both
/// bounds, containment answers the question: strictly containing is an ancestor,
/// equal is oneself, and two live shards never contain each other.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
struct State {
    #[serde(default)]
    shards: BTreeMap<String, ShardState>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
struct ShardState {
    /// Last transaction whose staging is known to have committed to the recovery
    /// log. Post-commit-apply resumes from it.
    #[serde(default)]
    txn: i64,
    /// Per-resource count of documents this shard has appended, as of the last
    /// committed transaction. The document-counter class compares it against the
    /// destination's own count to decide what to skip.
    #[serde(default)]
    appended: BTreeMap<String, i64>,
}

/// A binding of the open session. The connector treats a document's key as
/// opaque text, so the table shape is all it needs to remember.
struct Binding {
    table: Table,
}

/// The open session: everything a transaction needs, and nothing that outlives
/// the process.
pub struct Session {
    store: Store,
    class: Class,
    defects: Vec<Defect>,
    bindings: Vec<Binding>,
    /// Range this session owns, as the fence and staging key. Distinct from the
    /// range the runtime sent when the `ignore-key-range` defect is on.
    key_begin: u32,
    key_end: u32,
    nonce: i64,
    /// Documents of the transaction in progress, awaiting whatever the class does
    /// with them.
    buffered: Vec<(Table, Row)>,
    /// Transaction being accumulated (post-commit-apply).
    staging_txn: i64,
    /// Transaction awaiting its `Acknowledge` (post-commit-apply).
    committed_txn: i64,
    /// Documents still to be skipped on this session's first transaction,
    /// per binding (document-counter).
    skip: Vec<i64>,
    /// Documents this session has been asked to store, for the `drop-documents`
    /// defect to count against.
    stored: u64,
}

impl Session {
    fn has(&self, defect: Defect) -> bool {
        self.defects.contains(&defect)
    }
}

/// Serve the materialization protocol on stdio until the runtime closes it.
///
/// Synchronous by design: the protocol is strictly request-ordered from the
/// connector's side, and SQLite is synchronous, so an async loop here would add
/// machinery without buying concurrency.
pub fn serve(codec: connector_init::Codec) -> anyhow::Result<()> {
    use std::io::{Read, Write};

    let mut stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let mut buffer = Vec::with_capacity(32 * 1024);
    let mut chunk = vec![0u8; 32 * 1024];
    let mut encoded = Vec::new();
    let mut session: Option<Session> = None;

    loop {
        let n = stdin.read(&mut chunk).context("reading stdin")?;
        if n == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&chunk[..n]);

        for request in codec.decode::<materialize::Request>(&mut buffer)? {
            for response in handle(&mut session, request)? {
                encoded.clear();
                codec.encode(&response, &mut encoded);
                stdout.write_all(&encoded)?;
            }
            stdout.flush()?;
        }
    }
}

/// Dispatch one request, yielding the responses it calls for.
fn handle(
    session: &mut Option<Session>,
    request: materialize::Request,
) -> anyhow::Result<Vec<materialize::Response>> {
    if request.spec.is_some() {
        return Ok(vec![spec()]);
    }
    if let Some(validate) = request.validate {
        return Ok(vec![validate_bindings(validate)?]);
    }
    if let Some(apply) = request.apply {
        return Ok(vec![apply_spec(apply)?]);
    }
    if let Some(open) = request.open {
        let (new, response) = open_session(open)?;
        *session = Some(new);
        return Ok(vec![response]);
    }

    let session = session
        .as_mut()
        .context("received a transaction request before Open")?;

    if let Some(load) = request.load {
        return load_document(session, load);
    }
    if request.flush.is_some() {
        return Ok(vec![materialize::Response {
            flushed: Some(materialize::response::Flushed { state: None }),
            ..Default::default()
        }]);
    }
    if let Some(store) = request.store {
        store_document(session, store)?;
        return Ok(Vec::new());
    }
    if let Some(start_commit) = request.start_commit {
        return start_commit_txn(session, start_commit).map(|r| vec![r]);
    }
    if request.acknowledge.is_some() {
        return acknowledge(session).map(|r| vec![r]);
    }

    anyhow::bail!("unhandled request: {request:?}")
}

fn spec() -> materialize::Response {
    // Hand-written rather than derived: the schemas are part of this connector's
    // contract with the harness's catalog builder, and writing them out keeps
    // both sides readable in one place.
    let config_schema = serde_json::json!({
        "type": "object",
        "title": "Reference destination",
        "required": ["path"],
        "properties": {
            "path": {"type": "string", "title": "Destination SQLite file (absolute)"},
            "class": {
                "type": "string",
                "title": "Consistency class implemented by this build",
                "enum": ["remoteAuthoritative", "postCommitApply", "documentCounter", "atLeastOnce"],
                "default": "remoteAuthoritative",
            },
            "defects": {
                "type": "array",
                "title": "Defects to enable",
                "items": {"type": "string", "enum": [
                    "nonIdempotentAcknowledge", "commitDuringStore", "ignoreKeyRange",
                    "skipFenceCheck", "dropDocumentCounter", "resetCounterOnOpen", "noApplyDrain",
                ]},
            },
        },
    });
    let resource_schema = serde_json::json!({
        "type": "object",
        "required": ["table"],
        "properties": {
            "table": {"type": "string", "title": "Destination table", "x-collection-name": true},
            "delta": {"type": "boolean", "title": "Delta updates", "default": false},
        },
    });

    materialize::Response {
        spec: Some(materialize::response::Spec {
            protocol: 3032023,
            config_schema_json: config_schema.to_string().into(),
            resource_config_schema_json: resource_schema.to_string().into(),
            documentation_url: "https://github.com/estuary/flow/tree/master/crates/materialize-consistency".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn validate_bindings(
    validate: materialize::request::Validate,
) -> anyhow::Result<materialize::Response> {
    use materialize::response::validated::constraint::Type;
    use materialize::response::validated::{Constraint, ProjectionConstraint};

    let mut bindings = Vec::new();

    for binding in &validate.bindings {
        let resource: ResourceConfig = serde_json::from_slice(&binding.resource_config_json)
            .context("parsing resource configuration")?;
        let collection = binding
            .collection
            .as_ref()
            .context("binding is missing its collection")?;

        let mut projection_constraints = Vec::new();

        for projection in &collection.projections {
            // The root document is required because the connector stores it
            // verbatim: every invariant the suite checks is computed from the
            // document, so a field-per-column layout would put the checkers at
            // the mercy of this connector's flattening rules.
            let (r#type, reason) = if projection.ptr.is_empty() {
                (
                    Type::LocationRequired,
                    "the root document is stored as a single column",
                )
            } else if collection.key.contains(&projection.ptr) {
                (Type::LocationRequired, "components of the collection key")
            } else {
                (Type::FieldOptional, "")
            };

            projection_constraints.push(ProjectionConstraint {
                field: projection.field.clone(),
                constraint: Some(Constraint {
                    r#type: r#type as i32,
                    reason: reason.to_string(),
                    folded_field: String::new(),
                }),
            });
        }

        bindings.push(materialize::response::validated::Binding {
            resource_path: vec![resource.table],
            delta_updates: resource.delta,
            projection_constraints,
            ..Default::default()
        });
    }

    Ok(materialize::Response {
        validated: Some(materialize::response::Validated { bindings }),
        ..Default::default()
    })
}

/// `Apply` performs the destination's DDL, and must first commit the pending work
/// of every affected binding.
///
/// That ordering is the contract: `Apply` runs while the materialization is
/// quiescent but *after* a session may have staged work it never got to apply, so
/// DDL that made committing impossible — dropping the table a staged row targets
/// — would destroy an in-flight transaction. `Apply` may also be called more than
/// once for the same version, so everything here is idempotent.
fn apply_spec(apply: materialize::request::Apply) -> anyhow::Result<materialize::Response> {
    let materialization = apply
        .materialization
        .as_ref()
        .context("Apply is missing its materialization")?;
    let config: EndpointConfig = serde_json::from_slice(&materialization.config_json)
        .context("parsing endpoint configuration")?;
    let store = Store::open(std::path::Path::new(&config.path))?;

    let mut actions = Vec::new();

    let tables = bindings_of(materialization)?;
    let last: Vec<Table> = match &apply.last_materialization {
        Some(last) => bindings_of(last)?.into_iter().map(|b| b.table).collect(),
        None => Vec::new(),
    };

    // Drain first, then perform DDL. Draining every shard's staged work is correct
    // here precisely because Apply runs while the task is quiescent: no session can
    // be adding more. Idempotently, because Apply may be called again for the same
    // version.
    for (begin, end) in store.staged_shard_keys()? {
        for txn in store.staged_txns(begin, end)? {
            if store.apply_staged(begin, end, txn, true)? {
                actions.push(format!(
                    "drained staged transaction {txn} of range [{begin:08x}, {end:08x}]"
                ));
            }
        }
    }

    for binding in &tables {
        store.ensure_table(&binding.table)?;
    }

    // A binding that has gone away takes its table with it. Reaching here having
    // drained means its staged work has already landed.
    for table in last {
        if !tables.iter().any(|b| b.table.name == table.name) {
            store.drop_table(&table.name)?;
            actions.push(format!("dropped {}", table.name));
        }
    }

    store.record_applied_spec(&apply.version, &actions.join("; "))?;

    Ok(materialize::Response {
        applied: Some(materialize::response::Applied {
            action_description: actions.join("; "),
            state: None,
        }),
        ..Default::default()
    })
}

fn bindings_of(spec: &flow::MaterializationSpec) -> anyhow::Result<Vec<Binding>> {
    spec.bindings
        .iter()
        .map(|binding| {
            let resource: ResourceConfig = serde_json::from_slice(&binding.resource_config_json)
                .context("parsing resource configuration")?;
            Ok(Binding {
                table: Table {
                    name: resource.table,
                    delta: binding.delta_updates,
                },
            })
        })
        .collect()
}

fn open_session(
    open: materialize::request::Open,
) -> anyhow::Result<(Session, materialize::Response)> {
    let materialization = open
        .materialization
        .as_ref()
        .context("Open is missing its materialization")?;
    let config: EndpointConfig = serde_json::from_slice(&materialization.config_json)
        .context("parsing endpoint configuration")?;

    let bindings = bindings_of(materialization)?;
    let store = Store::open(std::path::Path::new(&config.path))?;

    let range = open.range.clone().unwrap_or_default();
    let (key_begin, key_end) = if config.defects.contains(&Defect::IgnoreKeyRange) {
        // Every shard claims the whole keyspace, so two shards of a split task
        // fence each other off and overwrite each other's checkpoint.
        (0, u32::MAX)
    } else {
        (range.key_begin, range.key_end)
    };

    let (nonce, checkpoint) = store.fence(key_begin, key_end)?;

    let state: State = if open.state_json.is_empty() || &open.state_json[..] == b"{}" {
        State::default()
    } else {
        serde_json::from_slice(&open.state_json).context("parsing connector state")?
    };
    let shard_state = state
        .shards
        .get(&shard_key(key_begin, key_end))
        .cloned()
        .unwrap_or_default();

    let mut session = Session {
        store,
        class: config.class,
        defects: config.defects,
        bindings,
        key_begin,
        key_end,
        nonce,
        buffered: Vec::new(),
        staging_txn: shard_state.txn + 1,
        committed_txn: shard_state.txn,
        skip: Vec::new(),
        stored: 0,
    };

    // Only shard zero may propose a runtime checkpoint.
    //
    // Under V2 the non-zero shards of a leaderful task are *stateless*: they have no
    // recovery log and acquire everything through the leader protocol, so the leader
    // refuses an `Opened` from one that carries a checkpoint — it fails the whole
    // task with `expected Opened` during its fan-in. A destination-authoritative
    // connector therefore has to gate its checkpoint on being shard zero, however
    // authoritative its destination is for the *data*.
    //
    // Tested against the range the runtime sent, not the one `ignore-key-range` may
    // have substituted: which shard this is, is not the connector's to decide.
    let shard_zero = range.key_begin == 0 && range.r_clock_begin == 0;

    let runtime_checkpoint = match session.class {
        Class::RemoteAuthoritative if shard_zero => decode_checkpoint(checkpoint.as_deref())?,
        Class::RemoteAuthoritative => None,
        Class::PostCommitApply => {
            // Staging beyond the last committed transaction belongs to a transaction
            // the recovery log never committed. It must never become visible, and the
            // runtime is about to replay that input, so drop it.
            session.store.discard_staged_after(
                session.key_begin,
                session.key_end,
                session.committed_txn,
            )?;

            // Then settle what an ancestor left behind. A split child owns keys whose
            // staged work is filed under the wider range that used to contain them,
            // and the ancestor is gone, so nobody else will: its committed
            // transactions must be applied and its uncommitted ones discarded.
            //
            // Both children of a split do this for the same ancestor, which is
            // harmless — applying is idempotent per transaction and discarding is a
            // delete. Neither can touch the other's staging, because a sibling is
            // never an ancestor.
            for (begin, end, ancestor) in ancestors(&state, session.key_begin, session.key_end) {
                session
                    .store
                    .discard_staged_after(begin, end, ancestor.txn)?;

                for txn in session.store.staged_txns(begin, end)? {
                    session.store.apply_staged(
                        begin,
                        end,
                        txn,
                        !session.has(Defect::NonIdempotentAcknowledge),
                    )?;
                }
            }
            None
        }
        Class::DocumentCounter => {
            open_counters(&mut session, &shard_state)?;
            None
        }
        Class::AtLeastOnce => None,
    };

    Ok((
        session,
        materialize::Response {
            opened: Some(materialize::response::Opened {
                runtime_checkpoint,
                disable_load_optimization: false,
            }),
            ..Default::default()
        },
    ))
}

/// Reconcile this session's per-binding append counts against the destination's,
/// deciding how many documents of the replayed transaction to skip.
///
/// This is the whole of the document-counter class's exactly-once claim. The
/// destination's count is ahead of the checkpoint's exactly when a transaction
/// appended rows and then failed before the recovery log committed; those rows
/// are already there, so the replay must skip precisely that many. Behind is
/// impossible and is refused rather than guessed at.
fn open_counters(session: &mut Session, shard_state: &ShardState) -> anyhow::Result<()> {
    if session.has(Defect::ResetCounterOnOpen) {
        session
            .store
            .reset_appended(session.key_begin, session.key_end)?;
    }

    session.skip = Vec::with_capacity(session.bindings.len());

    for binding in &session.bindings {
        let destination = session
            .store
            .appended(session.key_begin, session.key_end, &binding.table.name)?;
        let checkpointed = shard_state
            .appended
            .get(&binding.table.name)
            .copied()
            .unwrap_or(0);

        let skip = if session.has(Defect::DropDocumentCounter) {
            // The destination's committed count is never consulted, so the
            // replayed transaction re-appends everything it already holds.
            0
        } else if destination >= checkpointed {
            destination - checkpointed
        } else if session.has(Defect::ResetCounterOnOpen) {
            // Guessing at an impossible state instead of refusing it. The
            // correct branch is below.
            0
        } else {
            anyhow::bail!(
                "destination holds {destination} appends of {} but the committed checkpoint \
                 claims {checkpointed}: the destination cannot be behind the checkpoint, so \
                 refusing to guess",
                binding.table.name,
            );
        };

        session.skip.push(skip);
    }
    Ok(())
}

fn shard_key(key_begin: u32, key_end: u32) -> String {
    format!("{key_begin:08x}-{key_end:08x}")
}

/// The range a state key names.
fn parse_shard_key(key: &str) -> Option<(u32, u32)> {
    let (begin, end) = key.split_once('-')?;
    Some((
        u32::from_str_radix(begin, 16).ok()?,
        u32::from_str_radix(end, 16).ok()?,
    ))
}

/// Entries whose range *strictly* contains this session's — its ancestors.
///
/// A split child inherits keys whose staged work is still filed under the range that
/// contained them, and nobody else is going to reconcile it: the ancestor is gone.
/// Strict containment excludes the session itself, and no two live shards contain
/// one another, so a sibling can never appear here.
fn ancestors(state: &State, key_begin: u32, key_end: u32) -> Vec<(u32, u32, ShardState)> {
    state
        .shards
        .iter()
        .filter_map(|(key, entry)| {
            let (begin, end) = parse_shard_key(key)?;
            let contains = begin <= key_begin && end >= key_end;

            (contains && (begin, end) != (key_begin, key_end))
                .then(|| (begin, end, entry.clone()))
        })
        .collect()
}

fn decode_checkpoint(bytes: Option<&[u8]>) -> anyhow::Result<Option<proto_flow::RuntimeCheckpoint>> {
    let Some(bytes) = bytes else { return Ok(None) };
    if bytes.is_empty() {
        return Ok(None);
    }
    let checkpoint = <proto_flow::RuntimeCheckpoint as prost::Message>::decode(bytes)
        .context("decoding stored runtime checkpoint")?;
    Ok(Some(checkpoint))
}

fn load_document(
    session: &mut Session,
    load: materialize::request::Load,
) -> anyhow::Result<Vec<materialize::Response>> {
    let binding = session
        .bindings
        .get(load.binding as usize)
        .context("Load names an unknown binding")?;

    let key = std::str::from_utf8(&load.key_json).context("Load key is not UTF-8")?;

    let Some(doc) = session
        .store
        .load(session.key_begin, session.key_end, &binding.table, key)? else {
        return Ok(Vec::new()); // Keys not found MUST be omitted.
    };

    Ok(vec![materialize::Response {
        loaded: Some(materialize::response::Loaded {
            binding: load.binding,
            doc_json: doc.into(),
        }),
        ..Default::default()
    }])
}

/// Documents per staging batch.
///
/// Post-commit-apply stages *during* `Store` rather than at `StartCommit`, which
/// is what makes "a crash mid-Store leaves staged data that must not
/// double-apply" a real situation rather than a hypothetical. Batching keeps that
/// property while paying one transaction per 64 documents instead of per
/// document.
const STAGE_BATCH: usize = 64;

fn store_document(
    session: &mut Session,
    store: materialize::request::Store,
) -> anyhow::Result<()> {
    let binding_index = store.binding as usize;
    let binding = session
        .bindings
        .get(binding_index)
        .context("Store names an unknown binding")?;

    let table = binding.table.clone();
    let row = Row {
        binding: binding_index,
        key: std::str::from_utf8(&store.key_json)
            .context("Store key is not UTF-8")?
            .to_string(),
        doc: std::str::from_utf8(&store.doc_json)
            .context("Store document is not UTF-8")?
            .to_string(),
        delete: store.delete,
    };

    session.stored += 1;

    // Every 37th document silently goes nowhere. An odd stride so it does not
    // align with the staging batch size or with a transaction boundary, and so it
    // cannot be mistaken for a boundary-handling artifact.
    if session.has(Defect::DropDocuments) && session.stored % 37 == 0 {
        return Ok(());
    }

    match session.class {
        Class::RemoteAuthoritative if session.has(Defect::CommitDuringStore) => {
            // Visible before the transaction commits, and with no record of
            // having been applied: a crash before `StartCommit` leaves it behind
            // and the replay applies it again.
            session.store.commit(
                session.key_begin,
                session.key_end,
                session.nonce,
                None,
                &[(table, row)],
                !session.has(Defect::SkipFenceCheck),
            )?;
        }
        Class::RemoteAuthoritative => session.buffered.push((table, row)),

        Class::PostCommitApply => {
            session.buffered.push((table, row));
            if session.buffered.len() >= STAGE_BATCH {
                let batch = std::mem::take(&mut session.buffered);
                session
                    .store
                    .stage(session.key_begin, session.key_end, session.staging_txn, &batch)?;
            }
        }

        Class::DocumentCounter => {
            if session.skip[binding_index] > 0 {
                session.skip[binding_index] -= 1;
                return Ok(());
            }
            session.buffered.push((table, row));
            if session.buffered.len() >= STAGE_BATCH {
                let batch = std::mem::take(&mut session.buffered);
                session
                    .store
                    .append_counted(session.key_begin, session.key_end, &batch)?;
            }
        }

        Class::AtLeastOnce => {
            session.store.commit(
                session.key_begin,
                session.key_end,
                session.nonce,
                None,
                &[(table, row)],
                false,
            )?;
        }
    }
    Ok(())
}

fn start_commit_txn(
    session: &mut Session,
    start_commit: materialize::request::StartCommit,
) -> anyhow::Result<materialize::Response> {
    let checkpoint = start_commit
        .runtime_checkpoint
        .map(|c| prost::Message::encode_to_vec(&c));

    let state = match session.class {
        Class::RemoteAuthoritative => {
            let rows = std::mem::take(&mut session.buffered);
            session.store.commit(
                session.key_begin,
                session.key_end,
                session.nonce,
                checkpoint.as_deref(),
                &rows,
                !session.has(Defect::SkipFenceCheck),
            )?;
            None
        }

        Class::PostCommitApply => {
            let rows = std::mem::take(&mut session.buffered);
            if !rows.is_empty() {
                session
                    .store
                    .stage(session.key_begin, session.key_end, session.staging_txn, &rows)?;
            }
            session.committed_txn = session.staging_txn;
            session.staging_txn += 1;

            Some(shard_patch(
                session.key_begin,
                session.key_end,
                ShardState {
                    txn: session.committed_txn,
                    appended: BTreeMap::new(),
                },
            ))
        }

        Class::DocumentCounter => {
            let rows = std::mem::take(&mut session.buffered);
            if !rows.is_empty() {
                session
                    .store
                    .append_counted(session.key_begin, session.key_end, &rows)?;
            }

            // Read each channel's offset back *from the destination* rather than
            // reporting a count the connector kept itself.
            //
            // The destination is the authority: it increments the offset atomically
            // with accepting the row, and recovery works by comparing that offset
            // against the one this checkpoint records. A connector-side mirror of it
            // would be a second copy of the only number that matters, and its drift
            // is invisible precisely in the situation the class exists to survive —
            // a process that died between accepting a row and noting that it had.
            //
            // Recording it here is what makes it trustworthy: this state rides in
            // `StartedCommit` and so commits atomically with the recovery log.
            // Dropping it is the `drop-document-counter` defect.
            let appended = if session.has(Defect::DropDocumentCounter) {
                BTreeMap::new()
            } else {
                let mut offsets = BTreeMap::new();
                for binding in &session.bindings {
                    offsets.insert(
                        binding.table.name.clone(),
                        session.store.appended(
                            session.key_begin,
                            session.key_end,
                            &binding.table.name,
                        )?,
                    );
                }
                offsets
            };

            Some(shard_patch(
                session.key_begin,
                session.key_end,
                ShardState { txn: 0, appended },
            ))
        }

        Class::AtLeastOnce => None,
    };

    Ok(materialize::Response {
        started_commit: Some(materialize::response::StartedCommit { state }),
        ..Default::default()
    })
}

fn acknowledge(session: &mut Session) -> anyhow::Result<materialize::Response> {
    if session.class == Class::PostCommitApply {
        // Apply *every* committed-but-unapplied transaction, not just the newest.
        //
        // The steady state has exactly one — the transaction whose recovery-log commit
        // this Acknowledge reports — so the loop is usually a single pass. Recovery is
        // where it matters: a session fenced mid-flight can leave more than one
        // transaction staged and log-committed but unacknowledged, and applying only
        // the newest leaves the older ones stranded forever. `discard_staged_after`
        // will not reclaim them either, since it only removes transactions *after* the
        // committed one — so they are neither applied nor discarded, and their
        // documents are simply lost.
        //
        // That is what made `split-during-commit` lose one transaction's worth of
        // documents: ~20 of them, spread thinly over half the accounts at early
        // sequences, with no duplicates. The connector had been handed every document
        // (the trace showed *more* Stores than the collection holds, because the split
        // replays) and applied fewer.
        //
        // Idempotent per transaction, so re-applying an already-claimed one is a
        // no-op and this is safe to run on every Acknowledge.
        for txn in session
            .store
            .staged_txns(session.key_begin, session.key_end)?
        {
            if txn > session.committed_txn {
                continue; // Not yet committed to the recovery log.
            }
            session.store.apply_staged(
                session.key_begin,
                session.key_end,
                txn,
                !session.has(Defect::NonIdempotentAcknowledge),
            )?;
        }
    }

    Ok(materialize::Response {
        acknowledged: Some(materialize::response::Acknowledged { state: None }),
        ..Default::default()
    })
}

/// A merge patch carrying only this shard's entry.
fn shard_patch(key_begin: u32, key_end: u32, state: ShardState) -> flow::ConnectorState {
    let patch = serde_json::json!({"shards": {shard_key(key_begin, key_end): state}});

    flow::ConnectorState {
        updated_json: patch.to_string().into(),
        merge_patch: true,
    }
}

/// Emit every row of a materialized resource as newline-delimited JSON.
///
/// The harness reads destinations through the connector binary rather than
/// reaching into them, so that the same code path serves the reference connector
/// and real ones (where it is `materialize-boilerplate`'s `read` subcommand over
/// an interface every SQL destination already implements).
pub fn read(config: &EndpointConfig, table: &str, delta: bool) -> anyhow::Result<()> {
    use std::io::Write;

    let store = Store::open(std::path::Path::new(&config.path))?;
    let table = Table {
        name: table.to_string(),
        delta,
    };

    let mut stdout = std::io::stdout().lock();
    for doc in store.read_all(&table)? {
        writeln!(stdout, "{doc}")?;
    }
    stdout.flush()?;
    Ok(())
}
