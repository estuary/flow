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
    /// Staged work awaiting application, per binding.
    ///
    /// Keyed by binding rather than by transaction, following
    /// `materialize-databricks`: a binding that has gone away must not have its
    /// statements run against a table that may already be dropped, and a binding
    /// disabled after a failed apply has to be able to finish that work when it is
    /// re-enabled. Neither is expressible if the unit is a whole transaction.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pending: BTreeMap<String, PendingApply>,
    /// Per-resource count of documents this shard has appended, as of the last
    /// committed transaction. The document-counter class compares it against the
    /// destination's own count to decide what to skip.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    appended: BTreeMap<String, i64>,
}

/// One binding's staged-but-unapplied work, as the checkpoint carries it.
///
/// The statements themselves, not a reference to them — the same shape as
/// `materialize-databricks`' `checkpointItem{Queries, ToDelete}`. Holding the work
/// rather than a pointer to it is what makes the checkpoint self-sufficient: applying
/// it never requires asking the destination what was left behind, which cannot
/// distinguish work awaiting application from work that was abandoned.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
struct PendingApply {
    /// Statements which apply the staged batch, run in order as one transaction.
    queries: Vec<String>,
    /// Staged batches these statements consume, for reporting and for garbage
    /// collection of staging that no entry claims.
    to_delete: Vec<String>,
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
    /// Load keys staged by this transaction, read when `Flush` arrives. See
    /// [`stage_load`] for why the read cannot happen sooner.
    pending_loads: Vec<(u32, String)>,
    /// Work staged by the transaction in progress, not yet published in a checkpoint.
    pending: BTreeMap<String, PendingApply>,
    /// Work whose transaction the log has confirmed, awaiting release.
    ///
    /// Applied on *every* `Acknowledge` until released, because the runtime may retry an
    /// `Acknowledge` and the statements are idempotent — the trailing `DELETE` retires the
    /// batch, so a re-run changes nothing. Released at the next `StartCommit`, by which
    /// point the clearing patch has had its chance to commit. A connector that skipped the
    /// retirement would duplicate on the retry, which is the `non-idempotent-acknowledge`
    /// defect and is what makes it observable.
    confirmed: BTreeMap<String, PendingApply>,
    /// Published-but-unacknowledged work, oldest transaction first.
    ///
    /// One entry per `StartCommit`, released by the matching `Acknowledge`. The protocol
    /// pipelines — an `Acknowledge` confirming transaction N can arrive after
    /// `StartedCommit(N+1)` — so "everything currently staged" is the wrong thing to
    /// apply: it would commit N+1's work before the recovery log holds it, and a replay
    /// of N+1 would then reduce its documents onto a destination that already counts them.
    published: std::collections::VecDeque<BTreeMap<String, PendingApply>>,
    /// Pending work of *other* ranges — peers of this transaction, and ranges left by
    /// previous shard topologies. Tracked and executed only by the primary.
    peers: BTreeMap<String, BTreeMap<String, PendingApply>>,
    /// Whether this shard is the primary: the one that runs staged statements.
    ///
    /// Only the primary applies, which is how `materialize-databricks` keeps two shards
    /// from contending over one binding's table. It learns of its peers' work from the
    /// aggregated state patches the runtime delivers with `Acknowledge`.
    primary: bool,
    /// `begin-end` of the range this session owns, as its key in connector state.
    range_key: String,
    /// Serial for naming this session's staged batches.
    batch_seq: u64,
    /// Batch the transaction in progress is staging into, if it has begun staging.
    ///
    /// One batch per transaction, not per flush: `Store` may write several times as the
    /// buffer fills, and the statements rendered at `StartCommit` name a single batch.
    /// A fresh name per flush would strand every chunk but the last — staged, referenced
    /// by no checkpoint entry, and so never applied.
    batch: Option<String>,
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
        stage_load(session, load)?;
        return Ok(Vec::new());
    }
    if request.flush.is_some() {
        return flush_loads(session);
    }
    if let Some(store) = request.store {
        store_document(session, store)?;
        return Ok(Vec::new());
    }
    if let Some(start_commit) = request.start_commit {
        return start_commit_txn(session, start_commit).map(|r| vec![r]);
    }
    if let Some(ack) = request.acknowledge {
        return acknowledge(session, ack).map(|r| vec![r]);
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
            documentation_url:
                "https://github.com/estuary/flow/tree/master/crates/materialize-consistency"
                    .to_string(),
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

/// `Apply` performs the destination's DDL, and nothing else.
///
/// It is handed no connector state, so it cannot know which staged work committed —
/// and the destination cannot tell it: a transaction that died mid-commit leaves the
/// same trace as one that committed and was not yet applied. Draining on that basis
/// applied abandoned work, and because splitting a task republishes its spec, that
/// landed on exactly the recovery `split-during-commit` exercises.
///
/// Staged work is finished by whoever holds the checkpoint that describes it, in
/// `Acknowledge`. `Apply` may be called more than once for a version, so the DDL here
/// is idempotent.
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

    // Tested against the range the runtime sent, not the one `ignore-key-range` may have
    // substituted: which shard this is, is not the connector's to decide.
    let primary = range.key_begin == 0 && range.r_clock_begin == 0;
    let range_key = shard_key(key_begin, key_end);

    let state: State = if open.state_json.is_empty() || &open.state_json[..] == b"{}" {
        State::default()
    } else {
        serde_json::from_slice(&open.state_json).context("parsing connector state")?
    };
    let shard_state = state.shards.get(&range_key).cloned().unwrap_or_default();

    // A non-primary shard recovers no pending work at all.
    //
    // Following `materialize-databricks`: the primary replays the whole consolidated
    // state document, so a non-primary that also kept recovered entries and re-emitted
    // them would have the primary run them a second time — after the staged batch they
    // consume was already retired. Recovering nothing makes that impossible rather than
    // merely unlikely.
    let (pending, peers) = if !primary {
        (BTreeMap::new(), BTreeMap::new())
    } else {
        let peers = state
            .shards
            .iter()
            .filter(|(key, entry)| *key != &range_key && !entry.pending.is_empty())
            .map(|(key, entry)| (key.clone(), entry.pending.clone()))
            .collect();
        (shard_state.pending.clone(), peers)
    };

    let mut session = Session {
        store,
        class: config.class,
        defects: config.defects,
        bindings,
        key_begin,
        key_end,
        nonce,
        buffered: Vec::new(),
        pending_loads: Vec::new(),
        pending: BTreeMap::new(),
        // Recovered work is known committed, so it is releasable from the first
        // `Acknowledge` — which the protocol delivers before this session's first `Load`.
        confirmed: pending,
        published: std::collections::VecDeque::new(),
        peers,
        primary,
        range_key,
        batch_seq: 0,
        batch: None,
        skip: Vec::new(),
        stored: 0,
    };

    let runtime_checkpoint = match session.class {
        // Only shard zero may propose a runtime checkpoint.
        //
        // Under V2 the non-zero shards of a leaderful task are *stateless*: they have no
        // recovery log and acquire everything through the leader protocol, so the leader
        // accepts a non-default `Opened` only from shard zero and fails the whole task
        // otherwise (`recv_opened`, runtime-next). A destination-authoritative connector
        // therefore has to gate its checkpoint on being shard zero, however authoritative
        // its destination is for the *data*.
        Class::RemoteAuthoritative if primary => decode_checkpoint(checkpoint.as_deref())?,
        Class::RemoteAuthoritative => None,
        Class::PostCommitApply => {
            // Nothing is reclaimed or inspected here.
            //
            // Staging that no checkpoint entry names is inert: applying is driven purely
            // by the statements in state, so unreferenced rows are never run. They are
            // the exact analogue of a staged file a real connector abandoned, and a real
            // connector leaves those alone too.
            //
            // Sweeping them at `Open` would also be a race the deleter cannot win: a peer
            // may have staged a batch and emitted its patch moments ago, and a primary
            // reading a state snapshot from just before that would delete work it is about
            // to be told to apply.
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
        let destination =
            session
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

fn decode_checkpoint(
    bytes: Option<&[u8]>,
) -> anyhow::Result<Option<proto_flow::RuntimeCheckpoint>> {
    let Some(bytes) = bytes else { return Ok(None) };
    if bytes.is_empty() {
        return Ok(None);
    }
    let checkpoint = <proto_flow::RuntimeCheckpoint as prost::Message>::decode(bytes)
        .context("decoding stored runtime checkpoint")?;
    Ok(Some(checkpoint))
}

/// Record what a merge binding was handed as a reduction base, and what came back.
///
/// Only for non-delta tables, because they are the only ones whose stored value depends
/// on what `Load` returned: the runtime reduces new documents onto that base and stores
/// the result. A wrong base is invisible in the delivered *set* and shows up only as a
/// wrong sum, which is exactly the failure this exists to explain — the merge binding
/// disagreeing with its collection while the delta binding over the same collection
/// agrees.
/// Record an application: whose entry, which binding, and the batches it consumes.
///
/// The owner range matters and the session's own range does not: the question a failing
/// merged value raises is *whose* staged absolute was written, and when.
fn trace_apply(owner: &str, binding: &str, batches: &[String]) {
    if std::env::var_os(crate::protocol::ENV_TRACE_REDUCE).is_none() {
        return;
    }
    let Ok(dir) = std::env::var(crate::protocol::ENV_RUN_DIR) else {
        return;
    };
    let line = serde_json::json!({
        "event": "apply-pending",
        "owner": owner,
        "binding": binding,
        "batches": batches,
        "pid": std::process::id(),
    })
    .to_string()
        + "\n";

    use std::io::Write;
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(std::path::Path::new(&dir).join("reduce.jsonl"))
    {
        let _ = file.write_all(line.as_bytes());
    }
}

fn trace_reduce(event: &str, table: &Table, key: &str, doc: Option<&str>, txn: i64) {
    if table.delta {
        return;
    }
    // Opt-in: this writes two lines per merge-binding document, and a measured run should
    // not pay for it. Set `FLOW_CONSISTENCY_TRACE_REDUCE=1` when running the suite
    // environment to investigate a wrong stored sum.
    if std::env::var_os(crate::protocol::ENV_TRACE_REDUCE).is_none() {
        return;
    }
    let Ok(dir) = std::env::var(crate::protocol::ENV_RUN_DIR) else {
        return;
    };
    let delta = doc.and_then(|doc| {
        serde_json::from_str::<serde_json::Value>(doc)
            .ok()
            .and_then(|v| v.get("balanceDelta").and_then(|d| d.as_i64()))
    });
    let seq = doc.and_then(|doc| {
        serde_json::from_str::<serde_json::Value>(doc)
            .ok()
            .and_then(|v| v.get("seq").and_then(|d| d.as_i64()))
    });

    let line = serde_json::json!({
        "event": event, "table": table.name, "key": key,
        "balanceDelta": delta, "seq": seq, "txn": txn, "pid": std::process::id(),
    });

    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(std::path::Path::new(&dir).join("reduce.jsonl"))
    {
        use std::io::Write;
        let _ = file.write_all(format!("{line}\n").as_bytes());
    }
}

/// Stage a load key. The destination is *not* read here.
///
/// Every real connector of every class does it this way — `materialize-databricks`,
/// `-snowflake` and `-bigquery` write keys to a staging file inside the `it.Next()` loop,
/// and `materialize-postgres` queues them into a temp table — and only after the loop, once
/// `Flush` has arrived, do they join against the target table.
///
/// That is not merely a batching convenience: `Flush` is the runtime's signal that the
/// previous transaction was acknowledged **by every shard**. A connector where one shard
/// applies staged work on behalf of its peers has no other way to know that. Reading the
/// destination as each `Load` arrives would read a base that the applying shard has not
/// finished writing, and a merged binding reduced onto that base loses the difference.
fn stage_load(session: &mut Session, load: materialize::request::Load) -> anyhow::Result<()> {
    anyhow::ensure!(
        (load.binding as usize) < session.bindings.len(),
        "Load names an unknown binding",
    );
    let key = std::str::from_utf8(&load.key_json).context("Load key is not UTF-8")?;

    session.pending_loads.push((load.binding, key.to_string()));
    Ok(())
}

/// Read every staged load key, at `Flush`.
///
/// Keys not found MUST be omitted rather than answered as null.
fn flush_loads(session: &mut Session) -> anyhow::Result<Vec<materialize::Response>> {
    let mut responses = Vec::new();

    for (binding, key) in std::mem::take(&mut session.pending_loads) {
        let table = session.bindings[binding as usize].table.clone();
        let loaded = session.store.load(&table, &key)?;

        trace_reduce(
            "load",
            &table,
            &key,
            loaded.as_deref(),
            session.batch_seq as i64,
        );

        if let Some(doc) = loaded {
            responses.push(materialize::Response {
                loaded: Some(materialize::response::Loaded {
                    binding,
                    doc_json: doc.into(),
                }),
                ..Default::default()
            });
        }
    }

    responses.push(materialize::Response {
        flushed: Some(materialize::response::Flushed { state: None }),
        ..Default::default()
    });
    Ok(responses)
}

/// Documents per staging batch.
///
/// Post-commit-apply stages *during* `Store` rather than at `StartCommit`, which
/// is what makes "a crash mid-Store leaves staged data that must not
/// double-apply" a real situation rather than a hypothetical. Batching keeps that
/// property while paying one transaction per 64 documents instead of per
/// document.
const STAGE_BATCH: usize = 64;

fn store_document(session: &mut Session, store: materialize::request::Store) -> anyhow::Result<()> {
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

    trace_reduce(
        "store",
        &table,
        &row.key,
        Some(&row.doc),
        session.batch_seq as i64,
    );

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
                let rows = std::mem::take(&mut session.buffered);
                let batch = current_batch(session);
                session.store.stage(&batch, &rows)?;
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
            let staged_anything = session.batch.is_some() || !rows.is_empty();
            let batch = current_batch(session);
            if !rows.is_empty() {
                session.store.stage(&batch, &rows)?;
            }
            // The transaction's batch is closed here; the next one opens its own.
            session.batch = None;

            // Render the statements which will apply this batch, and record them against
            // each binding they touch. This is the checkpoint's whole content: an
            // `Acknowledge` — this session's or a successor's — needs nothing but these.
            let deduplicate = !session.has(Defect::NonIdempotentAcknowledge);

            for table in match staged_anything {
                true => staged_tables(&batch, session)?,
                false => Vec::new(),
            } {
                let entry = session.pending.entry(table.name.clone()).or_default();
                entry
                    .queries
                    .extend(Store::apply_statements(&batch, &table, deduplicate));
                entry.to_delete.push(batch.clone());
            }

            // Whatever was confirmed has been applied and its clearing patch emitted.
            session.confirmed.clear();

            let publishing = std::mem::take(&mut session.pending);
            let patch = pending_patch(&session.range_key, &publishing);
            session.published.push_back(publishing);
            Some(patch)
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
                // A counted channel stages nothing, so it has no pending work.
                ShardState {
                    pending: BTreeMap::new(),
                    appended,
                },
            ))
        }

        Class::AtLeastOnce => None,
    };

    Ok(materialize::Response {
        started_commit: Some(materialize::response::StartedCommit { state }),
        ..Default::default()
    })
}

/// Apply committed staging that belongs to a range other than this session's.
///
/// This is the coordinating half of post-commit-apply, and it runs at `Open` rather than on
/// every `Acknowledge` for two reasons. It is rare — only a membership change leaves work
/// under a range with no owner — and it must complete before the session writes anything,
/// since inherited staging is older than whatever comes next and applying it afterwards
/// would overwrite newer values.
///
/// Applying it on the hot path instead, with writers waiting for a single applier, collapses
/// throughput: the task stays healthy but stops keeping up, and the destination ends the run
/// behind its collection.
/// Fold the peers' aggregated state patches into the primary's bookkeeping.
///
/// The runtime delivers every shard's just-committed `StartedCommit` patch here, which is
/// how the primary learns what its peers staged — including ranges from a previous shard
/// topology, whose shards are gone and whose work nobody else will finish.
fn merge_peer_patches(session: &mut Session, patches: &[u8]) -> anyhow::Result<()> {
    if !session.primary || patches.is_empty() {
        return Ok(());
    }

    let patches: Vec<serde_json::Value> =
        serde_json::from_slice(patches).context("decoding aggregated state patches")?;

    for patch in patches {
        anyhow::ensure!(
            !patch.is_null(),
            "unexpected state reset patch: a peer replaced the consolidated state \
             document rather than merging into it, which would clobber every range",
        );

        let Some(shards) = patch.get("shards").and_then(|s| s.as_object()) else {
            continue;
        };
        for (range_key, entry) in shards {
            if range_key == &session.range_key {
                continue; // Our own contribution, echoed back; already in `pending`.
            }
            let Some(pending) = entry.get("pending") else {
                continue;
            };
            let pending: BTreeMap<String, PendingApply> =
                serde_json::from_value(pending.clone())
                    .with_context(|| format!("parsing peer pending work for {range_key}"))?;

            session
                .peers
                .entry(range_key.clone())
                .or_default()
                .extend(pending);
        }
    }
    Ok(())
}

fn acknowledge(
    session: &mut Session,
    acknowledge: materialize::request::Acknowledge,
) -> anyhow::Result<materialize::Response> {
    if session.class != Class::PostCommitApply {
        return Ok(materialize::Response {
            acknowledged: Some(materialize::response::Acknowledged { state: None }),
            ..Default::default()
        });
    }

    merge_peer_patches(session, &acknowledge.state_patches_json)?;

    // A non-primary shard stages and nothing more. Its entries are executed by the
    // primary, which saw them in the aggregated patches, so it drops them from its own
    // bookkeeping — mirroring the clearing the primary emits for them.
    if !session.primary {
        session.confirmed.clear();
        session.published.pop_front();
        return Ok(materialize::Response {
            acknowledged: Some(materialize::response::Acknowledged { state: None }),
            ..Default::default()
        });
    }

    // Own work first, then every other range's, oldest range first for determinism.
    // Predecessors are finished *here* rather than at `Open`: the contract is that a
    // recovered checkpoint's staged update is re-applied as part of `Acknowledge`, and
    // doing it here means it happens under the same rules as ordinary work instead of on
    // a separate path that only recovery exercises.
    let mut cleared: BTreeMap<String, Vec<String>> = BTreeMap::new();

    // This `Acknowledge` confirms one more transaction has reached the recovery log, so
    // promote exactly one — the oldest still unconfirmed.
    //
    // A retry finds nothing left to promote and simply re-applies what is confirmed, which
    // is a no-op for a connector that retires its batches and a duplicate for one that does
    // not. That is what makes `non-idempotent-acknowledge` observable.
    //
    // The residual hazard is a retry arriving *after* the next `StartCommit`, which would
    // promote a transaction the log has not confirmed. The connector cannot tell that case
    // apart: `Acknowledge` carries no transaction identity, only the aggregated patches of
    // whichever transaction it confirms. Closing it needs the protocol to say which.
    if let Some(promoted) = session.published.pop_front() {
        session.confirmed.extend(promoted);
    }

    let executed = apply_pending(
        session,
        &session.range_key.clone(),
        &session.confirmed.clone(),
    )?;
    if !executed.is_empty() {
        cleared.insert(session.range_key.clone(), executed);
    }

    for (range_key, pending) in session.peers.clone() {
        let executed = apply_pending(session, &range_key, &pending)?;
        if executed.is_empty() {
            continue;
        }
        if let Some(bucket) = session.peers.get_mut(&range_key) {
            for state_key in &executed {
                bucket.remove(state_key);
            }
            if bucket.is_empty() {
                session.peers.remove(&range_key);
            }
        }
        cleared.insert(range_key, executed);
    }

    let state = (!cleared.is_empty()).then(|| clearing_patch(&cleared));

    Ok(materialize::Response {
        acknowledged: Some(materialize::response::Acknowledged { state }),
        ..Default::default()
    })
}

/// Run the statements of every entry whose binding still exists, returning the bindings
/// that were executed.
///
/// An entry for a binding that has gone away is skipped rather than run: its table may
/// already have been dropped. Its entry is left in place, so re-enabling the binding
/// finishes the work rather than losing it.
fn apply_pending(
    session: &Session,
    owner: &str,
    pending: &BTreeMap<String, PendingApply>,
) -> anyhow::Result<Vec<String>> {
    let mut executed = Vec::new();

    for (state_key, item) in pending {
        if !session.bindings.iter().any(|b| &b.table.name == state_key) {
            continue;
        }
        session
            .store
            .execute(&item.queries)
            .with_context(|| format!("applying staged work of binding {state_key}"))?;

        trace_apply(owner, state_key, &item.to_delete);
        executed.push(state_key.clone());
    }
    Ok(executed)
}

/// A merge patch carrying only this shard's entry.
/// The post-commit-apply state patch: what this session has staged and applied, plus
/// every departed range it finished at `Open`.
///
/// Carrying the predecessors is what retires them. Their entries outlive the shards that
/// wrote them — the runtime consolidates state and has no notion that a range is gone —
/// so without this, every later session would walk the same ranges forever, and the
/// record of "staged work awaiting application" would never become empty.
/// The batch this transaction stages into, creating it on first use.
///
/// Unique per session and transaction, standing in for a staged file's path.
fn current_batch(session: &mut Session) -> String {
    if let Some(batch) = &session.batch {
        return batch.clone();
    }
    session.batch_seq += 1;
    let batch = format!(
        "{}-{}-{}",
        session.range_key,
        std::process::id(),
        session.batch_seq
    );
    session.batch = Some(batch.clone());
    batch
}

/// The tables a staged batch touches, so statements are rendered per binding.
fn staged_tables(batch: &str, session: &Session) -> anyhow::Result<Vec<Table>> {
    let names = session.store.staged_tables(batch)?;

    Ok(session
        .bindings
        .iter()
        .map(|b| b.table.clone())
        .filter(|t| names.contains(&t.name))
        .collect())
}

/// This range's pending work, as a merge patch of its own bucket.
///
/// Only its own: range keys are disjoint across a task's shards, so concurrent patches
/// commute and none clobbers another when the runtime consolidates connector state.
fn pending_patch(
    range_key: &str,
    pending: &BTreeMap<String, PendingApply>,
) -> flow::ConnectorState {
    flow::ConnectorState {
        updated_json: serde_json::json!({"shards": {range_key: {"pending": pending}}})
            .to_string()
            .into(),
        merge_patch: true,
    }
}

/// A merge patch clearing the entries named in `cleared`, keyed by range.
///
/// Nulls rather than a rewritten document: a merge patch that sets an entry to null
/// deletes it, which is how state empties as work is finished instead of accumulating a
/// record of every range that ever existed. Only what was actually executed is cleared —
/// a binding whose statements failed keeps its entry so a later session can finish it.
fn clearing_patch(cleared: &BTreeMap<String, Vec<String>>) -> flow::ConnectorState {
    let mut shards = serde_json::Map::new();

    for (range_key, state_keys) in cleared {
        let mut pending = serde_json::Map::new();
        for state_key in state_keys {
            pending.insert(state_key.clone(), serde_json::Value::Null);
        }
        shards.insert(range_key.clone(), serde_json::json!({"pending": pending}));
    }

    flow::ConnectorState {
        updated_json: serde_json::json!({"shards": shards}).to_string().into(),
        merge_patch: true,
    }
}

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
