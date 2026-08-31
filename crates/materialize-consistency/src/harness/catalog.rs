//! The catalog a scenario publishes: the workload, and the materialization under
//! test.
//!
//! # The workload
//!
//! An exactly-once violation is only detectable when destination state depends on
//! *how many times* a document was applied. Under last-write-wins merge semantics
//! applying a document twice is invisible, so the workload has to pair a
//! `sum`-reduced counter with an append-only binding or it cannot see the thing it
//! exists to detect. Hence two collections over the same documents:
//!
//! - `merged`, keyed `[/id]` with `sum` on the balance delta. The runtime performs
//!   the reduction, so the reduced document's balance delta *is* the account's
//!   balance, and the oracle says what that balance should be.
//! - `log`, keyed `[/id, /seq]` and append-only. Every document has a distinct
//!   key, so a duplicate delivery is an extra row rather than a re-reduction.
//!
//! # Two captures, not one capture with two bindings
//!
//! `source-soak` routes each document to `id % len(bindings)`, so one capture with
//! two bindings would *partition* accounts between the two collections rather than
//! writing both. Conservation would then hold only over the union of the two
//! destinations, and standard-vs-delta agreement would be uncheckable — the two
//! collections would describe disjoint accounts. Two single-binding captures give
//! each collection a self-conserving population instead, so every invariant is
//! checkable per binding and a failure localizes. The capture connector itself is
//! reused unmodified either way.
//!
//! # Reproducibility
//!
//! Nothing here is seeded, and it does not need to be. The capture and the
//! materialization are separate tasks joined only by the collection's journals:
//! once a document is written it is durable and immutable, and interrupting the
//! materialization neither touches the capture nor rewrites the collection. Every
//! crash-and-replay scenario therefore replays byte-identical input by
//! construction.

use crate::protocol::{ENV_FAULTS, ENV_RUN_DIR, ENV_TRACE_REDUCE, FaultRule};
use anyhow::Context;

/// The wire contract of the workload, shared with the soak suite so there is one
/// source of truth for it. Compiled in rather than referenced by path, because
/// the catalog is published from a temporary file and a relative `$ref` would
/// resolve against that.
const EVENTS_SCHEMA: &str = include_str!("../../../../tests/soak/capture/events.schema.json");

/// The subject's resource config naming one of this run's destination tables.
///
/// Shared with the verification read, which addresses the same tables through the same
/// connector and would otherwise construct this a second way.
pub fn resource_config(
    shape: Option<&crate::harness::subject::ResourceShape>,
    names: &Names,
    table: &str,
    delta: bool,
) -> serde_json::Value {
    let table = names.table(table);
    match shape {
        Some(shape) => shape.resource(&table, delta),
        None => serde_json::json!({"table": table, "delta": delta}),
    }
}

/// Destination resource names, before any per-run suffix. See [`Names::table`].
pub const TABLE_STANDARD: &str = "accounts";
pub const TABLE_MERGED_DELTA: &str = "accounts_delta";
pub const TABLE_LOG: &str = "events";

/// Catalog names of one run. Every name carries the run's random suffix, so
/// concurrent runs — several agents on one stack — never touch each other's
/// tasks.
#[derive(Clone, Debug)]
pub struct Names {
    pub prefix: String,
    pub source_merged: String,
    pub source_log: String,
    pub merged: String,
    pub log: String,
    pub sink: String,
    /// Suffix distinguishing this run's tables, or empty.
    ///
    /// The reference connector needs none: its destination is a file of this run's own,
    /// so nothing can collide. A real connector materializes into a *shared* catalog and
    /// schema, where every concurrent scenario would otherwise write the same three
    /// tables — so there the names carry the run id.
    table_suffix: String,
}

impl Names {
    pub fn new(tenant: &str, scenario: &str, run_id: &str, shared_destination: bool) -> Self {
        let prefix = format!("{tenant}/consistency/{scenario}-{run_id}");

        Self {
            source_merged: format!("{prefix}/source-merged"),
            source_log: format!("{prefix}/source-log"),
            merged: format!("{prefix}/merged"),
            log: format!("{prefix}/log"),
            sink: format!("{prefix}/sink"),
            // Underscored, not hyphenated: these become SQL identifiers.
            table_suffix: match shared_destination {
                // `_flow_test_<unix>` is the connectors repository's convention for a test
                // resource, and `testctl -mode sweep` will only remove names carrying it —
                // the timestamp is how a sweep leaves a concurrent run's tables alone. So a
                // run that is killed before its own cleanup is still recoverable, which
                // by-name dropping cannot achieve: it can only remove what the caller knows
                // it created.
                true => format!(
                    "_{run_id}_flow_test_{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or_default(),
                ),
                false => String::new(),
            },
            prefix,
        }
    }

    /// This run's name for one of the destination tables.
    pub fn table(&self, base: &str) -> String {
        format!("{base}{}", self.table_suffix)
    }
}

/// How much workload a scenario runs against.
pub struct Workload {
    /// Documents per second, per collection.
    pub rate: f64,
    /// Width of the account-id window. A narrow window concentrates events on few
    /// accounts, which is what makes a duplicate or a gap show up quickly.
    pub id_range: u32,
    /// Lower bound on transaction duration. Together with the rate this shapes
    /// transaction size to roughly `rate × duration`; the runtime's document- and
    /// byte-count limits are not yet threaded through from the spec, so
    /// transaction boundaries are approximate. Scenarios are keyed on protocol
    /// events rather than document identity precisely because of that.
    ///
    /// It also sets the pace of every gate in a run. A scenario waits out a warmup and a
    /// settle measured in *commits*, so transaction duration is most of a run's wall
    /// clock — measured at 18.8s of a 35s run. Shortening it costs nothing in coverage,
    /// for the same reason the boundaries are approximate: nothing here is keyed on how
    /// many documents a transaction carries.
    pub min_txn: std::time::Duration,
    /// Upper bound on transaction duration, which has to leave room for a subject that
    /// commits slowly: the runtime closes a transaction at this bound whether or not the
    /// connector has kept up.
    pub max_txn: std::time::Duration,
}

impl Default for Workload {
    /// Shaped for a subject whose destination is local and commits in milliseconds.
    fn default() -> Self {
        Self {
            rate: 40.0,
            id_range: 40,
            min_txn: std::time::Duration::from_millis(500),
            max_txn: std::time::Duration::from_secs(2),
        }
    }
}

impl Workload {
    /// Shaped for a subject that commits to a remote system.
    ///
    /// A real materialization does not commit in milliseconds: it stages files to object
    /// storage and runs a MERGE on a warehouse, which is seconds to tens of seconds per
    /// transaction. Asking such a connector for a transaction every 500ms — the local
    /// default — does not make it faster, it makes it fall behind until the gates that
    /// count commits time out, which is exactly how `materialize-databricks` failed here.
    ///
    /// So transactions are long and the rate is lower: fewer, larger transactions cover
    /// the same protocol ground, and every scenario is keyed on protocol events rather
    /// than on document counts, so nothing is lost by carrying less data.
    pub fn remote() -> Self {
        Self {
            rate: 10.0,
            id_range: 40,
            min_txn: std::time::Duration::from_secs(15),
            max_txn: std::time::Duration::from_secs(30),
        }
    }
}

pub struct Plan<'a> {
    pub names: &'a Names,
    pub subject: &'a crate::scenarios::Subject,
    pub shim: &'a std::path::Path,
    pub capture: &'a std::path::Path,
    pub run_dir: &'a std::path::Path,
    pub faults: &'a [FaultRule],
    pub workload: &'a Workload,
    /// Whether to materialize the merged collection with standard (merge) semantics
    /// in addition to the two delta bindings. See `Scenario::standard_binding`.
    pub standard_binding: bool,
    /// Where the table name and delta flag live in the subject's resource config.
    ///
    /// The reference connector's own shape when absent. A real connector's is discovered
    /// from its `spec`.
    pub resource_shape: Option<&'a crate::harness::subject::ResourceShape>,
}

/// Build the whole catalog of a run.
pub fn build(plan: &Plan<'_>) -> anyhow::Result<models::Catalog> {
    let mut catalog = models::Catalog::default();

    let base: serde_json::Value =
        serde_json::from_str(EVENTS_SCHEMA).context("parsing the workload's event schema")?;

    catalog.collections.insert(
        models::Collection::new(&plan.names.merged),
        collection(merged_schema(base.clone())?, &["/id"])?,
    );
    catalog.collections.insert(
        models::Collection::new(&plan.names.log),
        collection(base, &["/id", "/seq"])?,
    );

    for (task, target) in [
        (&plan.names.source_merged, &plan.names.merged),
        (&plan.names.source_log, &plan.names.log),
    ] {
        catalog
            .captures
            .insert(models::Capture::new(task), capture(plan, target, false)?);
    }

    catalog.materializations.insert(
        models::Materialization::new(&plan.names.sink),
        materialization(plan)?,
    );

    Ok(catalog)
}

/// The `merged` collection's schema: the workload's contract plus the reduction
/// annotations that make an over- or under-delivery arithmetically visible.
fn merged_schema(mut schema: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let object = schema
        .as_object_mut()
        .context("the event schema is not an object")?;

    object.insert(
        "reduce".to_string(),
        serde_json::json!({"strategy": "merge"}),
    );
    object.insert(
        "description".to_string(),
        serde_json::json!(
            "A soak-test event, reduced per account: `sum` over the balance delta \
             makes the reduced document's delta the account's balance, which its \
             own oracle independently states."
        ),
    );

    let properties = object
        .get_mut("properties")
        .and_then(|p| p.as_object_mut())
        .context("the event schema has no properties")?;

    // Everything except the summed counter is replaced wholesale by the
    // highest-sequence document, so the reduced document carries that document's
    // oracle. Left to `merge`'s recursion, `set` and `transfer` would accumulate
    // keys from superseded events.
    for (field, strategy) in [
        ("balanceDelta", "sum"),
        ("oracle", "lastWriteWins"),
        ("set", "lastWriteWins"),
        ("transfer", "lastWriteWins"),
    ] {
        let Some(property) = properties.get_mut(field).and_then(|p| p.as_object_mut()) else {
            anyhow::bail!("the event schema has no `{field}` property to annotate");
        };
        property.insert(
            "reduce".to_string(),
            serde_json::json!({"strategy": strategy}),
        );
    }

    Ok(schema)
}

fn collection(schema: serde_json::Value, key: &[&str]) -> anyhow::Result<models::CollectionDef> {
    Ok(models::CollectionDef {
        schema: Some(models::Schema::new(models::RawValue::from_value(&schema))),
        write_schema: None,
        read_schema: None,
        key: models::CompositeKey::new(
            key.iter()
                .map(|p| models::JsonPointer::new(*p))
                .collect::<Vec<_>>(),
        ),
        projections: Default::default(),
        journals: Default::default(),
        derive: None,
        expect_pub_id: None,
        delete: false,
        reset: false,
    })
}

/// A catalog that stops the workload and nothing else.
///
/// It carries only the two captures, disabled. Republishing the materialization
/// here would bump its version and so drive an Apply and a fresh session over the
/// very task under test — perturbing it at the exact moment the run has stopped
/// perturbing it on purpose.
pub fn disable_captures(plan: &Plan<'_>) -> anyhow::Result<models::Catalog> {
    let mut catalog = models::Catalog::default();

    for (task, target) in [
        (&plan.names.source_merged, &plan.names.merged),
        (&plan.names.source_log, &plan.names.log),
    ] {
        catalog
            .captures
            .insert(models::Capture::new(task), capture(plan, target, true)?);
    }
    Ok(catalog)
}

fn capture(plan: &Plan<'_>, target: &str, disable: bool) -> anyhow::Result<models::CaptureDef> {
    let config = serde_json::json!({
        "rate": plan.workload.rate,
        "docsPerCheckpoint": 10,
        "idRange": plan.workload.id_range,
        // One resource, so every document lands in this capture's single
        // collection rather than being partitioned across several.
        "collections": ["events"],
    });

    Ok(models::CaptureDef {
        auto_discover: None,
        endpoint: models::CaptureEndpoint::Local(models::LocalConfig {
            command: vec![plan.capture.to_string_lossy().to_string()],
            config: models::RawValue::from_value(&config),
            env: Default::default(),
            protobuf: false,
        }),
        bindings: vec![models::CaptureBinding {
            resource: models::RawValue::from_value(&serde_json::json!({"name": "events"})),
            disable: false,
            target: models::Collection::new(target),
            backfill: 0,
        }],
        interval: std::time::Duration::from_secs(1),
        redact_salt: None,
        shards: models::ShardTemplate {
            disable,
            log_level: Some("warn".to_string()),
            ..runtime_v2()
        },
        expect_pub_id: None,
        delete: false,
        reset: false,
    })
}

/// The scenario's catalog with the materialization *disabled*, and the captures left
/// running.
///
/// Half of the escalation in `harness::restart_task`, which publishes this and then the enabled
/// catalog again; that function carries the reasoning for why a restart is sometimes needed where
/// unassigning is not enough.
pub fn sink_disabled(plan: &Plan<'_>) -> anyhow::Result<models::Catalog> {
    let mut catalog = build(plan)?;

    for def in catalog.materializations.values_mut() {
        def.shards.disable = true;
    }
    Ok(catalog)
}

fn materialization(plan: &Plan<'_>) -> anyhow::Result<models::MaterializationDef> {
    let mut env = std::collections::BTreeMap::new();
    env.insert(
        ENV_RUN_DIR.to_string(),
        plan.run_dir.to_string_lossy().to_string(),
    );
    env.insert(
        ENV_FAULTS.to_string(),
        serde_json::to_string(plan.faults).context("encoding fault rules")?,
    );

    // Forwarded rather than set, so it is off unless a person asks for it.
    //
    // The reduction trace records every `Load` and `Store` of a merged key, plus each staged
    // batch an `Acknowledge` applies — which is the only way to see *why* a reduced value came
    // out wrong: the delivered rows say what the connector was told, never what it read before
    // reducing onto it. Too voluminous to leave on, too useful to reinvent.
    //
    // Not recovery decisions, despite an earlier version of this comment: the counted channel's
    // skip, decided in `open_counters`, is traced nowhere.
    if std::env::var_os(ENV_TRACE_REDUCE).is_some() {
        env.insert(ENV_TRACE_REDUCE.to_string(), "1".to_string());
    }

    // Whatever the subject itself asked for, last so a caller can override the above knowingly.
    // See `subject::ENV_SUBJECT_ENV` for why a connector needs this at all: run from its image it
    // has an environment its Dockerfile built, and run as a `local:` binary it does not.
    env.extend(plan.subject.env.clone());

    // The shim is the catalog's connector; the real one is its argument. This is
    // the whole of the interposition: no change to Flow, and no change to the
    // connector under test.
    // Protobuf against a real connector, JSON against the reference one.
    //
    // The shim relays requests without transcoding, so runtime, shim and connector must
    // agree on one codec — and a Go materialization connector cannot use JSON. Under the
    // JSON codec the runtime populates `Load.key_json` and leaves `key_packed` empty, which
    // is by design (see `Load` in `go/protocols/materialize/materialize.proto`: "the runtime
    // populates exactly one of `key_json` or `key_packed` per the negotiated codec"). But
    // `Request_Load.Validate` in `go/protocols/materialize/extensions.go` requires
    // `KeyPacked` and carries the note "KeyJson is not checked yet", and the boilerplate
    // reads only `KeyPacked` — so every `Load` and `Store` is rejected outright.
    //
    // So this is not a preference. JSON is available only to the reference connector, which
    // is Rust and reads whichever field is set. The shim's `trace.jsonl` is human-readable
    // either way, so nothing is lost by the fleet speaking protobuf.
    let protobuf = plan.resource_shape.is_some();

    let mut command = vec![plan.shim.to_string_lossy().to_string()];
    if protobuf {
        command.push("--protobuf".to_string());
    }
    command.extend(plan.subject.connector.iter().cloned());

    let binding = |collection: &str, table: &str, delta: bool| models::MaterializationBinding {
        resource: models::RawValue::from_value(&resource_config(
            plan.resource_shape,
            plan.names,
            table,
            delta,
        )),
        source: models::Source::Collection(models::Collection::new(collection)),
        disable: false,
        priority: 0,
        fields: Default::default(),
        backfill: 0,
        on_incompatible_schema_change: None,
    };

    Ok(models::MaterializationDef {
        source: None,
        target_naming: None,
        on_incompatible_schema_change: Default::default(),
        endpoint: models::MaterializationEndpoint::Local(models::LocalConfig {
            command,
            config: models::RawValue::from_value(&plan.subject.config),
            env,
            protobuf,
        }),
        bindings: plan
            .standard_binding
            .then(|| binding(&plan.names.merged, TABLE_STANDARD, false))
            .into_iter()
            .chain([
                binding(&plan.names.merged, TABLE_MERGED_DELTA, true),
                binding(&plan.names.log, TABLE_LOG, true),
            ])
            .collect(),
        shards: models::ShardTemplate {
            log_level: Some("info".to_string()),
            min_txn_duration: Some(plan.workload.min_txn),
            max_txn_duration: Some(plan.workload.max_txn),
            ..runtime_v2()
        },
        expect_pub_id: None,
        triggers: None,
        delete: false,
        reset: false,
    })
}

/// Scenarios are written against V2 semantics only: that is where the
/// coordinator/shard-zero state scatter-gather lives, and where idempotent
/// transaction replay — which the document-counter class depends on — exists.
fn runtime_v2() -> models::ShardTemplate {
    models::ShardTemplate {
        flags: [(
            models::Token::new("enable-runtime-v2"),
            models::Token::new("true"),
        )]
        .into_iter()
        .collect(),
        ..Default::default()
    }
}
