use anyhow::Context;
use gazette::broker::journal_spec;
use proto_flow::flow;
use proto_gazette::{
    broker::{self, JournalSpec, Label, LabelSelector, LabelSet},
    consumer::{self, ShardSpec},
};
use serde_json::json;
use std::collections::BTreeMap;

// A Shard or Journal change to be applied.
#[derive(serde::Serialize)]
pub enum Change {
    Shard(consumer::apply_request::Change),
    Journal(broker::apply_request::Change),
}

// JournalSplit describes a collection partition or a shard recovery log.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct JournalSplit {
    pub name: String,
    pub labels: LabelSet,
    pub mod_revision: i64,
    pub suspend: Option<journal_spec::Suspend>,
}

// ShardSplit describes a task partition.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ShardSplit {
    pub id: String,
    pub labels: LabelSet,
    pub mod_revision: i64,
    pub primary_hints: Option<recoverylog::FsmHints>,
}

#[derive(Copy, Clone, Debug)]
pub struct TaskTemplate<'a> {
    pub shard: &'a ShardSpec,
    pub recovery: &'a JournalSpec,
}

// Map a CaptureSpec into its activation TaskTemplate.
pub fn capture_template(
    task_spec: Option<&flow::CaptureSpec>,
) -> anyhow::Result<Option<TaskTemplate>> {
    let Some(task_spec) = task_spec else {
        return Ok(None);
    };

    let shard_template = task_spec
        .shard_template
        .as_ref()
        .context("CaptureSpec missing shard_template")?;

    let recovery_template = task_spec
        .recovery_log_template
        .as_ref()
        .context("CaptureSpec missing recovery_log_template")?;

    Ok(Some(TaskTemplate {
        shard: shard_template,
        recovery: recovery_template,
    }))
}

// Map a MaterializationSpec into its activation TaskTemplate.
pub fn materialization_template(
    task_spec: Option<&flow::MaterializationSpec>,
) -> anyhow::Result<Option<TaskTemplate>> {
    let Some(task_spec) = task_spec else {
        return Ok(None);
    };

    let shard_template = task_spec
        .shard_template
        .as_ref()
        .context("MaterializationSpec missing shard_template")?;

    let recovery_template = task_spec
        .recovery_log_template
        .as_ref()
        .context("MaterializationSpec missing recovery_log_template")?;

    Ok(Some(TaskTemplate {
        shard: shard_template,
        recovery: recovery_template,
    }))
}

// Map a CollectionSpeck into its activation partition template and,
// if a derivation, its activation TaskTemplate.
pub fn collection_template(
    task_spec: Option<&flow::CollectionSpec>,
) -> anyhow::Result<(Option<&JournalSpec>, Option<TaskTemplate>)> {
    let Some(task_spec) = task_spec else {
        return Ok((None, None));
    };

    let partition_template = task_spec
        .partition_template
        .as_ref()
        .context("CollectionSpec missing partition_template")?;

    let task_template = if let Some(derivation) = &task_spec.derivation {
        let shard_template = derivation
            .shard_template
            .as_ref()
            .context("CollectionSpec.Derivation missing shard_template")?;

        let recovery_template = derivation
            .recovery_log_template
            .as_ref()
            .context("CollectionSpec.Derivation missing recovery_log_template")?;

        Some(TaskTemplate {
            shard: shard_template,
            recovery: recovery_template,
        })
    } else {
        None
    };

    Ok((Some(partition_template), task_template))
}

/// Activate a capture into a data-plane.
pub async fn activate_capture(
    journal_client: &gazette::journal::Client,
    shard_client: &gazette::shard::Client,
    capture: &models::Capture,
    task_spec: Option<&flow::CaptureSpec>,
    ops_logs_template: Option<&broker::JournalSpec>,
    ops_stats_template: Option<&broker::JournalSpec>,
    initial_splits: usize,
) -> anyhow::Result<()> {
    let task_template = capture_template(task_spec)?;

    let (shards, recovery, ops_logs, ops_stats) = fetch_task_splits(
        journal_client,
        shard_client,
        ops::TaskType::Capture,
        capture,
        ops_logs_template,
        ops_stats_template,
    )
    .await?;

    let shards = apply_initial_splits(task_template, initial_splits, shards)?;
    let changes = task_changes(task_template, shards, recovery, ops_logs, ops_stats)?;

    apply_changes(journal_client, shard_client, changes).await
}

/// Activate a collection into a data-plane.
pub async fn activate_collection(
    journal_client: &gazette::journal::Client,
    shard_client: &gazette::shard::Client,
    collection: &models::Collection,
    task_spec: Option<&flow::CollectionSpec>,
    ops_logs_template: Option<&broker::JournalSpec>,
    ops_stats_template: Option<&broker::JournalSpec>,
    initial_splits: usize,
) -> anyhow::Result<()> {
    let (partition_template, task_template) = collection_template(task_spec)?;

    let ((shards, recovery, ops_logs, ops_stats), partitions) = futures::try_join!(
        fetch_task_splits(
            journal_client,
            shard_client,
            ops::TaskType::Derivation,
            collection,
            ops_logs_template,
            ops_stats_template,
        ),
        fetch_partition_splits(journal_client, collection),
    )?;

    let shards = apply_initial_splits(task_template, initial_splits, shards)?;
    let changes_1 = partition_changes(partition_template, partitions)?;
    let changes_2 = task_changes(task_template, shards, recovery, ops_logs, ops_stats)?;

    apply_changes(
        journal_client,
        shard_client,
        changes_1.into_iter().chain(changes_2),
    )
    .await
}

/// Activate a materialization into a data-plane.
pub async fn activate_materialization(
    journal_client: &gazette::journal::Client,
    shard_client: &gazette::shard::Client,
    materialization: &models::Materialization,
    task_spec: Option<&flow::MaterializationSpec>,
    ops_logs_template: Option<&broker::JournalSpec>,
    ops_stats_template: Option<&broker::JournalSpec>,
    initial_splits: usize,
) -> anyhow::Result<()> {
    let task_template = materialization_template(task_spec)?;

    let (shards, recovery, ops_logs, ops_stats) = fetch_task_splits(
        journal_client,
        shard_client,
        ops::TaskType::Materialization,
        materialization,
        ops_logs_template,
        ops_stats_template,
    )
    .await?;

    let shards = apply_initial_splits(task_template, initial_splits, shards)?;
    let changes = task_changes(task_template, shards, recovery, ops_logs, ops_stats)?;

    apply_changes(journal_client, shard_client, changes).await
}

pub async fn apply_changes(
    journal_client: &gazette::journal::Client,
    shard_client: &gazette::shard::Client,
    changes: impl IntoIterator<Item = Change>,
) -> anyhow::Result<()> {
    let mut journal_deletes = Vec::new();
    let mut journal_upserts = Vec::new();
    let mut shard_deletes = Vec::new();
    let mut shard_upserts = Vec::new();

    for change in changes {
        match change {
            Change::Journal(change @ broker::apply_request::Change { upsert: None, .. }) => {
                journal_deletes.push(change)
            }
            Change::Shard(change @ consumer::apply_request::Change { upsert: None, .. }) => {
                shard_deletes.push(change)
            }
            Change::Journal(change) => journal_upserts.push(change),
            Change::Shard(change) => shard_upserts.push(change),
        }
    }

    // We'll unassign any failed shards to get them running after updating their specs.
    let mut unassign_ids: Vec<_> = shard_upserts
        .iter()
        .map(|c| c.upsert.as_ref().unwrap().id.clone())
        .collect();

    const WINDOW: usize = 120;

    // We must create journals before we create the shards that use them.
    while !journal_upserts.is_empty() {
        let bound = WINDOW.max(journal_upserts.len()) - WINDOW;

        journal_client
            .apply(broker::ApplyRequest {
                changes: journal_upserts.split_off(bound),
            })
            .await
            .context("activating JournalSpec upserts")?;
    }
    std::mem::drop(journal_upserts);

    while !shard_upserts.is_empty() {
        let bound = WINDOW.max(shard_upserts.len()) - WINDOW;

        shard_client
            .apply(consumer::ApplyRequest {
                changes: shard_upserts.split_off(bound),
                ..Default::default()
            })
            .await
            .context("activating ShardSpec upserts")?;
    }
    std::mem::drop(shard_upserts);

    while !shard_deletes.is_empty() {
        let bound = WINDOW.max(shard_deletes.len()) - WINDOW;

        shard_client
            .apply(consumer::ApplyRequest {
                changes: shard_deletes.split_off(bound),
                ..Default::default()
            })
            .await
            .context("activating ShardSpec deletions")?;
    }
    std::mem::drop(shard_deletes);

    while !journal_deletes.is_empty() {
        let bound = WINDOW.max(journal_deletes.len()) - WINDOW;

        journal_client
            .apply(broker::ApplyRequest {
                changes: journal_deletes.split_off(bound),
            })
            .await
            .context("activating JournalSpec deletions")?;
    }
    std::mem::drop(journal_deletes);

    while !unassign_ids.is_empty() {
        let bound = WINDOW.max(unassign_ids.len()) - WINDOW;

        shard_client
            .unassign(consumer::UnassignRequest {
                shards: unassign_ids.split_off(bound),
                only_failed: true,
                dry_run: false,
            })
            .await
            .context("unassigning activated, previously failed shards")?;
    }
    std::mem::drop(unassign_ids);

    Ok(())
}

pub async fn fetch_task_splits<'a>(
    journal_client: &gazette::journal::Client,
    shard_client: &gazette::shard::Client,
    task_type: ops::TaskType,
    task_name: &str,
    ops_logs_template: Option<&broker::JournalSpec>,
    ops_stats_template: Option<&broker::JournalSpec>,
) -> anyhow::Result<(
    Vec<ShardSplit>,                                  // Shards.
    Vec<JournalSplit>,                                // Recovery logs.
    (String, Option<JournalSpec>, Vec<JournalSplit>), // Ops logs.
    (String, Option<JournalSpec>, Vec<JournalSplit>), // Ops stats.
)> {
    let (list_shards, list_recovery) = list_task_request(task_type, task_name);
    let list_ops_logs = list_ops_journal(journal_client, task_type, task_name, ops_logs_template);
    let list_ops_stats = list_ops_journal(journal_client, task_type, task_name, ops_stats_template);

    // List task shards, shard recovery logs, task ops logs, and task ops stats concurrently.
    let (shards, recovery, ops_logs, ops_stats) = futures::join!(
        shard_client.list(list_shards),
        journal_client.list(list_recovery),
        list_ops_logs,
        list_ops_stats,
    );

    // Unpack list responses.
    let shards = unpack_shard_listing(shards?)?;
    let recovery = unpack_journal_listing(recovery?)?;

    if !shards.is_sorted_by_key(|shard| &shard.id) {
        anyhow::bail!("shards are not sorted by id");
    }
    if !recovery.is_sorted_by_key(|recovery| &recovery.name) {
        anyhow::bail!("recovery logs are not sorted by name");
    }

    Ok((shards, recovery, ops_logs?, ops_stats?))
}

pub async fn fetch_partition_splits(
    journal_client: &gazette::journal::Client,
    collection: &str,
) -> anyhow::Result<Vec<JournalSplit>> {
    let list_partitions = list_partitions_request(&collection);

    let partitions = journal_client.list(list_partitions).await?;
    let partitions = unpack_journal_listing(partitions)?;

    if !partitions.is_sorted_by_key(|partition| &partition.name) {
        anyhow::bail!("partitions are not sorted by name");
    }

    Ok(partitions)
}

/// Build ListRequests of a Task's shard splits and recovery logs.
fn list_task_request(
    task_type: ops::TaskType,
    task_name: &str,
) -> (consumer::ListRequest, broker::ListRequest) {
    let list_shards = consumer::ListRequest {
        selector: Some(LabelSelector {
            include: Some(labels::build_set([
                (labels::TASK_TYPE, task_type.as_str_name()),
                (labels::TASK_NAME, task_name),
            ])),
            exclude: None,
        }),
        ..Default::default()
    };
    let list_recovery = broker::ListRequest {
        selector: Some(LabelSelector {
            include: Some(labels::build_set([
                (labels::CONTENT_TYPE, labels::CONTENT_TYPE_RECOVERY_LOG),
                (labels::TASK_TYPE, task_type.as_str_name()),
                (labels::TASK_NAME, task_name),
            ])),
            exclude: None,
        }),
        ..Default::default()
    };
    (list_shards, list_recovery)
}

/// Build a ListRequest of a collections partitions.
fn list_partitions_request(collection: &str) -> broker::ListRequest {
    broker::ListRequest {
        selector: Some(LabelSelector {
            include: Some(labels::build_set([
                ("name:prefix", format!("{collection}/").as_str()),
                (labels::COLLECTION, collection),
            ])),
            exclude: None,
        }),
        ..Default::default()
    }
}

/// Unpack a consumer ListResponse into its structured task splits.
fn unpack_shard_listing(resp: consumer::ListResponse) -> anyhow::Result<Vec<ShardSplit>> {
    let mut v = Vec::new();

    for resp in resp.shards {
        let Some(mut spec) = resp.spec else {
            anyhow::bail!("listing response is missing spec");
        };
        let Some(set) = spec.labels.take() else {
            anyhow::bail!("listing response spec is missing labels");
        };
        v.push(ShardSplit {
            id: spec.id,
            labels: set,
            mod_revision: resp.mod_revision,
        });
    }
    Ok(v)
}

/// Unpack a broker ListResponse into its structured collection splits.
fn unpack_journal_listing(resp: broker::ListResponse) -> anyhow::Result<Vec<JournalSplit>> {
    let mut v = Vec::new();

    for resp in resp.journals {
        let Some(mut spec) = resp.spec else {
            anyhow::bail!("listing response is missing spec");
        };
        let Some(set) = spec.labels.take() else {
            anyhow::bail!("listing response spec is missing labels");
        };
        v.push(JournalSplit {
            name: spec.name,
            labels: set,
            mod_revision: resp.mod_revision,
            suspend: spec.suspend,
        });
    }
    Ok(v)
}

/// Determine the consumer shard and broker recovery and ops journal changes
/// required to converge the desired splits towards the `template`.
pub fn task_changes<'a>(
    template: Option<TaskTemplate<'a>>,
    shards: Vec<ShardSplit>,
    recovery: Vec<JournalSplit>,
    ops_logs: (String, Option<JournalSpec>, Vec<JournalSplit>),
    ops_stats: (String, Option<JournalSpec>, Vec<JournalSplit>),
) -> anyhow::Result<Vec<Change>> {
    let (ops_logs_name, ops_logs_spec, ops_logs_splits) = ops_logs;
    let (ops_stats_name, ops_stats_spec, ops_stats_splits) = ops_stats;

    let mut recovery: BTreeMap<_, _> = recovery
        .into_iter()
        .map(|mut split| (std::mem::take(&mut split.name), split))
        .collect();

    let mut changes = Vec::new();
    let mut active = false;

    for ShardSplit {
        id,
        labels: split,
        mod_revision: shard_revision,
    } in shards
    {
        let template = match template {
            Some(template) if id.starts_with(&template.shard.id) => template,

            // Delete shards where `template` is None or the template prefix isn't matched.
            _ => {
                changes.push(Change::Shard(consumer::apply_request::Change {
                    expect_mod_revision: shard_revision,
                    upsert: None,
                    delete: id,
                }));
                continue;
            }
        };

        // Sanity-check that the current split matches its implied shard Id.
        let expect_id = format!(
            "{}/{}",
            template.shard.id,
            labels::shard::id_suffix(&split)?
        );
        if id != expect_id {
            anyhow::bail!("shard {id} doesn't match its expected Id, which is {expect_id}");
        }

        let mut shard_spec = ShardSpec {
            id,
            ..template.shard.clone()
        };

        // Next resolve the shard's recovery-log JournalSpec.
        let recovery_name = format!("{}/{}", shard_spec.recovery_log_prefix, shard_spec.id);
        let recovery_split = recovery.remove(&recovery_name).unwrap_or_default();

        let mut recovery_spec = JournalSpec {
            name: recovery_name,
            suspend: recovery_split.suspend, // Must be passed through.
            ..template.recovery.clone()
        };

        // Resolve the labels of the ShardSpec by merging labels managed the
        // control-plane versus the data-plane.
        let mut shard_labels = shard_spec.labels.take().unwrap_or_default();

        let build = labels::values(&shard_labels, labels::BUILD)
            .first()
            .map(|l| l.value.clone())
            .unwrap_or_default();

        for label in &split.labels {
            if label.name == labels::BUILD && label.value > build {
                anyhow::bail!(
                    "current ShardSpec {} has a newer build then the template ({} vs {})",
                    shard_spec.id,
                    label.value,
                    build
                );
            } else if !labels::is_data_plane_label(&label.name) {
                continue;
            }
            shard_labels = labels::add_value(shard_labels, &label.name, &label.value);

            // A shard which is actively being split from another
            // parent (source) shard should not have hot standbys,
            // since we must complete the split workflow to even know
            // what hints they should begin recovery log replay from.
            if label.name == labels::SPLIT_SOURCE {
                shard_spec.hot_standbys = 0
            }

            // A cordoned task is disabled with its recovery log marked read-only.
            if label.name == labels::CORDON {
                shard_spec.disable = true;
                recovery_spec.flags = proto_gazette::broker::journal_spec::Flag::ORdonly as u32;
            }
        }
        shard_labels = labels::set_value(shard_labels, labels::LOGS_JOURNAL, &ops_logs_name);
        shard_labels = labels::set_value(shard_labels, labels::STATS_JOURNAL, &ops_stats_name);
        shard_spec.labels = Some(shard_labels);

        changes.push(Change::Shard(consumer::apply_request::Change {
            expect_mod_revision: shard_revision,
            upsert: Some(shard_spec),
            delete: String::new(),
        }));
        changes.push(Change::Journal(broker::apply_request::Change {
            expect_mod_revision: recovery_split.mod_revision,
            upsert: Some(recovery_spec),
            delete: String::new(),
        }));

        active = true;
    }

    // Any remaining recovery logs are not paired with an active shard, and are deleted.
    for (name, JournalSplit { mod_revision, .. }) in recovery {
        changes.push(Change::Journal(broker::apply_request::Change {
            expect_mod_revision: mod_revision,
            upsert: None,
            delete: name,
        }));
    }

    // Apply ops partitions iff the task is active.
    if active {
        changes.extend(ops_journal_changes(ops_logs_spec, ops_logs_splits));
        changes.extend(ops_journal_changes(ops_stats_spec, ops_stats_splits));
    }

    Ok(changes)
}

/// Determine the broker partition changes required to converge
/// the desired `partitions` towards the `template`.
pub fn partition_changes(
    template: Option<&broker::JournalSpec>,
    partitions: Vec<JournalSplit>,
) -> anyhow::Result<Vec<Change>> {
    let mut changes = Vec::new();

    for JournalSplit {
        name,
        labels: split,
        mod_revision,
        suspend,
    } in partitions
    {
        let template = match template {
            Some(template) if name.starts_with(&template.name) => template,

            // Delete journals where `template` is None or the template prefix isn't matched.
            _ => {
                changes.push(Change::Journal(broker::apply_request::Change {
                    expect_mod_revision: mod_revision,
                    upsert: None,
                    delete: name.clone(),
                }));
                continue;
            }
        };

        // Sanity-check that the current split matches its implied journal name.
        let expect_name = format!(
            "{}/{}",
            template.name,
            labels::partition::name_suffix(&split)?
        );
        if name != expect_name {
            anyhow::bail!("journal {name} doesn't match its expected name, which is {expect_name}");
        }

        let mut spec = JournalSpec {
            name,
            suspend, // Must be passed through.
            ..template.clone()
        };
        let mut spec_labels = spec.labels.take().unwrap_or_default();

        let build = labels::values(&spec_labels, labels::BUILD)
            .first()
            .map(|l| l.value.clone())
            .unwrap_or_default();

        for label in &split.labels {
            if label.name == labels::BUILD && label.value > build {
                anyhow::bail!(
                    "current JournalSpec {} has a newer build then the template ({} vs {})",
                    spec.name,
                    label.value,
                    build
                );
            } else if !labels::is_data_plane_label(&label.name) {
                continue;
            }
            spec_labels = labels::add_value(spec_labels, &label.name, &label.value);

            // A cordoned journal is marked as read-only to prevent further writes.
            if label.name == labels::CORDON {
                spec.flags = proto_gazette::broker::journal_spec::Flag::ORdonly as u32;
            }
        }
        spec.labels = Some(spec_labels);

        changes.push(Change::Journal(broker::apply_request::Change {
            expect_mod_revision: mod_revision,
            upsert: Some(spec),
            delete: String::new(),
        }));
    }

    Ok(changes)
}

pub fn ops_partition_spec(
    task_type: ops::TaskType,
    task_name: &str,
    template: &JournalSpec,
) -> JournalSpec {
    let mut spec = template.clone();
    let set = spec.labels.take().unwrap_or_default();
    let set = labels::partition::encode_key_range(set, 0, u32::MAX);
    let set = labels::partition::add_value(set, "name", &json!(task_name)).unwrap();
    let set = labels::partition::add_value(set, "kind", &json!(task_type.as_str_name())).unwrap();

    spec.name = format!(
        "{}/{}",
        spec.name,
        labels::partition::name_suffix(&set).unwrap()
    );
    spec.labels = Some(set);

    spec
}

fn list_ops_journal_request(
    task_type: ops::TaskType,
    task_name: &str,
    template: &JournalSpec,
) -> (broker::ListRequest, JournalSpec) {
    let spec = ops_partition_spec(task_type, task_name, template);

    let list_req = broker::ListRequest {
        selector: Some(LabelSelector {
            include: Some(labels::build_set([("name", spec.name.as_str())])),
            exclude: None,
        }),
        ..Default::default()
    };

    (list_req, spec)
}

async fn list_ops_journal(
    journal_client: &gazette::journal::Client,
    task_type: ops::TaskType,
    task_name: &str,
    template: Option<&JournalSpec>,
) -> anyhow::Result<(String, Option<JournalSpec>, Vec<JournalSplit>)> {
    let Some(template) = template else {
        // `local` redirects task logs to application logs (for testing contexts).
        return Ok(("local".to_string(), None, Vec::new()));
    };

    let (request, spec) = list_ops_journal_request(task_type, task_name, template);
    let splits = unpack_journal_listing(journal_client.list(request).await?)?;
    Ok((spec.name.clone(), Some(spec), splits))
}

fn ops_journal_changes(spec: Option<JournalSpec>, splits: Vec<JournalSplit>) -> Option<Change> {
    let Some(spec) = spec else {
        return None;
    };

    // If the journal exists then there's nothing to do (we don't update it).
    if !splits.is_empty() {
        return None;
    }
    Some(Change::Journal(broker::apply_request::Change {
        upsert: Some(spec),
        expect_mod_revision: 0, // Will be created.
        delete: String::new(),
    }))
}

fn apply_initial_splits<'a>(
    template: Option<TaskTemplate<'a>>,
    initial_splits: usize,
    mut shards: Vec<ShardSplit>,
) -> anyhow::Result<Vec<ShardSplit>> {
    let Some(template) = template else {
        return Ok(shards);
    };
    if template.shard.disable {
        return Ok(shards);
    }
    if shards
        .iter()
        .any(|split| split.id.starts_with(&template.shard.id))
    {
        return Ok(shards);
    }
    // The task is being upsert-ed, it's not disabled, and no current shards
    // have its template prefix.

    // Invent `initial_splits` new shards.
    for pivot in 0..initial_splits {
        let range = flow::RangeSpec {
            key_begin: ((1 << 32) * (pivot + 0) / initial_splits) as u32,
            key_end: (((1 << 32) * (pivot + 1) / initial_splits) - 1) as u32,
            r_clock_begin: 0,
            r_clock_end: u32::MAX,
        };
        let labels = labels::shard::encode_range_spec(LabelSet::default(), &range);
        let id = format!(
            "{}/{}",
            template.shard.id,
            labels::shard::id_suffix(&labels)?
        );
        shards.push(ShardSplit {
            id,
            labels,
            mod_revision: 0,
        });
    }

    Ok(shards)
}

/// Map a parent JournalSplit into two subdivided splits.
#[allow(dead_code)]
fn map_partition_to_split(parent: &JournalSplit) -> anyhow::Result<(JournalSplit, JournalSplit)> {
    let (parent_begin, parent_end) = labels::partition::decode_key_range(&parent.labels)?;

    let pivot = ((parent_begin as u64 + parent_end as u64 + 1) / 2) as u32;
    let lhs_labels =
        labels::partition::encode_key_range(parent.labels.clone(), parent_begin, pivot - 1);
    let rhs_labels = labels::partition::encode_key_range(parent.labels.clone(), pivot, parent_end);

    // Extract the journal name prefix and map into a new RHS journal name.
    let name_prefix = labels::partition::name_prefix(&parent.name, &parent.labels)
        .context("failed to split journal name into prefix and suffix")?;

    let rhs_name = format!(
        "{name_prefix}/{}",
        labels::partition::name_suffix(&rhs_labels).expect("we encoded the key range")
    );

    Ok((
        JournalSplit {
            name: parent.name.clone(),
            labels: lhs_labels,
            mod_revision: parent.mod_revision,
            suspend: parent.suspend, // LHS continues the parent's physical journal.
        },
        JournalSplit {
            name: rhs_name,
            labels: rhs_labels,
            mod_revision: 0,
            suspend: None,
        },
    ))
}

/// Map a parent ShardSplit into two splits subdivided on either key or r-clock.
#[allow(dead_code)]
fn map_shard_to_split(
    parent: &ShardSplit,
    split_on_key: bool,
) -> anyhow::Result<(ShardSplit, ShardSplit)> {
    let parent_range = labels::shard::decode_range_spec(&parent.labels)?;

    // Confirm the shard doesn't have an ongoing split.
    if let Some(Label { value, .. }) = labels::values(&parent.labels, labels::SPLIT_SOURCE).first()
    {
        anyhow::bail!(
            "shard {} is already splitting from source {value}",
            parent.id
        );
    }
    if let Some(Label { value, .. }) = labels::values(&parent.labels, labels::SPLIT_TARGET).first()
    {
        anyhow::bail!("shard {} is already splitting to target {value}", parent.id);
    }

    // Pick a split point of the parent range, which will divide the future
    // LHS & RHS children.
    let (mut lhs_range, mut rhs_range) = (parent_range.clone(), parent_range.clone());

    if split_on_key {
        let pivot = ((parent_range.key_begin as u64 + parent_range.key_end as u64 + 1) / 2) as u32;
        (lhs_range.key_end, rhs_range.key_begin) = (pivot - 1, pivot);
    } else {
        let pivot =
            ((parent_range.r_clock_begin as u64 + parent_range.r_clock_end as u64 + 1) / 2) as u32;
        (lhs_range.r_clock_end, rhs_range.r_clock_begin) = (pivot - 1, pivot);
    }

    // Deep-copy parent labels for the desired LHS / RHS updates.
    let (mut lhs_labels, mut rhs_labels) = (parent.labels.clone(), parent.labels.clone());

    // Update the `rhs` range but not the `lhs` range at this time.
    // That will happen when the `rhs` shard finishes playback
    // and completes the split workflow.
    rhs_labels = labels::shard::encode_range_spec(rhs_labels, &rhs_range);

    // Extract the Shard ID prefix and map into a new RHS Shard ID.
    let id_prefix = labels::shard::id_prefix(&parent.id)
        .context("failed to split shard ID into prefix and suffix")?;

    let rhs_id = format!(
        "{id_prefix}/{}",
        labels::shard::id_suffix(&rhs_labels).expect("we encoded the range spec")
    );

    // Mark the parent & child specs as having an in-progress split.
    lhs_labels = labels::set_value(lhs_labels, labels::SPLIT_TARGET, &rhs_id);
    rhs_labels = labels::set_value(rhs_labels, labels::SPLIT_SOURCE, &parent.id);

    Ok((
        ShardSplit {
            id: parent.id.clone(),
            labels: lhs_labels,
            mod_revision: parent.mod_revision,
        },
        ShardSplit {
            id: rhs_id,
            labels: rhs_labels,
            mod_revision: 0,
        },
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_list_partition_request() {
        insta::assert_debug_snapshot!(list_partitions_request(&models::Collection::new(
            "the/collection"
        )))
    }

    #[test]
    fn test_list_task_request() {
        insta::assert_debug_snapshot!(list_task_request(
            ops::TaskType::Derivation,
            "the/derivation",
        ),)
    }

    async fn managed_build(source: url::Url) -> build::Output {
        use tables::CatalogResolver;
        let file_root = std::path::Path::new("/");
        let draft = build::load(&source, file_root).await;
        if !draft.errors.is_empty() {
            return build::Output::new(draft, Default::default(), Default::default());
        }
        let catalog_names = draft.all_spec_names().collect();
        let live = build::NoOpCatalogResolver.resolve(catalog_names).await;
        if !live.errors.is_empty() {
            return build::Output::new(draft, live, Default::default());
        }

        build::validate(
            models::Id::new([32; 8]), // pub_id
            models::Id::new([1; 8]),  // build_id
            true,                     // allow_local
            "",                       // connector_network
            ops::tracing_log_handler,
            false, // don't no-op validations
            false, // don't no-op validations
            false, // don't no-op validations
            &build::project_root(&source),
            draft,
            live,
        )
        .await
    }

    #[tokio::test]
    async fn fixture_subtests() {
        let source = build::arg_source_to_url("./src/test.flow.yaml", false).unwrap();

        let build::Output { built, .. } = managed_build(source).await.into_result().unwrap();

        let tables::BuiltCollection { spec, .. } = built
            .built_collections
            .get_key(&models::Collection::new("example/collection"))
            .unwrap();

        let Some(flow::CollectionSpec {
            partition_template: Some(partition_template),
            partition_fields,
            projections,
            ..
        }) = spec
        else {
            unreachable!()
        };

        let tables::BuiltCollection { spec, .. } = built
            .built_collections
            .get_key(&models::Collection::new("example/derivation"))
            .unwrap();

        let Some(flow::CollectionSpec {
            derivation:
                Some(flow::collection_spec::Derivation {
                    recovery_log_template: Some(recovery_template),
                    shard_template: Some(shard_template),
                    ..
                }),
            ..
        }) = spec
        else {
            unreachable!()
        };

        let tables::BuiltCollection { spec, .. } = built
            .built_collections
            .get_key(&models::Collection::new("example/disabled"))
            .unwrap();

        let Some(flow::CollectionSpec {
            derivation:
                Some(flow::collection_spec::Derivation {
                    recovery_log_template: Some(disabled_recovery_template),
                    shard_template: Some(disabled_shard_template),
                    ..
                }),
            ..
        }) = spec
        else {
            unreachable!()
        };

        let tables::BuiltCollection { spec, .. } = built
            .built_collections
            .get_key(&models::Collection::new("ops/tasks/BASE_NAME/logs"))
            .unwrap();

        let Some(flow::CollectionSpec {
            partition_template: Some(ops_logs_template),
            ..
        }) = spec
        else {
            unreachable!()
        };

        let tables::BuiltCollection { spec, .. } = built
            .built_collections
            .get_key(&models::Collection::new("ops/tasks/BASE_NAME/stats"))
            .unwrap();

        let Some(flow::CollectionSpec {
            partition_template: Some(ops_stats_template),
            ..
        }) = spec
        else {
            unreachable!()
        };

        let extractors =
            extractors::for_fields(partition_fields, projections, &doc::SerPolicy::noop()).unwrap();

        let mut all_partitions = Vec::new();
        let mut all_shards = Vec::new();
        let mut all_shards_disabled = Vec::new();
        let mut all_recovery = Vec::new();
        let mut all_recovery_disabled = Vec::new();

        let task_template = TaskTemplate {
            shard: shard_template,
            recovery: recovery_template,
        };
        let disabled_task_template = TaskTemplate {
            shard: disabled_shard_template,
            recovery: disabled_recovery_template,
        };

        let mut make_partition = |key_begin, key_end, doc: serde_json::Value, labels: LabelSet| {
            let labels = labels::partition::encode_field_range(
                labels::add_value(labels, "extra", "1"),
                key_begin,
                key_end,
                partition_fields,
                &extractors,
                &doc,
            )
            .unwrap();

            all_partitions.push(JournalSplit {
                name: format!(
                    "{}/{}",
                    partition_template.name,
                    labels::partition::name_suffix(&labels).unwrap()
                ),
                labels,
                mod_revision: 111,
                suspend: Some(journal_spec::Suspend {
                    level: journal_spec::suspend::Level::Partial as i32,
                    offset: 112233,
                }),
            });
        };

        let mut make_task = |range_spec, labels: LabelSet| {
            let labels = labels::shard::encode_range_spec(
                labels::add_value(labels, "extra", "1"),
                range_spec,
            );
            let shard_id = format!(
                "{}/{}",
                shard_template.id,
                labels::shard::id_suffix(&labels).unwrap()
            );
            let disabled_shard_id = format!(
                "{}/{}",
                disabled_shard_template.id,
                labels::shard::id_suffix(&labels).unwrap()
            );
            all_recovery.push(JournalSplit {
                name: format!("{}/{}", shard_template.recovery_log_prefix, shard_id),
                labels: LabelSet::default(),
                mod_revision: 111,
                suspend: Some(journal_spec::Suspend {
                    level: journal_spec::suspend::Level::None as i32,
                    offset: 445566,
                }),
            });
            all_recovery_disabled.push(JournalSplit {
                name: format!(
                    "{}/{}",
                    disabled_shard_template.recovery_log_prefix, disabled_shard_id
                ),
                labels: LabelSet::default(),
                mod_revision: 111,
                suspend: Some(journal_spec::Suspend {
                    level: journal_spec::suspend::Level::Full as i32,
                    offset: 778899,
                }),
            });
            all_shards.push(ShardSplit {
                id: shard_id,
                labels: labels.clone(),
                mod_revision: 111,
            });
            all_shards_disabled.push(ShardSplit {
                id: disabled_shard_id,
                labels: labels,
                mod_revision: 111,
            });
        };

        make_partition(
            0x10000000,
            0x3fffffff,
            json!({"a_bool": true, "a_str": "a-val"}),
            LabelSet::default(),
        );
        make_partition(
            0x40000000,
            0x5fffffff,
            json!({"a_bool": true, "a_str": "a-val"}),
            LabelSet::default(),
        );
        make_partition(
            0,
            u32::MAX,
            json!({"a_bool": false, "a_str": "other-val"}),
            labels::build_set([(labels::CORDON, "true")]),
        );

        make_task(
            &flow::RangeSpec {
                key_begin: 0x10000000,
                key_end: 0x2fffffff,
                r_clock_begin: 0x60000000,
                r_clock_end: 0x9fffffff,
            },
            LabelSet::default(),
        );
        make_task(
            &flow::RangeSpec {
                key_begin: 0x30000000,
                key_end: 0x3fffffff,
                r_clock_begin: 0x60000000,
                r_clock_end: 0x7fffffff,
            },
            LabelSet::default(),
        );
        make_task(
            &flow::RangeSpec {
                key_begin: 0x30000000,
                key_end: 0x3fffffff,
                r_clock_begin: 0x80000000,
                r_clock_end: 0x9fffffff,
            },
            labels::build_set([(labels::CORDON, "true")]),
        );

        let (_, ops_logs_spec) = list_ops_journal_request(
            ops::TaskType::Derivation,
            "example/derivation",
            ops_logs_template,
        );
        let (_, ops_stats_spec) = list_ops_journal_request(
            ops::TaskType::Derivation,
            "example/derivation",
            ops_stats_template,
        );
        let all_ops = vec![JournalSplit {
            mod_revision: 123,
            ..Default::default()
        }];

        // Case: test update of existing specs.
        {
            let partition_changes =
                partition_changes(Some(&partition_template), all_partitions.clone()).unwrap();

            let task_changes = task_changes(
                Some(task_template),
                all_shards.clone(),
                all_recovery.clone(),
                (
                    ops_logs_spec.name.clone(),
                    Some(ops_logs_spec.clone()),
                    all_ops.clone(),
                ),
                (
                    ops_stats_spec.name.clone(),
                    Some(ops_stats_spec.clone()),
                    all_ops.clone(),
                ),
            )
            .unwrap();

            insta::assert_json_snapshot!("update", (partition_changes, task_changes));
        }

        // Case: test creation of new specs.
        {
            let shards = apply_initial_splits(Some(task_template), 4, Vec::new()).unwrap();

            let partition_changes =
                partition_changes(Some(&partition_template), Vec::new()).unwrap();
            let task_changes = task_changes(
                Some(task_template),
                shards,
                Vec::new(),
                (
                    ops_logs_spec.name.clone(),
                    Some(ops_logs_spec.clone()),
                    Vec::new(),
                ),
                (
                    ops_stats_spec.name.clone(),
                    Some(ops_stats_spec.clone()),
                    Vec::new(),
                ),
            )
            .unwrap();

            insta::assert_json_snapshot!("create", (partition_changes, task_changes));
        }

        // Case: test creation of new specs with no initial splits.
        {
            let shards = apply_initial_splits(Some(task_template), 0, Vec::new()).unwrap();

            let partition_changes =
                partition_changes(Some(&partition_template), Vec::new()).unwrap();
            let task_changes = task_changes(
                Some(task_template),
                shards,
                Vec::new(),
                (
                    ops_logs_spec.name.clone(),
                    Some(ops_logs_spec.clone()),
                    Vec::new(),
                ),
                (
                    ops_stats_spec.name.clone(),
                    Some(ops_stats_spec.clone()),
                    Vec::new(),
                ),
            )
            .unwrap();

            insta::assert_json_snapshot!("create-no-splits", (partition_changes, task_changes));
        }

        // Case: test update of existing specs when disabled.
        {
            let partition_changes =
                partition_changes(Some(&partition_template), all_partitions.clone()).unwrap();
            let task_changes = task_changes(
                Some(disabled_task_template),
                all_shards_disabled.clone(),
                all_recovery_disabled.clone(),
                (
                    ops_logs_spec.name.clone(),
                    Some(ops_logs_spec.clone()),
                    all_ops.clone(),
                ),
                (
                    ops_stats_spec.name.clone(),
                    Some(ops_stats_spec.clone()),
                    all_ops.clone(),
                ),
            )
            .unwrap();

            insta::assert_json_snapshot!("update-disabled", (partition_changes, task_changes));
        }

        // Case: test creation of new specs when disabled.
        {
            let shards = apply_initial_splits(Some(disabled_task_template), 4, Vec::new()).unwrap();

            let partition_changes =
                partition_changes(Some(&partition_template), Vec::new()).unwrap();
            let task_changes = task_changes(
                Some(TaskTemplate {
                    shard: disabled_shard_template,
                    recovery: disabled_recovery_template,
                }),
                shards,
                Vec::new(),
                (
                    ops_logs_spec.name.clone(),
                    Some(ops_logs_spec.clone()),
                    Vec::new(),
                ),
                (
                    ops_stats_spec.name.clone(),
                    Some(ops_stats_spec.clone()),
                    Vec::new(),
                ),
            )
            .unwrap();

            insta::assert_json_snapshot!("create-disabled", (partition_changes, task_changes));
        }

        // Case: test deletion of existing specs.
        {
            let partition_changes = partition_changes(None, all_partitions.clone()).unwrap();

            let task_changes = task_changes(
                None,
                all_shards.clone(),
                all_recovery.clone(),
                (
                    ops_logs_spec.name.clone(),
                    Some(ops_logs_spec.clone()),
                    all_ops.clone(),
                ),
                (
                    ops_stats_spec.name.clone(),
                    Some(ops_stats_spec.clone()),
                    all_ops.clone(),
                ),
            )
            .unwrap();

            insta::assert_json_snapshot!("delete", (partition_changes, task_changes));
        }

        // Case: test mixed deletion and creation.
        {
            // Simulate existing data-plane specs which were created under an
            // older initial publication ID, and which are now being swapped out.
            // This emulates a deletion followed by a re-creation, where we failed
            // to activate the intermediary deletion.
            let mut all_partitions = all_partitions.clone();
            let mut all_shards = all_shards.clone();
            let mut all_recovery = all_recovery.clone();

            for JournalSplit { name, .. } in all_partitions.iter_mut() {
                *name = name.replace("2020202020202020", "replaced-pub-id");
            }
            for ShardSplit { id, .. } in all_shards.iter_mut() {
                *id = id.replace("2020202020202020", "replaced-pub-id");
            }
            for JournalSplit { name, .. } in all_recovery.iter_mut() {
                *name = name.replace("2020202020202020", "replaced-pub-id");
            }

            let shards = apply_initial_splits(Some(task_template), 4, all_shards).unwrap();

            let partition_changes =
                partition_changes(Some(&partition_template), all_partitions).unwrap();
            let task_changes = task_changes(
                Some(task_template),
                shards,
                all_recovery,
                (
                    ops_logs_spec.name.clone(),
                    Some(ops_logs_spec.clone()),
                    all_ops.clone(),
                ),
                (
                    ops_stats_spec.name.clone(),
                    Some(ops_stats_spec.clone()),
                    all_ops.clone(),
                ),
            )
            .unwrap();

            insta::assert_json_snapshot!("create_and_delete", (partition_changes, task_changes));
        }

        // Case: split a shard on its key or clock.
        {
            let parent = all_shards.first().unwrap();

            let (key_lhs, key_rhs) = map_shard_to_split(parent, true).unwrap();
            let (clock_lhs, clock_rhs) = map_shard_to_split(parent, false).unwrap();

            let key_changes = task_changes(
                Some(task_template),
                vec![key_lhs.clone(), key_rhs.clone()],
                vec![all_recovery[0].clone()],
                (ops_logs_spec.name.clone(), None, Vec::new()),
                (ops_stats_spec.name.clone(), None, Vec::new()),
            )
            .unwrap();

            let clock_changes = task_changes(
                Some(task_template),
                vec![clock_lhs.clone(), clock_rhs.clone()],
                vec![all_recovery[0].clone()],
                (ops_logs_spec.name.clone(), None, Vec::new()),
                (ops_stats_spec.name.clone(), None, Vec::new()),
            )
            .unwrap();

            insta::assert_json_snapshot!(
                "shard_splits",
                json!([
                    "key_splits",
                    (&key_lhs, &key_rhs),
                    "clock_splits",
                    (&clock_lhs, &clock_rhs),
                    "key_changes",
                    key_changes,
                    "clock_changes",
                    clock_changes,
                ])
            );

            // Expect that an attempt to split an already-splitting parent fails.
            let err = map_shard_to_split(&key_lhs, true).unwrap_err();
            assert_eq!(err.to_string(), "shard derivation/example/derivation/2020202020202020/10000000-60000000 is already splitting to target derivation/example/derivation/2020202020202020/20000000-60000000");

            let err = map_shard_to_split(&key_rhs, true).unwrap_err();
            assert_eq!(err.to_string(), "shard derivation/example/derivation/2020202020202020/20000000-60000000 is already splitting from source derivation/example/derivation/2020202020202020/10000000-60000000");
        }

        // Case: split a partition on its key.
        {
            let parent = all_partitions.first().unwrap();

            let (lhs, rhs) = map_partition_to_split(parent).unwrap();
            let partition_changes =
                partition_changes(Some(partition_template), vec![lhs.clone(), rhs.clone()])
                    .unwrap();

            insta::assert_json_snapshot!(
                "partition_splits",
                json!(["splits", (lhs, rhs), "partition_changes", partition_changes])
            );
        }

        // Case: creation and updates of ops collection partitions.
        {
            let (list_req, spec) = list_ops_journal_request(
                ops::TaskType::Capture,
                "the/task/name",
                ops_logs_template,
            );
            let exists = JournalSplit {
                mod_revision: 123,
                ..Default::default()
            };

            insta::assert_json_snapshot!(
                "ops_collection_partition",
                json!([
                    "list_req",
                    list_req,
                    "spec",
                    spec,
                    "create",
                    ops_journal_changes(Some(spec.clone()), Vec::new()),
                    "update-exists",
                    ops_journal_changes(Some(spec.clone()), vec![exists]),
                ])
            );
        }
    }
}
