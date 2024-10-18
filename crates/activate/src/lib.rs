use anyhow::Context;
use proto_flow::flow::{self, materialization_spec};
use proto_gazette::{
    broker::{self, JournalSpec, Label, LabelSelector, LabelSet},
    consumer::{self, ShardSpec},
};
use serde_json::json;
use std::collections::BTreeMap;

#[derive(serde::Serialize)]
pub enum Change {
    Shard(consumer::apply_request::Change),
    Journal(broker::apply_request::Change),
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
    let task_template = if let Some(task_spec) = task_spec {
        let shard_template = task_spec
            .shard_template
            .as_ref()
            .context("CaptureSpec missing shard_template")?;

        let recovery_template = task_spec
            .recovery_log_template
            .as_ref()
            .context("CaptureSpec missing recovery_log_template")?;

        TaskTemplate::UpsertReal {
            shard: shard_template,
            recovery_journal: recovery_template,
        }
    } else {
        TaskTemplate::Delete
    };

    let changes = converge_task_changes(
        journal_client,
        shard_client,
        ops::TaskType::Capture,
        capture,
        task_template,
        ops_logs_template,
        ops_stats_template,
        initial_splits,
    )
    .await?;

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
    let (task_template, partition_template) = if let Some(task_spec) = task_spec {
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

            TaskTemplate::UpsertReal {
                shard: shard_template,
                recovery_journal: recovery_template,
            }
        } else {
            TaskTemplate::Delete
        };

        (task_template, Some(partition_template))
    } else {
        (TaskTemplate::Delete, None)
    };

    let (changes_1, changes_2) = futures::try_join!(
        converge_task_changes(
            journal_client,
            shard_client,
            ops::TaskType::Derivation,
            collection,
            task_template,
            ops_logs_template,
            ops_stats_template,
            initial_splits,
        ),
        converge_partition_changes(journal_client, collection, partition_template),
    )?;

    apply_changes(
        journal_client,
        shard_client,
        changes_1.into_iter().chain(changes_2.into_iter()),
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
    let task_template = match task_spec {
        Some(task_spec)
            if task_spec.connector_type == materialization_spec::ConnectorType::Dekaf as i32 =>
        {
            TaskTemplate::UpsertVirtual
        }
        Some(task_spec) => {
            let shard_template = task_spec
                .shard_template
                .as_ref()
                .context("MaterializationSpec missing shard_template")?;

            let recovery_template = task_spec
                .recovery_log_template
                .as_ref()
                .context("MaterializationSpec missing recovery_log_template")?;

            TaskTemplate::UpsertReal {
                shard: shard_template,
                recovery_journal: recovery_template,
            }
        }
        None => TaskTemplate::Delete,
    };

    let changes = converge_task_changes(
        journal_client,
        shard_client,
        ops::TaskType::Materialization,
        materialization,
        task_template,
        ops_logs_template,
        ops_stats_template,
        initial_splits,
    )
    .await?;

    apply_changes(journal_client, shard_client, changes).await
}

async fn apply_changes(
    journal_client: &gazette::journal::Client,
    shard_client: &gazette::shard::Client,
    changes: impl IntoIterator<Item = Change>,
) -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        try_apply_changes(journal_client, shard_client, changes).await
    })
    .await?
}

async fn try_apply_changes(
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

/// Describes the desired future state of a task.
/// Virtual tasks get logs and stats journals,
/// but are otherwise purely descriptive and
/// do not get shards and recovery log journals
/// created for them like real tasks do.
#[derive(Clone, Copy, Debug)]
enum TaskTemplate<'a> {
    UpsertReal {
        shard: &'a ShardSpec,
        recovery_journal: &'a JournalSpec,
    },
    UpsertVirtual,
    Delete,
}

/// Converge a task by listing data-plane ShardSpecs and recovery log
/// JournalSpecs, and then applying updates to bring them into alignment
/// with the templated task configuration.
async fn converge_task_changes<'a>(
    journal_client: &gazette::journal::Client,
    shard_client: &gazette::shard::Client,
    task_type: ops::TaskType,
    task_name: &str,
    template: TaskTemplate<'a>,
    ops_logs_template: Option<&broker::JournalSpec>,
    ops_stats_template: Option<&broker::JournalSpec>,
    initial_splits: usize,
) -> anyhow::Result<Vec<Change>> {
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        try_converge_task_changes(
            journal_client,
            shard_client,
            task_type,
            task_name,
            template,
            ops_logs_template,
            ops_stats_template,
            initial_splits,
        )
        .await
    })
    .await?
}

async fn try_converge_task_changes<'a>(
    journal_client: &gazette::journal::Client,
    shard_client: &gazette::shard::Client,
    task_type: ops::TaskType,
    task_name: &str,
    template: TaskTemplate<'a>,
    ops_logs_template: Option<&broker::JournalSpec>,
    ops_stats_template: Option<&broker::JournalSpec>,
    initial_splits: usize,
) -> anyhow::Result<Vec<Change>> {
    let (list_shards, list_recovery) = list_task_request(task_type, task_name);

    let (ops_logs_name, ops_logs_change) =
        converge_ops_journal(journal_client, task_type, task_name, ops_logs_template);
    let (ops_stats_name, ops_stats_change) =
        converge_ops_journal(journal_client, task_type, task_name, ops_stats_template);

    let (shards, recovery, ops_logs_change, ops_stats_change) = futures::join!(
        shard_client.list(list_shards),
        journal_client.list(list_recovery),
        ops_logs_change,
        ops_stats_change,
    );
    let shards = unpack_shard_listing(shards?)?;
    let recovery = unpack_journal_listing(recovery?)?;
    let ops_logs_change = ops_logs_change?;
    let ops_stats_change = ops_stats_change?;

    let mut changes = task_changes(
        template,
        &shards,
        &recovery,
        initial_splits,
        &ops_logs_name,
        &ops_stats_name,
    )?;

    // If (and only if) the task is being upserted,
    // then ensure the creation of its ops collection partitions.
    if matches!(
        template,
        TaskTemplate::UpsertVirtual | TaskTemplate::UpsertReal { .. }
    ) {
        changes.extend(ops_logs_change.into_iter());
        changes.extend(ops_stats_change.into_iter());
    }

    Ok(changes)
}

/// Converge a collection by listing data-plane partition JournalSpecs,
/// and then applying updates to bring them into alignment
/// with the templated collection configuration.
async fn converge_partition_changes(
    journal_client: &gazette::journal::Client,
    collection: &models::Collection,
    template: Option<&JournalSpec>,
) -> anyhow::Result<Vec<Change>> {
    let list_partitions = list_partitions_request(&collection);

    let partitions = journal_client.list(list_partitions).await?;
    let partitions = unpack_journal_listing(partitions)?;

    partition_changes(template, &partitions)
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
fn list_partitions_request(collection: &models::Collection) -> broker::ListRequest {
    broker::ListRequest {
        selector: Some(LabelSelector {
            include: Some(labels::build_set([
                ("name:prefix", format!("{collection}/").as_str()),
                (labels::COLLECTION, collection.as_str()),
            ])),
            exclude: None,
        }),
        ..Default::default()
    }
}

/// Unpack a consumer ListResponse into its structured task splits.
fn unpack_shard_listing(
    resp: consumer::ListResponse,
) -> anyhow::Result<Vec<(String, LabelSet, i64)>> {
    let mut v = Vec::new();

    for resp in resp.shards {
        let Some(mut spec) = resp.spec else {
            anyhow::bail!("listing response is missing spec");
        };
        let Some(set) = spec.labels.take() else {
            anyhow::bail!("listing response spec is missing labels");
        };
        v.push((spec.id, set, resp.mod_revision));
    }
    Ok(v)
}

/// Unpack a broker ListResponse into its structured collection splits.
fn unpack_journal_listing(
    resp: broker::ListResponse,
) -> anyhow::Result<Vec<(String, LabelSet, i64)>> {
    let mut v = Vec::new();

    for resp in resp.journals {
        let Some(mut spec) = resp.spec else {
            anyhow::bail!("listing response is missing spec");
        };
        let Some(set) = spec.labels.take() else {
            anyhow::bail!("listing response spec is missing labels");
        };
        v.push((spec.name, set, resp.mod_revision));
    }
    Ok(v)
}

/// Determine the consumer shard and broker recovery log changes required to
/// converge from current `shards` and `recovery` splits into the desired state.
fn task_changes(
    template: TaskTemplate,
    shards: &[(String, LabelSet, i64)],
    recovery: &[(String, LabelSet, i64)],
    initial_splits: usize,
    ops_logs_name: &str,
    ops_stats_name: &str,
) -> anyhow::Result<Vec<Change>> {
    let mut shards = shards.to_vec();

    // If the template is Some and no current shards match its prefix,
    // then instantiate `initial_splits` new shards to create.
    if let TaskTemplate::UpsertReal {
        shard: shard_template,
        ..
    } = template
    {
        if !shards
            .iter()
            .any(|(id, _, _)| id.starts_with(&shard_template.id))
        {
            // Invent initial splits.
            for pivot in 0..initial_splits {
                let set = labels::shard::encode_range_spec(
                    LabelSet::default(),
                    &flow::RangeSpec {
                        key_begin: ((1 << 32) * (pivot + 0) / initial_splits) as u32,
                        key_end: (((1 << 32) * (pivot + 1) / initial_splits) - 1) as u32,
                        r_clock_begin: 0,
                        r_clock_end: u32::MAX,
                    },
                );
                shards.push((shard_template.id.clone(), set, 0));
            }
        }
    }

    let mut recovery: BTreeMap<_, _> = recovery
        .iter()
        .map(|(recovery, set, revision)| (recovery, (set, revision)))
        .collect();

    let mut changes = Vec::new();

    for (id, split, shard_revision) in shards {
        match template {
            TaskTemplate::UpsertReal {
                shard: shard_template,
                recovery_journal: recovery_template,
            } if id.starts_with(&shard_template.id) => {
                let mut shard_spec = shard_template.clone();
                let mut shard_set = shard_spec.labels.take().unwrap_or_default();

                for label in &split.labels {
                    if !labels::is_data_plane_label(&label.name) {
                        continue;
                    }
                    shard_set = labels::add_value(shard_set, &label.name, &label.value);

                    // A shard which is actively being split from another
                    // parent (source) shard should not have hot standbys,
                    // since we must complete the split workflow to even know
                    // what hints they should begin recovery log replay from.
                    if label.name == labels::SPLIT_SOURCE {
                        shard_spec.hot_standbys = 0
                    }
                }

                shard_set = labels::set_value(shard_set, labels::LOGS_JOURNAL, ops_logs_name);
                shard_set = labels::set_value(shard_set, labels::STATS_JOURNAL, ops_stats_name);

                shard_spec.id = format!(
                    "{}/{}",
                    shard_spec.id,
                    labels::shard::id_suffix(&shard_set)?
                );
                shard_spec.labels = Some(shard_set);

                let mut recovery_spec = recovery_template.clone();
                recovery_spec.name =
                    format!("{}/{}", shard_spec.recovery_log_prefix, shard_spec.id);

                let recovery_revision = recovery
                    .remove(&recovery_spec.name)
                    .map(|(_, r)| *r)
                    .unwrap_or_default();

                changes.push(Change::Shard(consumer::apply_request::Change {
                    expect_mod_revision: shard_revision,
                    upsert: Some(shard_spec),
                    delete: String::new(),
                }));
                changes.push(Change::Journal(broker::apply_request::Change {
                    expect_mod_revision: recovery_revision,
                    upsert: Some(recovery_spec),
                    delete: String::new(),
                }));
            }
            _ => {
                changes.push(Change::Shard(consumer::apply_request::Change {
                    expect_mod_revision: shard_revision,
                    upsert: None,
                    delete: id,
                }));
            }
        }
    }

    // Any remaining recovery logs are not paired with a shard, and are deleted.
    for (recovery, (_set, mod_revision)) in recovery {
        changes.push(Change::Journal(broker::apply_request::Change {
            expect_mod_revision: *mod_revision,
            upsert: None,
            delete: recovery.clone(),
        }));
    }

    Ok(changes)
}

/// Determine the broker partition changes required to converge
/// from current `partitions` into the desired state.
fn partition_changes(
    template: Option<&broker::JournalSpec>,
    partitions: &[(String, LabelSet, i64)],
) -> anyhow::Result<Vec<Change>> {
    let mut changes = Vec::new();

    for (journal, split, mod_revision) in partitions {
        match template {
            Some(template) if journal.starts_with(&template.name) => {
                let mut partition_spec = template.clone();
                let mut partition_set = partition_spec.labels.take().unwrap_or_default();

                for label in &split.labels {
                    if !labels::is_data_plane_label(&label.name) {
                        continue;
                    }
                    partition_set = labels::add_value(partition_set, &label.name, &label.value);
                }
                partition_spec.name = format!(
                    "{}/{}",
                    partition_spec.name,
                    labels::partition::name_suffix(&partition_set)?
                );
                partition_spec.labels = Some(partition_set);

                changes.push(Change::Journal(broker::apply_request::Change {
                    expect_mod_revision: *mod_revision,
                    upsert: Some(partition_spec),
                    delete: String::new(),
                }));
            }
            _ => {
                changes.push(Change::Journal(broker::apply_request::Change {
                    expect_mod_revision: *mod_revision,
                    upsert: None,
                    delete: journal.clone(),
                }));
            }
        }
    }

    Ok(changes)
}

fn converge_ops_journal<'c>(
    journal_client: &'c gazette::journal::Client,
    task_type: ops::TaskType,
    task_name: &str,
    template: Option<&broker::JournalSpec>,
) -> (
    String,
    impl std::future::Future<Output = anyhow::Result<Vec<Change>>> + 'c,
) {
    let maybe_list =
        template.map(|template| list_ops_journal_request(task_type, task_name, template));

    let name = if let Some((_list, spec)) = &maybe_list {
        spec.name.clone()
    } else {
        "local".to_string() // Direct to reactor-level logs (for testing contexts).
    };

    let fut = async move {
        let Some((list_req, spec)) = maybe_list else {
            return Ok(Vec::new());
        };
        // If the journal exists then there's nothing to do (we don't update it).
        if !journal_client.list(list_req).await?.journals.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![Change::Journal(broker::apply_request::Change {
            upsert: Some(spec),
            expect_mod_revision: 0, // Will be created.
            delete: String::new(),
        })])
    };

    (name, fut)
}

fn list_ops_journal_request(
    task_type: ops::TaskType,
    task_name: &str,
    template: &JournalSpec,
) -> (broker::ListRequest, JournalSpec) {
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

    let list_req = broker::ListRequest {
        selector: Some(LabelSelector {
            include: Some(labels::build_set([("name", spec.name.as_str())])),
            exclude: None,
        }),
        ..Default::default()
    };

    (list_req, spec)
}

/// Map a parent partition, identified by its name and current LabelSet & Etcd
/// ModRevision, into desired splits (which can then be converged to changes).
#[allow(dead_code)]
fn map_partition_to_split(
    parent_name: &str,
    parent_set: &LabelSet,
    parent_revision: i64,
) -> anyhow::Result<Vec<(String, LabelSet, i64)>> {
    let (parent_begin, parent_end) = labels::partition::decode_key_range(&parent_set)?;

    let pivot = ((parent_begin as u64 + parent_end as u64 + 1) / 2) as u32;
    let lhs_set = labels::partition::encode_key_range(parent_set.clone(), parent_begin, pivot - 1);
    let rhs_set = labels::partition::encode_key_range(parent_set.clone(), pivot, parent_end);

    // Extract the journal name prefix and map into a new RHS journal name.
    let name_prefix = labels::partition::name_prefix(parent_name, &parent_set)
        .context("failed to split journal name into prefix and suffix")?;

    let rhs_name = format!(
        "{name_prefix}/{}",
        labels::partition::name_suffix(&rhs_set).expect("we encoded the key range")
    );

    Ok(vec![
        (parent_name.to_string(), lhs_set, parent_revision),
        (rhs_name, rhs_set, 0),
    ])
}

/// Map a parent task split, identified by its ID and current LabelSet & Etcd
/// ModRevision, into desired splits (which can then be converged to changes).
#[allow(dead_code)]
fn map_shard_to_split(
    parent_id: &str,
    parent_set: &LabelSet,
    parent_revision: i64,
    split_on_key: bool,
) -> anyhow::Result<Vec<(String, LabelSet, i64)>> {
    let parent_range = labels::shard::decode_range_spec(&parent_set)?;

    // Confirm the shard doesn't have an ongoing split.
    if let Some(Label { value, .. }) = labels::values(&parent_set, labels::SPLIT_SOURCE).first() {
        anyhow::bail!("shard {parent_id} is already splitting from source {value}");
    }
    if let Some(Label { value, .. }) = labels::values(&parent_set, labels::SPLIT_TARGET).first() {
        anyhow::bail!("shard {parent_id} is already splitting to target {value}");
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
    let (mut lhs_set, mut rhs_set) = (parent_set.clone(), parent_set.clone());

    // Update the `rhs` range but not the `lhs` range at this time.
    // That will happen when the `rhs` shard finishes playback
    // and completes the split workflow.
    rhs_set = labels::shard::encode_range_spec(rhs_set, &rhs_range);

    // Extract the Shard ID prefix and map into a new RHS Shard ID.
    let id_prefix = labels::shard::id_prefix(&parent_id)
        .context("failed to split shard ID into prefix and suffix")?;

    let rhs_id = format!(
        "{id_prefix}/{}",
        labels::shard::id_suffix(&rhs_set).expect("we encoded the range spec")
    );

    // Mark the parent & child specs as having an in-progress split.
    lhs_set = labels::set_value(lhs_set, labels::SPLIT_TARGET, &rhs_id);
    rhs_set = labels::set_value(rhs_set, labels::SPLIT_SOURCE, &parent_id);

    Ok(vec![
        (parent_id.to_string(), lhs_set, parent_revision),
        (rhs_id, rhs_set, 0),
    ])
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
            .get_key(&models::Collection::new("ops/tasks/BASE_NAME/logs"))
            .unwrap();

        let Some(flow::CollectionSpec {
            partition_template: Some(ops_logs_template),
            ..
        }) = spec
        else {
            unreachable!()
        };

        let extractors =
            extractors::for_fields(partition_fields, projections, &doc::SerPolicy::noop()).unwrap();

        let mut all_partitions = Vec::new();
        let mut all_shards = Vec::new();
        let mut all_recovery = Vec::new();

        let mut make_partition = |key_begin, key_end, doc: serde_json::Value| {
            let set = labels::partition::encode_field_range(
                labels::build_set([("extra", "1")]),
                key_begin,
                key_end,
                partition_fields,
                &extractors,
                &doc,
            )
            .unwrap();

            all_partitions.push((
                format!(
                    "{}/{}",
                    partition_template.name,
                    labels::partition::name_suffix(&set).unwrap()
                ),
                set,
                111,
            ));
        };

        let mut make_task = |range_spec| {
            let set =
                labels::shard::encode_range_spec(labels::build_set([("extra", "1")]), range_spec);
            let shard_id = format!(
                "{}/{}",
                shard_template.id,
                labels::shard::id_suffix(&set).unwrap()
            );
            all_recovery.push((
                format!("{}/{}", shard_template.recovery_log_prefix, shard_id),
                LabelSet::default(),
                111,
            ));
            all_shards.push((shard_id, set, 111));
        };

        make_partition(
            0x10000000,
            0x3fffffff,
            json!({"a_bool": true, "a_str": "a-val"}),
        );
        make_partition(
            0x40000000,
            0x5fffffff,
            json!({"a_bool": true, "a_str": "a-val"}),
        );
        make_partition(0, u32::MAX, json!({"a_bool": false, "a_str": "other-val"}));

        make_task(&flow::RangeSpec {
            key_begin: 0x10000000,
            key_end: 0x2fffffff,
            r_clock_begin: 0x60000000,
            r_clock_end: 0x9fffffff,
        });
        make_task(&flow::RangeSpec {
            key_begin: 0x30000000,
            key_end: 0x3fffffff,
            r_clock_begin: 0x60000000,
            r_clock_end: 0x7fffffff,
        });
        make_task(&flow::RangeSpec {
            key_begin: 0x30000000,
            key_end: 0x3fffffff,
            r_clock_begin: 0x80000000,
            r_clock_end: 0x9fffffff,
        });

        // Case: test update of existing specs.
        {
            let partition_changes =
                partition_changes(Some(&partition_template), &all_partitions).unwrap();
            let task_changes = task_changes(
                TaskTemplate::UpsertReal {
                    shard: shard_template,
                    recovery_journal: recovery_template,
                },
                &all_shards,
                &all_recovery,
                4,
                "ops/logs/name",
                "ops/stats/name",
            )
            .unwrap();

            insta::assert_json_snapshot!("update", (partition_changes, task_changes));
        }

        // Case: test creation of new specs.
        {
            let partition_changes = partition_changes(Some(&partition_template), &[]).unwrap();
            let task_changes = task_changes(
                TaskTemplate::UpsertReal {
                    shard: shard_template,
                    recovery_journal: recovery_template,
                },
                &[],
                &[],
                4,
                "ops/logs/name",
                "ops/stats/name",
            )
            .unwrap();

            insta::assert_json_snapshot!("create", (partition_changes, task_changes));
        }

        // Case: test deletion of existing specs.
        {
            let partition_changes = partition_changes(None, &all_partitions).unwrap();
            let task_changes = task_changes(
                TaskTemplate::Delete,
                &all_shards,
                &all_recovery,
                4,
                "ops/logs/name",
                "ops/stats/name",
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

            for (name, _, _) in all_partitions.iter_mut() {
                *name = name.replace("2020202020202020", "replaced-pub-id");
            }
            for (id, _, _) in all_shards.iter_mut() {
                *id = id.replace("2020202020202020", "replaced-pub-id");
            }
            for (name, _, _) in all_recovery.iter_mut() {
                *name = name.replace("2020202020202020", "replaced-pub-id");
            }

            let partition_changes =
                partition_changes(Some(&partition_template), &all_partitions).unwrap();
            let task_changes = task_changes(
                TaskTemplate::UpsertReal {
                    shard: shard_template,
                    recovery_journal: recovery_template,
                },
                &all_shards,
                &all_recovery,
                4,
                "ops/logs/name",
                "ops/stats/name",
            )
            .unwrap();

            insta::assert_json_snapshot!("create_and_delete", (partition_changes, task_changes));
        }

        // Case: split a shard on its key or clock.
        {
            let (parent_id, parent_set, parent_revision) = all_shards.first().unwrap();

            let key_splits =
                map_shard_to_split(parent_id, parent_set, *parent_revision, true).unwrap();
            let clock_splits =
                map_shard_to_split(parent_id, parent_set, *parent_revision, false).unwrap();

            let key_changes = task_changes(
                TaskTemplate::UpsertReal {
                    shard: shard_template,
                    recovery_journal: recovery_template,
                },
                &key_splits,
                &all_recovery[..1],
                4,
                "ops/logs/name",
                "ops/stats/name",
            )
            .unwrap();

            let clock_changes = task_changes(
                TaskTemplate::UpsertReal {
                    shard: shard_template,
                    recovery_journal: recovery_template,
                },
                &clock_splits,
                &all_recovery[..1],
                4,
                "ops/logs/name",
                "ops/stats/name",
            )
            .unwrap();

            insta::assert_json_snapshot!(
                "shard_splits",
                json!([
                    "key_splits",
                    &key_splits,
                    "clock_splits",
                    clock_splits,
                    "key_changes",
                    key_changes,
                    "clock_changes",
                    clock_changes,
                ])
            );

            // Expect that an attempt to split an already-splitting parent fails.
            let (parent_id, parent_set, parent_revision) = key_splits.first().unwrap();
            let err =
                map_shard_to_split(parent_id, parent_set, *parent_revision, true).unwrap_err();
            assert_eq!(err.to_string(), "shard derivation/example/derivation/2020202020202020/10000000-60000000 is already splitting to target derivation/example/derivation/2020202020202020/20000000-60000000");

            let (parent_id, parent_set, parent_revision) = key_splits.last().unwrap();
            let err =
                map_shard_to_split(parent_id, parent_set, *parent_revision, true).unwrap_err();
            assert_eq!(err.to_string(), "shard derivation/example/derivation/2020202020202020/20000000-60000000 is already splitting from source derivation/example/derivation/2020202020202020/10000000-60000000");
        }

        // Case: split a partition on its key.
        {
            let (parent_name, parent_set, parent_revision) = all_partitions.first().unwrap();

            let splits = map_partition_to_split(parent_name, parent_set, *parent_revision).unwrap();
            let partition_changes = partition_changes(Some(partition_template), &splits).unwrap();

            insta::assert_json_snapshot!(
                "partition_splits",
                json!(["splits", splits, "partition_changes", partition_changes])
            );
        }

        // Case: generation of ops collection partition.
        {
            let (list_req, spec) = list_ops_journal_request(
                ops::TaskType::Capture,
                "the/task/name",
                ops_logs_template,
            );

            insta::assert_json_snapshot!(
                "ops_collection_partition",
                json!(["list_req", list_req, "spec", spec])
            );
        }
    }
}
