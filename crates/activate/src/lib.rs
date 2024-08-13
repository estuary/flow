use anyhow::Context;
use proto_flow::flow;
use proto_gazette::{
    broker::{self, JournalSpec, Label, LabelSelector, LabelSet},
    consumer::{self, ShardSpec},
};
use std::collections::BTreeMap;

/// Activate a capture into a data-plane.
pub async fn activate_capture(
    journal_client: &gazette::journal::Client,
    shard_client: &gazette::shard::Client,
    capture: &models::Capture,
    spec: Option<&flow::CaptureSpec>,
    initial_splits: usize,
) -> anyhow::Result<()> {
    let task_template = if let Some(spec) = spec {
        let shard_template = spec
            .shard_template
            .as_ref()
            .context("CaptureSpec missing shard_template")?;

        let log_template = spec
            .recovery_log_template
            .as_ref()
            .context("CaptureSpec missing recovery_log_template")?;

        Some((shard_template, log_template))
    } else {
        None
    };

    converge_task_shards(
        journal_client,
        shard_client,
        capture,
        ops::TaskType::Capture,
        task_template,
        initial_splits,
    )
    .await
}

/// Activate a collection into a data-plane.
pub async fn activate_collection(
    journal_client: &gazette::journal::Client,
    shard_client: &gazette::shard::Client,
    collection: &models::Collection,
    spec: Option<&flow::CollectionSpec>,
    initial_splits: usize,
) -> anyhow::Result<()> {
    let (task_template, partition_template) = if let Some(spec) = spec {
        let partition_template = spec
            .partition_template
            .as_ref()
            .context("CollectionSpec missing partition_template")?;

        let task_template = if let Some(derivation) = &spec.derivation {
            let shard_template = derivation
                .shard_template
                .as_ref()
                .context("CollectionSpec.Derivation missing shard_template")?;

            let log_template = derivation
                .recovery_log_template
                .as_ref()
                .context("CollectionSpec.Derivation missing recovery_log_template")?;

            Some((shard_template, log_template))
        } else {
            None
        };

        (task_template, Some(partition_template))
    } else {
        (None, None)
    };

    futures::try_join!(
        converge_task_shards(
            journal_client,
            shard_client,
            collection,
            ops::TaskType::Derivation,
            task_template,
            initial_splits,
        ),
        converge_partition_journals(journal_client, collection, partition_template),
    )?;

    Ok(())
}

/// Activate a materialization into a data-plane.
pub async fn activate_materialization(
    journal_client: &gazette::journal::Client,
    shard_client: &gazette::shard::Client,
    materialization: &models::Materialization,
    spec: Option<&flow::MaterializationSpec>,
    initial_splits: usize,
) -> anyhow::Result<()> {
    let task_template = if let Some(spec) = spec {
        let shard_template = spec
            .shard_template
            .as_ref()
            .context("MaterializationSpec missing shard_template")?;

        let log_template = spec
            .recovery_log_template
            .as_ref()
            .context("MaterializationSpec missing recovery_log_template")?;

        Some((shard_template, log_template))
    } else {
        None
    };

    converge_task_shards(
        journal_client,
        shard_client,
        materialization,
        ops::TaskType::Materialization,
        task_template,
        initial_splits,
    )
    .await
}

/// Converge a task by listing data-plane ShardSpecs and recovery log
/// JournalSpecs, and then applying updates to bring them into alignment
/// with the templated task configuration.
async fn converge_task_shards(
    journal_client: &gazette::journal::Client,
    shard_client: &gazette::shard::Client,
    task_name: &str,
    task_type: ops::TaskType,
    template: Option<(&ShardSpec, &JournalSpec)>,
    initial_splits: usize,
) -> anyhow::Result<()> {
    let (list_shards, list_logs) = list_task_request(task_type, task_name);

    let (shards, logs) = futures::try_join!(
        shard_client.list(list_shards),
        journal_client.list(list_logs),
    )?;
    let shards = unpack_shard_listing(shards)?;
    let logs = unpack_journal_listing(logs)?;

    let (shard_changes, log_changes) = task_changes(template, &shards, &logs, initial_splits)?;

    // We must create recovery logs before we create their shards.
    journal_client.apply(log_changes).await?;
    shard_client.apply(shard_changes).await?;

    if template.is_some() {
        // Unassign any failed shards to get them running again after updating their specs
        let shard_ids = shards.into_iter().map(|s| s.0).collect();
        let unassign_req = consumer::UnassignRequest {
            shards: shard_ids,
            only_failed: true,
            dry_run: false,
        };
        shard_client
            .unassign(unassign_req)
            .await
            .context("unassigning failed shards")?;
    }

    Ok(())
}

/// Converge a collection by listing data-plane partition JournalSpecs,
/// and then applying updates to bring them into alignment
/// with the templated collection configuration.
async fn converge_partition_journals(
    journal_client: &gazette::journal::Client,
    collection: &models::Collection,
    template: Option<&JournalSpec>,
) -> anyhow::Result<()> {
    let list_partitions = list_partitions_request(&collection);

    let partitions = journal_client.list(list_partitions).await?;
    let partitions = unpack_journal_listing(partitions)?;

    let changes = partition_changes(template, &partitions)?;

    journal_client.apply(changes).await?;

    Ok(())
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
    let list_logs = broker::ListRequest {
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
    (list_shards, list_logs)
}

/// Build a ListRequest of a collections partitions.
fn list_partitions_request(collection: &models::Collection) -> broker::ListRequest {
    broker::ListRequest {
        selector: Some(LabelSelector {
            include: Some(labels::build_set([(
                labels::COLLECTION,
                collection.as_str(),
            )])),
            exclude: None,
        }),
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
/// converge from current `shards` and `logs` splits into the desired state.
fn task_changes(
    template: Option<(&ShardSpec, &JournalSpec)>,
    shards: &[(String, LabelSet, i64)],
    logs: &[(String, LabelSet, i64)],
    initial_splits: usize,
) -> anyhow::Result<(consumer::ApplyRequest, broker::ApplyRequest)> {
    let mut shards = shards.to_vec();

    // If the template is Some and no current shards match its prefix,
    // then instantiate `initial_splits` new shards to create.
    if let Some((shard_template, _)) = template {
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

    let mut logs: BTreeMap<_, _> = logs
        .iter()
        .map(|(log, set, revision)| (log, (set, revision)))
        .collect();

    let mut shard_changes = Vec::new();
    let mut log_changes = Vec::new();

    for (id, split, shard_revision) in shards {
        match template {
            Some((shard_template, log_template)) if id.starts_with(&shard_template.id) => {
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
                shard_spec.id = format!(
                    "{}/{}",
                    shard_spec.id,
                    labels::shard::id_suffix(&shard_set)?
                );
                shard_spec.labels = Some(shard_set);

                let mut log_spec = log_template.clone();
                log_spec.name = format!("{}/{}", shard_spec.recovery_log_prefix, shard_spec.id);

                let log_revision = logs
                    .remove(&log_spec.name)
                    .map(|(_, r)| *r)
                    .unwrap_or_default();

                shard_changes.push(consumer::apply_request::Change {
                    expect_mod_revision: shard_revision,
                    upsert: Some(shard_spec),
                    delete: String::new(),
                });
                log_changes.push(broker::apply_request::Change {
                    expect_mod_revision: log_revision,
                    upsert: Some(log_spec),
                    delete: String::new(),
                });
            }
            _ => {
                shard_changes.push(consumer::apply_request::Change {
                    expect_mod_revision: shard_revision,
                    upsert: None,
                    delete: id,
                });
            }
        }
    }

    // Any remaining recovery logs are not paired with a shard, and are deleted.
    for (log, (_set, mod_revision)) in logs {
        log_changes.push(broker::apply_request::Change {
            expect_mod_revision: *mod_revision,
            upsert: None,
            delete: log.clone(),
        });
    }

    Ok((
        consumer::ApplyRequest {
            changes: shard_changes,
            ..Default::default()
        },
        broker::ApplyRequest {
            changes: log_changes,
        },
    ))
}

/// Determine the broker partition changes required to converge
/// from current `partitions` into the desired state.
fn partition_changes(
    template: Option<&broker::JournalSpec>,
    partitions: &[(String, LabelSet, i64)],
) -> anyhow::Result<broker::ApplyRequest> {
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

                changes.push(broker::apply_request::Change {
                    expect_mod_revision: *mod_revision,
                    upsert: Some(partition_spec),
                    delete: String::new(),
                });
            }
            _ => {
                changes.push(broker::apply_request::Change {
                    expect_mod_revision: *mod_revision,
                    upsert: None,
                    delete: journal.clone(),
                });
            }
        }
    }

    Ok(broker::ApplyRequest { changes })
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
        )),)
    }

    #[test]
    fn test_list_task_request() {
        insta::assert_debug_snapshot!(list_task_request(
            ops::TaskType::Derivation,
            "the/derivation"
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
            true,                     // generate_ops_collections
            ops::tracing_log_handler,
            false, // don't no-op validations
            false, // don't no-op validations
            false, // don't no-op validations
            4,     // max_concurrent_validations
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
                    recovery_log_template: Some(log_template),
                    shard_template: Some(shard_template),
                    ..
                }),
            ..
        }) = spec
        else {
            unreachable!()
        };

        let extractors =
            extractors::for_fields(partition_fields, projections, &doc::SerPolicy::noop()).unwrap();

        let mut all_partitions = Vec::new();
        let mut all_shards = Vec::new();
        let mut all_logs = Vec::new();

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
            all_logs.push((
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
            let (shard_changes, log_changes) = task_changes(
                Some((&shard_template, &log_template)),
                &all_shards,
                &all_logs,
                4,
            )
            .unwrap();

            insta::assert_json_snapshot!("update", (partition_changes, shard_changes, log_changes));
        }

        // Case: test creation of new specs.
        {
            let partition_changes = partition_changes(Some(&partition_template), &[]).unwrap();
            let (shard_changes, log_changes) =
                task_changes(Some((&shard_template, &log_template)), &[], &[], 4).unwrap();

            insta::assert_json_snapshot!("create", (partition_changes, shard_changes, log_changes));
        }

        // Case: test deletion of existing specs.
        {
            let partition_changes = partition_changes(None, &all_partitions).unwrap();
            let (shard_changes, log_changes) =
                task_changes(None, &all_shards, &all_logs, 4).unwrap();

            insta::assert_json_snapshot!("delete", (partition_changes, shard_changes, log_changes));
        }

        // Case: test mixed deletion and creation.
        {
            // Simulate existing data-plane specs which were created under an
            // older initial publication ID, and which are now being swapped out.
            // This emulates a deletion followed by a re-creation, where we failed
            // to activate the intermediary deletion.
            let mut all_partitions = all_partitions.clone();
            let mut all_shards = all_shards.clone();
            let mut all_logs = all_logs.clone();

            for (name, _, _) in all_partitions.iter_mut() {
                *name = name.replace("2020202020202020", "replaced-pub-id");
            }
            for (id, _, _) in all_shards.iter_mut() {
                *id = id.replace("2020202020202020", "replaced-pub-id");
            }
            for (name, _, _) in all_logs.iter_mut() {
                *name = name.replace("2020202020202020", "replaced-pub-id");
            }

            let partition_changes =
                partition_changes(Some(&partition_template), &all_partitions).unwrap();
            let (shard_changes, log_changes) = task_changes(
                Some((&shard_template, &log_template)),
                &all_shards,
                &all_logs,
                4,
            )
            .unwrap();

            insta::assert_json_snapshot!(
                "create_and_delete",
                (partition_changes, shard_changes, log_changes)
            );
        }

        // Case: split a shard on its key or clock.
        {
            let (parent_id, parent_set, parent_revision) = all_shards.first().unwrap();

            let key_splits =
                map_shard_to_split(parent_id, parent_set, *parent_revision, true).unwrap();
            let clock_splits =
                map_shard_to_split(parent_id, parent_set, *parent_revision, false).unwrap();

            let (key_shard_changes, key_log_changes) = task_changes(
                Some((&shard_template, &log_template)),
                &key_splits,
                &all_logs[..1],
                4,
            )
            .unwrap();

            let (clock_shard_changes, clock_log_changes) = task_changes(
                Some((&shard_template, &log_template)),
                &clock_splits,
                &all_logs[..1],
                4,
            )
            .unwrap();

            insta::assert_json_snapshot!(
                "shard_splits",
                json!([
                    "key_splits",
                    &key_splits,
                    "clock_splits",
                    clock_splits,
                    "key_shard_changes",
                    key_shard_changes,
                    "key_log_changes",
                    key_log_changes,
                    "clock_shard_changes",
                    clock_shard_changes,
                    "clock_log_changes",
                    clock_log_changes,
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
    }
}
