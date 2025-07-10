use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use gazette::broker::journal_spec;
use itertools::Itertools;
use proto_gazette::{broker, consumer};

pub mod automation;

pub async fn migrate_data_planes(
    pg_pool: &sqlx::PgPool,
    src_data_plane: &str,
    tgt_data_plane: &str,
    catalog_prefix: &str,
) -> anyhow::Result<()> {
    let tmp_do_storage_update = src_data_plane == "ops/dp/public/gcp-us-central1-c1"
        && tgt_data_plane == "ops/dp/public/gcp-us-central1-c2";

    let src_data_plane = fetch_data_plane(pg_pool, src_data_plane).await?;
    let tgt_data_plane = fetch_data_plane(pg_pool, tgt_data_plane).await?;

    // Phase one: identify covered specs in the source data-plane, cordon them,
    // and migrate them to the target data-plane in a cordoned state.
    let spec_migrations: Vec<SpecMigration> =
        fetch_spec_migrations(pg_pool, catalog_prefix, src_data_plane.row.control_id)
            .try_collect()
            .await?;

    () = futures::stream::iter(spec_migrations.iter().map(Ok))
        .try_for_each_concurrent(CONCURRENCY, |spec_migration| {
            phase_one(spec_migration, &src_data_plane, &tgt_data_plane)
        })
        .await?;

    // Phase two: update the data-plane ID of migrated specs to the target.
    let live_spec_ids: Vec<models::Id> = spec_migrations
        .iter()
        .map(|spec| spec.live_spec_id)
        .collect();

    sqlx::query!(
        r#"UPDATE live_specs SET data_plane_id = $2 WHERE id=ANY($1)"#,
        live_spec_ids as Vec<models::Id>,
        tgt_data_plane.row.control_id as models::Id,
    )
    .execute(pg_pool)
    .await?;

    // Phase three: re-query for live specs that match the prefix and target
    // data-plane, and un-cordon them. We re-query to pick up any specs which
    // had previously completed phase one and two, but not three.
    let spec_migrations: Vec<SpecMigration> =
        fetch_spec_migrations(pg_pool, catalog_prefix, tgt_data_plane.row.control_id)
            .try_collect()
            .await?;

    // Reverse ordering to un-cordon collections, then tasks.
    () = futures::stream::iter(spec_migrations.iter().rev().map(Ok))
        .try_for_each_concurrent(CONCURRENCY, |spec_migration| {
            phase_three(spec_migration, &tgt_data_plane)
        })
        .await?;

    // TODO(johnny): Temporary support of the cronut migration.
    // Remove when that migration is completed.
    if tmp_do_storage_update {
        sqlx::query!(
            r#"
        UPDATE storage_mappings
        SET spec = (
            jsonb_set(
                spec::jsonb,               -- work in jsonb
                '{data_planes}',           -- path to overwrite
                (
                    SELECT jsonb_agg(      -- rebuild the array
                        CASE v.value
                            WHEN 'ops/dp/public/gcp-us-central1-c1'::text
                            THEN to_jsonb('ops/dp/public/gcp-us-central1-c2'::text)
                            ELSE to_jsonb(v.value)
                        END
                        ORDER BY v.ordinality   -- preserve order
                    )
                    FROM jsonb_array_elements_text(spec::jsonb->'data_planes')
                    WITH ORDINALITY AS v(value, ordinality)
                )
            )
        )::json                           -- cast back to the column type
        WHERE  catalog_prefix = $1        -- only on exact match
        AND  spec::jsonb->'data_planes'
            ? 'ops/dp/public/gcp-us-central1-c1'::text;  -- update only if present
        "#,
            catalog_prefix,
        )
        .execute(pg_pool)
        .await?;
    }

    Ok(())
}

// phase_one cordons and suspends shards and journals in the source data-plane,
// and copies equivalent shard and journal splits into the target data plane.
async fn phase_one(
    spec_migration: &SpecMigration,
    src_data_plane: &DataPlane,
    tgt_data_plane: &DataPlane,
) -> anyhow::Result<()> {
    let SpecMigration {
        catalog_name,
        built_spec,
        live_spec_id: _,
    } = spec_migration;

    let DataPlane {
        journal_client: src_journal_client,
        ops_logs_template: src_ops_logs_template,
        ops_stats_template: src_ops_stats_template,
        row: src_data_plane,
        shard_client: src_shard_client,
    } = src_data_plane;

    let DataPlane {
        journal_client: tgt_journal_client,
        ops_logs_template: tgt_ops_logs_template,
        ops_stats_template: tgt_ops_stats_template,
        row: tgt_data_plane,
        shard_client: tgt_shard_client,
    } = tgt_data_plane;

    let (task_type, task_template, partition_template) = unpack_templates(built_spec)?;

    // Munge FQDNs for use within Gazette label values.
    //
    // If an input has a :port, that port is preserved in the output as /port:
    // - https://flow.localhost:9000 => flow.localhost/9000
    // Otherwise if no :port is present, the default port 443 is used:
    // - https://reactor.aws-eu-west-1-c1.dp.estuary-data.com => reactor.aws-eu-west-1-c1.dp.estuary-data.com/443
    // - https://reactor.f3c2b732d1763a1c.dp.estuary-data.com => reactor.f3c2b732d1763a1c.dp.estuary-data.com/443
    // As a special case, the internal Cronut service address is re-mapped to the data-plane-gateway:
    // - http://flow-reactor.flow.svc.cluster.local:8080 => us-central1.v1.estuary-data.dev/443
    let munge_fqdn = |fqdn: &str| {
        if fqdn == "http://flow-reactor.flow.svc.cluster.local:8080" {
            return "us-central1.v1.estuary-data.dev/443".to_string();
        }
        let fqdn = fqdn.strip_prefix("https://").unwrap_or(fqdn);
        if fqdn.contains(':') {
            fqdn.replace(":", "/")
        } else {
            format!("{}/443", fqdn)
        }
    };

    // Fetch and cordon shards and journals from the source data-plane.
    let (mut src_shards, src_recovery, src_partitions) = update_cordon(
        &src_data_plane.data_plane_name,
        src_journal_client,
        src_shard_client,
        catalog_name,
        task_type,
        task_template,
        partition_template,
        Some(src_ops_logs_template),
        Some(src_ops_stats_template),
        Some((
            munge_fqdn(&tgt_data_plane.broker_address).as_str(),
            munge_fqdn(&tgt_data_plane.reactor_address).as_str(),
        )), // Cordon.
    )
    .await
    .with_context(|| format!("failed to cordon {catalog_name}"))?;

    () = attach_shard_primary_hints(src_shard_client, &mut src_shards)
        .await
        .with_context(|| format!("failed to fetch shard hints of {catalog_name}"))?;

    // Fetch shards and journals from the target data-plane.
    // They may exist in a cordoned state if we're migrating back.
    let ((tgt_shards, tgt_recovery, tgt_ops_logs, tgt_ops_stats), tgt_partitions) =
        futures::try_join!(
            activate::fetch_task_splits(
                tgt_journal_client,
                tgt_shard_client,
                task_type,
                catalog_name,
                Some(tgt_ops_logs_template),
                Some(tgt_ops_stats_template),
            ),
            activate::fetch_partition_splits(tgt_journal_client, catalog_name),
        )
        .with_context(|| format!("failed to fetch target-plane splits of {catalog_name}"))?;

    let shards: Vec<activate::ShardSplit> = merge_splits(
        src_shards,
        tgt_shards,
        |l, r| l.id.cmp(&r.id),
        |s| &mut s.labels,
        |s| &mut s.mod_revision,
    );
    let recovery: Vec<activate::JournalSplit> = merge_splits(
        src_recovery,
        tgt_recovery,
        |l, r| l.name.cmp(&r.name),
        |s| &mut s.labels,
        |s| &mut s.mod_revision,
    );
    let partitions: Vec<activate::JournalSplit> = merge_splits(
        src_partitions,
        tgt_partitions,
        |l, r| l.name.cmp(&r.name),
        |s| &mut s.labels,
        |s| &mut s.mod_revision,
    );

    let mut changes =
        activate::task_changes(task_template, shards, recovery, tgt_ops_logs, tgt_ops_stats)?;
    changes.extend(activate::partition_changes(partition_template, partitions)?);

    print_changes(&changes, &tgt_data_plane.data_plane_name);
    () = activate::apply_changes(tgt_journal_client, tgt_shard_client, changes)
        .await
        .with_context(|| format!("failed to apply target-plane splits of {catalog_name}"))?;

    Ok(())
}

fn print_changes(changes: &[activate::Change], data_plane: &str) {
    for change in changes {
        match change {
            activate::Change::Journal(broker::apply_request::Change {
                upsert: Some(upsert),
                expect_mod_revision,
                ..
            }) => {
                let cordon = labels::values(upsert.labels.as_ref().unwrap(), labels::CORDON)
                    .first()
                    .map(|l| l.value.as_str());
                tracing::info!(data_plane, name=%upsert.name, cordon, suspend=?upsert.suspend, %expect_mod_revision, "journal upsert");
            }
            activate::Change::Journal(broker::apply_request::Change {
                upsert: None,
                expect_mod_revision,
                delete,
            }) => {
                tracing::info!(data_plane, name=%delete, %expect_mod_revision, "journal delete");
            }

            activate::Change::Shard(consumer::apply_request::Change {
                upsert: Some(upsert),
                expect_mod_revision,
                primary_hints,
                ..
            }) => {
                let cordon = labels::values(upsert.labels.as_ref().unwrap(), labels::CORDON)
                    .first()
                    .map(|l| l.value.as_str());
                tracing::info!(data_plane, id=%upsert.id, cordon, hints=primary_hints.is_some(), %expect_mod_revision, "shard upsert");
            }
            activate::Change::Shard(consumer::apply_request::Change {
                upsert: None,
                expect_mod_revision,
                delete,
                ..
            }) => {
                tracing::info!(data_plane, id=%delete, %expect_mod_revision, "shard delete");
            }
        }
    }
}

async fn phase_three(
    spec_migration: &SpecMigration,
    tgt_data_plane: &DataPlane,
) -> anyhow::Result<()> {
    let SpecMigration {
        catalog_name,
        built_spec,
        live_spec_id: _,
    } = spec_migration;

    let DataPlane {
        journal_client: tgt_journal_client,
        ops_logs_template: tgt_ops_logs_template,
        ops_stats_template: tgt_ops_stats_template,
        row: tgt_data_plane,
        shard_client: tgt_shard_client,
    } = tgt_data_plane;

    let (task_type, task_template, partition_template) = unpack_templates(built_spec)?;

    // Fetch and un-cordon shards and journals of the target data-plane.
    let (_shards, _recovery, _partitions) = update_cordon(
        &tgt_data_plane.data_plane_name,
        tgt_journal_client,
        tgt_shard_client,
        catalog_name,
        task_type,
        task_template,
        partition_template,
        Some(tgt_ops_logs_template),
        Some(tgt_ops_stats_template),
        None, // Remove cordon.
    )
    .await
    .with_context(|| format!("failed to un-cordon {catalog_name}"))?;

    Ok(())
}

async fn update_cordon<'a>(
    data_plane_name: &str,
    journal_client: &gazette::journal::Client,
    shard_client: &gazette::shard::Client,
    catalog_name: &str,
    task_type: ops::TaskType,
    task_template: Option<activate::TaskTemplate<'a>>,
    partition_template: Option<&'a broker::JournalSpec>,
    ops_logs_template: Option<&broker::JournalSpec>,
    ops_stats_template: Option<&broker::JournalSpec>,
    cordon: Option<(&str, &str)>,
) -> anyhow::Result<(
    Vec<activate::ShardSplit>,
    Vec<activate::JournalSplit>,
    Vec<activate::JournalSplit>,
)> {
    loop {
        let ((mut shards, recovery, ops_logs, ops_stats), mut partitions) = futures::try_join!(
            activate::fetch_task_splits(
                journal_client,
                shard_client,
                task_type,
                catalog_name,
                ops_logs_template,
                ops_stats_template,
            ),
            activate::fetch_partition_splits(journal_client, &catalog_name)
        )
        .context("fetching splits")?;

        if apply_cordon_label(
            cordon.map(|(cordon_journals, _)| cordon_journals),
            partitions.iter_mut().map(|journal| &mut journal.labels),
        ) || apply_cordon_label(
            cordon.map(|(_, cordon_shards)| cordon_shards),
            shards.iter_mut().map(|shard| &mut shard.labels),
        ) {
            let mut changes =
                activate::task_changes(task_template, shards, recovery, ops_logs, ops_stats)?;
            changes.extend(activate::partition_changes(partition_template, partitions)?);

            print_changes(&changes, data_plane_name);
            () = activate::apply_changes(journal_client, shard_client, changes)
                .await
                .context("applying cordon label")?;

            continue; // Loop to try again.
        }
        if cordon.is_some() {
            if apply_journal_suspension(journal_client, recovery.iter().chain(partitions.iter()))
                .await
                .context("suspending journal")?
            {
                continue; // Loop to try again.
            }
        }

        return Ok((shards, recovery, partitions));
    }
}

fn apply_cordon_label<'a, 'b>(
    desired: Option<&'a str>,
    it: impl Iterator<Item = &'b mut broker::LabelSet>,
) -> bool {
    let mut changed = false;

    for set in it {
        let current = labels::values(set, labels::CORDON)
            .first()
            .map(|l| l.value.as_str());

        if current == desired {
            continue; // No change.
        } else if let Some(desired) = desired {
            *set = labels::add_value(std::mem::take(set), labels::CORDON, desired);
            changed = true;
        } else {
            *set = labels::remove(std::mem::take(set), labels::CORDON);
            changed = true;
        }
    }
    changed
}

async fn apply_journal_suspension<'a>(
    journal_client: &gazette::journal::Client,
    it: impl Iterator<Item = &'a activate::JournalSplit>,
) -> anyhow::Result<bool> {
    use futures::TryStreamExt;

    let mut to_suspend = Vec::new();

    for journal in it {
        if matches!(&journal.suspend, Some(journal_spec::Suspend { level, .. }) if *level != 0) {
            continue; // Already suspended.
        }

        let response = journal_client.append(
            broker::AppendRequest {
                journal: journal.name.clone(),
                suspend: broker::append_request::Suspend::Now as i32,
                ..Default::default()
            },
            || futures::stream::empty(),
        );

        to_suspend.push(async move {
            futures::pin_mut!(response);

            loop {
                match response.try_next().await {
                    Err(gazette::RetryError {
                        inner: gazette::Error::BrokerStatus(broker::Status::Suspended),
                        ..
                    }) => {
                        tracing::info!(name=%journal.name, "suspended journal");
                        return Ok(());
                    }
                    Err(gazette::RetryError {
                        attempt,
                        inner: err,
                    }) if attempt < 5 && err.is_transient() => {
                        tracing::warn!(attempt, ?err, "failed to suspend journal (will retry)");
                    }
                    Ok(Some(response)) => {
                        anyhow::bail!("received unexpected AppendResponse {response:?} (wanted SUSPENDED status)")
                    }
                    Ok(None) => anyhow::bail!("received unexpected EOF (wanted AppendResponse)"),
                    Err(gazette::RetryError { inner: err, .. }) => return Err(err.into()),
                }
            }
        });
    }

    let suspended: Vec<()> = futures::stream::iter(to_suspend)
        .buffer_unordered(CONCURRENCY)
        .try_collect()
        .await?;

    Ok(!suspended.is_empty())
}

async fn attach_shard_primary_hints(
    shard_client: &gazette::shard::Client,
    shards: &mut [activate::ShardSplit],
) -> anyhow::Result<()> {
    use futures::TryStreamExt;

    () = futures::stream::iter(shards.iter_mut().map(Ok))
        .try_for_each_concurrent(CONCURRENCY, |shard| async {
            let hints = shard_client
                .get_hints(consumer::GetHintsRequest {
                    shard: shard.id.clone(),
                    ..Default::default()
                })
                .await?;

            if let Some(hints) = hints.primary_hints {
                shard.primary_hints = hints.hints;
            }

            Ok::<_, gazette::Error>(())
        })
        .await?;

    Ok(())
}

fn merge_splits<S>(
    src: Vec<S>,
    tgt: Vec<S>,
    cmp_fn: impl Fn(&S, &S) -> std::cmp::Ordering,
    labels_fn: impl Fn(&mut S) -> &mut gazette::broker::LabelSet,
    mod_revision_fn: impl Fn(&mut S) -> &mut i64,
) -> Vec<S> {
    // Within the source data-plane, the estuary.dev/cordon label has been
    // updated to point to the target data-plane FQDN. In the target plane,
    // use an empty value to denote it's incoming. The connector networking
    // feature has special handling for this label to determine when it should
    // forward across data-planes, and expects target cordon labels to be
    // empty when forwarding should not be done.
    src.into_iter()
        .merge_join_by(tgt, cmp_fn)
        .map(|eob| match eob {
            itertools::EitherOrBoth::Left(mut src) => {
                let labels = std::mem::take(labels_fn(&mut src));
                *labels_fn(&mut src) = labels::set_value(labels, labels::CORDON, "");
                *mod_revision_fn(&mut src) = 0; // Doesn't exist in target data-plane.
                src
            }
            itertools::EitherOrBoth::Right(tgt) => tgt,
            itertools::EitherOrBoth::Both(mut src, mut tgt) => {
                let labels = std::mem::take(labels_fn(&mut src));
                *labels_fn(&mut src) = labels::set_value(labels, labels::CORDON, "");
                *mod_revision_fn(&mut src) = *mod_revision_fn(&mut tgt);
                src
            }
        })
        .collect()
}

struct DataPlane {
    row: tables::DataPlane,
    shard_client: gazette::shard::Client,
    journal_client: gazette::journal::Client,
    ops_logs_template: broker::JournalSpec,
    ops_stats_template: broker::JournalSpec,
}

async fn fetch_data_plane(pg_pool: &sqlx::PgPool, name: &str) -> anyhow::Result<DataPlane> {
    let row = sqlx::query_as!(
        tables::DataPlane,
        r#"
        SELECT
            id AS "control_id: models::Id",
            data_plane_name,
            data_plane_fqdn,
            false AS "is_default!: bool",
            hmac_keys,
            encrypted_hmac_keys as "encrypted_hmac_keys: models::RawValue",
            broker_address,
            reactor_address,
            ops_logs_name AS "ops_logs_name: models::Collection",
            ops_stats_name AS "ops_stats_name: models::Collection"
        FROM data_planes
        WHERE data_plane_name = $1
        "#,
        name
    )
    .fetch_one(pg_pool)
    .await
    .with_context(|| format!("failed to fetch data-plane {name}"))?;

    // Resolve ops collection templates for this data-plane.
    let r = sqlx::query!(
        r#"
        SELECT
            l.built_spec->'partitionTemplate' AS "logs!:  sqlx::types::Json<broker::JournalSpec>",
            s.built_spec->'partitionTemplate' AS "stats!: sqlx::types::Json<broker::JournalSpec>"
        FROM live_specs l, live_specs s
        WHERE l.catalog_name = $1 AND l.spec_type = 'collection'
        AND   s.catalog_name = $2 AND s.spec_type = 'collection'
        "#,
        &row.ops_logs_name,
        &row.ops_stats_name,
    )
    .fetch_one(pg_pool)
    .await
    .context("failed to fetch data-plane ops collections")?;
    let (ops_logs_template, ops_stats_template) = (r.logs.0, r.stats.0);

    let mut metadata = gazette::Metadata::default();
    metadata
        .signed_claims(
            proto_gazette::capability::APPEND
                | proto_gazette::capability::APPLY
                | proto_gazette::capability::LIST
                | proto_gazette::capability::READ,
            &row.data_plane_fqdn,
            std::time::Duration::from_secs(900),
            &row.hmac_keys,
            broker::LabelSelector::default(),
            "migrate-tool",
        )
        .context("failed to sign claims for data-plane")?;

    // Create the journal and shard clients that are used for interacting with the data plane
    let router = gazette::Router::new("local");
    let journal_client =
        gazette::journal::Client::new(row.broker_address.clone(), metadata.clone(), router.clone());
    let shard_client = gazette::shard::Client::new(row.reactor_address.clone(), metadata, router);

    Ok(DataPlane {
        row,
        shard_client,
        journal_client,
        ops_logs_template,
        ops_stats_template,
    })
}

struct SpecMigration {
    catalog_name: String,
    live_spec_id: models::Id,
    built_spec: proto_flow::AnyBuiltSpec,
}

fn fetch_spec_migrations<'a>(
    pg_pool: &'a sqlx::PgPool,
    catalog_prefix: &'a str,
    data_plane_id: models::Id,
) -> impl futures::Stream<Item = anyhow::Result<SpecMigration>> + 'a {
    sqlx::query!(
        r#"
        SELECT
            id AS "id: models::Id",
            catalog_name,
            spec_type AS "spec_type!: models::CatalogType",
            built_spec AS "built_spec!: sqlx::types::Json<models::RawValue>"
        FROM live_specs
        WHERE starts_with(catalog_name, $1)
        AND   built_spec IS NOT NULL
        AND   data_plane_id = $2
        -- Migrate tasks first, then collections.
        -- This minimizes shard failures due to cordoned journals.
        ORDER BY spec_type = 'collection', catalog_name
        "#,
        catalog_prefix,
        data_plane_id as models::Id,
    )
    .fetch(pg_pool)
    .map_err(|err| anyhow::anyhow!(err))
    .and_then(|r| async move {
        let spec = match r.spec_type {
            models::CatalogType::Capture => {
                proto_flow::AnyBuiltSpec::Capture(serde_json::from_str(r.built_spec.get())?)
            }
            models::CatalogType::Collection => {
                proto_flow::AnyBuiltSpec::Collection(serde_json::from_str(r.built_spec.get())?)
            }
            models::CatalogType::Materialization => {
                proto_flow::AnyBuiltSpec::Materialization(serde_json::from_str(r.built_spec.get())?)
            }
            models::CatalogType::Test => {
                proto_flow::AnyBuiltSpec::Test(serde_json::from_str(r.built_spec.get())?)
            }
        };

        Ok(SpecMigration {
            catalog_name: r.catalog_name,
            live_spec_id: r.id,
            built_spec: spec,
        })
    })
}

fn unpack_templates<'a>(
    built_spec: &'a proto_flow::AnyBuiltSpec,
) -> anyhow::Result<(
    ops::TaskType,
    Option<activate::TaskTemplate<'a>>,
    Option<&'a broker::JournalSpec>,
)> {
    Ok(match built_spec {
        proto_flow::AnyBuiltSpec::Capture(spec) => {
            let task_template = activate::capture_template(Some(spec))?;
            (ops::TaskType::Capture, task_template, None)
        }
        proto_flow::AnyBuiltSpec::Collection(spec) => {
            let (partition_template, task_template) = activate::collection_template(Some(spec))?;
            (ops::TaskType::Derivation, task_template, partition_template)
        }
        proto_flow::AnyBuiltSpec::Materialization(spec) => {
            let task_template = activate::materialization_template(Some(&spec))?;
            (ops::TaskType::Materialization, task_template, None)
        }
        proto_flow::AnyBuiltSpec::Test(_spec) => (ops::TaskType::InvalidType, None, None),
    })
}

const CONCURRENCY: usize = 25;
