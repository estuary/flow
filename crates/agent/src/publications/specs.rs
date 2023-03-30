use super::draft::Error;
use agent_sql::publications::{ExpandedRow, SpecRow, Tenant};
use agent_sql::{Capability, CatalogType, Id};
use anyhow::Context;
use itertools::Itertools;
use sqlx::types::Uuid;
use std::collections::{BTreeMap, HashMap};

// resolve_specifications returns the definitive set of specifications which
// are changing in this publication. It obtains sufficient locks to ensure
// that raced publications to returned specifications are serialized with
// this publication.
pub async fn resolve_specifications(
    draft_id: Id,
    pub_id: Id,
    user_id: Uuid,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<Vec<SpecRow>> {
    // Attempt to create a row in live_specs for each of our draft_specs.
    // This allows us next inner-join over draft and live spec rows.
    // Inner join (vs a left-join) is required for "for update" semantics.
    //
    // We're intentionally running with read-committed isolation, and that
    // means a concurrent transaction may have committed a new row to live_specs
    // which technically isn't serializable with this transaction...
    // but we don't much care. Postgres will silently skip it under
    // "on conflict .. do nothing" semantics, and we'll lock the new row next.
    //
    // See: https://www.postgresql.org/docs/14/transaction-iso.html#XACT-READ-COMMITTED
    let rows = agent_sql::publications::insert_new_live_specs(draft_id, pub_id, txn)
        .await
        .context("inserting new live_specs")?;

    tracing::debug!(rows, "inserted new live_specs");

    // Fetch all of the draft's patches, along with their (now locked) live specifications.
    // This query is where we determine "before" and "after" states for each specification,
    // and determine exactly what changed.
    //
    // "for update" tells postgres that access to these rows should be serially sequenced,
    // meaning the user can't change a draft_spec out from underfoot, and a live_spec also
    // can't be silently changed. In both cases a concurrent update will block on our locks.
    //
    // It's possible that the user adds a new draft_spec at any time -- even between our last
    // statement and this one. Thus the result-set of this inner join is the final determiner
    // of what's "in" this publication, and what's not. Anything we don't pick up here will
    // be left behind as a draft_spec, and this is the reason we don't delete the draft
    // itself within this transaction.
    let mut spec_rows = agent_sql::publications::resolve_spec_rows(draft_id, user_id, txn)
        .await
        .context("selecting joined draft & live specs")?;

    // The query may return live specifications that the user is not
    // authorized to know anything about. Tweak such rows to appear
    // as if the spec is being created.
    for row in &mut spec_rows {
        if row.user_capability.is_none() {
            row.last_build_id = pub_id;
            row.last_pub_id = pub_id;
            row.live_spec = None;
            row.live_spec_id = row.draft_spec_id;
            row.live_type = None;
            row.spec_capabilities.0 = Vec::new();
        }
    }

    Ok(spec_rows)
}

// expanded_specifications returns additional specifications which should be
// included in this publication's build. These specifications are not changed
// by the publication and are read with read-committed transaction semantics,
// but (if not a dry-run) we do re-activate each specification within the
// data-plane with the outcome of this publication's build.
pub async fn expanded_specifications(
    spec_rows: &[SpecRow],
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<Vec<ExpandedRow>> {
    // We seed expansion with the set of live specifications
    // (that the user must be authorized to administer).
    let seed_ids: Vec<Id> = spec_rows
        .iter()
        .map(|r| {
            assert!(matches!(r.user_capability, Some(Capability::Admin)));
            r.live_spec_id
        })
        .collect();

    let expanded_rows = agent_sql::publications::resolve_expanded_rows(seed_ids, txn)
        .await
        .context("selecting expanded specs")?;

    Ok(expanded_rows)
}

pub fn validate_transition(
    draft: &models::Catalog,
    live: &models::Catalog,
    pub_id: Id,
    spec_rows: &[SpecRow],
) -> Vec<Error> {
    let mut errors = Vec::new();

    for spec_row @ SpecRow {
        catalog_name,
        draft_spec: _,
        draft_spec_id: _,
        draft_type,
        expect_pub_id,
        last_build_id: _,
        last_pub_id,
        live_spec: _,
        live_spec_id: _,
        live_type,
        spec_capabilities,
        user_capability,
    } in spec_rows
    {
        // Check that the user is authorized to change this spec.
        if !matches!(user_capability, Some(Capability::Admin)) {
            errors.push(Error {
                catalog_name: catalog_name.clone(),
                detail: format!("User is not authorized to create or change this catalog name"),
                ..Default::default()
            });
            // Continue because we'll otherwise produce superfluous auth errors
            // of referenced collections.
            continue;
        }
        // Check that the specification is authorized to its referants.
        let (reads_from, writes_to, _) = extract_spec_metadata(draft, spec_row);

        for source in reads_from.iter().flatten() {
            if !spec_capabilities.iter().any(|c| {
                source.starts_with(&c.object_role)
                    && matches!(
                        c.capability,
                        Capability::Read | Capability::Write | Capability::Admin
                    )
            }) {
                errors.push(Error {
                    catalog_name: catalog_name.clone(),
                    detail: format!(
                        "Specification is not read-authorized to '{source}'.\nAvailable grants are: {}",
                        serde_json::to_string_pretty(&spec_capabilities.0).unwrap(),
                    ),
                    ..Default::default()
                });
            }
        }
        for target in writes_to.iter().flatten() {
            if !spec_capabilities.iter().any(|c| {
                target.starts_with(&c.object_role)
                    && matches!(c.capability, Capability::Write | Capability::Admin)
            }) {
                errors.push(Error {
                    catalog_name: catalog_name.clone(),
                    detail: format!(
                        "Specification is not write-authorized to '{target}'.\nAvailable grants are: {}",
                        serde_json::to_string_pretty(&spec_capabilities.0).unwrap(),
                    ),
                    ..Default::default()
                });
            }
        }

        // If neither `live_type` nor `draft_type` is deleted, then they must agree.
        if matches!((live_type, draft_type), (Some(live_type), Some(draft_type)) if live_type != draft_type)
        {
            errors.push(Error {
                catalog_name: catalog_name.clone(),
                detail: format!(
                    "Draft has an incompatible type {draft_type:?} vs current type {live_type:?}. This may be caused by an attempt to create a {draft_type:?} while an existing {live_type:?} with this name exists.",
                    draft_type = draft_type.as_ref().unwrap(),
                    live_type = live_type.as_ref().unwrap(),
                ),
                ..Default::default()
            });
        }

        match expect_pub_id {
            Some(id) if id.is_zero() && *last_pub_id == pub_id => {
                // The spec is expected to be created, and it is.
            }
            Some(id) if id.is_zero() => {
                errors.push(Error {
                    catalog_name: catalog_name.clone(),
                    detail: format!(
                        "Publication expected to create this specification, but it already exists from publication {last_pub_id}"
                    ),
                    ..Default::default()
                });
            }
            Some(id) if id == last_pub_id => {
                // The spec is expected to exist at |id|, and it does.
            }
            Some(id) => {
                errors.push(Error {
                    catalog_name: catalog_name.clone(),
                    detail: format!(
                        "Draft expects a last publication ID of {id}, but it's now {last_pub_id}"
                    ),
                    ..Default::default()
                });
            }
            None => {
                // No constraint.
            }
        };

        // Verify that the live specification has not existed and then been deleted in the past.
        // TODO(johnny): remove once we introduce data plane pet-names.
        if live_type.is_none() && draft_type.is_some() && *last_pub_id != pub_id {
            errors.push(Error {
                    catalog_name: catalog_name.clone(),
                    detail: format!(
                        "A specification with this name previously existed and then was deleted. At present Flow does not allow for re-creation with this same name."
                    ),
                    ..Default::default()
                });
        }
    }

    for eob in draft
        .collections
        .iter()
        .merge_join_by(live.collections.iter(), |(n1, _), (n2, _)| n1.cmp(n2))
    {
        let (catalog_name, draft, live) = match eob.both() {
            Some(((catalog_name, draft), (_, live))) => (catalog_name, draft, live),
            None => continue,
        };

        if !draft.key.iter().eq(live.key.iter()) {
            errors.push(Error {
                catalog_name: catalog_name.to_string(),
                detail: format!(
                    "Cannot change key of an established collection from {:?} to {:?}",
                    &live.key, &draft.key,
                ),
                ..Default::default()
            });
        }

        let partitions = |projections: &BTreeMap<models::Field, models::Projection>| {
            projections
                .iter()
                .filter_map(|(field, proj)| {
                    if matches!(
                        proj,
                        models::Projection::Extended {
                            partition: true,
                            ..
                        }
                    ) {
                        Some(field.to_string())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };

        let draft_partitions = partitions(&draft.projections);
        let live_partitions = partitions(&live.projections);

        if draft_partitions != live_partitions {
            errors.push(Error {
                catalog_name: catalog_name.to_string(),
                detail: format!(
                    "Cannot change partitions of an established collection (from {live_partitions:?} to {draft_partitions:?})",
                ),
                ..Default::default()
            });
        }
    }

    errors
}

pub async fn enforce_resource_quotas(
    spec_rows: &[SpecRow],
    prev_tenants: Vec<Tenant>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<Vec<Error>> {
    let prev_tenant_usages = prev_tenants
        .into_iter()
        .map(|tenant| (tenant.name.clone(), tenant))
        .collect::<HashMap<_, _>>();
    let spec_ids = spec_rows
        .iter()
        .map(|spec_row| spec_row.live_spec_id)
        .collect();

    let errors = agent_sql::publications::find_tenant_quotas(spec_ids, txn)
        .await?
        .into_iter()
        .flat_map(|tenant| {
            let mut errs = vec![];

            let prev_tenant_tasks_usage = prev_tenant_usages
                .get(&tenant.name)
                .map(|t| t.tasks_used)
                .unwrap_or(0);
            let prev_tenant_collections_usage = prev_tenant_usages
                .get(&tenant.name)
                .map(|t| t.collections_used)
                .unwrap_or(0);

            let tasks_delta = tenant.tasks_used - prev_tenant_tasks_usage;
            let collections_delta = tenant.collections_used - prev_tenant_collections_usage;

            // We don't want to stop you from disabling tasks if you're at/over your quota
            // NOTE: technically this means that you can add new tasks even if your usage
            // exceeds your quota, so long as you remove/disable more tasks than you add.
            if tasks_delta >= 0 && tenant.tasks_used > tenant.tasks_quota {
                errs.push(format!(
                    "Request to add {} task(s) would exceed tenant '{}' quota of {}. {} are currently in use.",
                    tasks_delta,
                    tenant.name,
                    tenant.tasks_quota,
                    prev_tenant_tasks_usage
                ))
            }
            if collections_delta >= 0 && tenant.collections_used > tenant.collections_quota {
                errs.push(format!(
                    "Request to add {} collections(s) would exceed tenant '{}' quota of {}. {} are currently in use.",
                    collections_delta,
                    tenant.name,
                    tenant.collections_quota,
                    prev_tenant_collections_usage
                ))
            }
            errs
        })
        .map(|err_str| Error {
            detail: err_str,
            ..Default::default()
        })
        .collect();

    Ok(errors)
}

pub async fn apply_updates_for_row(
    catalog: &models::Catalog,
    detail: Option<&String>,
    pub_id: Id,
    spec_row: &SpecRow,
    user_id: Uuid,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<()> {
    let SpecRow {
        catalog_name,
        draft_spec,
        draft_spec_id,
        draft_type,
        expect_pub_id: _,
        last_build_id: _,
        last_pub_id: _,
        live_spec: _,
        live_spec_id,
        live_type,
        spec_capabilities: _,
        user_capability,
    } = spec_row;

    assert!(matches!(user_capability, Some(Capability::Admin)));

    agent_sql::drafts::delete_spec(*draft_spec_id, txn)
        .await
        .context("delete from draft_specs")?;

    // Clear out data-flow edges that we'll replace.
    match live_type {
        Some(live_type) => {
            agent_sql::publications::delete_stale_flow(*live_spec_id, *live_type, txn)
                .await
                .with_context(|| format!("delete stale {live_type:?} edges"))?;
        }
        None => {} // No-op.
    }

    agent_sql::publications::insert_publication_spec(
        *live_spec_id,
        pub_id,
        detail,
        draft_spec,
        draft_type,
        user_id,
        txn,
    )
    .await
    .context("insert into publication_specs")?;

    // Draft is an update of a live spec. The semantic insertion and deletion
    // cases are also an update: we previously created a `live_specs` rows for
    // the draft `catalog_name` in order to lock it. If the draft is a deletion,
    // that's marked as a DB NULL of `spec` and `spec_type`.

    let (reads_from, writes_to, image_parts) = extract_spec_metadata(catalog, spec_row);

    agent_sql::publications::update_published_live_spec(
        catalog_name,
        image_parts.as_ref().map(|p| &p.0),
        image_parts.as_ref().map(|p| &p.1),
        draft_spec,
        draft_type,
        *live_spec_id,
        pub_id,
        &reads_from,
        &writes_to,
        txn,
    )
    .await
    .context("update live_specs")?;

    agent_sql::publications::insert_live_spec_flows(
        *live_spec_id,
        draft_type,
        reads_from,
        writes_to,
        txn,
    )
    .await
    .context("insert live_spec_flow edges")?;

    Ok(())
}

fn extract_spec_metadata<'a>(
    catalog: &'a models::Catalog,
    spec_row: &'a SpecRow,
) -> (
    Option<Vec<&'a str>>,
    Option<Vec<&'a str>>,
    Option<(String, String)>,
) {
    let SpecRow {
        user_capability: _,
        spec_capabilities: _,
        catalog_name,
        draft_spec: _,
        draft_spec_id: _,
        draft_type,
        expect_pub_id: _,
        last_build_id: _,
        last_pub_id: _,
        live_spec: _,
        live_spec_id: _,
        live_type: _,
    } = spec_row;

    let mut reads_from = Vec::new();
    let mut writes_to = Vec::new();
    let mut image_parts = None;

    match *draft_type {
        Some(CatalogType::Capture) => {
            let key = models::Capture::new(catalog_name);
            let capture = catalog.captures.get(&key).unwrap();

            let models::CaptureEndpoint::Connector(config) = &capture.endpoint;
            image_parts = Some(split_tag(&config.image));

            for binding in &capture.bindings {
                writes_to.push(binding.target.as_ref());
            }
            writes_to.reserve(1);
        }
        Some(CatalogType::Collection) => {
            let key = models::Collection::new(catalog_name);
            let collection = catalog.collections.get(&key).unwrap();

            if let Some(derivation) = &collection.derive {
                for tdef in &derivation.transforms {
                    reads_from.push(tdef.source.collection().as_ref());
                }
                reads_from.reserve(1);
            }
        }
        Some(CatalogType::Materialization) => {
            let key = models::Materialization::new(catalog_name);
            let materialization = catalog.materializations.get(&key).unwrap();

            // TODO(johnny): should we disallow sqlite? or remove sqlite altogether as an endpoint?
            if let models::MaterializationEndpoint::Connector(config) = &materialization.endpoint {
                image_parts = Some(split_tag(&config.image));
            }
            for binding in &materialization.bindings {
                reads_from.push(binding.source.collection().as_ref());
            }
            reads_from.reserve(1);
        }
        Some(CatalogType::Test) => {
            let key = models::Test::new(catalog_name);
            let steps = catalog.tests.get(&key).unwrap();

            for step in steps {
                match step {
                    models::TestStep::Ingest(ingest) => writes_to.push(ingest.collection.as_ref()),
                    models::TestStep::Verify(verify) => {
                        reads_from.push(verify.collection.collection().as_ref())
                    }
                }
            }
            writes_to.reserve(1);
            reads_from.reserve(1);
        }
        None => {} // No-op.
    }

    for v in [&mut reads_from, &mut writes_to] {
        v.sort();
        v.dedup();
    }

    (
        if reads_from.capacity() != 0 {
            Some(reads_from)
        } else {
            None
        },
        if writes_to.capacity() != 0 {
            Some(writes_to)
        } else {
            None
        },
        image_parts,
    )
}

fn split_tag(image_full: &str) -> (String, String) {
    let mut image = image_full.to_string();

    if let Some(pivot) = image.find("@sha256:").or_else(|| image.find(":")) {
        let tag = image.split_off(pivot);
        (image, tag)
    } else {
        (image, String::new())
    }
}

#[cfg(test)]
mod test {

    use crate::publications::JobStatus;

    use super::super::PublishHandler;
    use agent_sql::Id;
    use reqwest::Url;
    use serde::Deserialize;
    use serde_json::Value;
    use sqlx::{Connection, Postgres, Transaction};

    // Squelch warnings about struct fields never being read.
    // They actually are read by insta when snapshotting.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    struct LiveSpec {
        catalog_name: String,
        connector_image_name: Option<String>,
        connector_image_tag: Option<String>,
        reads_from: Option<Vec<String>>,
        writes_to: Option<Vec<String>>,
        spec: Option<Value>,
        spec_type: Option<String>,
    }
    #[allow(dead_code)]
    #[derive(Debug)]
    struct ScenarioResult {
        draft_id: Id,
        status: JobStatus,
        errors: Vec<String>,
        live_specs: Vec<LiveSpec>,
    }

    const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

    async fn execute_publications(txn: &mut Transaction<'_, Postgres>) -> Vec<ScenarioResult> {
        let bs_url: Url = "http://example.com".parse().unwrap();

        let (logs_tx, mut logs_rx) = tokio::sync::mpsc::channel(8192);

        // Just in case anything gets through
        logs_rx.close();

        let mut handler = PublishHandler::new("", &bs_url, &bs_url, "", &bs_url, &logs_tx);

        let mut results: Vec<ScenarioResult> = vec![];

        while let Some(row) = agent_sql::publications::dequeue(&mut *txn).await.unwrap() {
            let row_draft_id = row.draft_id.clone();
            let (pub_id, status) = handler.process(row, &mut *txn, true).await.unwrap();

            agent_sql::publications::resolve(pub_id, &status, &mut *txn)
                .await
                .unwrap();

            match status {
                JobStatus::Success => {
                    let specs = sqlx::query_as!(
                        LiveSpec,
                        r#"
                        select catalog_name as "catalog_name!",
                               connector_image_name,
                               connector_image_tag,
                               reads_from,
                               writes_to,
                               spec,
                               spec_type as "spec_type: String"
                        from live_specs
                        where live_specs.last_pub_id = $1::flowid;"#,
                        pub_id as Id
                    )
                    .fetch_all(&mut *txn)
                    .await
                    .unwrap();

                    results.push(ScenarioResult {
                        draft_id: row_draft_id,
                        status: JobStatus::Success,
                        errors: vec![],
                        live_specs: specs,
                    })
                }
                _ => {
                    let errors = sqlx::query!(
                        r#"
                select draft_id as "draft_id: Id", scope, detail
                from draft_errors
                where draft_errors.draft_id = $1::flowid;"#,
                        row_draft_id as Id
                    )
                    .fetch_all(&mut *txn)
                    .await
                    .unwrap();

                    let formatted_errors: Vec<String> =
                        errors.into_iter().map(|e| e.detail).collect();

                    results.push(ScenarioResult {
                        draft_id: row_draft_id,
                        status: status.clone(),
                        errors: formatted_errors,
                        live_specs: vec![],
                    });
                }
            };
        }

        results
    }

    #[tokio::test]
    #[serial_test::parallel]
    async fn test_happy_path() {
        let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
            .await
            .unwrap();
        let mut txn = conn.begin().await.unwrap();

        sqlx::query(include_str!("test_resources/happy_path.sql"))
            .execute(&mut txn)
            .await
            .unwrap();

        let results = execute_publications(&mut txn).await;

        insta::assert_debug_snapshot!(results, @r###"
        [
            ScenarioResult {
                draft_id: 1110000000000000,
                status: Success,
                errors: [],
                live_specs: [
                    LiveSpec {
                        catalog_name: "usageB/DerivationA",
                        connector_image_name: None,
                        connector_image_tag: None,
                        reads_from: Some(
                            [
                                "usageB/CollectionA",
                            ],
                        ),
                        writes_to: None,
                        spec: Some(
                            Object {
                                "derive": Object {
                                    "transforms": Array [
                                        Object {
                                            "name": String("my-name"),
                                            "source": String("usageB/CollectionA"),
                                        },
                                    ],
                                    "using": Object {
                                        "sqlite": Object {},
                                    },
                                },
                                "key": Array [
                                    String("foo"),
                                ],
                                "schema": Object {},
                            },
                        ),
                        spec_type: Some(
                            "collection",
                        ),
                    },
                ],
            },
        ]
        "###);
    }

    #[tokio::test]
    #[serial_test::parallel]
    async fn test_forbidden_connector() {
        let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
            .await
            .unwrap();
        let mut txn = conn.begin().await.unwrap();

        sqlx::query(r#"
            with p1 as (
              insert into auth.users (id) values
              ('43a18a3e-5a59-11ed-9b6a-0242ac120002')
            ),
            p2 as (
              insert into drafts (id, user_id) values
              ('1110000000000000', '43a18a3e-5a59-11ed-9b6a-0242ac120002')
            ),
            p3 as (
              insert into draft_specs (id, draft_id, catalog_name, spec, spec_type) values
              ('1111000000000000', '1110000000000000', 'usageB/CaptureC', '{
                  "bindings": [{"target": "usageB/CaptureC", "resource": {"binding": "foo", "syncMode": "incremental"}}],
                  "endpoint": {"connector": {"image": "forbidden_connector", "config": {}}}
              }'::json, 'capture')
            ),
            p4 as (
              insert into publications (id, job_status, user_id, draft_id) values
              ('1111100000000000', '{"type": "queued"}'::json, '43a18a3e-5a59-11ed-9b6a-0242ac120002', '1110000000000000')
            ),
            p5 as (
              insert into role_grants (subject_role, object_role, capability) values
              ('usageB/', 'usageB/', 'admin')
            ),
            p6 as (
              insert into user_grants (user_id, object_role, capability) values
              ('43a18a3e-5a59-11ed-9b6a-0242ac120002', 'usageB/', 'admin')
            )
            select 1;
        "#).execute(&mut txn).await.unwrap();

        let results = execute_publications(&mut txn).await;

        insta::assert_debug_snapshot!(results, @r#"
        [
            ScenarioResult {
                draft_id: 1110000000000000,
                status: BuildFailed,
                errors: [
                    "Forbidden connector image 'forbidden_connector'",
                ],
                live_specs: [],
            },
        ]
        "#);
    }

    #[tokio::test]
    #[serial_test::parallel]
    async fn test_allowed_connector() {
        let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
            .await
            .unwrap();
        let mut txn = conn.begin().await.unwrap();

        sqlx::query(r#"
            with p1 as (
              insert into auth.users (id) values
              ('43a18a3e-5a59-11ed-9b6a-0242ac120002')
            ),
            p2 as (
              insert into drafts (id, user_id) values
              ('1110000000000000', '43a18a3e-5a59-11ed-9b6a-0242ac120002')
            ),
            p3 as (
              insert into draft_specs (id, draft_id, catalog_name, spec, spec_type) values
              ('1111000000000000', '1110000000000000', 'usageB/CaptureC', '{
                  "bindings": [{"target": "usageB/CaptureC", "resource": {"binding": "foo", "syncMode": "incremental"}}],
                  "endpoint": {"connector": {"image": "allowed_connector", "config": {}}}
              }'::json, 'capture')
            ),
            p4 as (
              insert into publications (id, job_status, user_id, draft_id) values
              ('1111100000000000', '{"type": "queued"}'::json, '43a18a3e-5a59-11ed-9b6a-0242ac120002', '1110000000000000')
            ),
            p5 as (
              insert into role_grants (subject_role, object_role, capability) values
              ('usageB/', 'usageB/', 'admin')
            ),
            p6 as (
              insert into user_grants (user_id, object_role, capability) values
              ('43a18a3e-5a59-11ed-9b6a-0242ac120002', 'usageB/', 'admin')
            ),
            p7 as (
                insert into connectors (external_url, image_name, title, short_description, logo_url) values
                    ('http://example.com', 'allowed_connector', '{"en-US": "foo"}'::json, '{"en-US": "foo"}'::json, '{"en-US": "foo"}'::json)
            )
            select 1;
            "#).execute(&mut txn).await.unwrap();

        let results = execute_publications(&mut txn).await;

        insta::assert_debug_snapshot!(results, @r#"
        [
            ScenarioResult {
                draft_id: 1110000000000000,
                status: Success,
                errors: [],
                live_specs: [
                    LiveSpec {
                        catalog_name: "usageB/CaptureC",
                        connector_image_name: Some(
                            "allowed_connector",
                        ),
                        connector_image_tag: Some(
                            "",
                        ),
                        reads_from: None,
                        writes_to: Some(
                            [
                                "usageB/CaptureC",
                            ],
                        ),
                        spec: Some(
                            Object {
                                "bindings": Array [
                                    Object {
                                        "resource": Object {
                                            "binding": String("foo"),
                                            "syncMode": String("incremental"),
                                        },
                                        "target": String("usageB/CaptureC"),
                                    },
                                ],
                                "endpoint": Object {
                                    "connector": Object {
                                        "config": Object {},
                                        "image": String("allowed_connector"),
                                    },
                                },
                            },
                        ),
                        spec_type: Some(
                            "capture",
                        ),
                    },
                ],
            },
        ]
        "#);
    }

    #[tokio::test]
    #[serial_test::parallel]
    async fn test_quota_single_task() {
        let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
            .await
            .unwrap();
        let mut txn = conn.begin().await.unwrap();

        sqlx::query(r#"
            with p1 as (
                insert into live_specs (id, catalog_name, spec, spec_type, last_build_id, last_pub_id) values
                ('1000000000000000', 'usageB/CollectionA', '{}'::json, 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
                ('1100000000000000', 'usageB/CollectionB', '{}'::json, 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
                ('2000000000000000', 'usageB/CaptureA', '{"endpoint": {},"bindings": []}'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
                ('3000000000000000', 'usageB/CaptureB', '{"endpoint": {},"bindings": []}'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
                ('4000000000000000', 'usageB/CaptureDisabled', '{"shards": {"disable": true}}'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb')
              ),
              p2 as (
                  insert into tenants (tenant, tasks_quota, collections_quota) values
                  ('usageB/', 2, 2)
              ),
              p3 as (
                insert into auth.users (id) values
                ('43a18a3e-5a59-11ed-9b6a-0242ac120002')
              ),
              p4 as (
                insert into drafts (id, user_id) values
                ('1110000000000000', '43a18a3e-5a59-11ed-9b6a-0242ac120002')
              ),
              p5 as (
                insert into draft_specs (id, draft_id, catalog_name, spec, spec_type) values
                ('1111000000000000', '1110000000000000', 'usageB/CaptureC', '{
                    "bindings": [{"target": "usageB/CaptureC", "resource": {"binding": "foo", "syncMode": "incremental"}}],
                    "endpoint": {"connector": {"image": "foo", "config": {}}}
                }'::json, 'capture')
              ),
              p6 as (
                insert into publications (id, job_status, user_id, draft_id) values
                ('1111100000000000', '{"type": "queued"}'::json, '43a18a3e-5a59-11ed-9b6a-0242ac120002', '1110000000000000')
              ),
              p7 as (
                insert into role_grants (subject_role, object_role, capability) values
                ('usageB/', 'usageB/', 'admin')
              ),
              p8 as (
                insert into user_grants (user_id, object_role, capability) values
                ('43a18a3e-5a59-11ed-9b6a-0242ac120002', 'usageB/', 'admin')
              )
              select 1;
              "#).execute(&mut txn).await.unwrap();

        let results = execute_publications(&mut txn).await;

        insta::assert_debug_snapshot!(results, @r#"
        [
            ScenarioResult {
                draft_id: 1110000000000000,
                status: BuildFailed,
                errors: [
                    "Request to add 1 task(s) would exceed tenant 'usageB/' quota of 2. 2 are currently in use.",
                ],
                live_specs: [],
            },
        ]
        "#);
    }

    #[tokio::test]
    #[serial_test::parallel]
    async fn test_quota_derivations() {
        let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
            .await
            .unwrap();
        let mut txn = conn.begin().await.unwrap();

        sqlx::query(r#"
            with p1 as (
                insert into live_specs (id, catalog_name, spec, spec_type, last_build_id, last_pub_id) values
                ('1000000000000000', 'usageB/CollectionA', '{}'::json, 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
                ('1100000000000000', 'usageB/CollectionB', '{}'::json, 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
                ('2000000000000000', 'usageB/CaptureA', '{"endpoint": {},"bindings": []}'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
                ('3000000000000000', 'usageB/CaptureB', '{"endpoint": {},"bindings": []}'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
                ('4000000000000000', 'usageB/CaptureDisabled', '{"shards": {"disable": true}}'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb')
              ),
              p2 as (
                  insert into tenants (tenant, tasks_quota, collections_quota) values
                  ('usageB/', 2, 2)
              ),
              p3 as (
                insert into auth.users (id) values
                ('43a18a3e-5a59-11ed-9b6a-0242ac120002')
              ),
              p4 as (
                insert into drafts (id, user_id) values
                ('1120000000000000', '43a18a3e-5a59-11ed-9b6a-0242ac120002')
              ),
              p5 as (
                insert into draft_specs (id, draft_id, catalog_name, spec, spec_type) values
                ('1112000000000000', '1120000000000000', 'usageB/DerivationA', '{"schema": {}, "key": ["foo"], "derivation": {"transform":{"key": {"source": {"name": "usageB/CollectionA"}}}}}'::json, 'collection')
              ),
              p6 as (
                insert into publications (id, job_status, user_id, draft_id) values
                ('1111200000000000', '{"type": "queued"}'::json, '43a18a3e-5a59-11ed-9b6a-0242ac120002', '1120000000000000')
              ),
              p7 as (
                insert into role_grants (subject_role, object_role, capability) values
                ('usageB/', 'usageB/', 'admin')
              ),
              p8 as (
                insert into user_grants (user_id, object_role, capability) values
                ('43a18a3e-5a59-11ed-9b6a-0242ac120002', 'usageB/', 'admin')
              )
              select 1;
              "#).execute(&mut txn).await.unwrap();

        let results = execute_publications(&mut txn).await;

        insta::assert_debug_snapshot!(results, @r#"
        [
            ScenarioResult {
                draft_id: 1120000000000000,
                status: BuildFailed,
                errors: [
                    "Request to add 1 task(s) would exceed tenant 'usageB/' quota of 2. 2 are currently in use.",
                    "Request to add 1 collections(s) would exceed tenant 'usageB/' quota of 2. 2 are currently in use.",
                ],
                live_specs: [],
            },
        ]
        "#);
    }

    // Testing that we can disable tasks to reduce usage when at quota
    #[tokio::test]
    #[serial_test::parallel]
    async fn test_disable_when_over_quota() {
        let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
            .await
            .unwrap();
        let mut txn = conn.begin().await.unwrap();

        sqlx::query(r#"
            with p1 as (
                insert into live_specs (id, catalog_name, spec, spec_type, last_build_id, last_pub_id) values
                ('a100000000000000', 'usageC/CollectionA', '{"schema": {}, "key": ["foo"]}'::json, 'collection', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
                ('a200000000000000', 'usageC/CaptureA', '{
                    "bindings": [{"target": "usageC/CollectionA", "resource": {"binding": "foo", "syncMode": "incremental"}}],
                    "endpoint": {"connector": {"image": "foo", "config": {}}}
                }'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb'),
                ('a300000000000000', 'usageC/CaptureB', '{
                    "bindings": [{"target": "usageC/CollectionA", "resource": {"binding": "foo", "syncMode": "incremental"}}],
                    "endpoint": {"connector": {"image": "foo", "config": {}}}
                }'::json, 'capture', 'bbbbbbbbbbbbbbbb', 'bbbbbbbbbbbbbbbb')
              ),
              p2 as (
                  insert into tenants (tenant, tasks_quota, collections_quota) values
                  ('usageC/', 1, 1)
              ),
              p3 as (
                insert into auth.users (id) values
                ('43a18a3e-5a59-11ed-9b6a-0242ac120002')
              ),
              p4 as (
                insert into drafts (id, user_id) values
                ('1130000000000000', '43a18a3e-5a59-11ed-9b6a-0242ac120002')
              ),
              p5 as (
                insert into draft_specs (id, draft_id, catalog_name, spec, spec_type) values
                ('1113000000000000', '1130000000000000', 'usageC/CaptureA', '{
                    "bindings": [{"target": "usageC/CollectionA", "resource": {"binding": "foo", "syncMode": "incremental"}}],
                    "endpoint": {"connector": {"image": "foo", "config": {}}},
                    "shards": {"disable": true}
                }'::json, 'capture')
              ),
              p6 as (
                insert into publications (id, job_status, user_id, draft_id) values
                ('1111300000000000', '{"type": "queued"}'::json, '43a18a3e-5a59-11ed-9b6a-0242ac120002', '1130000000000000')
              ),
              p7 as (
                insert into role_grants (subject_role, object_role, capability) values
                ('usageC/', 'usageC/', 'admin')
              ),
              p8 as (
                insert into user_grants (user_id, object_role, capability) values
                ('43a18a3e-5a59-11ed-9b6a-0242ac120002', 'usageC/', 'admin')
              ),
              p9 as (
                insert into connectors (external_url, image_name, title, short_description, logo_url) values
                    ('http://example.com', 'foo', '{"en-US": "foo"}'::json, '{"en-US": "foo"}'::json, '{"en-US": "foo"}'::json)
            )
              select 1;
              "#).execute(&mut txn).await.unwrap();

        let results = execute_publications(&mut txn).await;

        insta::assert_debug_snapshot!(results, @r#"
        [
            ScenarioResult {
                draft_id: 1130000000000000,
                status: Success,
                errors: [],
                live_specs: [
                    LiveSpec {
                        catalog_name: "usageC/CaptureA",
                        connector_image_name: Some(
                            "foo",
                        ),
                        connector_image_tag: Some(
                            "",
                        ),
                        reads_from: None,
                        writes_to: Some(
                            [
                                "usageC/CollectionA",
                            ],
                        ),
                        spec: Some(
                            Object {
                                "bindings": Array [
                                    Object {
                                        "resource": Object {
                                            "binding": String("foo"),
                                            "syncMode": String("incremental"),
                                        },
                                        "target": String("usageC/CollectionA"),
                                    },
                                ],
                                "endpoint": Object {
                                    "connector": Object {
                                        "config": Object {},
                                        "image": String("foo"),
                                    },
                                },
                                "shards": Object {
                                    "disable": Bool(true),
                                },
                            },
                        ),
                        spec_type: Some(
                            "capture",
                        ),
                    },
                ],
            },
        ]
        "#);
    }
}
