use super::Error;
use crate::Id;

use anyhow::Context;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sqlx::types::{Json, Uuid};
use std::collections::BTreeMap;

#[derive(Debug, Copy, Clone, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "catalog_spec_type")]
#[sqlx(rename_all = "lowercase")]
pub enum CatalogType {
    Capture,
    Collection,
    Materialization,
    Test,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "grant_capability")]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "camelCase")]
pub enum Capability {
    Read,
    Write,
    Admin,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RoleGrant {
    pub subject_role: String,
    pub object_role: String,
    pub capability: Capability,
}

#[derive(Debug)]
pub struct SpecRow {
    // Name of the specification.
    pub catalog_name: String,
    // Specification which will be applied by this draft.
    pub draft_spec: Json<Box<RawValue>>,
    // ID of the draft specification.
    pub draft_spec_id: Id,
    // Spec type of this draft.
    // We validate and require that this equals `live_type`.
    pub draft_type: Option<CatalogType>,
    // Optional expected value for `last_pub_id` of the live spec.
    // A special all-zero value means "this should be a creation".
    pub expect_pub_id: Option<Id>,
    // Last publication ID of the live spec.
    // If the spec is being created, this is the current publication ID.
    pub last_pub_id: Id,
    // Current live specification which will be replaced by this draft.
    pub live_spec: Json<Box<RawValue>>,
    // ID of the live specification.
    pub live_spec_id: Id,
    // Spec type of the live specification.
    pub live_type: Option<CatalogType>,
    // Capabilities of the specification with respect to other roles.
    pub spec_capabilities: Json<Vec<RoleGrant>>,
    // User's capability to the specification `catalog_name`.
    pub user_capability: Option<Capability>,
}

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
    let rows = sqlx::query!(
        r#"
        insert into live_specs(catalog_name, spec, spec_type, last_pub_id) (
            select catalog_name, 'null', null, $2
            from draft_specs
            where draft_specs.draft_id = $1
            for update of draft_specs
        ) on conflict (catalog_name) do nothing
        "#,
        draft_id as Id,
        pub_id as Id,
    )
    .execute(&mut *txn)
    .await
    .context("inserting new live_specs")?;

    tracing::debug!(rows = %rows.rows_affected(), "inserted new live_specs");

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
    let mut spec_rows = sqlx::query_as!(
        SpecRow,
        r#"
        select
            draft_specs.catalog_name,
            draft_specs.expect_pub_id as "expect_pub_id: Id",
            draft_specs.spec as "draft_spec: Json<Box<RawValue>>",
            draft_specs.id as "draft_spec_id: Id",
            draft_specs.spec_type as "draft_type: CatalogType",
            live_specs.last_pub_id as "last_pub_id: Id",
            live_specs.spec as "live_spec: Json<Box<RawValue>>",
            live_specs.id as "live_spec_id: Id",
            live_specs.spec_type as "live_type: CatalogType",
            coalesce(
                (select json_agg(row_to_json(role_grants))
                from role_grants
                where starts_with(draft_specs.catalog_name, subject_role)),
                '[]'
            ) as "spec_capabilities!: Json<Vec<RoleGrant>>",
            (
                select max(capability) from internal.user_roles($2) r
                where starts_with(draft_specs.catalog_name, r.role_prefix)
            ) as "user_capability: Capability"
        from draft_specs
        join live_specs
            on draft_specs.catalog_name = live_specs.catalog_name
        where draft_specs.draft_id = $1
        for update of draft_specs, live_specs;
        "#,
        draft_id as Id,
        user_id,
    )
    .fetch_all(&mut *txn)
    .await
    .context("selecting joined draft & live specs")?;

    // The query may return live specifications that the user is not
    // authorized to know anything about. Tweak such rows to appear
    // as if the spec is being created.
    for row in &mut spec_rows {
        if row.user_capability.is_none() {
            row.last_pub_id = pub_id;
            row.live_spec = Json(RawValue::from_string("null".to_string()).unwrap());
            row.live_spec_id = row.draft_spec_id;
            row.live_type = None;
            row.spec_capabilities = Json(Vec::new());
        }
    }

    Ok(spec_rows)
}

#[derive(Debug)]
pub struct ExpandedRow {
    // Name of the specification.
    pub catalog_name: String,
    // Current live specification of this expansion.
    // It won't be changed by this publication.
    pub live_spec: Json<Box<RawValue>>,
    // ID of the expanded live specification.
    pub live_spec_id: Id,
    // Spec type of the live specification.
    pub live_type: CatalogType,
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

    let expanded_rows = sqlx::query_as!(
        ExpandedRow,
        r#"
        -- Perform a graph traversal which expands the seed set of
        -- specifications. Directly-adjacent captures and materializations
        -- are resolved, as is the full connected component of tests and
        -- derivations.
        with recursive expanded(id, seed) as (
            select id, true from unnest($1::flowid[]) as id
          union
            select
                case when expanded.id = e.source_id then e.target_id else e.source_id end,
                false
            from expanded join live_spec_flows as e
            on expanded.id = e.source_id or expanded.id = e.target_id
            where expanded.seed or e.flow_type in ('collection', 'test')
        )
        -- Join the expanded IDs with live_specs.
        select
            id as "live_spec_id: Id",
            catalog_name,
            spec_type as "live_type!: CatalogType",
            spec as "live_spec: Json<Box<RawValue>>"
        from live_specs natural join expanded
        -- Strip deleted specs which are still reach-able through a dataflow edge.
        where spec_type is not null
        -- Strip specs which are already part of the seed set.
        group by id having not bool_or(seed);
        "#,
        seed_ids as Vec<Id>,
    )
    .fetch_all(&mut *txn)
    .await
    .context("selecting expanded specs")?;

    Ok(expanded_rows)
}

pub async fn insert_errors(
    draft_id: Id,
    errors: Vec<Error>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<()> {
    for err in errors {
        sqlx::query!(
            r#"insert into draft_errors (
              draft_id,
              scope,
              detail
            ) values ($1, $2, $3)
            "#,
            draft_id as Id,
            err.scope.unwrap_or(err.catalog_name),
            err.detail,
        )
        .execute(&mut *txn)
        .await
        .context("inserting error")?;
    }
    Ok(())
}

pub fn extend_catalog<'a>(
    catalog: &mut models::Catalog,
    it: impl Iterator<Item = (CatalogType, &'a str, &'a RawValue)>,
) -> Vec<Error> {
    let mut errors = Vec::new();

    for (catalog_type, catalog_name, spec) in it {
        let mut on_err = |detail| {
            errors.push(Error {
                catalog_name: catalog_name.to_string(),
                detail,
                ..Error::default()
            });
        };

        match catalog_type {
            CatalogType::Collection => match serde_json::from_str(spec.get()) {
                Ok(spec) => {
                    catalog
                        .collections
                        .insert(models::Collection::new(catalog_name), spec);
                }
                Err(err) => on_err(format!("invalid collection {catalog_name}: {err:?}")),
            },
            CatalogType::Capture => match serde_json::from_str(spec.get()) {
                Ok(spec) => {
                    catalog
                        .captures
                        .insert(models::Capture::new(catalog_name), spec);
                }
                Err(err) => on_err(format!("invalid capture {catalog_name}: {err:?}")),
            },
            CatalogType::Materialization => match serde_json::from_str(spec.get()) {
                Ok(spec) => {
                    catalog
                        .materializations
                        .insert(models::Materialization::new(catalog_name), spec);
                }
                Err(err) => on_err(format!("invalid materialization {catalog_name}: {err:?}")),
            },
            CatalogType::Test => match serde_json::from_str(spec.get()) {
                Ok(spec) => {
                    catalog.tests.insert(models::Test::new(catalog_name), spec);
                }
                Err(err) => on_err(format!("invalid test {catalog_name}: {err:?}")),
            },
        }
    }

    errors
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
                        serde_json::to_string_pretty(&spec_capabilities).unwrap(),
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
                        serde_json::to_string_pretty(&spec_capabilities).unwrap(),
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
                    "Draft has an incompatible {draft_type:?} vs current {live_type:?}",
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
        last_pub_id: _,
        live_spec: _,
        live_spec_id,
        live_type,
        spec_capabilities: _,
        user_capability,
    } = spec_row;

    assert!(matches!(user_capability, Some(Capability::Admin)));

    sqlx::query!(
        r#"delete from draft_specs where id = $1 returning 1 as "must_exist";"#,
        *draft_spec_id as Id,
    )
    .fetch_one(&mut *txn)
    .await
    .context("delete from draft_specs")?;

    // Clear out data-flow edges that we'll replace.
    match live_type {
        Some(CatalogType::Capture) => {
            sqlx::query!(
                "delete from live_spec_flows where source_id = $1 and flow_type = 'capture'",
                *live_spec_id as Id,
            )
            .execute(&mut *txn)
            .await
            .context("delete stale capture edges")?;
        }
        Some(CatalogType::Collection) => {
            sqlx::query!(
                "delete from live_spec_flows where target_id = $1 and flow_type = 'collection'",
                *live_spec_id as Id,
            )
            .execute(&mut *txn)
            .await
            .context("delete stale derivation edges")?;
        }
        Some(CatalogType::Materialization) => {
            sqlx::query!(
                "delete from live_spec_flows where target_id = $1 and flow_type = 'materialization'",
                *live_spec_id as Id,
            )
            .execute(&mut *txn)
            .await
            .context("delete stale materialization edges")?;
        }
        Some(CatalogType::Test) => {
            sqlx::query!(
                "delete from live_spec_flows where (source_id = $1 or target_id = $1) and flow_type = 'test'",
                *live_spec_id as Id,
            )
            .execute(&mut *txn)
            .await
            .context("delete stale test edges")?;
        }
        None => {} // No-op.
    }

    sqlx::query!(
        r#"insert into publication_specs (
            live_spec_id,
            pub_id,
            detail,
            published_at,
            spec,
            spec_type,
            user_id
        ) values ($1, $2, $3, DEFAULT, $4, $5, $6);
        "#,
        *live_spec_id as Id,
        pub_id as Id,
        detail as Option<&String>,
        draft_spec as &Json<Box<RawValue>>,
        draft_type as &Option<CatalogType>,
        user_id as Uuid,
    )
    .execute(&mut *txn)
    .await
    .context("insert into publication_specs")?;

    // Draft is an update of a live spec. The semantic insertion and deletion
    // cases are also an update: we previously created a `live_specs` rows for
    // the draft `catalog_name` in order to lock it. If the draft is a deletion,
    // that's marked as a DB NULL `spec_type` with a JSON "null" `spec`.

    let (reads_from, writes_to, image_parts) = extract_spec_metadata(catalog, spec_row);

    sqlx::query!(
        r#"
        update live_specs set
            connector_image_name = $2,
            connector_image_tag = $3,
            last_pub_id = $4,
            reads_from = $5,
            spec = $6,
            spec_type = $7,
            updated_at = clock_timestamp(),
            writes_to = $8
        where catalog_name = $1
        returning 1 as "must_exist";
        "#,
        catalog_name,
        image_parts.as_ref().map(|p| &p.0),
        image_parts.as_ref().map(|p| &p.1),
        pub_id as Id,
        &reads_from as &Option<Vec<&str>>,
        draft_spec as &Json<Box<RawValue>>,
        draft_type as &Option<CatalogType>,
        &writes_to as &Option<Vec<&str>>,
    )
    .fetch_one(&mut *txn)
    .await
    .context("update live_specs")?;

    sqlx::query!(
        r#"
        insert into live_spec_flows (source_id, target_id, flow_type)
            select live_specs.id, $1, $2::catalog_spec_type
            from unnest($3::text[]) as n join live_specs on catalog_name = n
        union
            select $1, live_specs.id, $2
            from unnest($4::text[]) as n join live_specs on catalog_name = n;
        "#,
        *live_spec_id as Id,
        draft_type as &Option<CatalogType>,
        reads_from as Option<Vec<&str>>,
        writes_to as Option<Vec<&str>>,
    )
    .execute(&mut *txn)
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

            if let models::CaptureEndpoint::Connector(config) = &capture.endpoint {
                image_parts = Some(split_tag(&config.image));
            }
            for binding in &capture.bindings {
                writes_to.push(binding.target.as_ref());
            }
            writes_to.reserve(1);
        }
        Some(CatalogType::Collection) => {
            let key = models::Collection::new(catalog_name);
            let collection = catalog.collections.get(&key).unwrap();

            if let Some(derivation) = &collection.derivation {
                for (_, tdef) in &derivation.transform {
                    reads_from.push(tdef.source.name.as_ref());
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
                reads_from.push(binding.source.as_ref());
            }
            reads_from.reserve(1);
        }
        Some(CatalogType::Test) => {
            let key = models::Test::new(catalog_name);
            let steps = catalog.tests.get(&key).unwrap();

            for step in steps {
                match step {
                    models::TestStep::Ingest(ingest) => writes_to.push(ingest.collection.as_ref()),
                    models::TestStep::Verify(verify) => reads_from.push(verify.collection.as_ref()),
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
