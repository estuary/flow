use super::Error;
use crate::Id;

use anyhow::Context;
use itertools::Itertools;
use serde_json::value::RawValue;
use sqlx::types::Json;
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

#[derive(Debug)]
pub struct SpecRow {
    pub catalog_name: String,
    pub draft_spec: Json<Box<RawValue>>,
    pub draft_type: CatalogType,
    pub expect_pub_id: Option<Id>,
    pub last_pub_id: Id,
    pub live_spec: Json<Box<RawValue>>,
    pub live_spec_id: Id,
    pub live_type: CatalogType,
}

// resolve_specifications returns the definitive set of specifications which
// are changing in this publication. It obtains sufficient locks to ensure
// that raced publications to returned specifications are serialized with
// this publication.
pub async fn resolve_specifications(
    draft_id: Id,
    pub_id: Id,
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
    // "on conflict .. do nothing" semantics, and we'll next lock the new row.
    //
    // See: https://www.postgresql.org/docs/14/transaction-iso.html#XACT-READ-COMMITTED
    let rows = sqlx::query!(
        r#"
        insert into live_specs(catalog_name, spec_type, spec, last_pub_id) (
            select
                catalog_name,
                spec_type,
                'null',
                $2
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
    let spec_rows = sqlx::query_as!(
        SpecRow,
        r#"
        select
            draft_specs.catalog_name,
            draft_specs.expect_pub_id as "expect_pub_id: Id",
            draft_specs.spec as "draft_spec: Json<Box<RawValue>>",
            draft_specs.spec_type as "draft_type: CatalogType",
            live_specs.id as "live_spec_id: Id",
            live_specs.last_pub_id as "last_pub_id: Id",
            live_specs.spec as "live_spec: Json<Box<RawValue>>",
            live_specs.spec_type as "live_type: CatalogType"
        from draft_specs
        join live_specs
            on draft_specs.catalog_name = live_specs.catalog_name
        where draft_specs.draft_id = $1
        for update of draft_specs, live_specs;
        "#,
        draft_id as Id,
    )
    .fetch_all(&mut *txn)
    .await
    .context("selecting joined draft & live specs")?;

    Ok(spec_rows)
}

#[derive(Debug)]
pub struct ExpandedRow {
    pub catalog_name: String,
    pub live_spec: Json<Box<RawValue>>,
    pub live_spec_id: Id,
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
    let live_ids: Vec<Id> = spec_rows.iter().map(|r| r.live_spec_id).collect();

    let expanded_rows = sqlx::query_as!(
        ExpandedRow,
        r#"
        with recursive clique(id, seed) as (
            select id, true from unnest($1::flowid[]) as id
          union
            select
                case when clique.id = e.source_id then e.target_id else e.source_id end,
                false
            from clique join live_spec_flows as e
            on clique.id = e.source_id or clique.id = e.target_id
            where clique.seed or e.flow_type in ('collection', 'test')
        )
        select
            id as "live_spec_id: Id",
            catalog_name,
            spec_type as "live_type: CatalogType",
            spec as "live_spec: Json<Box<RawValue>>"
        from live_specs natural join clique
        group by id
        having not bool_or(seed);
        "#,
        live_ids as Vec<Id>,
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
    pub_id: Id,
    live: &models::Catalog,
    draft: &models::Catalog,
    spec_rows: &[SpecRow],
) -> Vec<Error> {
    let mut errors = Vec::new();

    for SpecRow {
        catalog_name,
        draft_spec,
        draft_type,
        expect_pub_id,
        last_pub_id,
        live_spec,
        live_spec_id: _,
        live_type,
    } in spec_rows
    {
        if draft_type != live_type {
            errors.push(Error {
                catalog_name: catalog_name.clone(),
                detail: format!(
                    "draft has an incompatible {draft_type:?} vs current {live_type:?}"
                ),
                ..Default::default()
            });
        }

        if draft_spec.get() == "null" && live_spec.get() == "null" {
            errors.push(Error {
                catalog_name: catalog_name.clone(),
                detail: format!(
                    "draft marks this specification for deletion, but it doesn't exist"
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
                        "publication expects to create this specification, but it exists from publication {last_pub_id}"
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
                        "draft expects a last publication ID of {id}, but it's now {last_pub_id}"
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
                    "cannot change key of an established collection from {:?} to {:?}",
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
                    "cannot change partitions of an established collection (from {live_partitions:?} to {draft_partitions:?})",
                ),
                ..Default::default()
            });
        }
    }

    errors
}

pub async fn apply_updates_for_row(
    pub_id: Id,
    draft_id: Id,
    catalog: &models::Catalog,
    spec_row: &SpecRow,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<()> {
    let SpecRow {
        catalog_name,
        draft_spec,
        draft_type,
        expect_pub_id: _,
        last_pub_id: _,
        live_spec,
        live_spec_id,
        live_type,
    } = spec_row;

    sqlx::query!(
        r#"delete from draft_specs where draft_id = $1 and catalog_name = $2
            returning 1 as "must_exist";
        "#,
        draft_id as Id,
        &catalog_name as &str,
    )
    .fetch_one(&mut *txn)
    .await
    .context("delete from draft_specs")?;

    // Clear out data-flow edges that we'll replace.
    match live_type {
        CatalogType::Capture => {
            sqlx::query!(
                "delete from live_spec_flows where source_id = $1 and flow_type = 'capture'",
                *live_spec_id as Id,
            )
            .execute(&mut *txn)
            .await
            .context("delete stale capture edges")?;
        }
        CatalogType::Collection => {
            sqlx::query!(
                "delete from live_spec_flows where target_id = $1 and flow_type = 'collection'",
                *live_spec_id as Id,
            )
            .execute(&mut *txn)
            .await
            .context("delete stale derivation edges")?;
        }
        CatalogType::Materialization => {
            sqlx::query!(
                "delete from live_spec_flows where target_id = $1 and flow_type = 'materialization'",
                *live_spec_id as Id,
            )
            .execute(&mut *txn)
            .await
            .context("delete stale materialization edges")?;
        }
        CatalogType::Test => {
            sqlx::query!(
                "delete from live_spec_flows where (source_id = $1 or target_id = $1) and flow_type = 'test'",
                *live_spec_id as Id,
            )
            .execute(&mut *txn)
            .await
            .context("delete stale test edges")?;
        }
    }

    sqlx::query!(
        r#"insert into publication_specs (
            pub_id,
            catalog_name,
            spec_type,
            spec_before,
            spec_after
        ) values ($1, $2, $3, $4, $5);
        "#,
        pub_id as Id,
        &catalog_name as &str,
        draft_type as &CatalogType,
        live_spec as &Json<Box<RawValue>>,
        draft_spec as &Json<Box<RawValue>>,
    )
    .execute(&mut *txn)
    .await
    .context("insert into publication_specs")?;

    if draft_spec.get() == "null" {
        // Draft is a deletion of a live spec.
        sqlx::query!(
            r#"delete from live_specs where id = $1
                returning 1 as "must_exist";
            "#,
            *live_spec_id as Id,
        )
        .fetch_one(&mut *txn)
        .await
        .context("delete from live_specs")?;

        return Ok(());
    }

    // Draft is an update of a live spec. The insertion case is also an update:
    // we previously created a live_specs rows for the draft in order to lock it.

    let mut image_parts = None;

    match *draft_type {
        CatalogType::Capture => {
            let key = models::Capture::new(catalog_name);
            let capture = catalog.captures.get(&key).unwrap();

            if let models::CaptureEndpoint::Connector(config) = &capture.endpoint {
                image_parts = Some(split_tag(&config.image));
            }
            let targets: Vec<String> = capture
                .bindings
                .iter()
                .map(|b| b.target.to_string())
                .sorted()
                .unique()
                .collect();

            sqlx::query!(
                r#"
                insert into live_spec_flows (source_id, target_id, flow_type)
                select $1, live_specs.id, 'capture'
                from unnest($2::text[]) as n join live_specs on catalog_name = n;
                "#,
                *live_spec_id as Id,
                &targets as &Vec<String>,
            )
            .execute(&mut *txn)
            .await
            .context("insert capture edges")?;
        }
        CatalogType::Collection => {
            let key = models::Collection::new(catalog_name);
            let collection = catalog.collections.get(&key).unwrap();

            if let Some(derivation) = &collection.derivation {
                let sources: Vec<String> = derivation
                    .transform
                    .iter()
                    .map(|(_, tdef)| tdef.source.name.to_string())
                    .sorted()
                    .unique()
                    .collect();

                sqlx::query!(
                    r#"
                    insert into live_spec_flows (source_id, target_id, flow_type)
                    select live_specs.id, $1, 'collection'
                    from unnest($2::text[]) as n join live_specs on catalog_name = n;
                    "#,
                    *live_spec_id as Id,
                    &sources as &Vec<String>,
                )
                .execute(&mut *txn)
                .await
                .context("insert derivation edges")?;
            }
        }
        CatalogType::Materialization => {
            let key = models::Materialization::new(catalog_name);
            let materialization = catalog.materializations.get(&key).unwrap();

            // TODO(johnny): should we disallow sqlite? or remove sqlite altogether as an endpoint?
            if let models::MaterializationEndpoint::Connector(config) = &materialization.endpoint {
                image_parts = Some(split_tag(&config.image));
            }
            let sources: Vec<String> = materialization
                .bindings
                .iter()
                .map(|b| b.source.to_string())
                .sorted()
                .unique()
                .collect();

            sqlx::query!(
                r#"
                insert into live_spec_flows (source_id, target_id, flow_type)
                select live_specs.id, $1, 'materialization'
                from unnest($2::text[]) as n join live_specs on catalog_name = n;
                "#,
                *live_spec_id as Id,
                &sources as &Vec<String>,
            )
            .execute(&mut *txn)
            .await
            .context("insert materialization edges")?;
        }
        CatalogType::Test => {
            let key = models::Test::new(catalog_name);
            let steps = catalog.tests.get(&key).unwrap();

            let (mut reads_from, mut writes_to) = (Vec::new(), Vec::new());
            for step in steps {
                match step {
                    models::TestStep::Ingest(ingest) => {
                        writes_to.push(ingest.collection.to_string())
                    }
                    models::TestStep::Verify(verify) => {
                        reads_from.push(verify.collection.to_string())
                    }
                }
            }
            for v in [&mut reads_from, &mut writes_to] {
                v.sort();
                v.dedup();
            }

            sqlx::query!(
                r#"
                insert into live_spec_flows (source_id, target_id, flow_type)
                    select live_specs.id, $1, 'test'::catalog_spec_type
                    from unnest($2::text[]) as n join live_specs on catalog_name = n
                union
                    select $1, live_specs.id, 'test'
                    from unnest($3::text[]) as n join live_specs on catalog_name = n;
                "#,
                *live_spec_id as Id,
                &reads_from as &Vec<String>,
                &writes_to as &Vec<String>,
            )
            .execute(&mut *txn)
            .await
            .context("insert test edges")?;
        }
    }

    sqlx::query!(
        r#"update live_specs set
            connector_image_name = $2,
            connector_image_tag = $3,
            last_pub_id = $4,
            spec = $5,
            updated_at = clock_timestamp()
        where catalog_name = $1
        returning 1 as "must_exist";
        "#,
        catalog_name,
        image_parts.as_ref().map(|p| &p.0),
        image_parts.as_ref().map(|p| &p.1),
        pub_id as Id,
        draft_spec as &Json<Box<RawValue>>,
    )
    .fetch_one(&mut *txn)
    .await
    .context("update live_specs")?;

    Ok(())
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
