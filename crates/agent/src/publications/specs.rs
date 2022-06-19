use super::Error;
use crate::Id;

use agent_sql::publications::{ExpandedRow, SpecRow};
use agent_sql::{Capability, CatalogType};
use anyhow::Context;
use itertools::Itertools;
use serde_json::value::RawValue;
use sqlx::types::Uuid;
use std::collections::BTreeMap;

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

pub async fn insert_errors(
    draft_id: Id,
    errors: Vec<Error>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<()> {
    for err in errors {
        agent_sql::publications::insert_error(
            draft_id,
            err.scope.unwrap_or(err.catalog_name),
            err.detail,
            txn,
        )
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
        last_build_id: _,
        last_pub_id: _,
        live_spec: _,
        live_spec_id,
        live_type,
        spec_capabilities: _,
        user_capability,
    } = spec_row;

    assert!(matches!(user_capability, Some(Capability::Admin)));

    agent_sql::publications::delete_draft_spec(*draft_spec_id, txn)
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
        pub_id,
        &reads_from,
        draft_spec,
        draft_type,
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
