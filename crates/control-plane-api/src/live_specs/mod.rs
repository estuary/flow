mod db;

use anyhow::Context;
use models::Capability;
use std::ops::Deref;
use uuid::Uuid;

pub use db::{
    InferredSchemaRow, LiveSpec, fetch_expanded_live_specs, fetch_inferred_schemas,
    fetch_live_spec_names_by_prefix, fetch_live_specs, hard_delete_live_spec,
};

/// Partitions the requested `names` by whether `principal` holds `capability`
/// to them, evaluated against the authorization `snapshot`. Returns
/// `(authorized, denied)` as references into `names`, each sorted and
/// deduplicated.
fn partition_by_authorization<'n>(
    principal: tables::Principal<'_>,
    names: &'n [String],
    capability: models::authz::CapabilitySet,
    snapshot: &crate::Snapshot,
) -> (Vec<&'n str>, Vec<&'n str>) {
    let (mut authorized, mut denied): (Vec<&str>, Vec<&str>) =
        names.iter().map(String::as_str).partition(|name| {
            tables::UserGrant::is_authorized(
                &snapshot.role_grants,
                &snapshot.user_grants,
                principal,
                name,
                capability,
            )
        });

    authorized.sort();
    authorized.dedup();
    denied.sort();
    denied.dedup();
    (authorized, denied)
}

/// Fetches live specs as a `tables::LiveCatalog`, silently filtering out
/// requested names to which the user does not hold `capability`, evaluated
/// against the authorization `snapshot`. Filtered names are simply absent
/// from the result — indistinguishable from specs which don't exist — and
/// never surface as an error.
///
/// The `snapshot` is trusted as-is: a grant committed after it was taken is
/// invisible until the watch's own background refresh cadence picks it up.
pub async fn get_live_specs_filtered(
    principal: tables::Principal<'_>,
    names: &[String],
    capability: impl Into<models::authz::CapabilitySet>,
    snapshot: &crate::Snapshot,
    db: &sqlx::PgPool,
) -> anyhow::Result<tables::LiveCatalog> {
    let (authorized, denied) =
        partition_by_authorization(principal, names, capability.into(), snapshot);

    if !denied.is_empty() {
        let user_id = principal.user_id;
        tracing::debug!(?denied, %user_id, "filtered unauthorized specs from fetch");
    }
    get_live_specs_unfiltered(principal.user_id, &authorized, db).await
}

/// Fetches live specs as a `tables::LiveCatalog` without any authorization
/// filtering: every requested name that has a live spec is returned.
///
/// `user_id` is bound into the query but unused: with both capability flags
/// disabled, `fetch_live_specs` never evaluates it.
pub async fn get_live_specs_unfiltered(
    user_id: Uuid,
    names: &[impl AsRef<str>],
    db: &sqlx::PgPool,
) -> anyhow::Result<tables::LiveCatalog> {
    let mut live = tables::LiveCatalog::default();

    // Chunking is inherited from when this query computed authorization
    // capabilities per name and risked statement timeouts on large fetches.
    // The plain fetch is much cheaper; chunks are kept to bound statement
    // size for very large name lists.
    for names_chunk in names.chunks(512) {
        let names_chunk: Vec<&str> = names_chunk.iter().map(AsRef::as_ref).collect();
        let rows = db::fetch_live_specs(
            user_id,
            &names_chunk,
            false, // authorization is not evaluated in SQL
            false, // we never need spec_capabilities here
            db,
        )
        .await?;
        for row in rows {
            // Spec type might be null because we used to set it to null when deleting specs.
            // For recently deleted specs, it will still be present.
            let Some(catalog_type) = row.spec_type.map(Into::into) else {
                continue;
            };
            let Some(model_json) = row.spec.as_deref() else {
                continue;
            };
            let built_spec_json = row.built_spec.as_ref().ok_or_else(|| {
                tracing::warn!(catalog_name = %row.catalog_name, id = %row.id, "got row with spec but not built_spec");
                anyhow::anyhow!("missing built_spec for {:?}, but spec is non-null", row.catalog_name)
            })?.deref();

            live.add_spec(
                catalog_type,
                &row.catalog_name,
                row.id.into(),
                row.data_plane_id.into(),
                row.last_pub_id.into(),
                row.last_build_id.into(),
                model_json,
                built_spec_json,
                row.dependency_hash,
            )
            .with_context(|| format!("deserializing specs for {:?}", row.catalog_name))?;
        }
    }

    Ok(live)
}

pub async fn get_connected_live_specs(
    user_id: Uuid,
    collection_names: &[&str],
    exclude_names: &[&str],
    filter_capability: Option<Capability>,
    db: &sqlx::PgPool,
) -> anyhow::Result<tables::LiveCatalog> {
    let expanded_rows =
        db::fetch_expanded_live_specs(user_id, collection_names, exclude_names, db).await?;
    let mut live = tables::LiveCatalog::default();
    for exp in expanded_rows {
        if let Some(minimum_capability) = filter_capability {
            if !exp
                .user_capability
                .map(|c| c >= minimum_capability)
                .unwrap_or(false)
            {
                continue;
            }
        }
        // TODO: These fields should be non-nullable, so we may be able to remove these checks.
        let Some(spec_type) = exp.spec_type else {
            anyhow::bail!("missing spec_type for expanded row: {:?}", exp.catalog_name);
        };
        let Some(model_json) = &exp.spec else {
            anyhow::bail!("missing spec for expanded row: {:?}", exp.catalog_name);
        };
        let Some(built_json) = &exp.built_spec else {
            anyhow::bail!(
                "missing built_spec for expanded row: {:?}",
                exp.catalog_name
            );
        };

        live.add_spec(
            spec_type,
            &exp.catalog_name,
            exp.id,
            exp.data_plane_id,
            exp.last_pub_id,
            exp.last_build_id,
            &model_json.0,
            &built_json,
            exp.dependency_hash.clone(),
        )?;
    }
    Ok(live)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_partition_by_authorization() {
        let snapshot = crate::Snapshot::build_fixture(None);
        // Bob (from the fixture): `write` on bobCo/, plus `read` to
        // acmeCo/shared/ via an admin grant to bobCo/tires/.
        let bob: Uuid = "20202020-2020-2020-2020-202020202020".parse().unwrap();
        let capability = models::authz::Capability::CatalogRead.into();

        // Names the user is authorized to produce no denials.
        let names = vec![
            "acmeCo/shared/collection".to_string(),
            "bobCo/tires/capture".to_string(),
        ];
        let (authorized, denied) = partition_by_authorization(
            tables::Principal::unscoped(bob),
            &names,
            capability,
            &snapshot,
        );
        assert_eq!(authorized, names);
        assert!(denied.is_empty());

        // Denials are computed from the requested names alone: a name with no
        // live spec and a name of an existing-but-unauthorized spec partition
        // identically. Duplicates collapse and both sides come back sorted.
        let names = vec![
            "aliceCo/anvils/pings".to_string(),
            "bobCo/tires/capture".to_string(),
            "acmeCo/private/collection".to_string(),
            "aliceCo/anvils/pings".to_string(),
        ];
        let (authorized, denied) = partition_by_authorization(
            tables::Principal::unscoped(bob),
            &names,
            capability,
            &snapshot,
        );
        assert_eq!(authorized, vec!["bobCo/tires/capture"]);
        assert_eq!(
            denied,
            vec!["acmeCo/private/collection", "aliceCo/anvils/pings"]
        );
    }
}
