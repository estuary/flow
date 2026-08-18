mod db;

use anyhow::Context;
use models::Capability;
use std::ops::Deref;
use uuid::Uuid;

pub use db::{
    InferredSchemaRow, LiveSpec, fetch_expanded_live_specs, fetch_inferred_schemas,
    fetch_live_spec_names_by_prefix, fetch_live_specs, hard_delete_live_spec,
};

/// NotAuthorized is raised when the user lacks a required capability to one
/// or more requested catalog names. It's evaluated purely over the requested
/// names — never over which specs exist — so a denial cannot reveal the
/// existence of a spec. Callers recognize it by downcasting through `anyhow`
/// wrapping, and must surface only its bare `Display` (not an accumulated
/// context chain) so that every denial renders identically.
#[derive(Debug)]
pub struct NotAuthorized {
    /// Requested catalog names which the user is not authorized to.
    /// Sorted and deduplicated.
    pub names: Vec<String>,
    /// The capability the user was required, and failed, to hold.
    pub capability: models::authz::CapabilitySet,
}

impl std::fmt::Display for NotAuthorized {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "not authorized to {}: {}",
            action_phrase(self.capability),
            self.names.join(", ")
        )
    }
}

impl std::error::Error for NotAuthorized {}

/// Renders the required capability as a natural action phrase where one
/// exists, falling back to the capability names themselves. The phrase is a
/// constant of the call site's required capability: it never varies with the
/// request's outcome or with which specs exist.
fn action_phrase(capability: models::authz::CapabilitySet) -> String {
    if capability == models::authz::CapabilitySet::only(models::authz::Capability::CatalogRead) {
        return "read".to_string();
    }
    capability
        .iter()
        .map(|cap| cap.to_string())
        .collect::<Vec<_>>()
        .join(" + ")
}

/// Returns the requested `names` to which the user does NOT hold `capability`,
/// evaluated against the authorization `snapshot`.
fn authorization_denials(
    user_id: Uuid,
    names: &[String],
    capability: models::authz::CapabilitySet,
    snapshot: &crate::Snapshot,
) -> Vec<String> {
    let mut denied: Vec<String> = names
        .iter()
        .filter(|name| {
            !tables::UserGrant::is_authorized(
                &snapshot.role_grants,
                &snapshot.user_grants,
                user_id,
                name,
                capability,
            )
        })
        .cloned()
        .collect();

    denied.sort();
    denied.dedup();
    denied
}

/// Fetches live specs as a `tables::LiveCatalog`, requiring that the user
/// holds `capability` to every requested name, evaluated against the
/// authorization `snapshot`.
///
/// Authorization is checked before any database access, and over the
/// requested names rather than over fetched rows: neither the timing nor the
/// content of a `NotAuthorized` error varies with whether a spec exists.
pub async fn get_live_specs_authorized(
    user_id: Uuid,
    names: &[String],
    capability: models::authz::CapabilitySet,
    snapshot: &crate::Snapshot,
    db: &sqlx::PgPool,
) -> anyhow::Result<tables::LiveCatalog> {
    let denied = authorization_denials(user_id, names, capability, snapshot);
    if !denied.is_empty() {
        return Err(NotAuthorized {
            names: denied,
            capability,
        }
        .into());
    }
    get_live_specs_unfiltered(user_id, names, db).await
}

/// Fetches live specs as a `tables::LiveCatalog` without any authorization
/// filtering: every requested name that has a live spec is returned.
///
/// `user_id` is bound into the query but unused: with both capability flags
/// disabled, `fetch_live_specs` never evaluates it.
pub async fn get_live_specs_unfiltered(
    user_id: Uuid,
    names: &[String],
    db: &sqlx::PgPool,
) -> anyhow::Result<tables::LiveCatalog> {
    let mut live = tables::LiveCatalog::default();

    // Chunking is inherited from when this query computed authorization
    // capabilities per name and risked statement timeouts on large fetches.
    // The plain fetch is much cheaper; chunks are kept to bound statement
    // size for very large name lists.
    for names_chunk in names.chunks(512) {
        let rows = db::fetch_live_specs(
            user_id,
            names_chunk,
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
    fn test_authorization_denials() {
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
        assert!(authorization_denials(bob, &names, capability, &snapshot).is_empty());

        // Denials are computed from the requested names alone: a name with no
        // live spec and a name of an existing-but-unauthorized spec are
        // indistinguishable in the rendered error.
        let names = vec![
            "aliceCo/anvils/pings".to_string(),
            "bobCo/tires/capture".to_string(),
            "acmeCo/private/collection".to_string(),
            "aliceCo/anvils/pings".to_string(),
        ];
        let denied = authorization_denials(bob, &names, capability, &snapshot);
        let error = NotAuthorized {
            names: denied,
            capability,
        };
        insta::assert_snapshot!(
            error.to_string(),
            @"not authorized to read: acmeCo/private/collection, aliceCo/anvils/pings"
        );

        // The rendered action derives from the required capability set, so a
        // future caller requiring a different capability renders truthfully.
        let error = NotAuthorized {
            names: vec!["acmeCo/private/collection".to_string()],
            capability: models::authz::Capability::SpecEdit.into(),
        };
        insta::assert_snapshot!(
            error.to_string(),
            @"not authorized to SpecEdit: acmeCo/private/collection"
        );
    }
}
