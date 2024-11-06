use anyhow::Context;
use models::Capability;
use std::ops::Deref;
use uuid::Uuid;

/// Fetches live specs, returning them as a `tables::LiveCatalog`. Optionally
/// filters the specs based on user capability. If `filter_capability` is
/// `None`, then no filtering will be done.
pub async fn get_live_specs(
    user_id: Uuid,
    names: &[String],
    filter_capability: Option<Capability>,
    db: &sqlx::PgPool,
) -> anyhow::Result<tables::LiveCatalog> {
    let rows = agent_sql::live_specs::fetch_live_specs(user_id, &names, db).await?;
    let mut live = tables::LiveCatalog::default();
    for row in rows {
        // Spec type might be null because we used to set it to null when deleting specs.
        // For recently deleted specs, it will still be present.
        let Some(catalog_type) = row.spec_type.map(Into::into) else {
            continue;
        };
        let Some(model_json) = row.spec.as_deref() else {
            continue;
        };
        if let Some(min_capability) = filter_capability {
            if !row
                .user_capability
                .is_some_and(|actual_capability| actual_capability >= min_capability)
            {
                continue;
            }
        }
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

    Ok(live)
}

pub async fn get_connected_live_specs(
    user_id: Uuid,
    collection_names: &[&str],
    exclude_names: &[&str],
    filter_capability: Option<Capability>,
    db: &sqlx::PgPool,
) -> anyhow::Result<tables::LiveCatalog> {
    let expanded_rows = agent_sql::live_specs::fetch_expanded_live_specs(
        user_id,
        collection_names,
        exclude_names,
        db,
    )
    .await?;
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
