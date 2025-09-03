use std::collections::BTreeMap;

use models::ModelDef;
use tables::{BuiltRow, LiveRow};

use crate::publications::db;

fn get_deltas(built: &build::Output) -> BTreeMap<&str, (i32, i32)> {
    let mut by_tenant: BTreeMap<&str, (i32, i32)> = BTreeMap::new();

    for r in built.built.built_captures.iter() {
        let tenant = tenant(r.catalog_name());
        let entry = by_tenant.entry(tenant).or_default();

        let built_enabled = r.model().map(|s| s.is_enabled()).unwrap_or_default();
        let prev_enabled = built
            .live
            .captures
            .get_by_key(r.catalog_name())
            .map(|l| l.model().is_enabled())
            .unwrap_or_default();
        if built_enabled && !prev_enabled {
            entry.0 += 1;
        } else if prev_enabled && !built_enabled {
            entry.0 -= 1;
        }
    }
    for r in built.built.built_collections.iter() {
        let tenant = tenant(r.catalog_name());
        let entry = by_tenant.entry(tenant).or_default();

        // Collection usage
        if r.is_insert() {
            entry.1 += 1;
        } else if r.is_delete() {
            entry.1 -= 1;
        }

        // Derivations are counted both as collections and as tasks
        let built_enabled = r
            .model()
            .map(|s| s.derive.is_some() && s.is_enabled())
            .unwrap_or_default();
        let prev_enabled = built
            .live
            .collections
            .get_by_key(r.catalog_name())
            .map(|l| l.model().derive.is_some() && l.model().is_enabled())
            .unwrap_or_default();
        if built_enabled && !prev_enabled {
            entry.0 += 1;
        } else if prev_enabled && !built_enabled {
            entry.0 -= 1;
        }
    }
    for r in built.built.built_materializations.iter() {
        let tenant = tenant(r.catalog_name());
        let entry = by_tenant.entry(tenant).or_default();
        let built_enabled = r.model().map(|s| s.is_enabled()).unwrap_or_default();
        let prev_enabled = built
            .live
            .materializations
            .get_by_key(r.catalog_name())
            .map(|l| l.model().is_enabled())
            .unwrap_or_default();
        if built_enabled && !prev_enabled {
            entry.0 += 1;
        } else if prev_enabled && !built_enabled {
            entry.0 -= 1;
        }
    }
    // Note that tests don't count against quotas.
    by_tenant
}

fn tenant(name: &impl AsRef<str>) -> &str {
    let idx = name
        .as_ref()
        .find('/')
        .expect("catalog name must contain at least one /");
    name.as_ref().split_at(idx + 1).0
}

pub async fn check_resource_quotas(
    built: &build::Output,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<tables::Errors> {
    let deltas = get_deltas(built);

    let tenant_names = deltas.keys().map(|k| *k).collect::<Vec<_>>();

    let errors = db::find_tenant_quotas(&tenant_names, &mut **txn)
        .await?
        .into_iter()
        .flat_map(|tenant| {
            let mut errs = vec![];

            let (tasks_delta, collections_delta) = deltas
                .get(tenant.name.as_str())
                .cloned()
                .unwrap_or_default();

            let new_tasks_used = tenant.tasks_used + tasks_delta;
            let new_collections_used = tenant.collections_used + collections_delta;

            // We don't want to stop you from disabling tasks if you're at/over your quota
            // NOTE: technically this means that you can add new tasks even if your usage
            // exceeds your quota, so long as you remove/disable more tasks than you add.
            if tasks_delta >= 0 && new_tasks_used > tenant.tasks_quota {
                let value = anyhow::anyhow!(
                    "Request to add {} task(s) would exceed tenant '{}' quota of {}. {} are currently in use.",
                    tasks_delta,
                    tenant.name,
                    tenant.tasks_quota,
                    tenant.tasks_used,
                );
                errs.push(tables::Error {
                    scope: err_scope(&tenant.name, "tasks"),
                    error: value,
                });
            }
            if collections_delta >= 0 && new_collections_used > tenant.collections_quota {
                let value = anyhow::anyhow!(
                    "Request to add {} collections(s) would exceed tenant '{}' quota of {}. {} are currently in use.",
                    collections_delta,
                    tenant.name,
                    tenant.collections_quota,
                    tenant.collections_used,
                );
                errs.push(tables::Error {
                    scope: err_scope(&tenant.name, "collections"),
                    error: value,
                });
            }
            tracing::debug!(tenant = ?tenant, err_count = %errs.len(), "checked tenant quotas");
            errs
        })
        .collect();

    Ok(errors)
}

fn err_scope(tenant: &str, tasks_or_collections: &str) -> url::Url {
    let mut url = url::Url::parse("flow://tenant-quotas/").unwrap();
    url.set_path(tenant);
    url.join(tasks_or_collections).unwrap()
}
