use std::collections::BTreeMap;

use tables::BuiltRow;

fn get_deltas(built: &build::Output) -> BTreeMap<&str, (i32, i32)> {
    let mut by_tenant: BTreeMap<&str, (i32, i32)> = BTreeMap::new();

    for r in built.built.built_captures.iter() {
        let tenant = tenant(r.catalog_name());
        let entry = by_tenant.entry(tenant).or_default();
        if r.is_insert() {
            entry.0 += 1;
        } else if r.is_delete() {
            entry.0 -= 1;
        }
    }
    for r in built.built.built_collections.iter() {
        let tenant = tenant(r.catalog_name());
        let entry = by_tenant.entry(tenant).or_default();
        if r.is_insert() {
            entry.1 += 1;
        } else if r.is_delete() {
            entry.1 -= 1;
        }
    }
    for r in built.built.built_materializations.iter() {
        let tenant = tenant(r.catalog_name());
        let entry = by_tenant.entry(tenant).or_default();
        if r.is_insert() {
            entry.0 += 1;
        } else if r.is_delete() {
            entry.0 -= 1;
        }
    }
    // Note that tests don't count against quotas.
    by_tenant
}

fn tenant(name: &impl AsRef<str>) -> &str {
    name.as_ref().split_once('/').unwrap().0
}

pub async fn check_resource_quotas(
    built: &build::Output,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<tables::Errors> {
    let deltas = get_deltas(built);

    let tenant_names = deltas.keys().map(|k| *k).collect::<Vec<_>>();

    let errors = agent_sql::publications::find_tenant_quotas(&tenant_names, txn)
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
