//! GraphQL API for per-prefix and per-task alert configuration stored in
//! `public.alert_configs`.
//!
//! Query results are limited to rows under prefixes the caller can read.
//! Updating a row requires admin access to its governing prefix. For exact
//! catalog names, the governing prefix is the parent prefix.

use super::filters;
use async_graphql::{
    Context,
    types::connection::{self, Connection},
};

const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_PREFIXES: usize = 20;

/// Optional filter for the `alertConfigs` query. When omitted, all accessible
/// rows are returned.
#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct AlertConfigsFilter {
    /// Filter on the `catalog_prefix_or_name` column.
    pub catalog_prefix_or_name: Option<filters::PrefixFilter>,
}

/// A single row from `public.alert_configs`.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct AlertConfigEntry {
    pub id: models::Id,
    pub catalog_prefix_or_name: String,
    pub config: async_graphql::Json<models::AlertConfig>,
    pub detail: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub last_modified_by: Option<sqlx::types::Uuid>,
}

#[async_graphql::ComplexObject]
impl AlertConfigEntry {
    /// The fully-resolved effective config at this scope, merging all
    /// ancestor prefix layers and controller defaults.
    async fn effective(&self, ctx: &Context<'_>) -> async_graphql::Result<EffectiveAlertConfig> {
        resolve_effective_alert_config(ctx, &self.catalog_prefix_or_name).await
    }
}

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct EffectiveAlertConfig {
    pub config: async_graphql::Json<models::AlertConfig>,
    pub provenance: Vec<FieldProvenance>,
}

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct FieldProvenance {
    pub path: String,
    pub source: Option<String>,
}

pub async fn resolve_effective_alert_config(
    ctx: &Context<'_>,
    catalog_prefix_or_name: &str,
) -> async_graphql::Result<EffectiveAlertConfig> {
    let env = ctx.data::<crate::Envelope>()?;
    let defaults = ctx.data_opt::<models::AlertConfig>();

    let (config, provenance_map) = crate::controllers::fetch_alert_config_with_provenance(
        catalog_prefix_or_name,
        &env.pg_pool,
        defaults,
    )
    .await
    .map_err(|e| async_graphql::Error::new(e.to_string()))?;

    let provenance = provenance_map
        .into_iter()
        .map(|(path, source)| FieldProvenance { path, source })
        .collect();

    Ok(EffectiveAlertConfig {
        config: async_graphql::Json(config),
        provenance,
    })
}

/// Result of the `updateAlertConfig` mutation.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct UpdateAlertConfigResult {
    pub id: models::Id,
    pub catalog_prefix_or_name: String,
    pub created: bool,
}

pub type PaginatedAlertConfigs = Connection<
    String,
    AlertConfigEntry,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

#[derive(Debug, Default)]
pub struct AlertConfigsQuery;

#[async_graphql::Object]
impl AlertConfigsQuery {
    /// Lists alert-config rows visible to the caller.
    ///
    /// Results are limited to readable prefixes and sorted by
    /// `catalog_prefix_or_name`. `filter.catalogPrefixOrName.startsWith` can
    /// narrow the results further. Passing a full catalog name returns at
    /// most one exact-name row.
    pub async fn alert_configs(
        &self,
        ctx: &Context<'_>,
        filter: Option<AlertConfigsFilter>,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<PaginatedAlertConfigs> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let prefix_starts_with = filter
            .and_then(|f| f.catalog_prefix_or_name)
            .and_then(|f| f.starts_with);

        let read_prefixes = super::authorized_prefixes::authorized_prefixes(
            &env.snapshot().role_grants,
            &env.snapshot().user_grants,
            claims.sub,
            models::Capability::Read,
            prefix_starts_with.as_deref(),
        );

        if read_prefixes.is_empty() {
            return Ok(PaginatedAlertConfigs::new(false, false));
        }
        if read_prefixes.len() > MAX_PREFIXES {
            return Err(async_graphql::Error::new(
                "Too many accessible prefixes; narrow results with a filter",
            ));
        }

        connection::query_with::<String, _, _, _, async_graphql::Error>(
            after,
            None,
            first,
            None,
            |after, _, first, _| async move {
                let limit = first.unwrap_or(DEFAULT_PAGE_SIZE);

                let rows = sqlx::query!(
                    r#"
                    SELECT
                        id as "id!: models::Id",
                        catalog_prefix_or_name::text as "catalog_prefix_or_name!: String",
                        config as "config!: crate::TextJson<models::AlertConfig>",
                        detail,
                        created_at,
                        updated_at,
                        last_modified_by
                    FROM alert_configs
                    WHERE catalog_prefix_or_name::text ^@ ANY($1)
                      AND ($2::text IS NULL OR catalog_prefix_or_name::text > $2)
                      AND ($3::text IS NULL OR catalog_prefix_or_name::text ^@ $3)
                    ORDER BY catalog_prefix_or_name ASC
                    LIMIT $4 + 1
                    "#,
                    &read_prefixes,
                    after.as_deref(),
                    prefix_starts_with.as_deref(),
                    limit as i64,
                )
                .fetch_all(&env.pg_pool)
                .await
                .map_err(async_graphql::Error::from)?;

                let has_next = rows.len() > limit;

                let edges: Vec<_> = rows
                    .into_iter()
                    .take(limit)
                    .map(|r| {
                        Ok(connection::Edge::new(
                            r.catalog_prefix_or_name.clone(),
                            AlertConfigEntry {
                                id: r.id,
                                catalog_prefix_or_name: r.catalog_prefix_or_name,
                                config: async_graphql::Json(r.config.0),
                                detail: r.detail,
                                created_at: r.created_at,
                                updated_at: r.updated_at,
                                last_modified_by: r.last_modified_by,
                            },
                        ))
                    })
                    .collect::<Result<Vec<_>, async_graphql::Error>>()?;

                let mut conn = PaginatedAlertConfigs::new(after.is_some(), has_next);
                conn.edges = edges;
                Ok(conn)
            },
        )
        .await
    }
}

#[derive(Debug, Default)]
pub struct AlertConfigsMutation;

#[async_graphql::Object]
impl AlertConfigsMutation {
    /// Creates or replaces the alert config at `catalogPrefixOrName`.
    ///
    /// `catalogPrefixOrName` is either a catalog prefix ending in `/`
    /// (applies to all tasks under that prefix) or an exact catalog name
    /// with no trailing slash (applies to that single task). Exact catalog
    /// names must refer to a task that currently exists in `live_specs`;
    /// prefixes have no such constraint.
    ///
    /// To clear all configured overrides while keeping the row, pass an empty
    /// `{}` config.
    ///
    /// If `detail` is omitted or `null` on update, the existing `detail`
    /// value is preserved.
    pub async fn update_alert_config(
        &self,
        ctx: &Context<'_>,
        catalog_prefix_or_name: String,
        config: async_graphql::Json<models::AlertConfig>,
        detail: Option<String>,
    ) -> async_graphql::Result<UpdateAlertConfigResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        let async_graphql::Json(config) = config;

        validate_prefix_or_name(&catalog_prefix_or_name)?;

        let gov = governing_prefix(&catalog_prefix_or_name)?;
        let policy_result = crate::server::evaluate_names_authorization(
            env.snapshot(),
            claims,
            models::Capability::Admin,
            [gov.as_str()],
        );
        env.authorization_outcome(policy_result).await?;

        if !catalog_prefix_or_name.ends_with('/') {
            let exists: bool = sqlx::query_scalar(
                "select exists(select 1 from live_specs where catalog_name = $1 and spec is not null)",
            )
            .bind(&catalog_prefix_or_name)
            .fetch_one(&env.pg_pool)
            .await?;

            if !exists {
                return Err(async_graphql::Error::new(format!(
                    "catalog task '{}' does not exist; use a trailing '/' to create a prefix-scoped config",
                    catalog_prefix_or_name,
                )));
            }
        }

        let config_json = sqlx::types::Json(&config);

        let row = sqlx::query!(
            r#"
            insert into alert_configs (
                catalog_prefix_or_name, config, detail, last_modified_by
            )
            values ($1, $2, $3, $4)
            on conflict (catalog_prefix_or_name) do update set
                config = excluded.config,
                detail = coalesce(excluded.detail, alert_configs.detail),
                last_modified_by = excluded.last_modified_by,
                updated_at = now()
            -- `xmax` is Postgres' MVCC "deleting txid" system column. On a
            -- fresh INSERT it is 0; on the UPDATE branch of ON CONFLICT it is
            -- set to the current txid. `(xmax = 0)` thus distinguishes the
            -- two branches in one round trip without a separate probe query.
            returning
                id as "id!: models::Id",
                (xmax = 0) as "created!: bool"
            "#,
            catalog_prefix_or_name,
            config_json as sqlx::types::Json<&models::AlertConfig>,
            detail,
            claims.sub,
        )
        .fetch_one(&env.pg_pool)
        .await?;

        tracing::info!(
            catalog_prefix_or_name = %catalog_prefix_or_name,
            created = row.created,
            user_id = %claims.sub,
            "wrote alert_configs row"
        );

        Ok(UpdateAlertConfigResult {
            id: row.id,
            catalog_prefix_or_name,
            created: row.created,
        })
    }
}

/// Validates `catalog_prefix_or_name` as either a `models::Prefix` (trailing
/// `/`) or a `models::Name` (no trailing `/`). The trailing slash is the
/// discriminant: prefixes scope to all tasks beneath them, while bare names
/// target a single task.
fn validate_prefix_or_name(catalog_prefix_or_name: &str) -> async_graphql::Result<()> {
    use validator::Validate;

    if catalog_prefix_or_name.ends_with('/') {
        models::Prefix::new(catalog_prefix_or_name)
            .validate()
            .map_err(|e| async_graphql::Error::new(format!("invalid catalog prefix: {e}")))
    } else {
        models::Name::new(catalog_prefix_or_name)
            .validate()
            .map_err(|e| async_graphql::Error::new(format!("invalid catalog name: {e}")))
    }
}

/// Returns the prefix used for authorization checks on
/// `catalog_prefix_or_name`: the row itself if it ends in `/`, otherwise its
/// parent prefix.
fn governing_prefix(catalog_prefix_or_name: &str) -> async_graphql::Result<models::Prefix> {
    if catalog_prefix_or_name.ends_with('/') {
        return Ok(models::Prefix::new(catalog_prefix_or_name.to_string()));
    }
    match catalog_prefix_or_name.rfind('/') {
        Some(i) => Ok(models::Prefix::new(
            catalog_prefix_or_name[..=i].to_string(),
        )),
        None => Err(async_graphql::Error::new(format!(
            "invalid catalog_prefix_or_name '{catalog_prefix_or_name}': must contain at least one '/'"
        ))),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_server;

    #[test]
    fn governing_prefix_handles_both_forms() {
        assert_eq!(
            governing_prefix("acmeCo/prod/").unwrap().as_str(),
            "acmeCo/prod/"
        );
        assert_eq!(
            governing_prefix("acmeCo/prod/source-pg").unwrap().as_str(),
            "acmeCo/prod/"
        );
        assert_eq!(
            governing_prefix("acmeCo/capture").unwrap().as_str(),
            "acmeCo/"
        );
        assert!(governing_prefix("no-slash-at-all").is_err());
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_update_alert_config_authorization(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let token = server.make_access_token(
            uuid::Uuid::from_bytes([0x11; 16]),
            Some("alice@example.test"),
        );

        // Alice can write a config under her own prefix.
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        updateAlertConfig(
                            catalogPrefixOrName: "aliceCo/"
                            config: {}
                        ) {
                            id
                            catalogPrefixOrName
                            created
                        }
                    }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!("create_on_own_prefix", response, {
            ".data.updateAlertConfig.id" => "[id]"
        });

        // Alice is denied on a prefix she doesn't admin.
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        updateAlertConfig(
                            catalogPrefixOrName: "notAliceCo/"
                            config: {}
                        ) {
                            id
                        }
                    }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!("denied_on_other_prefix", response);

        // Alice can write an exact-name config for a task that exists.
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        updateAlertConfig(
                            catalogPrefixOrName: "aliceCo/in/capture-foo"
                            config: {}
                        ) {
                            id
                            catalogPrefixOrName
                            created
                        }
                    }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!("create_on_existing_task", response, {
            ".data.updateAlertConfig.id" => "[id]"
        });

        // Alice is rejected for a task name that doesn't exist.
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        updateAlertConfig(
                            catalogPrefixOrName: "aliceCo/no-such-task"
                            config: {}
                        ) {
                            id
                        }
                    }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!("rejected_nonexistent_task", response);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_effective_alert_config_with_defaults(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let defaults = models::AlertConfig {
            data_movement_stalled: None,
            shard_failed: Some(models::ShardFailedConfig {
                enabled: Some(true),
                condition: Some(models::ShardFailedCondition {
                    failures: Some(3),
                    per: Some(std::time::Duration::from_secs(8 * 3600)),
                }),
            }),
            task_chronically_failing: Some(models::TaskChronicallyFailingConfig {
                enabled: Some(true),
                auto_disable: Some(false),
                condition: Some(models::TaskChronicallyFailingCondition {
                    failing_for: Some(std::time::Duration::from_secs(30 * 86400)),
                }),
            }),
            task_idle: Some(models::TaskIdleConfig {
                enabled: Some(true),
                auto_disable: Some(false),
                condition: Some(models::TaskIdleCondition {
                    idle_for: Some(std::time::Duration::from_secs(30 * 86400)),
                }),
            }),
        };

        let server = test_server::TestServer::start_with_defaults(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
            Some(defaults),
        )
        .await;

        let token = server.make_access_token(
            uuid::Uuid::from_bytes([0x11; 16]),
            Some("alice@example.test"),
        );

        // No alert_configs rows exist: effective config should be entirely defaults.
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        liveSpecs(by: { names: ["aliceCo/in/capture-foo"] }) {
                            edges {
                                node {
                                    catalogName
                                    liveSpec {
                                        effectiveAlertConfig {
                                            config
                                            provenance { path source }
                                        }
                                    }
                                }
                            }
                        }
                    }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!("effective_defaults_only", response);

        // Insert a prefix override for a single field.
        let _: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    mutation {
                        updateAlertConfig(
                            catalogPrefixOrName: "aliceCo/"
                            config: { shardFailed: { condition: { failures: 5 } } }
                        ) { id }
                    }"#
                }),
                Some(&token),
            )
            .await;

        // Query effective config on the same task: defaults + prefix override merged.
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                    query {
                        liveSpecs(by: { names: ["aliceCo/in/capture-foo"] }) {
                            edges {
                                node {
                                    catalogName
                                    liveSpec {
                                        effectiveAlertConfig {
                                            config
                                            provenance { path source }
                                        }
                                    }
                                }
                            }
                        }
                    }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!("effective_defaults_plus_prefix_override", response);
    }
}
