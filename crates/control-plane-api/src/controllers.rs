use crate::TextJson;
use chrono::prelude::*;
use models::{CatalogType, Id};
use serde_json::value::RawValue;
use sqlx::types::Uuid;
use std::fmt::Debug;

/// Messages that can be sent to a controller.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    /// A dependency of the controlled spec has been updated.
    DependencyUpdated,
    /// The controlled spec has just been published.
    SpecPublished {
        /// The ID of the publication that touched or modified the spec.
        pub_id: models::Id,
    },
    /// Signals that a publication of the spec is necessary, and should be performed
    /// as soon as practical after the controller receives this message.
    Republish {
        reason: String,
    },
    /// The inferred schema of the controlled collection spec has been updated.
    InferredSchemaUpdated,
    /// A request to trigger the controller manually. This is primarily used
    /// in tests to trigger the controller without waiting the `wake_at` time.
    ManualTrigger {
        /// The ID of the user who sent the message.
        user_id: uuid::Uuid,
    },
    ShardFailed,
    ConfigUpdated,
}

#[derive(Debug)]
pub struct ControllerJob {
    pub live_spec_id: Id,
    pub catalog_name: String,
    pub last_pub_id: Id,
    pub last_build_id: Id,
    pub live_spec: Option<TextJson<Box<RawValue>>>,
    pub built_spec: Option<TextJson<Box<RawValue>>>,
    pub spec_type: Option<CatalogType>,
    pub controller_version: i32,
    pub controller_updated_at: DateTime<Utc>,
    pub live_spec_updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub logs_token: Uuid,
    pub status: TextJson<Box<RawValue>>,
    pub failures: i32,
    pub error: Option<String>,
    pub data_plane_id: Id,
    pub data_plane_name: Option<String>,
    pub live_dependency_hash: Option<String>,
}

pub async fn fetch_controller_job(
    controller_task_id: Id,
    db: impl sqlx::PgExecutor<'static>,
) -> sqlx::Result<Option<ControllerJob>> {
    sqlx::query_as!(
        ControllerJob,
        r#"select
            ls.id as "live_spec_id: Id",
            ls.catalog_name as "catalog_name!: String",
            ls.last_pub_id as "last_pub_id: Id",
            ls.last_build_id as "last_build_id: Id",
            ls.spec as "live_spec: TextJson<Box<RawValue>>",
            ls.built_spec as "built_spec: TextJson<Box<RawValue>>",
            ls.spec_type as "spec_type: CatalogType",
            ls.dependency_hash as "live_dependency_hash",
            ls.created_at,
            ls.updated_at as "live_spec_updated_at",
            cj.controller_version as "controller_version: i32",
            cj.updated_at as "controller_updated_at",
            cj.logs_token,
            cj.status as "status: TextJson<Box<RawValue>>",
            cj.failures,
            cj.error,
            ls.data_plane_id as "data_plane_id: Id",
            dp.data_plane_name as "data_plane_name?: String"
        from internal.tasks t
        join live_specs ls on t.task_id = ls.controller_task_id
        join controller_jobs cj on ls.id = cj.live_spec_id
        left outer join data_planes dp on ls.data_plane_id = dp.id
        where t.task_id = $1::flowid;"#,
        controller_task_id as Id,
    )
    .fetch_optional(db)
    .await
}

pub async fn fetch_last_user_publication_at(
    live_spec_id: Id,
    system_user_id: Uuid,
    db: impl sqlx::PgExecutor<'static>,
) -> sqlx::Result<Option<DateTime<Utc>>> {
    sqlx::query_scalar!(
        r#"
        select ps.published_at
        from publication_specs ps
        where ps.live_spec_id = $1
            and ps.user_id != $2
        order by ps.pub_id desc
        limit 1
        "#,
        live_spec_id as Id,
        system_user_id as Uuid,
    )
    .fetch_optional(db)
    .await
}

pub async fn fetch_last_data_movement_ts(
    catalog_name: &str,
    db: impl sqlx::PgExecutor<'static>,
) -> sqlx::Result<Option<DateTime<Utc>>> {
    sqlx::query_scalar!(
        r#"
        select ts
        from catalog_stats_daily
        where catalog_name = $1
            and ts > now() - interval '60 days'
            and (bytes_written_by_me + bytes_read_by_me
                + bytes_written_to_me + bytes_read_from_me) > 0
        order by ts desc
        limit 1
        "#,
        catalog_name,
    )
    .fetch_optional(db)
    .await
}

/// Computes the effective `AlertConfig` for `catalog_name` by deep-merging
/// all matching `alert_configs` rows, shortest prefix first.
///
/// Returns `None` when no rows match.
pub async fn fetch_alert_config(
    catalog_name: &str,
    db: impl sqlx::PgExecutor<'static>,
) -> anyhow::Result<Option<models::AlertConfig>> {
    let (config, _) = fetch_alert_config_with_provenance(catalog_name, db, None).await?;
    if config == models::AlertConfig::default() {
        Ok(None)
    } else {
        Ok(Some(config))
    }
}

/// Computes the effective `AlertConfig` for `catalog_name` by deep-merging
/// all matching `alert_configs` rows, shortest prefix first. Also returns a
/// provenance map from dotted JSON path to the source prefix/name (`Some`)
/// or controller default (`None`) that provided each leaf value.
///
/// If `defaults` is provided, it is merged as the lowest-priority layer.
type Provenance = std::collections::BTreeMap<String, Option<String>>;

pub async fn fetch_alert_config_with_provenance(
    catalog_name: &str,
    db: impl sqlx::PgExecutor<'static>,
    defaults: Option<&models::AlertConfig>,
) -> anyhow::Result<(models::AlertConfig, Provenance)> {
    let candidates = ancestor_prefixes_and_name(catalog_name);

    let layers: Vec<(String, TextJson<serde_json::Value>)> = sqlx::query_as(
        r#"
        select catalog_prefix_or_name, config
        from alert_configs
        where catalog_prefix_or_name = any($1)
        order by length(catalog_prefix_or_name) asc
        "#,
    )
    .bind(&candidates)
    .fetch_all(db)
    .await?;

    let mut merged = serde_json::Value::Object(Default::default());
    let mut provenance = Provenance::new();

    if let Some(defaults) = defaults {
        let json = serde_json::to_value(defaults)
            .map_err(|e| anyhow::anyhow!("serializing alert config defaults: {e}"))?;
        deep_merge(&mut merged, json, None, &mut provenance, "");
    }
    for (source, TextJson(config)) in layers {
        deep_merge(&mut merged, config, Some(&source), &mut provenance, "");
    }

    let config: models::AlertConfig = serde_json::from_value(merged)
        .map_err(|e| anyhow::anyhow!("deserializing merged alert_config: {e}"))?;
    Ok((config, provenance))
}

/// Builds the set of candidate `catalog_prefix_or_name` values that could
/// match `catalog_name`: every ancestor prefix (each suffix after a `/`)
/// plus the exact name itself. Example: for `acmeCo/prod/source-pg` this
/// returns `["acmeCo/", "acmeCo/prod/", "acmeCo/prod/source-pg"]`.
fn ancestor_prefixes_and_name(catalog_name: &str) -> Vec<String> {
    catalog_name
        .match_indices('/')
        .map(|(i, _)| catalog_name[..=i].to_string())
        .chain(std::iter::once(catalog_name.to_string()))
        .collect()
}

fn deep_merge(
    dst: &mut serde_json::Value,
    src: serde_json::Value,
    source: Option<&str>,
    provenance: &mut Provenance,
    path: &str,
) {
    if let serde_json::Value::Object(src_map) = src {
        if !dst.is_object() {
            *dst = serde_json::Value::Object(Default::default());
        }
        let dst_map = dst.as_object_mut().unwrap();
        for (k, v) in src_map {
            let child = if path.is_empty() {
                k.clone()
            } else {
                format!("{path}.{k}")
            };
            deep_merge(
                dst_map.entry(k).or_insert(serde_json::Value::Null),
                v,
                source,
                provenance,
                &child,
            );
        }
    } else {
        *dst = src;
        provenance.insert(path.to_string(), source.map(str::to_string));
    }
}

/// Returns `alert_data_processing.evaluation_interval` for `catalog_name`, or
/// `None` if no row exists. This is the fallback `DataMovementStalled`
/// threshold when `alert_configs` does not configure one.
/// TODO(js): Once we finish the configurable alert conditions migration and nothing
/// writes to this table anymore, we can remove this.
pub async fn fetch_legacy_data_movement_stalled_threshold(
    catalog_name: &str,
    db: impl sqlx::PgExecutor<'static>,
) -> anyhow::Result<Option<chrono::Duration>> {
    let Some(secs) = sqlx::query_scalar!(
        r#"
        select extract(epoch from evaluation_interval)::float8 as "secs!: f64"
        from alert_data_processing
        where catalog_name = $1
        "#,
        catalog_name,
    )
    .fetch_optional(db)
    .await?
    else {
        return Ok(None);
    };
    Ok(Some(chrono::Duration::milliseconds((secs * 1000.0) as i64)))
}

/// Returns total bytes processed for `catalog_name` from `since` onward using
/// `catalog_stats_hourly`.
///
/// The sum includes `bytes_written_by_me`, `bytes_written_to_me`, and
/// `bytes_read_by_me`. The lower bound is rounded down to the containing hour
/// to match the table's hourly grain.
pub async fn fetch_bytes_processed_since(
    catalog_name: &str,
    since: DateTime<Utc>,
    db: impl sqlx::PgExecutor<'static>,
) -> sqlx::Result<i64> {
    sqlx::query_scalar!(
        r#"
        select coalesce(
            sum(bytes_written_by_me + bytes_written_to_me + bytes_read_by_me),
            0
        )::bigint as "bytes!: i64"
        from catalog_stats_hourly
        where catalog_name = $1
            and ts >= date_trunc('hour', $2::timestamptz)
        "#,
        catalog_name,
        since,
    )
    .fetch_one(db)
    .await
}

#[tracing::instrument(level = "debug", skip(txn, status, controller_version))]
pub async fn update_status(
    txn: &mut sqlx::PgConnection,
    live_spec_id: Id,
    controller_version: i32,
    status: &models::status::ControllerStatus,
    failures: i32,
    error: Option<&str>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        insert into controller_jobs(live_spec_id, controller_version, status, failures, error)
        values ($1, $2, $3, $4, $5)
        on conflict (live_spec_id) do update set
            controller_version = $2,
            status = $3,
            failures = $4,
            error = $5,
            updated_at = now()
        where controller_jobs.live_spec_id = $1;
        "#,
        live_spec_id as Id,
        controller_version as i32,
        status as &models::status::ControllerStatus,
        failures,
        error,
    )
    .execute(txn)
    .await?;
    Ok(())
}

/// Sends the given message to the controller for every collection, enabled
/// task, and test under the given `catalog_prefix`. Returns the number of
/// controllers that were sent the message. The message will _not_ be sent to
/// any disabled captures or materializations, but it will be sent to disabled
/// derivations since those are still just collections. This filtering is just
/// an optimization, since over time we tend to accumulate quite a few more
/// disabled tasks than enabled ones.
pub async fn broadcast_to_prefix<T: serde::Serialize>(
    catalog_prefix: &str,
    message: T,
    db: &mut sqlx::PgConnection,
) -> anyhow::Result<i64> {
    let message_json = serde_json::to_value(message)?;
    let result = sqlx::query_scalar!(r#"
        with ids as (
          select controller_task_id
          from live_specs
          where catalog_name::text ^@ $1
          and coalesce(spec->'shards'->>'disable', 'false') = 'false'
        ),
        sends as (
          select 1 as sent, internal.send_to_task(controller_task_id::flowid, '0000000000000000'::flowid, $2::json)
          from ids
        )
        select count(sent) from sends
          "#,
        catalog_prefix,
        TextJson(message_json) as TextJson<serde_json::Value>,
    ).fetch_one(db).await?;
    Ok(result.unwrap_or(0))
}

/// Trigger a controller sync of all dependents of the given `live_spec_id`.
#[tracing::instrument(err, ret, skip(pool))]
pub async fn notify_dependents(live_spec_id: Id, pool: &sqlx::PgPool) -> sqlx::Result<u64> {
    // If the spec is a source, then notify all all targets, but only if the flow_type is
    // not 'capture'. Capture flows treat the capture as the source. But in terms of publication
    // dependencies, the capture depends on the collection, not the other way around. (Because the
    // capture spec embeds the collection spec.)
    // We send a zero-valued id as the sender in `send_to_task` because we don't
    // currently use the sender for anything, so it doesn't seem worthwhile to
    // thread it through.
    let result = sqlx::query!(
        r#"
        with dependents as (
            select lsf.target_id as id
            from live_spec_flows lsf
            where lsf.source_id = $1 and lsf.flow_type != 'capture'
            union
            select lsf.source_id as id
            from live_spec_flows lsf
            where lsf.target_id = $1 and lsf.flow_type = 'capture'
        ),
        dependent_tasks as (
            select ls.controller_task_id
            from dependents
            join live_specs ls on dependents.id = ls.id
        )
        select internal.send_to_task(
            dependent_tasks.controller_task_id,
            '0000000000000000'::flowid,
            '{"type":"dependency_updated"}'
        )
        from dependent_tasks
        "#,
        live_spec_id as Id,
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

#[cfg(test)]
mod test {
    use serde_json::json;

    fn deep_merge(dst: &mut serde_json::Value, src: serde_json::Value) {
        super::deep_merge(dst, src, None, &mut Default::default(), "");
    }

    #[test]
    fn deep_merge_objects_recurse_per_key() {
        let mut dst = json!({ "a": { "x": 1, "y": 2 }, "b": 3 });
        deep_merge(&mut dst, json!({ "a": { "y": 20, "z": 30 }, "c": 4 }));
        assert_eq!(
            dst,
            json!({ "a": { "x": 1, "y": 20, "z": 30 }, "b": 3, "c": 4 })
        );
    }

    #[test]
    fn deep_merge_scalar_replaces() {
        let mut dst = json!({ "a": 1 });
        deep_merge(&mut dst, json!({ "a": 2 }));
        assert_eq!(dst, json!({ "a": 2 }));
    }

    #[test]
    fn deep_merge_non_object_replaces_object() {
        let mut dst = json!({ "a": { "x": 1 } });
        deep_merge(&mut dst, json!({ "a": null }));
        assert_eq!(dst, json!({ "a": null }));
    }

    #[test]
    fn deep_merge_object_replaces_scalar() {
        let mut dst = json!({ "a": 1 });
        deep_merge(&mut dst, json!({ "a": { "x": 2 } }));
        assert_eq!(dst, json!({ "a": { "x": 2 } }));
    }

    #[test]
    fn ancestor_prefixes_and_name_splits_correctly() {
        assert_eq!(
            super::ancestor_prefixes_and_name("acmeCo/prod/source-pg"),
            vec!["acmeCo/", "acmeCo/prod/", "acmeCo/prod/source-pg"]
        );
        assert_eq!(
            super::ancestor_prefixes_and_name("acmeCo/task"),
            vec!["acmeCo/", "acmeCo/task"]
        );
    }

    #[test]
    fn deep_merge_empty_objects() {
        let mut dst = json!({});
        deep_merge(&mut dst, json!({ "a": 1 }));
        assert_eq!(dst, json!({ "a": 1 }));

        let mut dst = json!({ "a": 1 });
        deep_merge(&mut dst, json!({}));
        assert_eq!(dst, json!({ "a": 1 }));
    }

    #[test]
    fn provenance_tracks_leaf_sources() {
        let mut merged = json!({});
        let mut provenance = Default::default();

        super::deep_merge(
            &mut merged,
            json!({ "a": { "x": 1 }, "b": 2 }),
            Some("layer1"),
            &mut provenance,
            "",
        );
        super::deep_merge(
            &mut merged,
            json!({ "a": { "y": 3 } }),
            Some("layer2"),
            &mut provenance,
            "",
        );

        insta::assert_json_snapshot!(
            "provenance_tracks_leaf_sources",
            json!({
                "merged": merged,
                "provenance": provenance,
            })
        );
    }

    #[test]
    fn provenance_defaults_then_override() {
        let mut merged = json!({});
        let mut provenance = Default::default();

        super::deep_merge(
            &mut merged,
            json!({ "a": { "x": 1 } }),
            None,
            &mut provenance,
            "",
        );
        super::deep_merge(
            &mut merged,
            json!({ "a": { "x": 99 } }),
            Some("override"),
            &mut provenance,
            "",
        );

        insta::assert_json_snapshot!(
            "provenance_defaults_then_override",
            json!({
                "merged": merged,
                "provenance": provenance,
            })
        );
    }
}
