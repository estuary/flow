use std::collections::HashMap;

use chrono::{DateTime, Utc};
use models::status::{ConnectorStatus, ControllerStatus, StatusSummaryType, Summary};

use super::PgDataLoader;

/// Status info related to the controller
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct Controller {
    pub next_run: Option<DateTime<Utc>>,
    pub error: Option<String>,
    pub failures: i32,
    /// The top-level fields of the controller status json are flattened into this struct
    /// in order to avoid the stuttering of `status.controller.status`.
    #[graphql(flatten)]
    pub status: ControllerStatus,
    pub updated_at: DateTime<Utc>,
}

/// The status of a LiveSpec
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct Status {
    pub r#type: StatusSummaryType,
    pub summary: String,
    pub controller: Controller,
    pub connector: Option<ConnectorStatus>,
}

impl Status {
    pub fn missing(catalog_type: models::CatalogType) -> Self {
        Status {
            r#type: StatusSummaryType::Error,
            summary: "No status information available".to_string(),
            // Set error and failures here to make sure it's obvious to the caller that something is wrong
            controller: Controller {
                next_run: None,
                error: Some("controller status information unavailable".to_string()),
                failures: 1,
                status: ControllerStatus::new(catalog_type),
                updated_at: Utc::now(),
            },
            connector: None,
        }
    }
}

impl TryFrom<StatusRow> for Status {
    type Error = serde_json::Error;

    fn try_from(value: StatusRow) -> Result<Self, Self::Error> {
        let StatusRow {
            catalog_name: _,
            disabled,
            last_build_id,
            connector_status,
            controller_next_run,
            controller_updated_at,
            controller_status_json,
            controller_version,
            controller_error,
            controller_failures,
        } = value;

        let controller_status = if controller_version == 0 {
            ControllerStatus::Uninitialized
        } else {
            serde_json::from_str(controller_status_json.as_str())?
        };

        let summary = Summary::of(
            disabled,
            last_build_id,
            controller_error.as_deref(),
            Some(&controller_status).filter(|s| !s.is_uninitialized()),
            connector_status.as_ref(),
        );

        Ok(Status {
            r#type: summary.status,
            summary: summary.message,
            controller: Controller {
                next_run: controller_next_run,
                error: controller_error,
                failures: controller_failures,
                status: controller_status,
                updated_at: controller_updated_at,
            },
            connector: connector_status,
        })
    }
}

/// Used to load status by catalog name
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct StatusKey(pub String);

impl async_graphql::dataloader::Loader<StatusKey> for PgDataLoader {
    type Value = Status;

    type Error = String;

    async fn load(
        &self,
        keys: &[StatusKey],
    ) -> Result<HashMap<StatusKey, Self::Value>, Self::Error> {
        let names: Vec<&str> = keys.iter().map(|k| k.0.as_str()).collect();
        let statuses = fetch_status(&self.0, &names)
            .await
            .map_err(|e| e.to_string())?;
        Ok(statuses)
    }
}

struct StatusRow {
    catalog_name: String,
    last_build_id: models::Id,
    disabled: bool,
    controller_version: i32,
    connector_status: Option<ConnectorStatus>,
    controller_next_run: Option<DateTime<Utc>>,
    controller_updated_at: DateTime<Utc>,
    controller_status_json: String,
    controller_error: Option<String>,
    controller_failures: i32,
}

async fn fetch_status(
    pool: &sqlx::PgPool,
    catalog_names: &[&str],
) -> anyhow::Result<HashMap<StatusKey, Status>> {
    let rows = sqlx::query_as!(
        StatusRow,
        r#"select
        ls.catalog_name as "catalog_name: String",
        ls.last_build_id as "last_build_id: models::Id",
        coalesce(ls.spec->'shards'->>'disable', ls.spec->'derive'->'shards'->>'disable', 'false') = 'true' as "disabled!: bool",
        cj.controller_version as "controller_version: i32",
        cs.flow_document as "connector_status?: ConnectorStatus",
        t.wake_at as "controller_next_run: DateTime<Utc>",
        cj.updated_at as "controller_updated_at: DateTime<Utc>",
        coalesce(cj.status::text, '{}') as "controller_status_json!: String",
        cj.error as "controller_error: String",
        cj.failures as "controller_failures: i32"
    from live_specs ls
    join controller_jobs cj on ls.id = cj.live_spec_id
    join internal.tasks t on ls.controller_task_id = t.task_id
    left outer join connector_status cs on ls.catalog_name = cs.catalog_name
    where ls.catalog_name::text = any($1::text[])
    and ls.spec_type is not null
        "#,
        catalog_names as &[&str],
    )
    .fetch_all(pool)
    .await?;

    let resp = rows
        .into_iter()
        .map(|mut row| {
            let name = StatusKey(std::mem::take(&mut row.catalog_name));
            let status = Status::try_from(row)?;
            Ok((name, status))
        })
        .collect::<anyhow::Result<HashMap<StatusKey, Status>>>()?;
    Ok(resp)
}
