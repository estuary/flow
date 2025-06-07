use anyhow::Result;
use automations::{task_types, Action, Executor, TaskType};
use models::Id;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::VecDeque;
use std::time::Duration;

// Import the migrate_data_planes function from the parent module (lib.rs)
use crate::migrate_data_planes;

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct MigrationTaskState {
    migration_id: Option<Id>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub enum MigrationTaskMessage {
    Initialize { migration_id: Id },
}

pub struct MigrationExecutor;

impl Executor for MigrationExecutor {
    const TASK_TYPE: TaskType = task_types::DATA_PLANE_MIGRATION;
    type Receive = MigrationTaskMessage;
    type State = MigrationTaskState;
    type Outcome = Action; // Outcome is just the next action

    async fn poll<'s>(
        &'s self,
        pool: &'s PgPool,
        task_id: Id,
        _parent_id: Option<Id>, // Unused for this task
        state: &'s mut Self::State,
        inbox: &'s mut VecDeque<(Id, Option<Self::Receive>)>,
    ) -> Result<Self::Outcome> {
        // Initialize state from message if migration_id is not already set
        if state.migration_id.is_none() {
            let Some((_sender_id, Some(message))) = inbox.pop_front() else {
                return Ok(Action::Suspend);
            };
            let MigrationTaskMessage::Initialize { migration_id } = message;
            state.migration_id = Some(migration_id);
            tracing::info!(%task_id, %migration_id, "initialized migration task");
        }
        let migration_id = state.migration_id.unwrap();

        // Fetch migration details, including joined data plane names
        let query_result = sqlx::query!(
            r#"
            SELECT
                dpm.catalog_name_or_prefix,
                dpm.cordon_at,
                dpm.active,
                src_dp.data_plane_name AS src_plane_name,
                tgt_dp.data_plane_name AS tgt_plane_name
            FROM public.data_plane_migrations dpm
            JOIN public.data_planes src_dp ON dpm.src_plane_id = src_dp.id
            JOIN public.data_planes tgt_dp ON dpm.tgt_plane_id = tgt_dp.id
            WHERE dpm.id = $1
            "#,
            migration_id as models::Id
        )
        .fetch_optional(pool)
        .await?;

        let Some(row) = query_result else {
            tracing::warn!(
                %migration_id,
                "data plane migration not found; marking task as done"
            );
            return Ok(Action::Done);
        };
        if !row.active {
            tracing::warn!(
                %migration_id,
                "data plane migration is not active; marking task as done"
            );
            return Ok(Action::Done);
        }

        let cordon_at = row.cordon_at;
        let src_plane_name = row.src_plane_name;
        let tgt_plane_name = row.tgt_plane_name;
        let catalog_name_or_prefix = row.catalog_name_or_prefix;

        // Check cordon_at time
        let now_utc = chrono::Utc::now();
        if now_utc < cordon_at {
            let sleep_duration = (cordon_at - now_utc)
                .to_std()
                .unwrap_or_else(|_| Duration::from_secs(1));
            // PostgreSQL INTERVAL doesn't support nanosecond precision, so truncate to microseconds
            let sleep_duration = Duration::from_micros(sleep_duration.as_micros() as u64);
            return Ok(Action::Sleep(sleep_duration));
        }

        tracing::info!(
            %migration_id,
            %src_plane_name,
            %tgt_plane_name,
            %catalog_name_or_prefix,
            "cordon time passed; starting migration"
        );

        migrate_data_planes(
            pool,
            &src_plane_name,
            &tgt_plane_name,
            &catalog_name_or_prefix,
        )
        .await?;

        // Mark the migration as completed
        sqlx::query!(
            "UPDATE data_plane_migrations SET active = false WHERE id = $1",
            migration_id as models::Id
        )
        .execute(pool)
        .await?;

        tracing::info!(%migration_id, "migration completed successfully");

        Ok(Action::Done)
    }
}
