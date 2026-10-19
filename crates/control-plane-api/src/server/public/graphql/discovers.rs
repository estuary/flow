use super::logs::LogLineConnection;

/// An asynchronous capture discovery that merges its results into a draft.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct Discover {
    id: models::Id,
    draft_id: models::Id,
    capture_name: models::Name,
    data_plane_name: String,
    status: models::discovers::JobStatus,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
    #[graphql(skip)]
    logs_token: uuid::Uuid,
}

#[async_graphql::ComplexObject]
impl Discover {
    /// Errors currently recorded on the associated draft.
    async fn errors(
        &self,
        ctx: &async_graphql::Context<'_>,
    ) -> async_graphql::Result<Vec<models::draft_error::Error>> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        super::drafts::errors_for_draft(self.draft_id, user_id, &env.pg_pool).await
    }

    /// Discovery logs in timestamp order. Pagination is best effort, and logs
    /// may arrive after discovery completes.
    /// `first` controls page size and cannot exceed 1000 log lines.
    async fn logs(
        &self,
        ctx: &async_graphql::Context<'_>,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<LogLineConnection> {
        let env = ctx.data::<crate::Envelope>()?;
        super::logs::page(&env.pg_pool, self.logs_token, after, first).await
    }
}

#[derive(Debug, Default)]
pub struct DiscoversQuery;

#[async_graphql::Object]
impl DiscoversQuery {
    /// Returns a discover visible to the caller, or null.
    async fn discover(
        &self,
        ctx: &async_graphql::Context<'_>,
        id: models::Id,
    ) -> async_graphql::Result<Option<Discover>> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        let row = sqlx::query!(
            r#"
            SELECT di.id AS "id!: models::Id", di.draft_id AS "draft_id!: models::Id",
                   di.capture_name AS "capture_name!: models::Name", di.data_plane_name,
                   di.job_status AS "status!: sqlx::types::Json<models::discovers::JobStatus>",
                   di.created_at, di.updated_at, di.logs_token
            FROM discovers di
            JOIN drafts d ON d.id = di.draft_id
            WHERE di.id = $1 AND d.user_id = $2
            "#,
            id as models::Id,
            user_id,
        )
        .fetch_optional(&env.pg_pool)
        .await?;

        Ok(row.map(|row| Discover {
            id: row.id,
            draft_id: row.draft_id,
            capture_name: row.capture_name,
            data_plane_name: row.data_plane_name,
            status: row.status.0,
            created_at: row.created_at,
            updated_at: row.updated_at,
            logs_token: row.logs_token,
        }))
    }
}

#[derive(Debug, Default)]
pub struct DiscoversMutation;

#[async_graphql::Object]
impl DiscoversMutation {
    /// Queue discovery for a capture using its staged or live definition.
    /// Discovery updates the given draft with its results.
    async fn create_discover(
        &self,
        ctx: &async_graphql::Context<'_>,
        draft_id: models::Id,
        capture_name: models::Name,
        #[graphql(
            desc = "Optional data plane name for discovery. Selected automatically when omitted."
        )]
        data_plane: Option<String>,
    ) -> async_graphql::Result<Discover> {
        let env = ctx.data::<crate::Envelope>()?;
        let subject = env.claims()?.subject();

        super::verify_authorization(
            env,
            capture_name.as_str(),
            models::authz::Capability::SpecEdit,
        )
        .await?;

        let row = match crate::discovers::create(
            &env.pg_pool,
            env.snapshot(),
            &subject,
            draft_id,
            capture_name.as_str(),
            data_plane.as_deref(),
        )
        .await
        {
            Ok(row) => row,
            Err(error) => match error.downcast::<tonic::Status>() {
                Ok(status) => env.authorization_outcome(Err(status)).await?.1,
                Err(error) => return Err(error.into()),
            },
        };
        Ok(Discover {
            id: row.id,
            draft_id,
            capture_name,
            data_plane_name: row.data_plane_name,
            status: models::discovers::JobStatus::Queued,
            created_at: row.created_at,
            updated_at: row.updated_at,
            logs_token: row.logs_token,
        })
    }
}

#[cfg(test)]
mod test;
