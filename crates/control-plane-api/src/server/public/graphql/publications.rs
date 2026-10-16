use super::logs::LogLineConnection;

/// A user-initiated publication of a draft.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct Publication {
    id: models::Id,
    draft_id: models::Id,
    /// Whether the publication builds and validates without committing.
    dry_run: bool,
    /// Outcome so far. `type` is `queued` until the publication has been
    /// processed. `lockFailures` is non-empty only for `buildIdLockFailure`.
    status: models::publications::JobStatus,
    /// The ID that `lastPubId` takes on each live specification this
    /// publication changes. Null unless `status.type` is `success`.
    pub_id: Option<models::Id>,
    #[graphql(skip)]
    logs_token: uuid::Uuid,
}

#[async_graphql::ComplexObject]
impl Publication {
    /// Errors that caused the publication to fail, once `status.type` is no
    /// longer `queued`.
    async fn errors(
        &self,
        ctx: &async_graphql::Context<'_>,
    ) -> async_graphql::Result<Vec<models::draft_error::Error>> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        super::drafts::errors_for_draft(self.draft_id, user_id, &env.pg_pool).await
    }

    /// Publication logs in timestamp order. Pagination is best effort, and
    /// logs may arrive after the publication completes. `first` controls
    /// page size and cannot exceed 1000 log lines.
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
pub struct PublicationsQuery;

#[async_graphql::Object]
impl PublicationsQuery {
    /// Returns the publication if the caller initiated it, and null otherwise.
    async fn publication(
        &self,
        ctx: &async_graphql::Context<'_>,
        id: models::Id,
    ) -> async_graphql::Result<Option<Publication>> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        let row = sqlx::query!(
            r#"
            SELECT id AS "id!: models::Id", draft_id AS "draft_id!: models::Id", dry_run,
                   job_status AS "status!: sqlx::types::Json<models::publications::JobStatus>",
                   pub_id AS "pub_id: models::Id", logs_token
            FROM publications
            WHERE id = $1 AND user_id = $2
            "#,
            id as models::Id,
            user_id,
        )
        .fetch_optional(&env.pg_pool)
        .await?;

        // A queued row holds a generated `pub_id` until the executor replaces
        // it, so it is withheld until the status is success.
        Ok(row.map(|row| Publication {
            id: row.id,
            draft_id: row.draft_id,
            dry_run: row.dry_run,
            pub_id: row.pub_id.filter(|_| row.status.0.is_success()),
            status: row.status.0,
            logs_token: row.logs_token,
        }))
    }
}

#[cfg(test)]
mod test;
