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

#[derive(Debug, Default)]
pub struct PublicationsMutation;

#[async_graphql::Object]
impl PublicationsMutation {
    /// Queue a publication of the draft `draftId`, which the caller must own.
    /// The caller needs `SpecEdit` on every specification the publication
    /// would change, which can include live specifications connected to a
    /// drafted collection, and `CatalogRead` on every other name the draft
    /// references. Building, validation and the commit happen afterwards:
    /// poll `publication(id)` until `status.type` is no longer `queued`. A
    /// successful publication that is not a dry run deletes the draft.
    async fn create_publication(
        &self,
        ctx: &async_graphql::Context<'_>,
        draft_id: models::Id,
        #[graphql(desc = "Build and validate without committing.")] dry_run: bool,
        #[graphql(
            desc = "Data plane for specifications this publication creates; existing specifications keep theirs. Chosen automatically when omitted. A plane that cannot be used fails the publication, not this request."
        )]
        default_data_plane: Option<String>,
        #[graphql(desc = "Optional free-text description recorded on the publication.")]
        detail: Option<String>,
    ) -> async_graphql::Result<Publication> {
        let env = ctx.data::<crate::Envelope>()?;
        let subject = env.claims()?.subject();

        let (id, logs_token) = match crate::publications::submit::submit(
            &env.pg_pool,
            env.snapshot(),
            &subject,
            draft_id,
            dry_run,
            default_data_plane.as_deref(),
            detail.as_deref(),
        )
        .await
        {
            Ok(queued) => queued,
            Err(error) => match error.downcast::<tonic::Status>() {
                Ok(status) => env.authorization_outcome(Err(status)).await?.1,
                Err(error) => return Err(error.into()),
            },
        };
        Ok(Publication {
            id,
            draft_id,
            dry_run,
            status: models::publications::StatusType::Queued.into(),
            pub_id: None,
            logs_token,
        })
    }
}

#[cfg(test)]
mod test;
