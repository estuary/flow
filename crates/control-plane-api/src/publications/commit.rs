use std::future::Future;

use super::UncommittedBuild;
use models::Id;
use models::publications::JobStatus;

/// A trait for database updates that should be performed as part of committing the publication.
pub trait WithCommit: Send + Sync {
    /// Called with the in-progress transaction and the publication being
    /// committed. This is only ever called when the publication is successful
    /// and _not_ a `dry_run`. Returning an error from `before_commit` will
    /// cause the transaction to be rolled back.
    /// This function should not commit the transaction, and it should also take
    /// care to return quickly, so as not to leave the transaction open too long.
    fn before_commit(
        &self,
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        committing_pub: &UncommittedBuild,
        status: &JobStatus,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}
impl<'a, T: WithCommit> WithCommit for &'a T {
    fn before_commit(
        &self,
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        committing_pub: &UncommittedBuild,
        status: &JobStatus,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        <T as WithCommit>::before_commit(*self, txn, committing_pub, status)
    }
}
pub struct NoopWithCommit;
impl WithCommit for NoopWithCommit {
    fn before_commit(
        &self,
        _txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        _committing_pub: &UncommittedBuild,
        _status: &JobStatus,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        async { Ok(()) }
    }
}

impl<A, B> WithCommit for (A, B)
where
    A: WithCommit,
    B: WithCommit,
{
    async fn before_commit(
        &self,
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        committing_pub: &UncommittedBuild,
        status: &JobStatus,
    ) -> anyhow::Result<()> {
        self.0.before_commit(txn, committing_pub, status).await?;
        self.1.before_commit(txn, committing_pub, status).await?;
        Ok(())
    }
}

pub struct ClearDraftErrors {
    pub draft_id: Id,
}

impl WithCommit for ClearDraftErrors {
    async fn before_commit(
        &self,
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        _committing_pub: &UncommittedBuild,
        _status: &JobStatus,
    ) -> anyhow::Result<()> {
        crate::draft::delete_errors(self.draft_id, txn).await?;
        Ok(())
    }
}

pub struct UpdatePublicationsRow {
    pub id: Id,
}

impl WithCommit for UpdatePublicationsRow {
    async fn before_commit(
        &self,
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        committing_pub: &UncommittedBuild,
        status: &JobStatus,
    ) -> anyhow::Result<()> {
        crate::publications::db::resolve(self.id, status, Some(committing_pub.publication_id), txn)
            .await?;
        Ok(())
    }
}
