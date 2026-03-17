use crate::Snapshot;
use std::sync::Arc;

pub struct GatedSnapshot {
    gate: bool,
    actual: Option<Snapshot>,
}

impl tokens::Source for GatedSnapshot {
    type Token = Snapshot;
    type Revoke = tokens::WaitForCancellationFutureOwned;

    async fn refresh(
        &mut self,
        _started: tokens::DateTime,
    ) -> tonic::Result<Result<(Self::Token, chrono::TimeDelta, Self::Revoke), chrono::TimeDelta>>
    {
        let snapshot = if self.gate {
            self.gate = false;
            Snapshot::empty()
        } else {
            self.actual
                .take()
                .expect("not refreshed again after actual snapshot")
        };

        let revoked = snapshot.revoke.clone().cancelled_owned();
        Ok(Ok((snapshot, chrono::TimeDelta::MAX, revoked)))
    }
}

pub async fn new_snapshot(pg_pool: sqlx::PgPool, gate: bool) -> Arc<dyn tokens::Watch<Snapshot>> {
    use tokens::Source;

    let mut actual = crate::snapshot::PgSnapshotSource::new(pg_pool);
    let (mut snapshot, _valid_for, _revoke) = actual
        .refresh(tokens::DateTime::UNIX_EPOCH)
        .await
        .unwrap()
        .unwrap();

    // Shift forward artificially so it's definitively "after" any following requests.
    snapshot.taken += chrono::TimeDelta::seconds(2);

    let source = GatedSnapshot {
        gate,
        actual: Some(snapshot),
    };
    tokens::watch(source).ready_owned().await
}
