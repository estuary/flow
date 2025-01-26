use super::App;
use anyhow::Context;
use std::sync::Arc;

// Snapshot is a point-in-time view of control-plane state
// that influences authorization decisions.
pub struct Snapshot {
    // Time immediately before the snapshot was taken.
    pub taken: chrono::DateTime<chrono::Utc>,
    // Platform collections, indexed on `journal_template_name`.
    pub collections: Vec<SnapshotCollection>,
    // Indices of `collections`, indexed on `collection_name`.
    pub collections_idx_name: Vec<usize>,
    // Platform data-planes.
    pub data_planes: tables::DataPlanes,
    // Data-plane migrations underway.
    pub migrations: Vec<SnapshotMigration>,
    // Platform role grants.
    pub role_grants: tables::RoleGrants,
    // Platform user grants.
    pub user_grants: tables::UserGrants,
    // Platform tasks, indexed on `shard_template_id`.
    pub tasks: Vec<SnapshotTask>,
    // Indices of `tasks`, indexed on `task_name`.
    pub tasks_idx_name: Vec<usize>,
    // `refresh` is take()-en when the current snapshot should be refreshed.
    pub refresh_tx: Option<futures::channel::oneshot::Sender<()>>,
}

// SnapshotCollection is the state of a live collection which influences authorization.
// It's indexed on `journal_template_name`.
pub struct SnapshotCollection {
    // Template journal name which prefixes all journals of the collection.
    pub journal_template_name: String,
    // Catalog name of the collection.
    pub collection_name: models::Collection,
    // Data-plane where this collection lives.
    pub data_plane_id: models::Id,
}

// SnapshotTask is the state of a live task which influences authorization.
// It's indexed on `shard_template_id`.
#[derive(Debug)]
pub struct SnapshotTask {
    // Template shard ID which prefixes all shard IDs of the task.
    pub shard_template_id: String,
    // Catalog name of the task.
    pub task_name: models::Name,
    // Catalog type of the task.
    pub spec_type: models::CatalogType,
    // Data-plane where this task lives.
    pub data_plane_id: models::Id,
}

// SnapshotMigration is the state of an underway data-plane migration.
#[derive(Debug)]
pub struct SnapshotMigration {
    // Catalog prefix to be migrated. This is *not* always slash terminated,
    // so that we can bulk migrate (for example) all tenants that start with 'e'.
    pub catalog_prefix: String,
    // Cordoning cutoff for this migration.
    // No authorizations are allowed to extend beyond `cordon_at`.
    pub cordon_at: chrono::DateTime<chrono::Utc>,
    // Data-plane being migrated from.
    pub src_plane_id: models::Id,
    // Data-plane being migrated to.
    pub tgt_plane_id: models::Id,
}

impl Snapshot {
    pub fn empty() -> Self {
        Self {
            taken: chrono::DateTime::UNIX_EPOCH,
            collections: Vec::new(),
            collections_idx_name: Vec::new(),
            data_planes: tables::DataPlanes::default(),
            migrations: Vec::new(),
            role_grants: tables::RoleGrants::default(),
            user_grants: tables::UserGrants::default(),
            tasks: Vec::new(),
            tasks_idx_name: Vec::new(),
            refresh_tx: None,
        }
    }

    /// Evaluate an authorization requested at time `iat`, which is evaluated
    /// according to `policy`, and returning one of:
    ///
    /// - `Ok((expiration, ok))`
    /// The authorization is valid through the given expiration.
    /// - `Err(Ok(retry_after))`
    /// The status of the authorization is not yet know, and should be retried
    /// after the returned TimeDelta.
    /// - Err(Err(err)):
    /// The authorization is invalid.
    ///
    /// The Policy function must return one of:
    /// - `Ok((None, ok))`
    /// The authorization is valid.
    /// - `Ok((Some(cordon_at), ok))`
    /// The authorization is valid, but is cordoned for migration after `cordon_at`.
    /// - `Err(err)`
    /// The authorization is invalid.
    ///
    pub fn evaluate<P, Ok>(
        mu: &std::sync::RwLock<Self>,
        started: chrono::DateTime<chrono::Utc>,
        policy: P,
    ) -> Result<(chrono::DateTime<chrono::Utc>, Ok), Result<std::time::Duration, anyhow::Error>>
    where
        P: FnOnce(&Self) -> anyhow::Result<(Option<chrono::DateTime<chrono::Utc>>, Ok)>,
    {
        let snapshot = mu.read().unwrap();

        // Select an expiration for the evaluated authorization (presuming it succeeds)
        // which is at-most MAX_AUTHORIZATION in the future relative to the Snapshot time.
        // Then, jitter to smooth the load of client re-authorizations over time.
        use rand::Rng;
        let exp = snapshot.taken
            + chrono::TimeDelta::seconds(rand::thread_rng().gen_range(
                (Snapshot::MAX_AUTHORIZATION.num_seconds() / 2)
                    ..Snapshot::MAX_AUTHORIZATION.num_seconds(),
            ));

        match policy(&snapshot) {
            // Authorization is valid and not cordoned.
            Ok((None, ok)) => return Ok((exp, ok)),
            // Authorization is valid but cordoned in the future.
            Ok((Some(cordon_at), ok)) if cordon_at > started => {
                return Ok((std::cmp::min(exp, cordon_at), ok))
            }
            // Authorization is invalid and the Snapshot was taken after the
            // start of the authorization request. Terminal failure.
            Err(err) if snapshot.taken > started => return Err(Err(err)),

            // Authorization is valid but is currently cordoned, and we must
            // hold it in limbo until the cordoned condition is resolved
            // by a future Snapshot.
            Ok((Some(_cordon_at), _ok)) => (),
            // Authorization is invalid but the Snapshot is older than the start
            // of the authorization request. It's possible that the requestor has
            // more-recent knowledge that the authorization is valid.
            Err(_err) => (),
        };

        // We must await a future Snapshot to determine the definitive outcome.

        let backoff =
            // Determine the remaining "cool off" time before the next Snapshot starts.
            std::cmp::max(
                (snapshot.taken + Snapshot::MIN_REFRESH_INTERVAL) - chrono::Utc::now(),
                chrono::TimeDelta::zero(),
            )
            // We don't know how long a Snapshot fetch will take -- currently it's ~1-5 seconds,
            // but our real objective here is to smooth the herd of retries awaiting a refresh.
            + chrono::TimeDelta::milliseconds(rand::thread_rng().gen_range(500..10_000));

        Snapshot::signal_refresh(snapshot, mu);

        Err(Ok(backoff.to_std().unwrap()))
    }

    // Retrieve task having the exact catalog `name`.
    pub fn task_by_catalog_name<'s>(&'s self, name: &str) -> Option<&'s SnapshotTask> {
        self.tasks_idx_name
            .binary_search_by(|i| self.tasks[*i].task_name.as_str().cmp(name))
            .ok()
            .map(|index| {
                let task = &self.tasks[self.tasks_idx_name[index]];
                assert_eq!(task.task_name.as_str(), name);
                task
            })
    }

    // Retrieve the task having a template ID matching `shard_id`.
    // `shard_id` must equal or have a *more* specific suffix than `shard_template_id`.
    pub fn task_by_shard_id<'s>(&'s self, shard_id: &str) -> Option<&'s SnapshotTask> {
        self.tasks
            .binary_search_by(|task| {
                if shard_id.starts_with(&task.shard_template_id) {
                    std::cmp::Ordering::Equal
                } else {
                    task.shard_template_id.as_str().cmp(shard_id)
                }
            })
            .ok()
            .map(|index| &self.tasks[index])
    }

    // Retrieve the collection having the exact catalog `name`.
    pub fn collection_by_catalog_name<'s>(&'s self, name: &str) -> Option<&'s SnapshotCollection> {
        self.collections_idx_name
            .binary_search_by(|i| self.collections[*i].collection_name.as_str().cmp(name))
            .ok()
            .map(|index| {
                let collection = &self.collections[self.collections_idx_name[index]];
                assert_eq!(collection.collection_name.as_str(), name);
                collection
            })
    }

    // Retrieve the collection having a template name matching `journal_name`.
    // `journal_name` must equal or have a *more* specific suffix than `journal_template_name`.
    pub fn collection_by_journal_name<'s>(
        &'s self,
        journal_name: &str,
    ) -> Option<&'s SnapshotCollection> {
        self.collections
            .binary_search_by(|collection| {
                if journal_name.starts_with(&collection.journal_template_name) {
                    std::cmp::Ordering::Equal
                } else {
                    collection.journal_template_name.as_str().cmp(journal_name)
                }
            })
            .ok()
            .map(|index| &self.collections[index])
    }

    // If there is a migration which covers `catalog_name`, running in `data_plane`,
    // then retrieve the time at which it's cordoned.
    pub fn cordon_at<'s>(
        &'s self,
        catalog_name: &str,
        data_plane: &tables::DataPlane,
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        self.migrations
            .binary_search_by(|migration| {
                if catalog_name.starts_with(&migration.catalog_prefix) {
                    std::cmp::Ordering::Equal
                } else {
                    migration.catalog_prefix.as_str().cmp(catalog_name)
                }
            })
            .ok()
            .and_then(|index| {
                let migration = &self.migrations[index];

                if migration.src_plane_id == data_plane.control_id {
                    Some(migration.cordon_at)
                } else {
                    None
                }
            })
    }

    fn signal_refresh<'m>(
        guard: std::sync::RwLockReadGuard<'_, Self>,
        mu: &'m std::sync::RwLock<Self>,
    ) {
        if guard.refresh_tx.is_none() {
            return; // Refresh is already underway.
        }
        // We must release our read-lock before we can acquire a write lock.
        std::mem::drop(guard);

        if let Some(tx) = mu.write().unwrap().refresh_tx.take() {
            () = tx.send(()).unwrap(); // Begin a refresh.
        }
    }

    // Minimal interval between Snapshot refreshes.
    // We will postpone a requested refresh prior to this interval.
    const MIN_REFRESH_INTERVAL: chrono::TimeDelta = chrono::TimeDelta::seconds(20);
    // Maximum interval between Snapshot refreshes.
    // We will refresh an older Snapshot in the background.
    const MAX_REFRESH_INTERVAL: chrono::TimeDelta = chrono::TimeDelta::minutes(5);
    // Maximum lifetime of an authorization produced from a Snapshot,
    // relative to the timestamp at which the Snapshot was taken.
    // This upper-bounds the lifetime of an authorization derived from a Snapshot.
    const MAX_AUTHORIZATION: chrono::TimeDelta = chrono::TimeDelta::minutes(80);
}

pub async fn fetch_loop(app: Arc<App>) {
    loop {
        let (next_tx, next_rx) = futures::channel::oneshot::channel();

        // We'll minimally wait for MIN_REFRESH_INTERVAL each iteration.
        let cooloff = tokio::time::sleep(Snapshot::MIN_REFRESH_INTERVAL.to_std().unwrap());
        // We'll wait for the first of `next_rx` or MAX_REFRESH_INTERVAL each iteration.
        let next_rx =
            tokio::time::timeout(Snapshot::MAX_REFRESH_INTERVAL.to_std().unwrap(), next_rx);

        match try_fetch(&app.pg_pool).await {
            Ok(mut snapshot) => {
                snapshot.refresh_tx = Some(next_tx);
                *app.snapshot.write().unwrap() = snapshot;
            }
            Err(err) => {
                tracing::error!(?err, "failed to fetch snapshot (will retry)");
                _ = next_tx.send(()); // Wake ourselves to retry.
            }
        }
        let ((), _) = futures::join!(cooloff, next_rx);
    }
}

async fn try_fetch(pg_pool: &sqlx::PgPool) -> anyhow::Result<Snapshot> {
    tracing::info!("started to fetch authorization snapshot");
    let taken = chrono::Utc::now();

    let mut collections = sqlx::query_as!(
        SnapshotCollection,
        r#"
            select
                journal_template_name as "journal_template_name!",
                catalog_name as "collection_name: models::Collection",
                data_plane_id as "data_plane_id: models::Id"
            from live_specs
            where journal_template_name is not null
            "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch view of live collections")?;

    let data_planes = sqlx::query_as!(
        tables::DataPlane,
        r#"
            select
                id as "control_id: models::Id",
                data_plane_name,
                data_plane_fqdn,
                false as "is_default!: bool",
                hmac_keys,
                broker_address,
                reactor_address,
                ops_logs_name as "ops_logs_name: models::Collection",
                ops_stats_name as "ops_stats_name: models::Collection"
            from data_planes
            "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch data_planes")?;

    let mut migrations = sqlx::query_as!(
        SnapshotMigration,
        r#"
            select
                catalog_prefix,
                src_plane_id as "src_plane_id: models::Id",
                tgt_plane_id as "tgt_plane_id: models::Id",
                cordon_at
            from internal.migrations
            "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch migrations")?;

    let role_grants = sqlx::query_as!(
        tables::RoleGrant,
        r#"
            select
                subject_role as "subject_role: models::Prefix",
                object_role as "object_role: models::Prefix",
                capability as "capability: models::Capability"
            from role_grants
            "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch role_grants")?;

    let user_grants = sqlx::query_as!(
        tables::UserGrant,
        r#"
            select
                user_id as "user_id: uuid::Uuid",
                object_role as "object_role: models::Prefix",
                capability as "capability: models::Capability"
            from user_grants
            "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch role_grants")?;

    let mut tasks = sqlx::query_as!(
        SnapshotTask,
        r#"
            select
                shard_template_id as "shard_template_id!",
                catalog_name as "task_name: models::Name",
                spec_type as "spec_type!: models::CatalogType",
                data_plane_id as "data_plane_id: models::Id"
            from live_specs
            where shard_template_id is not null
            "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch view of live tasks")?;

    let data_planes = tables::DataPlanes::from_iter(data_planes);
    let role_grants = tables::RoleGrants::from_iter(role_grants);
    let user_grants = tables::UserGrants::from_iter(user_grants);

    migrations.sort_by(|l, r| l.catalog_prefix.cmp(&r.catalog_prefix));

    // Shard ID and journal name templates are prefixes which are always
    // extended with a slash-separated suffix. Avoid inadvertent matches
    // over path component prefixes.
    for task in &mut tasks {
        task.shard_template_id.push('/');
    }
    for collection in &mut collections {
        collection.journal_template_name.push('/');
    }

    tasks.sort_by(|t1, t2| t1.shard_template_id.cmp(&t2.shard_template_id));
    collections.sort_by(|c1, c2| c1.journal_template_name.cmp(&c2.journal_template_name));

    let mut collections_idx_name = Vec::from_iter(0..collections.len());
    collections_idx_name.sort_by(|i1, i2| {
        collections[*i1]
            .collection_name
            .cmp(&collections[*i2].collection_name)
    });
    let mut tasks_idx_name = Vec::from_iter(0..tasks.len());
    tasks_idx_name.sort_by(|i1, i2| tasks[*i1].task_name.cmp(&tasks[*i2].task_name));

    tracing::info!(
        collections = collections.len(),
        data_planes = data_planes.len(),
        migrations = migrations.len(),
        role_grants = role_grants.len(),
        tasks = tasks.len(),
        user_grants = user_grants.len(),
        "fetched authorization snapshot",
    );

    Ok(Snapshot {
        taken,
        collections,
        collections_idx_name,
        data_planes,
        migrations,
        role_grants,
        user_grants,
        tasks,
        tasks_idx_name,
        refresh_tx: None,
    })
}
