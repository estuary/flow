use super::App;
use anyhow::Context;
use std::sync::Arc;

// Snapshot is a point-in-time view of control-plane state
// that influences authorization decisions.
pub struct Snapshot {
    // Time immediately before the snapshot was taken.
    pub taken: std::time::SystemTime,
    // Platform collections, indexed on `journal_template_name`.
    pub collections: Vec<SnapshotCollection>,
    // Indices of `collections`, indexed on `collection_name`.
    pub collections_idx_name: Vec<usize>,
    // Platform data-planes.
    pub data_planes: tables::DataPlanes,
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

impl Snapshot {
    pub fn evaluate<P, Ok>(
        mu: &std::sync::RwLock<Self>,
        iat: u64,
        policy: P,
    ) -> Result<Ok, Result<u64, anyhow::Error>>
    where
        P: FnOnce(&Self) -> anyhow::Result<Ok>,
    {
        let guard = mu.read().unwrap();

        let taken_unix = guard
            .taken
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // If the snapshot is too old then the client MUST retry.
        if iat > taken_unix + Snapshot::MAX_INTERVAL.as_secs() {
            Self::begin_refresh(guard, mu);

            return Err(Ok(jitter_millis()));
        }

        match policy(&guard) {
            Ok(ok) => Ok(ok),
            Err(err) if taken_unix > iat => {
                // The snapshot was taken AFTER the authorization request was minted,
                // which means the request cannot have prior knowledge of upcoming
                // state re-configurations, and this is a terminal error.
                Err(Err(err))
            }
            Err(_) => {
                let retry_millis = if let Some(remaining) =
                    Snapshot::MIN_INTERVAL.checked_sub(guard.taken.elapsed().unwrap_or_default())
                {
                    // Our current snapshot isn't old enough.
                    remaining.as_millis() as u64
                } else {
                    Snapshot::begin_refresh(guard, mu);
                    0
                } + jitter_millis();

                Err(Ok(retry_millis))
            }
        }
    }

    pub fn task_by_catalog_name<'s>(&'s self, name: &models::Name) -> Option<&'s SnapshotTask> {
        self.tasks_idx_name
            .binary_search_by(|i| self.tasks[*i].task_name.as_str().cmp(name))
            .ok()
            .map(|index| {
                let task = &self.tasks[self.tasks_idx_name[index]];
                assert_eq!(&task.task_name, name);
                task
            })
    }

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

    fn begin_refresh<'m>(
        guard: std::sync::RwLockReadGuard<'_, Self>,
        mu: &'m std::sync::RwLock<Self>,
    ) {
        // We must release our read-lock before we can acquire a write lock.
        std::mem::drop(guard);

        if let Some(tx) = mu.write().unwrap().refresh_tx.take() {
            () = tx.send(()).unwrap(); // Begin a refresh.
        }
    }

    const MIN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(20);
    const MAX_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300); // 5 minutes.
}

pub fn seed() -> (Snapshot, futures::channel::oneshot::Receiver<()>) {
    let (next_tx, next_rx) = futures::channel::oneshot::channel();

    (
        Snapshot {
            taken: std::time::SystemTime::UNIX_EPOCH,
            collections: Vec::new(),
            collections_idx_name: Vec::new(),
            data_planes: tables::DataPlanes::default(),
            role_grants: tables::RoleGrants::default(),
            user_grants: tables::UserGrants::default(),
            tasks: Vec::new(),
            tasks_idx_name: Vec::new(),
            refresh_tx: Some(next_tx),
        },
        next_rx,
    )
}
pub async fn fetch_loop(app: Arc<App>, mut refresh_rx: futures::channel::oneshot::Receiver<()>) {
    while let Ok(()) = refresh_rx.await {
        let (next_tx, next_rx) = futures::channel::oneshot::channel();
        refresh_rx = next_rx;

        match try_fetch(&app.pg_pool).await {
            Ok(mut snapshot) => {
                snapshot.refresh_tx = Some(next_tx);
                *app.snapshot.write().unwrap() = snapshot;
            }
            Err(err) => {
                tracing::error!(?err, "failed to fetch snapshot (will retry)");
                () = tokio::time::sleep(Snapshot::MIN_INTERVAL).await;
                _ = next_tx.send(()); // Wake ourselves to retry.
            }
        };
    }
}

async fn try_fetch(pg_pool: &sqlx::PgPool) -> anyhow::Result<Snapshot> {
    tracing::info!("started to fetch authorization snapshot");
    let taken = std::time::SystemTime::now();

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
        role_grants,
        user_grants,
        tasks,
        tasks_idx_name,
        refresh_tx: None,
    })
}

fn jitter_millis() -> u64 {
    use rand::Rng;

    // The returned jitter must always be positive.
    // In production, it can take a few seconds to fetch a snapshot.
    rand::thread_rng().gen_range(500..10_000)
}
