use super::App;
use anyhow::Context;
use std::collections::HashMap;
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
    // Signing HMAC key of data-planes, keyed on data_plane_name
    pub data_planes_hmac_keys: HashMap<String, Vec<String>>,
    // Indices of `data_planes`, indexed on `data_plane_fqdn`.
    pub data_planes_idx_fqdn: Vec<usize>,
    // Indices of `data_planes`, indexed on `data_plane_name`.
    pub data_planes_idx_name: Vec<usize>,
    // Data-plane migrations that are underway.
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
    pub catalog_name_or_prefix: String,
    // Cordoning cutoff for this migration.
    // No authorizations are allowed to extend beyond `cordon_at`.
    pub cordon_at: chrono::DateTime<chrono::Utc>,
    // Data-plane being migrated from.
    pub src_plane_id: models::Id,
    // Data-plane being migrated to.
    pub tgt_plane_id: models::Id,
}

impl Snapshot {
    /// Construct a new, empty Snapshot.
    pub fn empty() -> Self {
        Self {
            taken: chrono::DateTime::UNIX_EPOCH,
            collections: Vec::new(),
            collections_idx_name: Vec::new(),
            data_planes: tables::DataPlanes::default(),
            data_planes_hmac_keys: HashMap::new(),
            data_planes_idx_fqdn: Vec::new(),
            data_planes_idx_name: Vec::new(),
            migrations: Vec::new(),
            role_grants: tables::RoleGrants::default(),
            user_grants: tables::UserGrants::default(),
            tasks: Vec::new(),
            tasks_idx_name: Vec::new(),
            refresh_tx: None,
        }
    }

    /// Construct a Snapshot from the provided tables.
    pub fn new(
        taken: chrono::DateTime<chrono::Utc>,
        mut collections: Vec<SnapshotCollection>,
        data_planes: Vec<tables::DataPlane>,
        data_planes_hmac_keys: HashMap<String, Vec<String>>,
        mut migrations: Vec<SnapshotMigration>,
        role_grants: Vec<tables::RoleGrant>,
        user_grants: Vec<tables::UserGrant>,
        mut tasks: Vec<SnapshotTask>,
    ) -> Self {
        let data_planes = tables::DataPlanes::from_iter(data_planes);
        let role_grants = tables::RoleGrants::from_iter(role_grants);
        let user_grants = tables::UserGrants::from_iter(user_grants);

        migrations.sort_by(|l, r| l.catalog_name_or_prefix.cmp(&r.catalog_name_or_prefix));

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
        let mut data_planes_idx_fqdn = Vec::from_iter(0..data_planes.len());
        let mut data_planes_idx_name = Vec::from_iter(0..data_planes.len());
        let mut tasks_idx_name = Vec::from_iter(0..tasks.len());

        collections_idx_name.sort_by(|i1, i2| {
            collections[*i1]
                .collection_name
                .cmp(&collections[*i2].collection_name)
        });
        data_planes_idx_fqdn.sort_by(|i1, i2| {
            data_planes[*i1]
                .data_plane_fqdn
                .cmp(&data_planes[*i2].data_plane_fqdn)
        });
        data_planes_idx_name.sort_by(|i1, i2| {
            data_planes[*i1]
                .data_plane_name
                .cmp(&data_planes[*i2].data_plane_name)
        });
        tasks_idx_name.sort_by(|i1, i2| tasks[*i1].task_name.cmp(&tasks[*i2].task_name));

        Snapshot {
            taken,
            collections,
            collections_idx_name,
            data_planes,
            data_planes_hmac_keys,
            data_planes_idx_fqdn,
            data_planes_idx_name,
            migrations,
            role_grants,
            user_grants,
            tasks,
            tasks_idx_name,
            refresh_tx: None,
        }
    }

    /// Evaluate an authorization requested at time `iat`, which is evaluated
    /// according to `policy`, and returning one of:
    ///
    /// Ok((expire_at, ok)):
    ///     The authorization is valid through the given `expire_at`.
    ///
    /// Err(Ok(retry_after)):
    ///     The status of the authorization is not yet know, and should be retried
    ///     after the returned time interval.
    ///
    /// Err(Err(err)):
    ///     The authorization is invalid.
    ///
    /// The Policy function must return one of:
    ///
    /// Ok((None, ok)):
    ///     The authorization is valid.
    ///
    /// Ok((Some(cordon_at), ok)):
    ///     The authorization is valid, but must expire no later than `cordon_at`.
    ///
    /// Err(err):
    ///     The authorization is invalid.
    ///
    pub fn evaluate<P, Ok>(
        mu: &std::sync::RwLock<Self>,
        started: chrono::DateTime<chrono::Utc>,
        policy: P,
    ) -> Result<
        (chrono::DateTime<chrono::Utc>, Ok),
        Result<std::time::Duration, crate::api::ApiError>,
    >
    where
        P: FnOnce(
            &Self,
        )
            -> Result<(Option<chrono::DateTime<chrono::Utc>>, Ok), crate::api::ApiError>,
    {
        let snapshot = mu.read().unwrap();

        // Select an expiration for the evaluated authorization (presuming it succeeds)
        // which is at-most MAX_AUTHORIZATION in the future relative to when the
        // Snapshot was taken. Jitter to smooth the load of re-authorizations.
        use rand::Rng;
        let exp = snapshot.taken
            + chrono::TimeDelta::seconds(rand::thread_rng().gen_range(
                (Snapshot::MAX_AUTHORIZATION.num_seconds() / 2)
                    ..Snapshot::MAX_AUTHORIZATION.num_seconds(),
            ));

        match policy(&snapshot) {
            // Authorization is valid and not cordoned.
            Ok((None, ok)) => return Ok((exp, ok)),
            // Authorization is valid but cordoned after a future `cordon_at`.
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
            // We don't know how long a Snapshot fetch will take. Currently it's ~1-5 seconds,
            // but our real objective here is to smooth the herd of retries awaiting a refresh.
            + chrono::TimeDelta::milliseconds(rand::thread_rng().gen_range(500..10_000));

        Self::signal_refresh(snapshot, mu);

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

    // Retrieve the data-plane having the exact catalog `name`.
    pub fn data_plane_by_catalog_name<'s>(&'s self, name: &str) -> Option<&'s tables::DataPlane> {
        self.data_planes_idx_name
            .binary_search_by(|i| self.data_planes[*i].data_plane_name.as_str().cmp(name))
            .ok()
            .map(|index| {
                let data_plane = &self.data_planes[self.data_planes_idx_name[index]];
                assert_eq!(data_plane.data_plane_name.as_str(), name);
                data_plane
            })
    }

    pub fn data_plane_first_hmac_key<'s>(&'s self, data_plane_name: &str) -> Option<&'s str> {
        self.data_planes_hmac_keys
            .get(data_plane_name)
            .and_then(|v| v.first())
            .map(|v| v.as_str())
    }

    pub fn verify_data_plane_token<'s>(
        &'s self,
        iss_fqdn: &str,
        token: &str,
    ) -> Result<Option<&'s tables::DataPlane>, jsonwebtoken::errors::Error> {
        let data_plane = self
            .data_planes_idx_fqdn
            .binary_search_by(|i| self.data_planes[*i].data_plane_fqdn.as_str().cmp(iss_fqdn))
            .ok()
            .map(|index| {
                let data_plane = &self.data_planes[self.data_planes_idx_fqdn[index]];
                assert_eq!(data_plane.data_plane_fqdn, iss_fqdn);
                data_plane
            });

        let Some(data_plane) = data_plane else {
            return Ok(None);
        };

        let validation = jsonwebtoken::Validation::default();

        if let Some(hmac_keys) = self.data_planes_hmac_keys.get(&data_plane.data_plane_name) {
            for hmac_key in hmac_keys {
                let key = jsonwebtoken::DecodingKey::from_base64_secret(hmac_key)?;

                if jsonwebtoken::decode::<proto_gazette::Claims>(token, &key, &validation).is_ok() {
                    return Ok(Some(data_plane));
                }
            }
        }
        Ok(None)
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

        // Take `refresh_tx` and drop to awake receiver.
        std::mem::drop(mu.write().unwrap().refresh_tx.take());
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
                if catalog_name.starts_with(&migration.catalog_name_or_prefix) {
                    std::cmp::Ordering::Equal
                } else {
                    migration.catalog_name_or_prefix.as_str().cmp(catalog_name)
                }
            })
            .ok()
            .and_then(|index| {
                let migration = &self.migrations[index];

                if migration.src_plane_id == data_plane.control_id
                    || migration.tgt_plane_id == data_plane.control_id
                {
                    Some(migration.cordon_at)
                } else {
                    None
                }
            })
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
    let mut decrypted_hmac_keys = HashMap::new();
    loop {
        let (next_tx, next_rx) = futures::channel::oneshot::channel();

        // We'll minimally wait for MIN_REFRESH_INTERVAL each iteration.
        let cool_off = tokio::time::sleep(Snapshot::MIN_REFRESH_INTERVAL.to_std().unwrap());
        // We'll wait for the first of `next_rx` or MAX_REFRESH_INTERVAL each iteration.
        let next_rx =
            tokio::time::timeout(Snapshot::MAX_REFRESH_INTERVAL.to_std().unwrap(), next_rx);

        match try_fetch(&app.pg_pool, &mut decrypted_hmac_keys).await {
            Ok(mut snapshot) => {
                snapshot.refresh_tx = Some(next_tx);
                *app.snapshot.write().unwrap() = snapshot;
            }
            Err(err) => {
                tracing::error!(?err, "failed to fetch snapshot (will retry)");
                _ = next_tx.send(()); // Wake ourselves to retry.
            }
        }
        let ((), _) = futures::join!(cool_off, next_rx);
    }
}

async fn try_fetch(
    pg_pool: &sqlx::PgPool,
    decrypted_hmac_keys: &mut HashMap<String, Vec<String>>,
) -> anyhow::Result<Snapshot> {
    tracing::info!("started to fetch authorization snapshot");
    let taken = chrono::Utc::now();

    let collections = sqlx::query_as!(
        SnapshotCollection,
        r#"
        SELECT
            l.journal_template_name AS "journal_template_name!",
            l.catalog_name AS "collection_name: models::Collection",
            l.data_plane_id AS "data_plane_id: models::Id"
        FROM live_specs l
        WHERE journal_template_name IS NOT NULL
        "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch view of live collections")?;

    let data_planes = sqlx::query_as!(
        tables::DataPlane,
        r#"
        SELECT
            d.id AS "control_id: models::Id",
            d.data_plane_name,
            d.data_plane_fqdn,
            false AS "is_default!: bool",
            '{}'::text[] as "hmac_keys!",
            d.encrypted_hmac_keys,
            d.broker_address,
            d.reactor_address,
            d.ops_logs_name AS "ops_logs_name: models::Collection",
            d.ops_stats_name AS "ops_stats_name: models::Collection"
        FROM data_planes d
        "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch data_planes")?;

    let migrations = sqlx::query_as!(
        SnapshotMigration,
        r#"
        SELECT
            m.catalog_name_or_prefix,
            m.cordon_at,
            m.src_plane_id "src_plane_id: models::Id",
            m.tgt_plane_id "tgt_plane_id: models::Id"
        FROM data_plane_migrations m
        WHERE m.active
        "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch migrations")?;

    let role_grants = sqlx::query_as!(
        tables::RoleGrant,
        r#"
        SELECT
            g.subject_role AS "subject_role: models::Prefix",
            g.object_role AS "object_role: models::Prefix",
            g.capability AS "capability: models::Capability"
        FROM role_grants g
        "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch role_grants")?;

    let user_grants = sqlx::query_as!(
        tables::UserGrant,
        r#"
        SELECT
            g.user_id AS "user_id: uuid::Uuid",
            g.object_role AS "object_role: models::Prefix",
            g.capability AS "capability: models::Capability"
        FROM user_grants g
        "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch role_grants")?;

    let tasks = sqlx::query_as!(
        SnapshotTask,
        r#"
        SELECT
            l.shard_template_id AS "shard_template_id!",
            l.catalog_name AS "task_name: models::Name",
            l.spec_type AS "spec_type!: models::CatalogType",
            l.data_plane_id AS "data_plane_id: models::Id"
        FROM live_specs l
        WHERE shard_template_id IS NOT NULL
        "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch view of live tasks")?;

    tracing::info!(
        collections = collections.len(),
        data_planes = data_planes.len(),
        migrations = migrations.len(),
        role_grants = role_grants.len(),
        user_grants = user_grants.len(),
        tasks = tasks.len(),
        "fetched authorization snapshot",
    );

    let mut cached_keys = decrypted_hmac_keys.keys().collect::<Vec<&String>>();
    cached_keys.sort();
    let mut current_keys = data_planes
        .iter()
        .map(|d| d.data_plane_name.as_str())
        .collect::<Vec<&str>>();
    current_keys.sort();
    if cached_keys != current_keys {
        let decrypted_keys = futures::future::try_join_all(
            data_planes
                .iter()
                .map(|dp| crate::decrypt_hmac_keys(&dp.encrypted_hmac_keys)),
        )
        .await?;

        *decrypted_hmac_keys = data_planes
            .iter()
            .map(|dp| dp.data_plane_name.clone())
            .zip(decrypted_keys.into_iter())
            .collect();
    }

    Ok(Snapshot::new(
        taken,
        collections,
        data_planes,
        decrypted_hmac_keys.clone(),
        migrations,
        role_grants,
        user_grants,
        tasks,
    ))
}

#[cfg(test)]
impl Snapshot {
    /// Build a basic Snapshot fixture for testing purposes.
    pub fn build_fixture(taken: Option<chrono::DateTime<chrono::Utc>>) -> Self {
        let taken = taken.unwrap_or(chrono::DateTime::from_timestamp(100_000, 0).unwrap());

        let collections = [
            ("acmeCo/pineapples", 1),
            ("acmeCo/bananas", 1),
            ("ops/tasks/public/plane-one/logs", 1),
            ("ops/tasks/public/plane-one/stats", 1),
            ("bobCo/widgets/mangoes", 2),
            ("bobCo/widgets/squashes", 2),
            ("bobCo/anvils/peaches", 2),
            ("ops/tasks/public/plane-two/logs", 2),
            ("ops/tasks/public/plane-two/stats", 2),
        ]
        .into_iter()
        .map(|(name, data_plane_id)| SnapshotCollection {
            journal_template_name: format!("{name}/1122334455667788"),
            collection_name: models::Collection::new(name),
            data_plane_id: models::Id::new([data_plane_id as u8; 8]),
        })
        .collect::<Vec<_>>();

        let data_planes = vec![
            tables::DataPlane {
                control_id: models::Id::new([1; 8]),
                data_plane_name: "ops/dp/public/plane-one".to_string(),
                data_plane_fqdn: "fqdn1".to_string(),
                is_default: false,
                hmac_keys: Vec::new(),
                encrypted_hmac_keys: "encrypted-gibberish".to_string(),
                broker_address: "broker.1".to_string(),
                reactor_address: "reactor.1".to_string(),
                ops_logs_name: models::Collection::new("ops/tasks/public/plane-one/logs"),
                ops_stats_name: models::Collection::new("ops/tasks/public/plane-one/stats"),
            },
            tables::DataPlane {
                control_id: models::Id::new([2; 8]),
                data_plane_name: "ops/dp/public/plane-two".to_string(),
                data_plane_fqdn: "fqdn2".to_string(),
                is_default: false,
                hmac_keys: Vec::new(),
                encrypted_hmac_keys: "encrypted-gibberish".to_string(),
                broker_address: "broker.2".to_string(),
                reactor_address: "reactor.2".to_string(),
                ops_logs_name: models::Collection::new("ops/tasks/public/plane-two/logs"),
                ops_stats_name: models::Collection::new("ops/tasks/public/plane-two/stats"),
            },
        ];

        let data_planes_hmac_keys = HashMap::from([
            (
                "ops/dp/public/plane-one".to_string(),
                vec![base64::encode("key1"), base64::encode("key2")],
            ),
            (
                "ops/dp/public/plane-two".to_string(),
                vec![base64::encode("key3")],
            ),
        ]);

        let migrations = vec![
            SnapshotMigration {
                catalog_name_or_prefix: "acmeCo/bananas".to_string(),
                cordon_at: chrono::DateTime::from_timestamp(200_000, 0).unwrap(),
                src_plane_id: models::Id::new([1; 8]),
                tgt_plane_id: models::Id::new([2; 8]),
            },
            SnapshotMigration {
                catalog_name_or_prefix: "acmeCo/source-banana".to_string(),
                cordon_at: chrono::DateTime::from_timestamp(200_000, 0).unwrap(),
                src_plane_id: models::Id::new([1; 8]),
                tgt_plane_id: models::Id::new([2; 8]),
            },
            SnapshotMigration {
                catalog_name_or_prefix: "bobCo/widgets".to_string(),
                cordon_at: chrono::DateTime::from_timestamp(300_000, 0).unwrap(),
                src_plane_id: models::Id::new([2; 8]),
                tgt_plane_id: models::Id::new([1; 8]),
            },
        ];

        let role_grants = vec![
            tables::RoleGrant {
                subject_role: models::Prefix::new("acmeCo/"),
                object_role: models::Prefix::new("acmeCo/"),
                capability: models::Capability::Write,
            },
            tables::RoleGrant {
                subject_role: models::Prefix::new("bobCo/"),
                object_role: models::Prefix::new("bobCo/"),
                capability: models::Capability::Write,
            },
            tables::RoleGrant {
                subject_role: models::Prefix::new("bobCo/tires/"),
                object_role: models::Prefix::new("acmeCo/shared/"),
                capability: models::Capability::Read,
            },
            tables::RoleGrant {
                subject_role: models::Prefix::new("bobCo/"),
                object_role: models::Prefix::new("ops/dp/public/"),
                capability: models::Capability::Read,
            },
        ];

        let user_grants = vec![
            tables::UserGrant {
                user_id: uuid::Uuid::from_bytes([32; 16]),
                object_role: models::Prefix::new("bobCo/"),
                capability: models::Capability::Write,
            },
            tables::UserGrant {
                user_id: uuid::Uuid::from_bytes([32; 16]),
                object_role: models::Prefix::new("bobCo/tires/"),
                capability: models::Capability::Admin,
            },
        ];

        let tasks = [
            ("acmeCo/source-pineapple", models::CatalogType::Capture, 1),
            ("acmeCo/source-banana", models::CatalogType::Capture, 1),
            (
                "acmeCo/materialize-pear",
                models::CatalogType::Materialization,
                1,
            ),
            (
                "bobCo/widgets/source-squash",
                models::CatalogType::Capture,
                2,
            ),
            (
                "bobCo/widgets/materialize-mango",
                models::CatalogType::Materialization,
                2,
            ),
            (
                "bobCo/anvils/materialize-orange",
                models::CatalogType::Materialization,
                2,
            ),
        ]
        .into_iter()
        .map(|(name, spec_type, data_plane_id)| SnapshotTask {
            shard_template_id: format!("{spec_type}/{name}/0011223344556677"),
            task_name: models::Name::new(name),
            spec_type,
            data_plane_id: models::Id::new([data_plane_id as u8; 8]),
        })
        .collect::<Vec<_>>();

        Snapshot::new(
            taken,
            collections,
            data_planes,
            data_planes_hmac_keys,
            migrations,
            role_grants,
            user_grants,
            tasks,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_lookups() {
        let snapshot = Snapshot::build_fixture(None);

        // Verify exact catalog name lookups.
        assert_eq!(
            snapshot
                .task_by_catalog_name("bobCo/widgets/materialize-mango")
                .unwrap()
                .task_name
                .as_str(),
            "bobCo/widgets/materialize-mango"
        );
        assert_eq!(
            snapshot
                .task_by_catalog_name("acmeCo/source-pineapple")
                .unwrap()
                .task_name
                .as_str(),
            "acmeCo/source-pineapple"
        );
        assert!(snapshot
            .task_by_catalog_name("bobCo/widgets/materialize-mang")
            .is_none()); // Partial name should not match.

        // Verify shard ID lookups with exact and more specific matches.
        assert_eq!(
            snapshot
                .task_by_shard_id("capture/acmeCo/source-banana/0011223344556677/pivot=00")
                .unwrap()
                .task_name
                .as_str(),
            "acmeCo/source-banana"
        );
        assert_eq!(
            snapshot
                .task_by_shard_id(
                    "materialization/bobCo/widgets/materialize-mango/0011223344556677/pivot=00"
                )
                .unwrap()
                .task_name
                .as_str(),
            "bobCo/widgets/materialize-mango"
        );
        assert!(snapshot
            .task_by_shard_id("materialization/bobCo/widgets/materialize-mango")
            .is_none()); // Must be _more_ specific to match.

        // Verify that non-existent shard IDs return None.
        assert!(snapshot
            .task_by_shard_id("capture/nonexistent-task/0011223344556677")
            .is_none());
        assert!(snapshot
            .task_by_shard_id("materialization/acmeCo/nonexistent/0011223344556677")
            .is_none());
    }

    #[test]
    fn test_collection_lookups() {
        let snapshot = Snapshot::build_fixture(None);

        // Verify exact catalog name lookups.
        assert_eq!(
            snapshot
                .collection_by_catalog_name("acmeCo/pineapples")
                .unwrap()
                .collection_name
                .as_str(),
            "acmeCo/pineapples"
        );
        assert_eq!(
            snapshot
                .collection_by_catalog_name("bobCo/widgets/mangoes")
                .unwrap()
                .collection_name
                .as_str(),
            "bobCo/widgets/mangoes"
        );
        assert!(snapshot
            .collection_by_catalog_name("acmeCo/nonexistent")
            .is_none()); // Non-existent name should not match.

        // Verify journal name lookups with exact and more specific matches.
        assert_eq!(
            snapshot
                .collection_by_journal_name("acmeCo/pineapples/1122334455667788/suffix=00")
                .unwrap()
                .collection_name
                .as_str(),
            "acmeCo/pineapples"
        );
        assert_eq!(
            snapshot
                .collection_by_journal_name("bobCo/widgets/mangoes/1122334455667788/suffix=00")
                .unwrap()
                .collection_name
                .as_str(),
            "bobCo/widgets/mangoes"
        );
        assert!(snapshot
            .collection_by_journal_name("bobCo/widgets/mangoes")
            .is_none()); // Must be _more_ specific to match.

        // Verify that non-existent journal names return None.
        assert!(snapshot
            .collection_by_journal_name("acmeCo/nonexistent/1122334455667788")
            .is_none());
        assert!(snapshot
            .collection_by_journal_name("bobCo/widgets/nonexistent/1122334455667788")
            .is_none());
    }

    #[test]
    fn test_data_plane_lookups() {
        let snapshot = Snapshot::build_fixture(None);

        assert_eq!(
            snapshot
                .data_plane_by_catalog_name("ops/dp/public/plane-one")
                .unwrap()
                .data_plane_name
                .as_str(),
            "ops/dp/public/plane-one"
        );
        assert_eq!(
            snapshot
                .data_plane_by_catalog_name("ops/dp/public/plane-two")
                .unwrap()
                .data_plane_name
                .as_str(),
            "ops/dp/public/plane-two"
        );
        assert!(snapshot
            .data_plane_by_catalog_name("ops/dp/public/plane-one/1")
            .is_none()); // Non-existent name should not match.
    }

    #[test]
    fn test_verify_data_plane_token() {
        let snapshot = Snapshot::build_fixture(None);
        let now = jsonwebtoken::get_current_timestamp();

        // Create a valid token signed with "key1".
        let claims = proto_gazette::Claims {
            iat: now,
            exp: now + 100,
            cap: proto_gazette::capability::APPEND as u32,
            iss: "fqdn1".to_string(),
            sel: proto_gazette::LabelSelector::default(),
            sub: "subject".to_string(),
        };
        let key = jsonwebtoken::EncodingKey::from_secret("key1".as_bytes());
        let token = jsonwebtoken::encode(&jsonwebtoken::Header::default(), &claims, &key).unwrap();

        // Verify the token against the correct data plane.
        let result = snapshot.verify_data_plane_token("fqdn1", &token).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().data_plane_fqdn, "fqdn1");

        // Verify the token against an incorrect data plane.
        let result = snapshot.verify_data_plane_token("fqdn2", &token).unwrap();
        assert!(result.is_none());

        // Verify an invalid token.
        let invalid_token = "invalid.token";
        let result = snapshot
            .verify_data_plane_token("fqdn1", invalid_token)
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_cordon_at() {
        let snapshot = Snapshot::build_fixture(None);

        // Verify cordon_at for a catalog name covered by a migration in the source data plane.
        let src_data_plane = snapshot
            .data_planes
            .iter()
            .find(|dp| dp.control_id == models::Id::new([1; 8]))
            .unwrap();
        let cordon_time = snapshot.cordon_at("acmeCo/bananas", src_data_plane);
        assert!(cordon_time.is_some());
        assert_eq!(
            cordon_time.unwrap(),
            chrono::DateTime::from_timestamp(200_000, 0).unwrap()
        );

        // Verify cordon_at for a catalog name covered by a migration in the target data plane.
        let tgt_data_plane = snapshot
            .data_planes
            .iter()
            .find(|dp| dp.control_id == models::Id::new([2; 8]))
            .unwrap();
        let cordon_time = snapshot.cordon_at("acmeCo/bananas", tgt_data_plane);
        assert!(cordon_time.is_some());
        assert_eq!(
            cordon_time.unwrap(),
            chrono::DateTime::from_timestamp(200_000, 0).unwrap()
        );

        // Verify cordon_at for a catalog name not covered by any migration.
        let cordon_time = snapshot.cordon_at("acmeCo/nonexistent", src_data_plane);
        assert!(cordon_time.is_none());

        // Verify cordon_at for a catalog name with a more specific prefix match.
        let cordon_time = snapshot.cordon_at("bobCo/widgets/source-squash", src_data_plane);
        assert!(cordon_time.is_some());
        assert_eq!(
            cordon_time.unwrap(),
            chrono::DateTime::from_timestamp(300_000, 0).unwrap()
        );
    }
}
