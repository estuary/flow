use anyhow::Context;
use std::collections::HashMap;

// SnapshotData encapsulates all data required to construct a Snapshot.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SnapshotData {
    // Platform collections.
    pub collections: Vec<SnapshotCollection>,
    // Platform data-planes.
    pub data_planes: Vec<tables::DataPlane>,
    // Data-plane migrations that are underway.
    pub migrations: Vec<SnapshotMigration>,
    // Platform role grants.
    pub role_grants: Vec<tables::RoleGrant>,
    // Platform user grants.
    pub user_grants: Vec<tables::UserGrant>,
    // Platform tasks.
    pub tasks: Vec<SnapshotTask>,
}

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
    // Cancelling `revoke` triggers a Snapshot refresh ahead of its expiry.
    pub revoke: tokens::CancellationToken,
}

// SnapshotCollection is the state of a live collection which influences authorization.
// It's indexed on `journal_template_name`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
            data_planes_idx_fqdn: Vec::new(),
            data_planes_idx_name: Vec::new(),
            migrations: Vec::new(),
            role_grants: tables::RoleGrants::default(),
            user_grants: tables::UserGrants::default(),
            tasks: Vec::new(),
            tasks_idx_name: Vec::new(),
            revoke: tokens::CancellationToken::new(),
        }
    }

    /// Construct a Snapshot from the provided SnapshotData.
    pub fn new(taken: tokens::DateTime, data: SnapshotData) -> Self {
        let SnapshotData {
            mut collections,
            data_planes,
            mut migrations,
            role_grants,
            user_grants,
            mut tasks,
        } = data;

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
            data_planes_idx_fqdn,
            data_planes_idx_name,
            migrations,
            role_grants,
            user_grants,
            tasks,
            tasks_idx_name,
            revoke: tokens::CancellationToken::new(),
        }
    }

    /// Returns true if the Snapshot was taken after (and is authoritative for)
    /// an operation that started at `started`, allowing for clock skew.
    pub fn taken_after(&self, started: tokens::DateTime) -> bool {
        self.taken > (started + Self::TEMPORAL_SKEW)
    }

    // Retrieve all tasks whose names start with the given `prefix`.
    pub fn tasks_by_prefix<'s>(
        &'s self,
        prefix: &'s str,
    ) -> impl Iterator<Item = &'s SnapshotTask> + 's {
        let start = self
            .tasks_idx_name
            .partition_point(|i| self.tasks[*i].task_name.as_str() < prefix);

        self.tasks_idx_name[start..]
            .iter()
            .map(|i| &self.tasks[*i])
            .take_while(move |task| task.task_name.as_str().starts_with(prefix))
    }

    // Retrieve all collections whose names start with the given `prefix`.
    pub fn collections_by_prefix<'s>(
        &'s self,
        prefix: &'s str,
    ) -> impl Iterator<Item = &'s SnapshotCollection> + 's {
        let start = self
            .collections_idx_name
            .partition_point(|i| self.collections[*i].collection_name.as_str() < prefix);

        self.collections_idx_name[start..]
            .iter()
            .map(|i| &self.collections[*i])
            .take_while(move |coll| coll.collection_name.as_str().starts_with(prefix))
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

    /// Verify a data-plane token and return its DataPlane if valid.
    /// Returns `Ok(None)` if the data-plane FQDN is unknown or the token doesn't verify.
    /// We discard error information to avoid leaking the existence of data-planes.
    pub fn verify_data_plane_token<'s>(
        &'s self,
        iss_fqdn: &str,
        token: &str,
    ) -> tonic::Result<Option<&'s tables::DataPlane>> {
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

        let (_encode_key, decode_keys) =
            tokens::jwt::parse_base64_hmac_keys(data_plane.hmac_keys.iter())?;

        Ok(
            tokens::jwt::verify::<proto_gazette::Claims>(token.as_bytes(), 0, &decode_keys)
                .ok()
                .map(|_verified| data_plane),
        )
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
    pub const MIN_REFRESH_INTERVAL: chrono::TimeDelta = chrono::TimeDelta::seconds(20);
    // Maximum interval between Snapshot refreshes.
    // We will refresh an older Snapshot in the background.
    pub const MAX_REFRESH_INTERVAL: chrono::TimeDelta = chrono::TimeDelta::minutes(5);
    // Maximum lifetime of an authorization produced from a Snapshot,
    // relative to the timestamp at which the Snapshot was taken.
    // This upper-bounds the lifetime of an authorization derived from a Snapshot.
    pub const MAX_AUTHORIZATION: chrono::TimeDelta = chrono::TimeDelta::minutes(80);
    /// Maximum amount of skew to allow for between distributed clocks,
    /// when evaluating "happened before" ordering of a Snapshot's taken
    /// timestamp and an externally-provided operation start timestamp.
    /// We assume linux servers running NTP, which are typically within 1ms of each other.
    pub const TEMPORAL_SKEW: chrono::TimeDelta = chrono::TimeDelta::milliseconds(250);
}

/// PgSnapshotSource is a tokens::Source which fetches Snapshots from Postgres.
pub struct PgSnapshotSource {
    /// Postgres Database connection pool.
    pg_pool: sqlx::PgPool,
    /// DateTime of the last Snapshot yielded by this Source.
    last_taken: tokens::DateTime,
    /// Cache of decrypted HMAC keys, keyed on data-plane name,
    /// having the original encrypted keys and their decryption.
    decrypted_hmac_keys: HashMap<String, (models::RawValue, Vec<String>)>,
}

impl PgSnapshotSource {
    pub fn new(pg_pool: sqlx::PgPool) -> Self {
        Self {
            pg_pool,
            last_taken: tokens::DateTime::UNIX_EPOCH,
            decrypted_hmac_keys: HashMap::new(),
        }
    }
}

impl tokens::Source for PgSnapshotSource {
    type Token = Snapshot;
    type Revoke = tokens::WaitForCancellationFutureOwned;

    async fn refresh(
        &mut self,
        _started: tokens::DateTime,
    ) -> tonic::Result<Result<(Self::Token, chrono::TimeDelta, Self::Revoke), chrono::TimeDelta>>
    {
        let mut taken = tokens::now();

        // Snapshot should be no more frequent then MIN_REFRESH_INTERVAL.
        let cool_off = (self.last_taken + Snapshot::MIN_REFRESH_INTERVAL) - taken;
        if let Some(cool_off) = cool_off.to_std().ok() {
            // `cool_off` is positive: we must wait.
            tokio::time::sleep(cool_off).await;
            taken += cool_off;
        }

        let result = try_fetch(&self.pg_pool, &mut self.decrypted_hmac_keys).await;
        match result {
            Ok(data) => {
                self.last_taken = taken;
                let snapshot = Snapshot::new(taken, data);
                let revoked = snapshot.revoke.clone().cancelled_owned();
                Ok(Ok((snapshot, Snapshot::MAX_REFRESH_INTERVAL, revoked)))
            }
            Err(err) => {
                tracing::error!(?err, "failed to fetch snapshot (will retry)");
                Ok(Err(Snapshot::MIN_REFRESH_INTERVAL))
            }
        }
    }
}

pub async fn try_fetch(
    pg_pool: &sqlx::PgPool,
    decrypted_hmac_keys: &mut HashMap<String, (models::RawValue, Vec<String>)>,
) -> anyhow::Result<SnapshotData> {
    tracing::info!("started to fetch authorization snapshot");

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

    let mut data_planes = sqlx::query_as!(
        tables::DataPlane,
        r#"
        SELECT
            d.id AS "control_id: models::Id",
            d.data_plane_name,
            d.data_plane_fqdn,
            d.hmac_keys,
            d.encrypted_hmac_keys as "encrypted_hmac_keys: models::RawValue",
            d.broker_address,
            d.reactor_address,
            d.dekaf_address,
            d.dekaf_registry_address,
            d.ops_logs_name AS "ops_logs_name: models::Collection",
            d.ops_stats_name AS "ops_stats_name: models::Collection",
            d.cidr_blocks::text[] AS "cidr_blocks!",
            d.gcp_service_account_email,
            d.aws_iam_user_arn
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

    // For each data-plane, if we have a decrypted HMAC key that matches the unchanged encryption, then use it.
    let mut decrypt_jobs = Vec::new();
    for dp in data_planes.iter_mut() {
        if !dp.hmac_keys.is_empty() {
            continue;
        }
        if let Some((enc, dec)) = decrypted_hmac_keys.get(&dp.data_plane_name)
            && enc.get() == dp.encrypted_hmac_keys.get()
        {
            dp.hmac_keys = dec.clone();
            continue;
        }

        // Start a decryption of this data-plane's encrypted keys.
        decrypt_jobs.push(async {
            let decrypted = crate::decrypt_hmac_keys(&dp.encrypted_hmac_keys).await?;
            Result::Ok::<(&mut tables::DataPlane, Vec<String>), anyhow::Error>((dp, decrypted))
        });
    }
    let decrypt_jobs: Vec<(&mut tables::DataPlane, Vec<String>)> =
        futures::future::try_join_all(decrypt_jobs).await?;

    for (dp, hmac_keys) in decrypt_jobs {
        decrypted_hmac_keys.insert(
            dp.data_plane_name.clone(),
            (dp.encrypted_hmac_keys.clone(), hmac_keys.clone()),
        );
        dp.hmac_keys = hmac_keys;
    }

    Ok(SnapshotData {
        collections,
        data_planes,
        migrations,
        role_grants,
        user_grants,
        tasks,
    })
}

#[cfg(test)]
impl Snapshot {
    pub fn build_fixture(taken: Option<tokens::DateTime>) -> Self {
        let data = include_str!("snapshot_fixture.json");
        let data: SnapshotData = serde_json::from_str(data).unwrap();

        let taken = taken.unwrap_or_default();
        Snapshot::new(taken, data)
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
        assert!(
            snapshot
                .task_by_catalog_name("bobCo/widgets/materialize-mang")
                .is_none()
        ); // Partial name should not match.

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
        assert!(
            snapshot
                .task_by_shard_id("materialization/bobCo/widgets/materialize-mango")
                .is_none()
        ); // Must be _more_ specific to match.

        // Verify that non-existent shard IDs return None.
        assert!(
            snapshot
                .task_by_shard_id("capture/nonexistent-task/0011223344556677")
                .is_none()
        );
        assert!(
            snapshot
                .task_by_shard_id("materialization/acmeCo/nonexistent/0011223344556677")
                .is_none()
        );
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
        assert!(
            snapshot
                .collection_by_catalog_name("acmeCo/nonexistent")
                .is_none()
        ); // Non-existent name should not match.

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
        assert!(
            snapshot
                .collection_by_journal_name("bobCo/widgets/mangoes")
                .is_none()
        ); // Must be _more_ specific to match.

        // Verify that non-existent journal names return None.
        assert!(
            snapshot
                .collection_by_journal_name("acmeCo/nonexistent/1122334455667788")
                .is_none()
        );
        assert!(
            snapshot
                .collection_by_journal_name("bobCo/widgets/nonexistent/1122334455667788")
                .is_none()
        );
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
        assert!(
            snapshot
                .data_plane_by_catalog_name("ops/dp/public/plane-one/1")
                .is_none()
        ); // Non-existent name should not match.
    }

    #[test]
    fn test_verify_data_plane_token() {
        let snapshot = Snapshot::build_fixture(None);
        let now = tokens::now().timestamp() as u64;

        // Create a valid token signed with "key1".
        let claims = proto_gazette::Claims {
            iat: now,
            exp: now + 100,
            cap: proto_gazette::capability::APPEND,
            iss: "fqdn1".to_string(),
            sel: proto_gazette::LabelSelector::default(),
            sub: "subject".to_string(),
        };
        let key = tokens::jwt::EncodingKey::from_secret("key1".as_bytes());
        let token = tokens::jwt::sign(&claims, &key).unwrap();

        // Verify the token against the correct data plane.
        let result = snapshot.verify_data_plane_token("fqdn1", &token).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().data_plane_fqdn, "fqdn1");

        // Verify the token against an incorrect data plane (wrong FQDN).
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
