use std::{io::Write, sync::Arc};

use crate::directives::JobStatus;
use anyhow::Context;
use control_plane_api::{
    directives::{
        Row,
        storage_mappings::{StorageMapping, fetch_storage_mappings, upsert_storage_mapping},
    },
    jobs, logs,
};
use serde::{Deserialize, Serialize};
use sqlx::types::Uuid;
use validator::Validate;

#[derive(Debug, Deserialize, Serialize, Validate, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Directive {}

#[derive(Debug, Deserialize, Serialize, Validate, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Claims {
    add_store: models::Store,
    catalog_prefix: models::Prefix,
}

#[tracing::instrument(skip_all, ret, err, fields(row.user_claims))]
pub async fn apply(
    _: Directive,
    row: Row,
    logs_tx: &logs::Tx,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    snapshot_watch: &Arc<dyn tokens::Watch<control_plane_api::Snapshot>>,
) -> anyhow::Result<JobStatus> {
    let detail = format!(
        "updated by user {} via applied directive {}",
        row.user_id, row.apply_id
    );
    let (collection_data, recovery) = match validate(txn, logs_tx, row, &snapshot_watch).await {
        Ok(c) => c,
        Err(err) => {
            return Ok(JobStatus::invalid_claims(err));
        }
    };

    let ProposedMapping {
        catalog_prefix,
        spec,
    } = collection_data;
    upsert_storage_mapping(Some(&detail), &catalog_prefix, spec, txn).await?;
    let ProposedMapping {
        catalog_prefix,
        spec,
    } = recovery;
    upsert_storage_mapping(Some(&detail), &catalog_prefix, spec, txn).await?;

    Ok(JobStatus::Success)
}

pub struct ProposedMapping {
    catalog_prefix: String,
    spec: models::StorageDef,
}

fn add_store(stores: &mut models::StorageDef, store: models::Store) {
    // If there's already an equivalent store, then remove it so that we don't end up with
    // duplicates. This could happen if someone added store A, then store B, then store A again.
    stores.stores.retain(|s| s != &store);
    stores.stores.insert(0, store);
}

async fn validate(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    logs_tx: &logs::Tx,
    row: Row,
    snapshot_watch: &Arc<dyn tokens::Watch<control_plane_api::Snapshot>>,
) -> anyhow::Result<(ProposedMapping, ProposedMapping)> {
    let claims: Claims =
        serde_json::from_str(row.user_claims.get()).context("parsing user_claims")?;

    if !claims.catalog_prefix.ends_with('/') {
        anyhow::bail!("invalid prefix, must end with '/'");
    }

    // Storage mappings can only be updated for an entire tenant. We may one day wish to support
    // updates to narrower prefixes, but are trying to keep it simple for now.
    if claims.catalog_prefix.matches('/').count() > 1 {
        anyhow::bail!(
            "catalog prefix contains too many path segments. Only top-level tenant prefixes can have storage mappings altered"
        );
    }

    // Note: we must assert that user has admin capability for the _entire tenant_, even if in the
    // future we allow for updating mappings of narrower prefixes. This is required because a new
    // storage mapping for `a/b/` may implicitly override the existing mapping for `a/`.
    let refresh = snapshot_watch.token();
    let snapshot = refresh.result().unwrap();

    let user_has_admin =
        user_can_admin_tenant(&snapshot, row.user_id, claims.catalog_prefix.as_str());
    anyhow::ensure!(
        user_has_admin,
        "user does not have required 'admin' capability to '{}'",
        claims.catalog_prefix
    );

    // Check that we can actually access the storage bucket before fetching (and locking) the
    // existing `storage_mappings` rows, since this check requires multiple network round trips.
    check_bucket_access(row.logs_token, logs_tx, &claims.add_store).await?;

    let recovery_prefix = format!("recovery/{}", claims.catalog_prefix);
    let existing_mappings =
        fetch_storage_mappings(&claims.catalog_prefix, &recovery_prefix, txn).await?;
    let (mut collection_data, mut recovery_data) = parse_existing(existing_mappings)?;

    let mut collection_store = claims.add_store.clone();
    // The storage mapping for collection data should always have a `collection-data/` prefix in
    // order to segregate it from recovery log data. This makes it easier to apply bucket lifecycle
    // policies, since you can target just the `collection-data/` prefix. If the store already
    // specifies a prefix, then we'll add `collection-data/` to the end.
    collection_store
        .prefix_mut()
        .as_mut_string()
        .push_str("collection-data/");
    add_store(&mut collection_data.spec, collection_store);
    // The recovery log store doesn't need a separate prefix because all recovery log journals
    // already begin with `recovery/`.
    add_store(&mut recovery_data.spec, claims.add_store.clone());

    Ok((collection_data, recovery_data))
}

/// Returns whether `user_id` holds `admin` capability on `catalog_prefix`
/// itself, or on any prefix nested beneath it, according to `snapshot`.
///
/// This deliberately mirrors the legacy
/// `internal.user_roles($user, 'admin') where starts_with(role_prefix, catalog_prefix)`
/// SQL check that it replaced: a user who administers only a *sub-prefix* of
/// the tenant (e.g. `acmeCo/team/` when altering `acmeCo/`) remains authorized.
/// Note this is the reverse of `tables::UserGrant::is_authorized`, which
/// considers only grants at or *above* `catalog_prefix`.
fn user_can_admin_tenant(
    snapshot: &control_plane_api::Snapshot,
    user_id: Uuid,
    catalog_prefix: &str,
) -> bool {
    snapshot
        .prefix_and_capabilities_per_user(user_id)
        .iter()
        .any(|(role_prefix, capabilities)| {
            role_prefix.starts_with(catalog_prefix) && capabilities.1 >= models::Capability::Admin
        })
}

// This is a macro instead of a function to work around the fact that file paths are `OsStr`s
// instead of regular `&str`s.
macro_rules! check_command {
    ($prog:expr, $($arg:expr),* $(; region $region:expr)?) => {{
        let mut cmd = std::process::Command::new($prog);
        $(
            cmd.arg($arg);
        )*
        $(
          if let Some(region_name) = $region {
              cmd.arg("--region");
              cmd.arg(region_name);
          }
        )?
        cmd
    } };
}

async fn check_bucket_access(
    logs_token: Uuid,
    logs_tx: &logs::Tx,
    store: &models::Store,
) -> anyhow::Result<()> {
    let mut test_file = tempfile::NamedTempFile::new().context("creating temp file")?;
    test_file
        .write_all(TEST_FILE_CONTENT.as_bytes())
        .context("writing test file content")?;
    let test_file_path = test_file.path();

    let commands = match store {
        models::Store::S3(conf) => vec![
            (
                "put object",
                check_command!(
                    "aws",
                    "s3",
                    "cp",
                    test_file_path,
                    without_query(conf.as_url()).join(TEST_FILENAME)?.to_string()
                    ; region conf.region.as_ref()),
            ),
            // List comes after put, so the prefix, if configured, is guaranteed to exist.
            (
                "list bucket",
                check_command!("aws", "s3", "ls", without_query(conf.as_url()).to_string()
                    ; region conf.region.as_ref()),
            ),
            (
                "get object",
                // Copy to stdout to avoid needing to cleanup a temp file.
                // Don't use /dev/null because the cli will exit non-zero even when it gets the file successfully.
                check_command!(
                    "aws",
                    "s3",
                    "cp",
                    without_query(conf.as_url())
                        .join(TEST_FILENAME)?
                        .to_string(),
                    "/dev/stdout"
                    ; region conf.region.as_ref()
                ),
            ),
            (
                "delete object",
                check_command!(
                    "aws",
                    "s3",
                    "rm",
                    without_query(conf.as_url()).join(TEST_FILENAME)?.to_string()
                    ; region conf.region.as_ref()
                ),
            ),
        ],
        models::Store::Gcs(conf) => vec![
            (
                "put object",
                check_command!(
                    "gcloud",
                    "storage",
                    "cp",
                    test_file_path,
                    without_query(conf.as_url())
                        .join(TEST_FILENAME)?
                        .to_string()
                ),
            ),
            // List comes after put, so the prefix, if configured, is guaranteed to exist.
            (
                "list bucket",
                check_command!(
                    "gcloud",
                    "storage",
                    "ls",
                    without_query(conf.as_url()).to_string()
                ),
            ),
            (
                "get object",
                check_command!(
                    "gcloud",
                    "storage",
                    "cat",
                    without_query(conf.as_url())
                        .join(TEST_FILENAME)?
                        .to_string()
                ),
            ),
            (
                "delete object",
                check_command!(
                    "gcloud",
                    "storage",
                    "rm",
                    without_query(conf.as_url())
                        .join(TEST_FILENAME)?
                        .to_string()
                ),
            ),
        ],
        models::Store::Azure(_) => {
            anyhow::bail!("checking access for azure cloud storage is not yet implemented")
        }
        models::Store::Custom(_) => {
            anyhow::bail!("checking access for custom cloud storage is not supported")
        }
    };

    for (desc, mut cmd) in commands {
        tracing::info!(
            %desc,
            program = ?cmd.get_program(),
            args = ?cmd.get_args(),
            "running storage check"
        );
        let exit_status = jobs::run_without_removing_env(&desc, logs_tx, logs_token, &mut cmd)
            .await
            .with_context(|| {
                format!(
                    "failed to execute {desc} command: {:?} with: {:?}",
                    cmd.get_program(),
                    cmd.get_args()
                )
            })?;
        if !exit_status.success() {
            anyhow::bail!("failed to {desc}, please check that permissions are set appropriately");
        }
    }

    Ok(())
}

fn without_query(mut uri: url::Url) -> url::Url {
    uri.set_query(None);
    uri
}

fn parse_existing(
    mut existing: Vec<StorageMapping>,
) -> anyhow::Result<(ProposedMapping, ProposedMapping)> {
    if existing.len() != 2 {
        anyhow::bail!("expected 2 existing storage mappings, found: {existing:?}");
    }
    let Some(recovery_idx) = existing
        .iter()
        .position(|m| m.catalog_prefix.starts_with("recovery/"))
    else {
        anyhow::bail!("missing recovery/ storage mapping in {existing:?}");
    };
    let recovery = existing.remove(recovery_idx);
    let recovery_storage: models::StorageDef = serde_json::from_str(recovery.spec.get())
        .context("deserializing existing recovery/ storage mapping")?;

    let collection_data = existing.remove(0);
    let collection_store: models::StorageDef = serde_json::from_str(collection_data.spec.get())
        .context("deserializing existing storage mapping")?;

    Ok((
        ProposedMapping {
            catalog_prefix: collection_data.catalog_prefix,
            spec: collection_store,
        },
        ProposedMapping {
            catalog_prefix: recovery.catalog_prefix,
            spec: recovery_storage,
        },
    ))
}

const TEST_FILENAME: &str = "estuary_test.txt";

const TEST_FILE_CONTENT: &str = r#"Estuary storage test
This file is written to your storage bucket in order to test that we have the necessary access
permissions to create and delete objects. If you're seeing this file stick around, then it's
likely because we lacked the necessary permissions to delete it. You may remove this file at
any time, and doing so will not impact the function of Estuary."#;

#[cfg(test)]
mod test {
    use super::*;
    use control_plane_api::snapshot::SnapshotData;

    // Tenant prefix under test. Storage mappings may only be altered for a
    // top-level tenant, so this is always a single-segment prefix.
    const TENANT: &str = "acmeCo/";

    fn user_grant(
        user_id: Uuid,
        object_role: &str,
        capability: models::Capability,
    ) -> tables::UserGrant {
        tables::UserGrant {
            user_id,
            object_role: models::Prefix::new(object_role),
            capability,
            bundles: Vec::new(),
        }
    }

    fn role_grant(
        subject_role: &str,
        object_role: &str,
        capability: models::Capability,
    ) -> tables::RoleGrant {
        tables::RoleGrant {
            subject_role: models::Prefix::new(subject_role),
            object_role: models::Prefix::new(object_role),
            capability,
            bundles: Vec::new(),
        }
    }

    fn snapshot(
        user_grants: Vec<tables::UserGrant>,
        role_grants: Vec<tables::RoleGrant>,
    ) -> control_plane_api::Snapshot {
        let data = SnapshotData {
            collections: Vec::new(),
            data_planes: Vec::new(),
            migrations: Vec::new(),
            role_grants,
            user_grants,
            tasks: Vec::new(),
        };
        control_plane_api::Snapshot::new(chrono::DateTime::UNIX_EPOCH, data)
    }

    // Case 1: a user granted `admin` directly on the tenant root is authorized.
    // This is the case where the old SQL check and `is_authorized` agreed.
    #[test]
    fn admin_on_tenant_root_is_authorized() {
        let user_id = Uuid::from_bytes([1; 16]);
        let snapshot = snapshot(
            vec![user_grant(user_id, TENANT, models::Capability::Admin)],
            Vec::new(),
        );
        assert!(user_can_admin_tenant(&snapshot, user_id, TENANT));
    }

    // Case 2: a user granted `admin` only on a prefix *beneath* the tenant root
    // is still authorized. This is the behavior that would have been lost with a
    // plain `is_authorized` check, and which the sub-prefix scan preserves.
    #[test]
    fn admin_on_sub_prefix_is_authorized() {
        let user_id = Uuid::from_bytes([2; 16]);
        let snapshot = snapshot(
            vec![user_grant(
                user_id,
                "acmeCo/team/",
                models::Capability::Admin,
            )],
            Vec::new(),
        );
        assert!(user_can_admin_tenant(&snapshot, user_id, TENANT));
    }

    // Case 3: a user reaches `admin` on the tenant transitively, through a role
    // grant (e.g. an `estuary_support/` role that is itself granted admin over
    // the tenant). The grant-graph projection must be honored.
    #[test]
    fn admin_via_role_grant_projection_is_authorized() {
        let user_id = Uuid::from_bytes([3; 16]);
        let snapshot = snapshot(
            vec![user_grant(
                user_id,
                "estuary_support/",
                models::Capability::Admin,
            )],
            vec![role_grant(
                "estuary_support/",
                TENANT,
                models::Capability::Admin,
            )],
        );
        assert!(user_can_admin_tenant(&snapshot, user_id, TENANT));
    }

    // Negative control: a capability below `admin` on the tenant is not enough.
    #[test]
    fn write_capability_is_denied() {
        let user_id = Uuid::from_bytes([4; 16]);
        let snapshot = snapshot(
            vec![user_grant(user_id, TENANT, models::Capability::Write)],
            Vec::new(),
        );
        assert!(!user_can_admin_tenant(&snapshot, user_id, TENANT));
    }

    // Negative control: admin over a sibling tenant confers no authority here,
    // proving the sub-prefix scan does not match unrelated or ancestor prefixes.
    #[test]
    fn admin_on_unrelated_tenant_is_denied() {
        let user_id = Uuid::from_bytes([5; 16]);
        let snapshot = snapshot(
            vec![user_grant(user_id, "bobCo/", models::Capability::Admin)],
            Vec::new(),
        );
        assert!(!user_can_admin_tenant(&snapshot, user_id, TENANT));
    }
}
