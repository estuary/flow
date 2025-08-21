use std::io::Write;

use crate::directives::JobStatus;
use anyhow::Context;
use control_plane_api::{
    directives::{
        storage_mappings::{
            fetch_storage_mappings, upsert_storage_mapping, user_has_admin_capability,
            StorageMapping,
        },
        Row,
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
) -> anyhow::Result<JobStatus> {
    let detail = format!(
        "updated by user {} via applied directive {}",
        row.user_id, row.apply_id
    );
    let (collection_data, recovery) = match validate(txn, logs_tx, row).await {
        Ok(c) => c,
        Err(err) => {
            return Ok(JobStatus::invalid_claims(err));
        }
    };

    let ProposedMapping {
        catalog_prefix,
        spec,
    } = collection_data;
    upsert_storage_mapping(&detail, &catalog_prefix, spec, txn).await?;
    let ProposedMapping {
        catalog_prefix,
        spec,
    } = recovery;
    upsert_storage_mapping(&detail, &catalog_prefix, spec, txn).await?;

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
) -> anyhow::Result<(ProposedMapping, ProposedMapping)> {
    let claims: Claims =
        serde_json::from_str(row.user_claims.get()).context("parsing user_claims")?;

    if !claims.catalog_prefix.ends_with('/') {
        anyhow::bail!("invalid prefix, must end with '/'");
    }

    // Storage mappings can only be updated for an entire tenant. We may one day wish to support
    // updates to narrower prefixes, but are trying to keep it simple for now.
    if claims.catalog_prefix.matches('/').count() > 1 {
        anyhow::bail!("catalog prefix contains too many path segments. Only top-level tenant prefixes can have storage mappings altered");
    }

    // Note: we must assert that user has admin capability for the _entire tenant_, even if in the
    // future we allow for updating mappings of narrower prefixes. This is required because a new
    // storage mapping for `a/b/` may implicitly override the existing mapping for `a/`.
    let user_has_admin =
        user_has_admin_capability(row.user_id, &claims.catalog_prefix, txn).await?;
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
