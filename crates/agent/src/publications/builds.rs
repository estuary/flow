use super::{specs::SpecRow, Error};
use crate::{jobs, logs, Id};

use anyhow::Context;
use sqlx::types::Uuid;
use std::io::Write;
use std::path;
use tables::SqlTableObj;

pub async fn build_catalog(
    builds_root: &url::Url,
    catalog: &models::Catalog,
    connector_network: &str,
    flowctl: &str,
    logs_token: Uuid,
    logs_tx: &logs::Tx,
    pub_id: Id,
    tmpdir: &path::Path,
) -> anyhow::Result<Vec<Error>> {
    // We perform the build under a ./builds/ subdirectory, which is a
    // specific sub-path expected by temp-data-plane underneath its
    // working temporary directory. This lets temp-data-plane use the
    // build database in-place.
    let builds_dir = tmpdir.join("builds");
    std::fs::create_dir(&builds_dir).context("creating builds directory")?;

    // Write our catalog source file within the build directory.
    std::fs::File::create(&builds_dir.join(&format!("{}.flow.yaml", pub_id)))
        .and_then(|mut f| f.write_all(serde_json::to_string_pretty(catalog).unwrap().as_bytes()))
        .context("writing catalog file")?;

    let build_id = format!("{pub_id}");
    let db_path = builds_dir.join(&build_id);

    let job = jobs::run(
        "build",
        logs_tx,
        logs_token,
        tokio::process::Command::new(flowctl)
            .arg("api")
            .arg("build")
            .arg("--build-id")
            .arg(&build_id)
            .arg("--directory")
            .arg(&builds_dir)
            .arg("--fs-root")
            .arg(&builds_dir)
            .arg("--network")
            .arg(connector_network)
            .arg("--source")
            .arg(format!("file:///{pub_id}.flow.yaml"))
            .arg("--source-type")
            .arg("catalog")
            .arg("--ts-package")
            .arg("--log.level=warn")
            .arg("--log.format=color")
            .current_dir(tmpdir),
    )
    .await
    .with_context(|| format!("building catalog in {builds_dir:?}"))?;

    let db = rusqlite::Connection::open_with_flags(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;

    let mut errors = tables::Errors::new();
    errors.load_all(&db).context("loading build errors")?;

    if !job.success() && errors.is_empty() {
        anyhow::bail!("build_job exited with failure but errors is empty");
    }

    // Persist the build.
    let dest_url = builds_root.join(&pub_id.to_string())?;

    let job = jobs::run(
        "persist",
        &logs_tx,
        logs_token,
        tokio::process::Command::new("gsutil")
            .arg("cp")
            .arg(&db_path)
            .arg(dest_url.to_string()),
    )
    .await
    .with_context(|| format!("persisting build sqlite DB {db_path:?}"))?;

    if !job.success() {
        anyhow::bail!("persist of {db_path:?} exited with an error");
    }

    Ok(errors
        .into_iter()
        .map(|e| Error {
            scope: Some(e.scope.into()),
            detail: e.error.to_string(),
            ..Default::default()
        })
        .collect())
}

pub async fn data_plane(
    connector_network: &str,
    flowctl: &str,
    logs_token: Uuid,
    logs_tx: &logs::Tx,
    tmpdir: &path::Path,
) -> anyhow::Result<()> {
    // Start a data-plane. It will use ${tmp_dir}/builds as its builds-root,
    // which we also used as the build directory, meaning the build database
    // is already in-place.
    let data_plane_job = jobs::run(
        "temp-data-plane",
        logs_tx,
        logs_token,
        tokio::process::Command::new(flowctl)
            .arg("temp-data-plane")
            .arg("--network")
            .arg(connector_network)
            .arg("--tempdir")
            .arg(tmpdir)
            .arg("--unix-sockets")
            .arg("--log.level=warn")
            .arg("--log.format=color")
            .current_dir(tmpdir),
    )
    .await
    .with_context(|| format!("starting data-plane in {tmpdir:?}"))?;

    if !data_plane_job.success() {
        anyhow::bail!("data-plane in {tmpdir:?} exited with an unexpected error");
    }

    Ok(())
}

pub async fn test_catalog(
    connector_network: &str,
    flowctl: &str,
    logs_token: Uuid,
    logs_tx: &logs::Tx,
    pub_id: Id,
    tmpdir: &path::Path,
) -> anyhow::Result<Vec<Error>> {
    let mut errors = Vec::new();

    let broker_sock = format!(
        "unix://localhost/{}/gazette.sock",
        tmpdir.as_os_str().to_string_lossy()
    );
    let consumer_sock = format!(
        "unix://localhost/{}/consumer.sock",
        tmpdir.as_os_str().to_string_lossy()
    );
    let build_id = format!("{pub_id}");

    // Activate all derivations.
    let job = jobs::run(
        "setup",
        &logs_tx,
        logs_token,
        tokio::process::Command::new(flowctl)
            .arg("api")
            .arg("activate")
            .arg("--all-derivations")
            .arg("--build-id")
            .arg(&build_id)
            .arg("--network")
            .arg(connector_network)
            .arg("--broker.address")
            .arg(&broker_sock)
            .arg("--consumer.address")
            .arg(&consumer_sock)
            .arg("--log.level=warn")
            .arg("--log.format=color"),
    )
    .await
    .context("starting test setup")?;

    if !job.success() {
        errors.push(Error {
            detail: "test setup failed".to_string(),
            ..Default::default()
        });
        return Ok(errors);
    }

    // Run test cases.
    let job = jobs::run(
        "test",
        &logs_tx,
        logs_token,
        tokio::process::Command::new(flowctl)
            .arg("api")
            .arg("test")
            .arg("--build-id")
            .arg(&build_id)
            .arg("--broker.address")
            .arg(&broker_sock)
            .arg("--consumer.address")
            .arg(&consumer_sock)
            .arg("--log.level=warn")
            .arg("--log.format=color"),
    )
    .await
    .context("starting test runner")?;

    if !job.success() {
        errors.push(Error {
            detail: "one or more test cases failed".to_string(),
            ..Default::default()
        });
    }

    // Clean up derivations.
    let job = jobs::run(
        "cleanup",
        logs_tx,
        logs_token,
        tokio::process::Command::new(flowctl)
            .arg("api")
            .arg("delete")
            .arg("--all-derivations")
            .arg("--build-id")
            .arg(&build_id)
            .arg("--network")
            .arg(connector_network)
            .arg("--broker.address")
            .arg(&broker_sock)
            .arg("--consumer.address")
            .arg(&consumer_sock)
            .arg("--log.level=warn")
            .arg("--log.format=color"),
    )
    .await?;

    if !job.success() {
        errors.push(Error {
            detail: "test cleanup failed".to_string(),
            ..Default::default()
        });
    }

    Ok(errors)
}

pub async fn deploy_build(
    spec_rows: &[SpecRow],
    connector_network: &str,
    flowctl: &str,
    logs_token: Uuid,
    logs_tx: &logs::Tx,
    pub_id: Id,
) -> anyhow::Result<Vec<Error>> {
    let mut errors = Vec::new();

    let build_id = format!("{pub_id}");

    let job = jobs::run(
        "activate",
        &logs_tx,
        logs_token,
        tokio::process::Command::new("echo")
            .arg(flowctl)
            .arg("api")
            .arg("activate")
            .arg("--build-id")
            .arg(&build_id)
            .arg("--network")
            .arg(connector_network)
            .args(spec_rows.iter().filter_map(|r| {
                if r.draft_spec.get() == "null" {
                    None
                } else {
                    Some(format!("--name={}", r.catalog_name))
                }
            }))
            .arg("--log.level=info")
            .arg("--log.format=color")
            .arg("--help"), // TODO make this a no-op for now.
    )
    .await
    .context("starting activation")?;

    if !job.success() {
        errors.push(Error {
            detail: "one or more activations failed".to_string(),
            ..Default::default()
        });
    }

    let job = jobs::run(
        "delete",
        &logs_tx,
        logs_token,
        tokio::process::Command::new("echo")
            .arg(flowctl)
            .arg("api")
            .arg("delete")
            .arg("--build-id")
            .arg(&build_id)
            .arg("--network")
            .arg(connector_network)
            .args(spec_rows.iter().filter_map(|r| {
                if r.draft_spec.get() == "null" && r.live_spec.get() != "null" {
                    Some(format!("--name={}", r.catalog_name))
                } else {
                    None
                }
            }))
            .arg("--log.level=info")
            .arg("--log.format=color")
            .arg("--help"), // TODO make this a no-op for now.
    )
    .await
    .context("starting deletions")?;

    if !job.success() {
        errors.push(Error {
            detail: "one or more deletions failed".to_string(),
            ..Default::default()
        });
    }

    Ok(errors)
}
