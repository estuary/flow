use super::Error;
use crate::{jobs, logs, Id};

use agent_sql::publications::{ExpandedRow, SpecRow};
use agent_sql::CatalogType;
use anyhow::Context;
use itertools::Itertools;
use sqlx::types::Uuid;
use std::io::Write;
use std::path;
use tables::SqlTableObj;

pub async fn build_catalog(
    builds_root: &url::Url,
    catalog: &models::Catalog,
    connector_network: &str,
    bindir: &str,
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
    std::fs::File::create(&builds_dir.join("flow.json"))
        .and_then(|mut f| f.write_all(serde_json::to_string_pretty(catalog).unwrap().as_bytes()))
        .context("writing catalog file")?;

    let build_id = format!("{pub_id}");
    let db_path = builds_dir.join(&build_id);

    let job = jobs::run(
        "build",
        logs_tx,
        logs_token,
        tokio::process::Command::new(format!("{bindir}/flowctl-go"))
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
            .arg("file:///flow.json")
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

    // The gsutil job needs to access the GOOGLE_APPLICATION_CREDENTIALS environment variable, so
    // we cannot use `jobs::run` here.
    let job = jobs::run_without_removing_env(
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
    bindir: &str,
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
        tokio::process::Command::new(format!("{bindir}/flowctl-go"))
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
    bindir: &str,
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
        tokio::process::Command::new(format!("{bindir}/flowctl-go"))
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
        tokio::process::Command::new(format!("{bindir}/flowctl-go"))
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
        tokio::process::Command::new(format!("{bindir}/flowctl-go"))
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
    bindir: &str,
    broker_address: &url::Url,
    connector_network: &str,
    consumer_address: &url::Url,
    expanded_rows: &[ExpandedRow],
    logs_token: Uuid,
    logs_tx: &logs::Tx,
    pub_id: Id,
    spec_rows: &[SpecRow],
) -> anyhow::Result<Vec<Error>> {
    let mut errors = Vec::new();

    let spec_rows = spec_rows
        .iter()
        // Filter specs which are tests, or are deletions of already-deleted specs.
        .filter(|r| match (r.live_type, r.draft_type) {
            (None, None) => false, // Before and after are both deleted.
            (Some(CatalogType::Test), _) | (_, Some(CatalogType::Test)) => false,
            _ => true,
        });

    // Activate non-deleted drafts plus all non-test expanded specifications.
    let activate_names = spec_rows
        .clone()
        .filter(|r| r.draft_type.is_some())
        .map(|r| format!("--name={}", r.catalog_name))
        .chain(
            expanded_rows
                .iter()
                .filter(|r| !matches!(r.live_type, CatalogType::Test))
                .map(|r| format!("--name={}", r.catalog_name)),
        );

    let job = jobs::run(
        "activate",
        logs_tx,
        logs_token,
        tokio::process::Command::new(format!("{bindir}/flowctl-go"))
            .arg("api")
            .arg("activate")
            .arg("--broker.address")
            .arg(broker_address.as_str())
            .arg("--build-id")
            .arg(format!("{pub_id}"))
            .arg("--consumer.address")
            .arg(consumer_address.as_str())
            .arg("--network")
            .arg(connector_network)
            .arg("--no-wait")
            .args(activate_names)
            .arg("--log.level=info")
            .arg("--log.format=color"),
    )
    .await
    .context("starting activation")?;

    if !job.success() {
        errors.push(Error {
            detail: "one or more activations failed".to_string(),
            ..Default::default()
        });
    }

    // Delete drafts which are deleted, grouped on their `last_build_id`
    // under which they're deleted. Note that `api delete` requires that
    // we give the correct --build-id of the running specification,
    // in order to provide the last-applicable built specification to
    // connector ApplyDelete RPCs.

    let delete_groups = spec_rows
        .filter(|r| r.draft_type.is_none())
        .map(|r| (r.last_build_id, format!("--name={}", r.catalog_name)))
        .sorted()
        .group_by(|(last_build_id, _)| *last_build_id)
        .into_iter()
        .map(|(last_build_id, delete_names)| {
            (
                last_build_id,
                delete_names.map(|(_, name)| name).collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();

    for (last_build_id, delete_names) in delete_groups {
        let job = jobs::run(
            "delete",
            logs_tx,
            logs_token,
            tokio::process::Command::new(format!("{bindir}/flowctl-go"))
                .arg("api")
                .arg("delete")
                .arg("--broker.address")
                .arg(broker_address.as_str())
                .arg("--build-id")
                .arg(format!("{last_build_id}"))
                .arg("--consumer.address")
                .arg(consumer_address.as_str())
                .arg("--network")
                .arg(connector_network)
                .args(delete_names)
                .arg("--log.level=info")
                .arg("--log.format=color"),
        )
        .await
        .context("starting deletions")?;

        if !job.success() {
            errors.push(Error {
                detail: "one or more deletions failed".to_string(),
                ..Default::default()
            });
        }
    }

    Ok(errors)
}

/*
5y/o Abby walks in while Johnny's working in this code, he types / she reads:
Abigail!!!!

Your name is Abby. Kadabby. Bobaddy. Fi Fi Momaddy Abby.

Hey hey! Hey! Hey! Hey!

I love you!! Let's play together. Oh it's fun. I said "oh it's fun".
We are laughing! Together! I was scared of the audio book.
What was the book about?
It was dragons love tacos part 2.

Oh and was there a fire alarm in the audio book? Yes.

Is that what was scary ?  You still remember it.

I know you're not kidding my love.

I think it's time to get back in bed.
You need your rest and sleep.
Otherwise you'll be soooo sleepy tomorrow!

Good job!
You're an amazing reader.

Okay kiddo, I'll walk you back to bed. Let's do it!
*/
