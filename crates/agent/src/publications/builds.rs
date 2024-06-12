use crate::{jobs, logs};
use anyhow::Context;
use build::Connectors;
use models::Id;
use proto_flow::ops::log::Level as LogLevel;
use sqlx::types::Uuid;
use std::path;

pub async fn build_catalog(
    allow_local: bool,
    builds_root: &url::Url,
    draft: tables::DraftCatalog,
    live: tables::LiveCatalog,
    connector_network: String,
    pub_id: Id,
    build_id: Id,
    tmpdir: &path::Path,
    logs_tx: logs::Tx,
    logs_token: sqlx::types::Uuid,
) -> anyhow::Result<build::Output> {
    let log_handler = logs::ops_handler(logs_tx.clone(), "build".to_string(), logs_token);

    // We perform the build under a ./builds/ subdirectory, which is a
    // specific sub-path expected by temp-data-plane underneath its
    // working temporary directory. This lets temp-data-plane use the
    // build database in-place.
    let builds_dir = tmpdir.join("builds");
    std::fs::create_dir(&builds_dir).context("creating builds directory")?;
    tracing::debug!(?builds_dir, "using build directory");

    // colons were causing grpc validation errors
    let build_id_str = build_id.to_string();
    let db_path = builds_dir.join(&build_id_str);
    let project_root = url::Url::parse("file:///").unwrap();
    let source = url::Url::parse("file:///flow.json").unwrap();

    // Build a tokio::Runtime that dispatches all tracing events to `log_handler`.
    let tokio_context = runtime::TokioContext::new(
        LogLevel::Warn,
        log_handler.clone(),
        format!("agent-build-{build_id_str}"),
        1,
    );

    let runtime = runtime::Runtime::new(
        allow_local,
        connector_network.to_string(),
        log_handler,
        None,
        format!("build/{build_id}"),
    );
    let connectors = if cfg!(test) {
        Connectors::new(runtime).with_noop_validations()
    } else {
        Connectors::new(runtime)
    };

    let build_result = tokio_context
        .spawn(async move {
            let built = validation::validate(
                pub_id,
                build_id,
                &project_root,
                &connectors,
                &draft,
                &live,
                true, // fail_fast
            )
            .await;
            build::Output { draft, live, built }
        })
        .await
        .context("unable to join catalog build handle due to panic")?;

    // Persist the build before we do anything else.
    build::persist(
        proto_flow::flow::build_api::Config {
            build_db: db_path.to_string_lossy().to_string(),
            build_id: build_id_str,
            source: source.into(),
            source_type: proto_flow::flow::ContentType::Catalog as i32,
            ..Default::default()
        },
        &db_path,
        &build_result,
    )?;
    let dest_url = builds_root.join(&build_id.to_string())?;

    // The gsutil job needs to access the GOOGLE_APPLICATION_CREDENTIALS environment variable,
    // so we cannot use `jobs::run` here.
    let persist_job = jobs::run_without_removing_env(
        "persist",
        &logs_tx,
        logs_token,
        async_process::Command::new("gsutil")
            .arg("-q")
            .arg("cp")
            .arg(&db_path)
            .arg(dest_url.to_string()),
    )
    .await
    .with_context(|| format!("persisting built sqlite DB {db_path:?}"))?;

    if !persist_job.success() {
        anyhow::bail!("persist of {db_path:?} exited with an error");
    }
    Ok(build_result)
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
        async_process::Command::new(format!("{bindir}/flowctl-go"))
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
    build_id: Id,
    tmpdir: &path::Path,
) -> anyhow::Result<Vec<tables::Error>> {
    let mut errors = Vec::new();

    let broker_sock = format!(
        "unix://localhost/{}/gazette.sock",
        tmpdir.as_os_str().to_string_lossy()
    );
    let consumer_sock = format!(
        "unix://localhost/{}/consumer.sock",
        tmpdir.as_os_str().to_string_lossy()
    );
    let build_id = build_id.to_string();

    // Activate all derivations.
    let job = jobs::run(
        "setup",
        &logs_tx,
        logs_token,
        async_process::Command::new(format!("{bindir}/flowctl-go"))
            .arg("api")
            .arg("activate")
            .arg("--all-derivations")
            .arg("--build-id")
            .arg(&build_id)
            // Use >1 splits to catch logic failures of shuffle configuration.
            .arg("--initial-splits=3")
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
        errors.push(tables::Error {
            error: anyhow::anyhow!(
                "Test setup failed. View logs for details and reach out to support@estuary.dev"
            ),
            scope: url::Url::parse("flow://publication/test/api/activate").unwrap(),
        });
        return Ok(errors);
    }

    // Run test cases.
    let job = jobs::run(
        "test",
        &logs_tx,
        logs_token,
        async_process::Command::new(format!("{bindir}/flowctl-go"))
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
        errors.push(tables::Error {
            error: anyhow::anyhow!("One or more test cases failed. View logs for details."),
            scope: url::Url::parse("flow://publication/test/api/test").unwrap(),
        });
    }

    // Clean up derivations.
    let job = jobs::run(
        "cleanup",
        logs_tx,
        logs_token,
        async_process::Command::new(format!("{bindir}/flowctl-go"))
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
        errors.push(tables::Error {
            error: anyhow::anyhow!(
                "Test cleanup failed. View logs for details and reach out to support@estuary.dev"
            ),
            scope: url::Url::parse("flow://publication/test/api/delete").unwrap(),
        });
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
