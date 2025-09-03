use crate::{jobs, logs};
use anyhow::Context;
use models::Id;
use rand::RngCore;
use sqlx::types::Uuid;
use std::path;
use tables::BuiltRow;
use validation::Connectors;

pub async fn build_catalog<Conn: Connectors>(
    builds_root: &url::Url,
    draft: tables::DraftCatalog,
    live: tables::LiveCatalog,
    pub_id: Id,
    build_id: Id,
    tmpdir: &path::Path,
    logs_tx: logs::Tx,
    logs_token: sqlx::types::Uuid,
    connectors: &Conn,
) -> anyhow::Result<build::Output> {
    // We perform the build under a ./builds/ subdirectory, which is a
    // specific sub-path expected by temp-data-plane underneath its
    // working temporary directory. This lets temp-data-plane use the
    // build database in-place.
    let builds_dir = tmpdir.join("builds");
    std::fs::create_dir(&builds_dir).context("creating builds directory")?;
    tracing::debug!(?builds_dir, "using build directory");

    let build_id_str = build_id.to_string();
    let db_path = builds_dir.join(&build_id_str);
    let project_root = url::Url::parse("file:///").unwrap();
    let source = url::Url::parse("file:///flow.json").unwrap();

    // Generate a random initialization vector for the validation.
    // Currently, this is used to derive unique but deterministic redact salts.
    let mut init_vector: [u8; 16] = Default::default();
    rand::thread_rng().fill_bytes(&mut init_vector);

    let built = validation::validate(
        pub_id,
        build_id,
        &project_root,
        connectors,
        &draft,
        &live,
        true,  // Fail_fast.
        false, // Don't no-op capture validation.
        false, // Don't no-op derivation validation.
        false, // Don't no-op materialization validation.
        &init_vector,
    )
    .await;
    let output = build::Output { draft, live, built };

    // Persist the build before we do anything else.
    build::persist(
        proto_flow::flow::build_api::Config {
            build_db: db_path.to_string_lossy().to_string(),
            build_id: build_id_str.clone(),
            source: source.into(),
            source_type: proto_flow::flow::ContentType::Catalog as i32,
            ..Default::default()
        },
        &db_path,
        &output,
    )?;
    let dest_url = builds_root.join(&build_id_str)?;

    // The gsutil job needs to access the GOOGLE_APPLICATION_CREDENTIALS environment variable,
    // so we cannot use `jobs::run` here.
    let persist_job = jobs::run_without_removing_env(
        "persist",
        &logs_tx,
        logs_token,
        &mut if dest_url.scheme() == "file" {
            // Allow tests to run in environments without `gsutil`.
            let mut cmd = async_process::Command::new("cp");
            cmd.arg(&db_path).arg(dest_url.path());
            cmd
        } else {
            let mut cmd = async_process::Command::new("gsutil");
            cmd.arg("-q")
                .arg("cp")
                .arg(&db_path)
                .arg(dest_url.to_string());
            cmd
        },
    )
    .await
    .with_context(|| format!("persisting built sqlite DB {db_path:?}"))?;

    if !persist_job.success() {
        anyhow::bail!("persist of {db_path:?} exited with an error");
    }
    Ok(output)
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
    bindir: &str,
    logs_token: Uuid,
    logs_tx: &logs::Tx,
    build_id: Id,
    tmpdir: &path::Path,
    catalog: &build::Output,
) -> anyhow::Result<tables::Errors> {
    let mut errors = tables::Errors::default();

    // The tmpdir path will always begin with a /, so we don't need to add one
    let broker_socket_path = tmpdir.join("gazette.sock");
    let broker_sock = format!("unix://localhost{}", broker_socket_path.display());
    let consumer_socket_path = tmpdir.join("consumer.sock");
    let consumer_sock = format!("unix://localhost{}", consumer_socket_path.display());

    let build_id = format!("{build_id}");

    // Activate all derivations.
    let metadata = gazette::Metadata::default();
    let router = gazette::Router::new("local");
    let journal_client =
        gazette::journal::Client::new(broker_sock.clone(), metadata.clone(), router.clone());
    let shard_client = gazette::shard::Client::new(consumer_sock.clone(), metadata, router);

    for built in catalog
        .built
        .built_collections
        .iter()
        .filter(|c| c.model().is_some_and(|m| m.derive.is_some()))
    {
        let mut spec = built.spec().cloned().unwrap();
        let shards = spec
            .derivation
            .as_mut()
            .unwrap()
            .shard_template
            .as_mut()
            .unwrap();
        let build_label = shards
            .labels
            .as_mut()
            .unwrap()
            .labels
            .iter_mut()
            .find(|l| l.name == labels::BUILD)
            .unwrap();
        build_label.value = build_id.clone();

        if let Err(err) = activate::activate_collection(
            &journal_client,
            &shard_client,
            &built.collection,
            Some(&spec),
            None, // Use "local" logging.
            None,
            3, // use 3 splits to try to catch shuffle errors
        )
        .await
        .context("activating derivation for test")
        {
            tracing::error!(error = ?err, derivation = %built.catalog_name(), "failed to activate derivation in temp-data-plane");
            errors.insert(tables::Error {
                error: anyhow::anyhow!(
                    "Test setup failed. View logs for details and reach out to support@estuary.dev"
                ),
                scope: url::Url::parse("flow://publication/test/activate").unwrap(),
            });
            // Fail fast on first activation error
            return Ok(errors);
        };
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
        errors.insert(tables::Error {
            error: anyhow::anyhow!("One or more test cases failed. View logs for details."),
            scope: url::Url::parse("flow://publication/test/api/test").unwrap(),
        });
    }

    // Clean up derivations.
    for built in catalog
        .built
        .built_collections
        .iter()
        .filter(|c| c.model().is_some_and(|m| m.derive.is_some()))
    {
        if let Err(error) = activate::activate_collection(
            &journal_client,
            &shard_client,
            &built.collection,
            None,
            None,
            None,
            1,
        )
        .await
        .context("cleaning up derivation after test")
        {
            tracing::error!(?error, derivation = %built.catalog_name(), "failed to delete derivation from temp-data-plane");
            errors.insert(tables::Error {
                error: anyhow::anyhow!(
                    "Test cleanup failed. View logs for details and reach out to support@estuary.dev"
                ),
                scope: url::Url::parse("flow://publication/test/api/delete").unwrap(),
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
