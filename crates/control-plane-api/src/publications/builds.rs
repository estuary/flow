use crate::{jobs, logs, proxy_connectors::MakeConnectors};
use anyhow::Context;
use models::Id;
use rand::RngCore;
use sqlx::types::Uuid;
use std::path;
use validation::Connectors;

#[async_trait::async_trait]
pub trait Builder: Send + Sync + std::fmt::Debug {
    async fn build(
        &self,
        builds_root: &url::Url,
        draft: tables::DraftCatalog,
        live: tables::LiveCatalog,
        pub_id: Id,
        build_id: Id,
        tmpdir: &path::Path,
        logs_tx: logs::Tx,
        logs_token: sqlx::types::Uuid,
        explicit_plane_name: Option<&str>,
    ) -> anyhow::Result<build::Output>;
}

#[derive(Debug)]
pub struct BuilderImpl<MC: MakeConnectors> {
    make_connectors: MC,
}

impl<MC: MakeConnectors> BuilderImpl<MC> {
    pub fn new(make_connectors: MC) -> Self {
        Self { make_connectors }
    }
}

#[async_trait::async_trait]
impl<MC: MakeConnectors> Builder for BuilderImpl<MC> {
    async fn build(
        &self,
        builds_root: &url::Url,
        draft: tables::DraftCatalog,
        live: tables::LiveCatalog,
        pub_id: Id,
        build_id: Id,
        tmpdir: &path::Path,
        logs_tx: logs::Tx,
        logs_token: sqlx::types::Uuid,
        explicit_plane_name: Option<&str>,
    ) -> anyhow::Result<build::Output> {
        let connectors = self.make_connectors.make_connectors(logs_token);
        build_catalog(
            builds_root,
            draft,
            live,
            pub_id,
            build_id,
            tmpdir,
            logs_tx,
            logs_token,
            &connectors,
            explicit_plane_name,
        )
        .await
    }
}

/// Create a new Builder instance
pub fn new_builder<MC: MakeConnectors>(make_connectors: MC) -> Box<dyn Builder> {
    Box::new(BuilderImpl::new(make_connectors))
}

async fn build_catalog<Conn: Connectors>(
    builds_root: &url::Url,
    draft: tables::DraftCatalog,
    live: tables::LiveCatalog,
    pub_id: Id,
    build_id: Id,
    tmpdir: &path::Path,
    logs_tx: logs::Tx,
    logs_token: sqlx::types::Uuid,
    connectors: &Conn,
    explicit_plane_name: Option<&str>,
) -> anyhow::Result<build::Output> {
    // Stage the build database under a ./builds/ subdirectory of the working
    // temporary directory; it is uploaded to `builds_root` further below.
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
    rand::rng().fill_bytes(&mut init_vector);

    let built = validation::validate(
        pub_id,
        build_id,
        &project_root,
        connectors,
        explicit_plane_name,
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

/// Run a built catalog's tests on the local `runtime-harness`, returning a
/// `tables::Error` per failing test case. Derivations execute as resident
/// runtime-next sessions (derive-sqlite in-process; image derivations as
/// containers on `connector_network`, with three splits to exercise multi-shard
/// key routing) and each test's ingest / verify steps run against an in-memory
/// collection store. No Gazette broker, etcd, Go consumer, or `flowctl-go`
/// binary is involved. Connector and runtime logs stream to the job's `logs_tx`.
pub async fn test_catalog(
    logs_token: Uuid,
    logs_tx: &logs::Tx,
    connector_network: &str,
    catalog: &build::Output,
) -> anyhow::Result<tables::Errors> {
    let mut errors = tables::Errors::default();
    if catalog.built.built_tests.is_empty() {
        return Ok(errors);
    }

    // Stream the harness's ops logs (connector output + flattened runtime events)
    // to the publication's job logs, matching the prior `flowctl-go api test`
    // job's "test" stream.
    let ops_handler = logs::ops_handler(logs_tx.clone(), "test".to_string(), logs_token);
    let options = runtime_harness::Options {
        network: connector_network.to_string(),
        splits: 3, // Exercise multi-shard key routing, as the prior path did.
        snapshot_dir: None,
        log_handler: std::sync::Arc::new(move |log: &ops::Log| {
            runtime::LogHandler::log(&ops_handler, log)
        }),
    };

    let results = runtime_harness::run_tests(&catalog.built, options)
        .await
        .context("running catalog tests")?;

    for outcome in &results.outcomes {
        let Some(failure) = &outcome.error else {
            continue;
        };
        // The scope is the failing step's source URL with a JSON-pointer
        // fragment, so failures anchor to the exact test step.
        let scope = url::Url::parse(&outcome.scope)
            .unwrap_or_else(|_| url::Url::parse("flow://publication/test").unwrap());
        errors.insert(tables::Error {
            error: anyhow::anyhow!("test {} failed:\n{failure}", outcome.name),
            scope,
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
