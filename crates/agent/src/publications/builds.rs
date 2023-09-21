use crate::{draft::Error, jobs, logs, Id};
use agent_sql::publications::{ExpandedRow, SpecRow};
use agent_sql::CatalogType;
use anyhow::Context;
use itertools::Itertools;
use proto_flow::{
    materialize::response::validated::constraint::Type as ConstraintType,
    ops::log::Level as LogLevel,
};
use serde::{Deserialize, Serialize};
use sqlx::types::Uuid;
use std::collections::{BTreeMap, HashSet};
use std::io::Write;
use std::path;

#[derive(Default)]
pub struct BuildOutput {
    pub errors: tables::Errors,
    pub built_captures: tables::BuiltCaptures,
    pub built_collections: tables::BuiltCollections,
    pub built_materializations: tables::BuiltMaterializations,
    pub built_tests: tables::BuiltTests,
}

/// Reasons why a draft collection spec would need to be published under a new name.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub enum ReCreateReason {
    /// The collection key in the draft differs from that of the live spec.
    KeyChange,
    /// One or more collection partition fields in the draft differs from that of the live spec.
    PartitionChange,
    /// A live spec with the same name has already been created and was subsequently deleted.
    PrevDeletedSpec,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct IncompatibleCollection {
    pub collection: String,
    /// Reasons why the collection would need to be re-created in order for a publication of the draft spec to succeed.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub requires_recreation: Vec<ReCreateReason>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub affected_materializations: Vec<AffectedConsumer>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct AffectedConsumer {
    pub name: String,
    pub fields: Vec<RejectedField>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct RejectedField {
    pub field: String,
    pub reason: String,
}

impl BuildOutput {
    pub fn draft_errors(&self) -> Vec<Error> {
        self.errors
            .iter()
            .map(|e| Error {
                scope: Some(e.scope.to_string()),
                // Use "alternate" form to include compact, chained error causes.
                // See: https://docs.rs/anyhow/latest/anyhow/struct.Error.html#display-representations
                detail: format!("{:#}", e.error),
                ..Default::default()
            })
            .collect()
    }

    pub fn get_incompatible_collections(&self) -> Vec<IncompatibleCollection> {
        // We'll collect a map of collection names to lists of materializations that have rejected the proposed collection changes.
        let mut naughty_collections = BTreeMap::new();

        // Look at materialization validation responses for any collections that have been rejected due to unsatisfiable constraints.
        for mat in self.built_materializations.iter() {
            for (i, binding) in mat.validated.bindings.iter().enumerate() {
                let Some(collection_name) = mat.spec.bindings[i].collection.as_ref().map(|c| c.name.as_str()) else {
                    continue;
                };
                let naughty_fields: Vec<RejectedField> = binding
                    .constraints
                    .iter()
                    .filter(|(_, constraint)| {
                        constraint.r#type == ConstraintType::Unsatisfiable as i32
                    })
                    .map(|(field, constraint)| RejectedField {
                        field: field.clone(),
                        reason: constraint.reason.clone(),
                    })
                    .collect();
                if !naughty_fields.is_empty() {
                    let affected_consumers = naughty_collections
                        .entry(collection_name.to_owned())
                        .or_insert_with(|| Vec::new());
                    affected_consumers.push(AffectedConsumer {
                        name: mat.materialization.clone(),
                        fields: naughty_fields,
                    });
                }
            }
        }

        naughty_collections
            .into_iter()
            .map(
                |(collection, affected_materializations)| IncompatibleCollection {
                    collection,
                    affected_materializations,
                    requires_recreation: Vec::new(),
                },
            )
            .collect()
    }
}

pub async fn build_catalog(
    allow_local: bool,
    builds_root: &url::Url,
    catalog: &models::Catalog,
    connector_network: &str,
    control_plane: super::ControlPlane,
    logs_token: Uuid,
    logs_tx: &logs::Tx,
    pub_id: Id,
    tmpdir: &path::Path,
) -> anyhow::Result<BuildOutput> {
    // We perform the build under a ./builds/ subdirectory, which is a
    // specific sub-path expected by temp-data-plane underneath its
    // working temporary directory. This lets temp-data-plane use the
    // build database in-place.
    let builds_dir = tmpdir.join("builds");
    std::fs::create_dir(&builds_dir).context("creating builds directory")?;
    tracing::debug!(?builds_dir, "using build directory");

    // Write our catalog source file within the build directory.
    std::fs::File::create(&builds_dir.join("flow.json"))
        .and_then(|mut f| f.write_all(serde_json::to_string_pretty(catalog).unwrap().as_bytes()))
        .context("writing catalog file")?;

    let build_id = format!("{pub_id}");
    let db_path = builds_dir.join(&build_id);
    let log_handler = logs::ops_handler(logs_tx.clone(), "build".to_string(), logs_token);
    let project_root = url::Url::parse("file:///").unwrap();
    let source = url::Url::parse("file:///flow.json").unwrap();

    let managed_build = build::managed_build(
        allow_local,
        build_id.clone(),
        connector_network.to_string(),
        Box::new(control_plane),
        builds_dir.clone(), // Root for file:// resolution.
        log_handler.clone(),
        project_root,
        source.clone(),
    );

    // Build a tokio::Runtime that dispatches all tracing events to `log_handler`.
    let tokio_context = runtime::TokioContext::new(
        LogLevel::Warn,
        log_handler,
        format!("agent-build-{build_id}"),
        1,
    );
    let build_result = tokio_context
        .spawn(managed_build)
        .await
        .context("unable to join catalog build handle due to panic")?;

    // Persist the build before we do anything else.
    build::persist(
        proto_flow::flow::build_api::Config {
            build_db: db_path.to_string_lossy().to_string(),
            build_id,
            source: source.into(),
            source_type: proto_flow::flow::ContentType::Catalog as i32,
            ..Default::default()
        },
        &db_path,
        build_result.as_ref(),
    )?;
    let dest_url = builds_root.join(&pub_id.to_string())?;

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
    .with_context(|| format!("persisting build sqlite DB {db_path:?}"))?;

    if !persist_job.success() {
        anyhow::bail!("persist of {db_path:?} exited with an error");
    }

    Ok(match build_result {
        Ok((
            _sources,
            tables::Validations {
                built_captures,
                built_collections,
                built_materializations,
                built_tests,
                errors: _,
            },
        )) => BuildOutput {
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            ..Default::default()
        },
        Err(errors) => BuildOutput {
            errors,
            ..Default::default()
        },
    })
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
        errors.push(Error {
            detail: "Test setup failed. View logs for details and reach out to support@estuary.dev"
                .to_string(),
            ..Default::default()
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
        errors.push(Error {
            detail: "One or more test cases failed. View logs for details.".to_string(),
            ..Default::default()
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
        errors.push(Error {
            detail:
                "Test cleanup failed. View logs for details and reach out to support@estuary.dev"
                    .to_string(),
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
    pruned_collections: &HashSet<String>,
) -> anyhow::Result<Vec<Error>> {
    let mut errors = Vec::new();

    let spec_rows = spec_rows
        .iter()
        // Filter specs to be activated
        .filter(|r| match (r.live_type, r.draft_type) {
            // Before and after are both deleted.
            (None, None) => false,
            // Tests don't need activated
            (Some(CatalogType::Test), _) | (_, Some(CatalogType::Test)) => false,
            // Collections may have been pruned if they are not bound to anything
            (Some(_), Some(CatalogType::Collection)) => {
                !pruned_collections.contains(r.catalog_name.as_str())
            }
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
        async_process::Command::new(format!("{bindir}/flowctl-go"))
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
            detail: "One or more task activations failed. View logs for details and reach out to support@estuary.dev".to_string(),
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
            async_process::Command::new(format!("{bindir}/flowctl-go"))
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
            detail: "One or more task deletions failed. View logs for details and reach out to support@estuary.dev".to_string(),
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
