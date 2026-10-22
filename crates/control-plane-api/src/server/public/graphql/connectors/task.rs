use super::{ConnectorProto, ConnectorSpec};
use anyhow::Context;
use proto_flow::{capture, connector, flow, materialize};

/// The definition of a capture or materialization, whose connector is asked for its Spec.
#[derive(Debug)]
enum Task {
    Capture(models::CaptureDef),
    Materialization(models::MaterializationDef),
}

/// Resolve the ConnectorSpec of a capture or materialization by dialing its
/// connector: as the task is staged in `draft_id`, or else as it's live.
/// Returns None if the task is neither staged nor live.
pub(super) async fn resolve(
    ctx: &async_graphql::Context<'_>,
    catalog_name: models::Name,
    draft_id: Option<models::Id>,
    requested_data_plane: Option<String>,
) -> async_graphql::Result<Option<ConnectorSpec>> {
    let env = ctx.data::<crate::Envelope>()?;
    let app = ctx.data::<std::sync::Arc<crate::App>>()?;
    let subject = env.claims()?.subject();

    // Dialing a task's connector requires that the caller can read the task.
    super::super::verify_authorization(
        env,
        catalog_name.as_str(),
        models::authz::Capability::CatalogRead,
    )
    .await?;

    let spec = match try_resolve(
        app,
        env,
        &subject,
        &catalog_name,
        draft_id,
        requested_data_plane.as_deref(),
    )
    .await
    {
        Ok(spec) => spec,
        Err(error) => match error.downcast::<tonic::Status>() {
            Ok(status) => env.authorization_outcome(Err(status)).await?.1,
            Err(error) => return Err(error.into()),
        },
    };
    Ok(spec)
}

async fn try_resolve(
    app: &crate::App,
    env: &crate::Envelope,
    subject: &models::authz::Subject,
    catalog_name: &models::Name,
    draft_id: Option<models::Id>,
    requested_data_plane: Option<&str>,
) -> anyhow::Result<Option<ConnectorSpec>> {
    let pool = &env.pg_pool;
    let snapshot = env.snapshot();

    // A draft owned by someone else reads as missing. `staged` distinguishes a
    // draft without an entry for the task from an entry staging a deletion.
    let staged = if let Some(draft_id) = draft_id {
        let draft = sqlx::query!(
            r#"
            SELECT ds.catalog_name IS NOT NULL AS "staged!",
                   ds.spec_type AS "spec_type?: models::CatalogType",
                   ds.spec::text AS spec
            FROM drafts d
            LEFT JOIN draft_specs ds ON ds.draft_id = d.id AND ds.catalog_name = $3
            WHERE d.id = $1 AND d.user_id = $2
            "#,
            draft_id as models::Id,
            subject.user_id,
            catalog_name.as_str(),
        )
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("draft not found"))?;

        draft.staged.then_some((draft.spec_type, draft.spec))
    } else {
        None
    };

    // The task is dialed in its live data plane even when its draft has edits.
    let live = sqlx::query!(
        r#"
        SELECT ls.spec_type AS "spec_type?: models::CatalogType",
               ls.spec::text AS "spec!",
               ls.data_plane_id AS "data_plane_id: models::Id"
        FROM live_specs ls
        WHERE ls.catalog_name = $1 AND ls.spec IS NOT NULL
        "#,
        catalog_name.as_str(),
    )
    .fetch_optional(pool)
    .await?;

    let Some(task) = select_task(
        staged
            .as_ref()
            .map(|(spec_type, spec)| (*spec_type, spec.as_deref())),
        live.as_ref()
            .map(|live| (live.spec_type, live.spec.as_str())),
    )?
    else {
        return Ok(None);
    };

    let data_plane = crate::storage_mappings::select_task_data_plane(
        snapshot,
        subject,
        catalog_name,
        task.catalog_type(),
        live.as_ref().map(|live| live.data_plane_id),
        requested_data_plane,
    )?;

    let (image, request) = spec_request(&task)?;
    let (image_name, image_tag) = models::split_image_tag(&image);

    let connectors = app
        .connector_factory
        .make_connectors(snapshot, "spec", uuid::Uuid::new_v4());
    let (started, _response) = connectors(data_plane.control_id, request)
        .await
        .with_context(|| format!("resolving the spec of {image}"))?;

    // Fields which aren't part of a Spec response come from the image tag's
    // `connector_tags` row, if it has a fetched spec as `fullImageName` requires.
    // There's no fallback to another tag.
    let tag = sqlx::query!(
        r#"
        SELECT ct.id AS "id: models::Id",
               ct.disable_backfill,
               ct.default_capture_interval::text AS "default_capture_interval"
        FROM connector_tags ct
        JOIN connectors c ON c.id = ct.connector_id
        WHERE c.image_name = $1 AND ct.image_tag = $2 AND ct.protocol IS NOT NULL
          AND ct.endpoint_spec_schema IS NOT NULL AND ct.resource_spec_schema IS NOT NULL
        "#,
        image_name,
        image_tag,
    )
    .fetch_optional(pool)
    .await?;

    let mut spec = spec_response(&task, image_tag, started)?;
    if let Some(tag) = tag {
        spec.id = Some(tag.id);
        spec.disable_backfill = tag.disable_backfill;
        spec.default_capture_interval = tag.default_capture_interval;
    }
    Ok(Some(spec))
}

impl Task {
    fn catalog_type(&self) -> models::CatalogType {
        match self {
            Task::Capture(_) => models::CatalogType::Capture,
            Task::Materialization(_) => models::CatalogType::Materialization,
        }
    }
}

/// Select the task's `staged` entry, if its draft has one, or else its `live` spec.
/// Each is its catalog type and model, and a staged model of None is a deletion.
fn select_task(
    staged: Option<(Option<models::CatalogType>, Option<&str>)>,
    live: Option<(Option<models::CatalogType>, &str)>,
) -> anyhow::Result<Option<Task>> {
    let (spec_type, spec) = match (staged, live) {
        (Some((spec_type, Some(spec))), _) => (spec_type, spec),
        (Some((_, None)), _) => anyhow::bail!("draft entry is a deletion"),
        (None, Some(live)) => live,
        (None, None) => return Ok(None),
    };
    let task = match spec_type {
        Some(models::CatalogType::Capture) => Task::Capture(
            serde_json::from_str(spec).map_err(|_| anyhow::anyhow!("invalid capture model"))?,
        ),
        Some(models::CatalogType::Materialization) => Task::Materialization(
            serde_json::from_str(spec)
                .map_err(|_| anyhow::anyhow!("invalid materialization model"))?,
        ),
        _ => anyhow::bail!("only captures and materializations have a connector spec"),
    };
    let delete = match &task {
        Task::Capture(model) => model.delete,
        Task::Materialization(model) => model.delete,
    };
    if delete {
        anyhow::bail!("draft entry is a deletion");
    }
    Ok(Some(task))
}

/// Map a task's endpoint into its connector image and Spec request,
/// as validation maps the endpoint into the connector it dials.
/// A local endpoint has no image, and is an error.
fn spec_request(task: &Task) -> anyhow::Result<(String, connector::Request)> {
    let (image, kind) = match task {
        Task::Capture(model) => {
            // A Python capture (`endpoint.python`) will map into its image
            // connector here, as validation's `capture_python_connector` does.
            let config = match &model.endpoint {
                models::CaptureEndpoint::Connector(config) => config.clone(),
                models::CaptureEndpoint::Local(_) => {
                    anyhow::bail!("a local connector has no image to ask for its spec")
                }
            };
            let request = capture::request::Spec {
                connector_type: flow::capture_spec::ConnectorType::Image as i32,
                config_json: serde_json::to_string(&config).unwrap().into(),
            };
            (
                config.image,
                connector::request::Kind::Capture(capture::Request {
                    kind: Some(capture::request::Kind::Spec(request)),
                    ..Default::default()
                }),
            )
        }
        Task::Materialization(model) => {
            let (image, connector_type, config_json) = match &model.endpoint {
                models::MaterializationEndpoint::Connector(config) => (
                    config.image.clone(),
                    flow::materialization_spec::ConnectorType::Image,
                    serde_json::to_string(config).unwrap(),
                ),
                models::MaterializationEndpoint::Dekaf(config) => (
                    config.image_name(),
                    flow::materialization_spec::ConnectorType::Dekaf,
                    serde_json::to_string(config).unwrap(),
                ),
                models::MaterializationEndpoint::Local(_) => {
                    anyhow::bail!("a local connector has no image to ask for its spec")
                }
            };
            let request = materialize::request::Spec {
                connector_type: connector_type as i32,
                config_json: config_json.into(),
            };
            (
                image,
                connector::request::Kind::Materialize(materialize::Request {
                    kind: Some(materialize::request::Kind::Spec(request)),
                    ..Default::default()
                }),
            )
        }
    };
    let request = connector::Request {
        start: Some(connector::request::Start::default()),
        kind: Some(kind),
    };
    Ok((image, request))
}

/// Map the Spec with which a task's connector `started` into a ConnectorSpec.
/// Fields which aren't part of a Spec response take their defaults.
fn spec_response(
    task: &Task,
    image_tag: String,
    started: connector::response::Started,
) -> anyhow::Result<ConnectorSpec> {
    use connector::response::started::Spec;

    let (protocol, config_schema_json, resource_config_schema_json, documentation_url) =
        match (task, started.spec) {
            (Task::Capture(_), Some(Spec::Capture(spec))) => {
                let capture::response::Spec {
                    config_schema_json,
                    resource_config_schema_json,
                    documentation_url,
                    ..
                } = *spec;
                (
                    ConnectorProto::Capture,
                    config_schema_json,
                    resource_config_schema_json,
                    documentation_url,
                )
            }
            (Task::Materialization(_), Some(Spec::Materialize(spec))) => {
                let materialize::response::Spec {
                    config_schema_json,
                    resource_config_schema_json,
                    documentation_url,
                    ..
                } = *spec;
                (
                    ConnectorProto::Materialization,
                    config_schema_json,
                    resource_config_schema_json,
                    documentation_url,
                )
            }
            _ => anyhow::bail!("connector did not return a {} spec", task.catalog_type()),
        };

    Ok(ConnectorSpec {
        id: None,
        image_tag,
        protocol: Some(protocol),
        documentation_url: Some(documentation_url).filter(|url| !url.is_empty()),
        endpoint_spec_schema: Some(async_graphql::Json(
            serde_json::from_slice(&config_schema_json)
                .context("parsing the endpoint config schema")?,
        )),
        resource_spec_schema: Some(async_graphql::Json(
            serde_json::from_slice(&resource_config_schema_json)
                .context("parsing the resource config schema")?,
        )),
        disable_backfill: false,
        default_capture_interval: None,
    })
}

#[cfg(test)]
mod test {
    use crate::test_server;
    use proto_flow::{capture, connector, materialize};

    const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);
    const BOB: uuid::Uuid = uuid::Uuid::from_bytes([0x22; 16]);
    const QUERY: &str = r#"
    query ($fullImageName: String, $catalogName: Name, $draftId: Id, $dataPlane: String) {
      connectorSpec(fullImageName: $fullImageName, catalogName: $catalogName, draftId: $draftId, dataPlane: $dataPlane) {
        id imageTag protocol documentationUrl endpointSpecSchema resourceSpecSchema
        disableBackfill defaultCaptureInterval
      }
    }"#;

    fn capture_model(image: &str, config: serde_json::Value) -> String {
        serde_json::json!({
            "endpoint": {"connector": {"image": image, "config": config}},
            "bindings": [],
        })
        .to_string()
    }

    fn materialization_model(endpoint: serde_json::Value) -> String {
        serde_json::json!({"endpoint": endpoint, "bindings": []}).to_string()
    }

    /// Serve a Snapshot read from `pool` through a fixed watch which is taken
    /// after every request, so that authorization denials are terminal.
    async fn start(pool: &sqlx::PgPool) -> test_server::TestServer {
        let data = crate::snapshot::try_fetch(pool, &mut Default::default())
            .await
            .unwrap();
        let snapshot = crate::Snapshot::new(tokens::now() + chrono::TimeDelta::hours(1), data);
        test_server::TestServer::start(
            pool.clone(),
            tokens::fixed(Ok(snapshot)).ready_owned().await,
        )
        .await
    }

    async fn insert_draft(pool: &sqlx::PgPool, user_id: uuid::Uuid) -> models::Id {
        sqlx::query_scalar("INSERT INTO drafts (user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn stage(
        pool: &sqlx::PgPool,
        draft_id: models::Id,
        name: &str,
        spec_type: &str,
        spec: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO draft_specs (draft_id, catalog_name, spec_type, spec) VALUES ($1, $2, $3::catalog_spec_type, $4::json)",
        )
        .bind(draft_id)
        .bind(name)
        .bind(spec_type)
        .bind(spec)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn query(
        server: &test_server::TestServer,
        token: &str,
        variables: serde_json::Value,
    ) -> serde_json::Value {
        server
            .graphql(
                &serde_json::json!({ "query": QUERY, "variables": variables }),
                Some(token),
            )
            .await
    }

    /// Drain the Spec requests which the server dialed, with their data planes.
    fn take_requests(server: &test_server::TestServer) -> Vec<serde_json::Value> {
        server
            .connectors
            .requests
            .lock()
            .unwrap()
            .drain(..)
            .map(|(data_plane_id, request)| {
                let (protocol, connector_type, config_json) = match request.kind {
                    Some(connector::request::Kind::Capture(capture::Request {
                        kind: Some(capture::request::Kind::Spec(spec)),
                        ..
                    })) => ("capture", spec.connector_type, spec.config_json),
                    Some(connector::request::Kind::Materialize(materialize::Request {
                        kind: Some(materialize::request::Kind::Spec(spec)),
                        ..
                    })) => ("materialize", spec.connector_type, spec.config_json),
                    _ => panic!("unexpected request {request:?}"),
                };
                serde_json::json!({
                    "dataPlaneId": data_plane_id,
                    "protocol": protocol,
                    "connectorType": connector_type,
                    "config": serde_json::from_slice::<serde_json::Value>(&config_json).unwrap(),
                })
            })
            .collect()
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../../fixtures",
            scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
        )
    )]
    async fn resolves_staged_or_live_tasks(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        for (name, model) in [
            (
                "aliceCo/in/capture-foo",
                capture_model("source/test:test", serde_json::json!({"live": true})),
            ),
            (
                "aliceCo/out/materialize-bar",
                materialization_model(serde_json::json!({
                    "connector": {"image": "materialize/test:test", "config": {"live": true}}
                })),
            ),
        ] {
            sqlx::query("UPDATE live_specs SET spec = $1::json WHERE catalog_name = $2")
                .bind(model)
                .bind(name)
                .execute(&pool)
                .await
                .unwrap();
        }
        let draft_id = insert_draft(&pool, ALICE).await;
        stage(
            &pool,
            draft_id,
            "aliceCo/in/capture-foo",
            "capture",
            Some(&capture_model(
                "source/multi-tag-test:v2",
                serde_json::json!({"staged": true}),
            )),
        )
        .await;
        stage(
            &pool,
            draft_id,
            "aliceCo/private/capture-new",
            "capture",
            Some(&capture_model(
                "source/test:test",
                serde_json::json!({"new": true}),
            )),
        )
        .await;
        stage(
            &pool,
            draft_id,
            "aliceCo/out/dekaf-new",
            "materialization",
            Some(&materialization_model(serde_json::json!({
                "dekaf": {"variant": "foo", "config": {"new": true}}
            }))),
        )
        .await;

        let server = start(&pool).await;
        let alice = server.make_access_token(ALICE, Some("alice@example.com"));
        let draft_id = serde_json::json!(draft_id);
        let mut results = serde_json::Map::new();

        for (case, variables) in [
            // Without a draft, the live task is used, and fields which a Spec
            // response doesn't carry come from its image tag's row.
            (
                "liveCapture",
                serde_json::json!({"catalogName": "aliceCo/in/capture-foo"}),
            ),
            // A staged entry takes precedence, and is dialed in the live data plane.
            // Its image tag's row has no fetched spec, so those fields take their
            // defaults. (The Dekaf image below has no row at all.)
            (
                "stagedCapture",
                serde_json::json!({"catalogName": "aliceCo/in/capture-foo", "draftId": draft_id}),
            ),
            // A draft without an entry for the task falls back to the live task.
            (
                "liveMaterializationOfDraft",
                serde_json::json!({"catalogName": "aliceCo/out/materialize-bar", "draftId": draft_id}),
            ),
            // A task which isn't live is dialed in a data plane of its storage
            // mapping, which for `aliceCo/private/` is plane two.
            (
                "newCapture",
                serde_json::json!({"catalogName": "aliceCo/private/capture-new", "draftId": draft_id}),
            ),
            (
                "newDekafMaterialization",
                serde_json::json!({"catalogName": "aliceCo/out/dekaf-new", "draftId": draft_id}),
            ),
            // A task which is neither staged nor live resolves to null.
            (
                "missing",
                serde_json::json!({"catalogName": "aliceCo/missing", "draftId": draft_id}),
            ),
        ] {
            let response = query(&server, &alice, variables).await;
            results.insert(
                case.to_string(),
                serde_json::json!({
                    "response": response,
                    "requests": take_requests(&server),
                }),
            );
        }
        insta::assert_json_snapshot!(results);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../../../../fixtures",
            scripts("data_planes", "alice", "drafts", "connectors", "storage_mappings")
        )
    )]
    async fn rejects_invalid_requests(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        sqlx::query(
            "UPDATE live_specs SET spec = $1::json WHERE catalog_name = 'aliceCo/in/capture-foo'",
        )
        .bind(capture_model("source/test:test", serde_json::json!({})))
        .execute(&pool)
        .await
        .unwrap();
        let draft_id = insert_draft(&pool, ALICE).await;
        let bobs_draft_id = insert_draft(&pool, BOB).await;
        stage(
            &pool,
            draft_id,
            "aliceCo/in/capture-deleted",
            "capture",
            None,
        )
        .await;
        stage(
            &pool,
            draft_id,
            "aliceCo/in/capture-local",
            "capture",
            Some(
                r#"{"endpoint":{"local":{"command":["source-local"],"config":{}}},"bindings":[]}"#,
            ),
        )
        .await;

        let server = start(&pool).await;
        let alice = server.make_access_token(ALICE, Some("alice@example.com"));
        let bob = server.make_access_token(BOB, Some("bob@example.test"));
        let mut results = serde_json::Map::new();

        for (case, token, variables) in [
            (
                "imageAndTask",
                &alice,
                serde_json::json!({"fullImageName": "source/test:test", "catalogName": "aliceCo/in/capture-foo"}),
            ),
            (
                "imageWithDraft",
                &alice,
                serde_json::json!({"fullImageName": "source/test:test", "draftId": draft_id}),
            ),
            ("neither", &alice, serde_json::json!({})),
            (
                "stagedDeletion",
                &alice,
                serde_json::json!({"catalogName": "aliceCo/in/capture-deleted", "draftId": draft_id}),
            ),
            (
                "localEndpoint",
                &alice,
                serde_json::json!({"catalogName": "aliceCo/in/capture-local", "draftId": draft_id}),
            ),
            (
                "collection",
                &alice,
                serde_json::json!({"catalogName": "aliceCo/data/foo"}),
            ),
            (
                "othersDraft",
                &alice,
                serde_json::json!({"catalogName": "aliceCo/in/capture-foo", "draftId": bobs_draft_id}),
            ),
            (
                "livePlaneMismatch",
                &alice,
                serde_json::json!({"catalogName": "aliceCo/in/capture-foo", "dataPlane": "ops/dp/public/gcp-us-central1-c2"}),
            ),
            (
                "unreadable",
                &bob,
                serde_json::json!({"catalogName": "aliceCo/in/capture-foo"}),
            ),
        ] {
            let response = query(&server, token, variables).await;
            results.insert(case.to_string(), response["errors"][0]["message"].clone());
        }
        assert!(take_requests(&server).is_empty(), "no connector is dialed");
        insta::assert_json_snapshot!(results);
    }
}
