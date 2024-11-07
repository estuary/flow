use super::{Discover, DiscoverHandler};
use crate::{draft, proxy_connectors::Connectors, HandleResult, Handler, Id};
use agent_sql::discovers::Row;
use anyhow::Context;
use serde::{Deserialize, Serialize};

/// JobStatus is the possible outcomes of a handled discover operation.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    WrongProtocol,
    TagFailed,
    ImageForbidden,
    PullFailed,
    DiscoverFailed,
    MergeFailed,
    Success {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        publication_id: Option<Id>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        specs_unchanged: bool,
    },
    DeprecatedBackground,
    NoDataPlane,
}

impl JobStatus {
    #[cfg(test)]
    pub fn is_success(&self) -> bool {
        match self {
            JobStatus::Success { .. } => true,
            _ => false,
        }
    }
}

#[async_trait::async_trait]
impl<C: Connectors> Handler for DiscoverHandler<C> {
    async fn handle(
        &mut self,
        pg_pool: &sqlx::PgPool,
        allow_background: bool,
    ) -> anyhow::Result<HandleResult> {
        let mut txn = pg_pool.begin().await?;

        let row: Row = match agent_sql::discovers::dequeue(&mut txn, allow_background).await? {
            None => return Ok(HandleResult::NoJobs),
            Some(row) => row,
        };

        let time_queued = chrono::Utc::now().signed_duration_since(row.updated_at);
        let (id, status) = self.process(row, &mut txn, pg_pool).await?;
        tracing::info!(%id, %time_queued, ?status, "finished");

        agent_sql::discovers::resolve(id, status, &mut txn).await?;
        txn.commit().await?;

        Ok(HandleResult::HadJob)
    }

    fn table_name(&self) -> &'static str {
        "discovers"
    }
}

impl<C: Connectors> DiscoverHandler<C> {
    #[tracing::instrument(err, skip_all, fields(id=?row.id, draft_id = ?row.draft_id, user_id = %row.user_id))]
    async fn process(
        &mut self,
        row: Row,
        txn: &mut sqlx::Transaction<'static, sqlx::Postgres>,
        pool: &sqlx::PgPool,
    ) -> anyhow::Result<(Id, JobStatus)> {
        tracing::info!(
            %row.background,
            %row.capture_name,
            %row.connector_tag_id,
            %row.connector_tag_job_success,
            %row.created_at,
            %row.data_plane_name,
            %row.draft_id,
            %row.image_name,
            %row.image_tag,
            %row.logs_token,
            %row.protocol,
            %row.updated_at,
            %row.user_id,
            "processing discover",
        );

        // Remove draft errors from a previous attempt.
        agent_sql::drafts::delete_errors(row.draft_id, txn)
            .await
            .context("clearing old errors")?;

        // Various pre-flight checks.
        if !row.connector_tag_job_success {
            return Ok((row.id, JobStatus::TagFailed));
        } else if row.protocol != "capture" {
            return Ok((row.id, JobStatus::WrongProtocol));
        } else if !agent_sql::connector_tags::does_connector_exist(&row.image_name, &mut *txn)
            .await?
        {
            return Ok((row.id, JobStatus::ImageForbidden));
        }
        // This used to be how automated discovers worked, but it's being
        // replaced by controllers. Fail any discovers that use `background` or
        // `auto_publish`, as there may be some during the rollout. These
        // failures should not impact the operation of the capture, since
        // controllers should be handling them.
        if row.auto_publish || row.background {
            tracing::warn!(
                "failing discover due to use of deprecated auto_publish or background columns"
            );
            return Ok((row.id, JobStatus::DeprecatedBackground));
        }
        let mut data_planes: tables::DataPlanes = agent_sql::data_plane::fetch_data_planes(
            pool,
            Vec::new(),
            row.data_plane_name.as_str(),
            row.user_id,
        )
        .await?;

        let Some(data_plane) = data_planes.pop().filter(|d| d.is_default) else {
            tracing::warn!(data_plane_name = ?row.data_plane_name, "data-plane not found or user may not be authorized");
            return Ok((row.id, JobStatus::NoDataPlane));
        };

        let image_composed = format!("{}{}", row.image_name, row.image_tag);
        let disco = prepare_discover(
            row.user_id,
            row.draft_id,
            models::Capture::new(&row.capture_name),
            row.endpoint_config.0.clone().into(),
            row.update_only,
            row.logs_token,
            image_composed,
            data_plane,
            pool,
        )
        .await?;
        let output = self.discover(pool, disco).await?;
        if !output.is_success() {
            let draft_errors = output
                .draft
                .errors
                .iter()
                .map(draft::Error::from_tables_error)
                .collect();
            crate::draft::insert_errors(row.draft_id, draft_errors, txn).await?;
            return Ok((row.id, JobStatus::DiscoverFailed));
        }

        draft::upsert_draft_catalog(row.draft_id, &output.draft, txn)
            .await
            .context("inserting draft specs")?;

        Ok((
            row.id,
            JobStatus::Success {
                publication_id: None,
                specs_unchanged: false,
            },
        ))
    }
}

/// Resolves the specs to be used as the base for a discover, and returns them
/// as part of a fully prepared `Discover`. This will always include the capture
/// spec, even if there is no extant drafted or live spec for it. The resolved
/// capture will always have the endpoint set to the values from the `discovers`
/// row, even if it differs from the endpoint on the drafted or live spec. All
/// other specs in the given draft will be loaded as they are and used as the
/// base for the merge after the discover completes.
async fn prepare_discover(
    user_id: uuid::Uuid,
    draft_id: Id,
    capture_name: models::Capture,
    endpoint_config: models::RawValue,
    update_only: bool,
    logs_token: uuid::Uuid,
    image_composed: String,
    data_plane: tables::DataPlane,
    pool: &sqlx::PgPool,
) -> anyhow::Result<Discover> {
    let mut draft = crate::draft::load_draft(draft_id, pool)
        .await
        .context("loading draft")?;

    let endpoint = models::CaptureEndpoint::Connector(models::ConnectorConfig {
        image: image_composed,
        config: endpoint_config,
    });
    if let Some(drafted) = draft.captures.get_mut_by_key(&capture_name) {
        if let Some(model) = drafted.model.as_mut() {
            model.endpoint = endpoint;
        }
    } else {
        let name = &[capture_name.to_string()];
        // Filter to only specs that the user can read. If they can't admin, then wait until they
        // try to publish to surface that error.
        let live =
            crate::live_specs::get_live_specs(user_id, name, Some(models::Capability::Read), pool)
                .await?;

        // See if there's an existing live capture with this name
        if let Some(tables::LiveCapture {
            capture,
            last_pub_id,
            mut model,
            ..
        }) = live.captures.into_iter().next()
        {
            model.endpoint = endpoint;
            draft.captures.insert(tables::DraftCapture {
                capture: capture.clone(),
                model: Some(model),
                expect_pub_id: Some(last_pub_id),
                scope: tables::synthetic_scope(models::CatalogType::Capture, &capture_name),
                is_touch: true, // This will get updated if the discover returns any changes
            });
        } else {
            // There's no existing live or draft spec, so insert a starter spec.
            let new_model = models::CaptureDef {
                endpoint,
                auto_discover: Some(models::AutoDiscover {
                    add_new_bindings: true,
                    evolve_incompatible_collections: true,
                }),
                interval: models::CaptureDef::default_interval(),
                shards: models::ShardTemplate::default(),
                expect_pub_id: None,
                bindings: Vec::new(),
                delete: false,
            };
            draft.captures.insert(tables::DraftCapture {
                capture: capture_name.clone(),
                model: Some(new_model),
                expect_pub_id: Some(Id::zero()),
                scope: tables::synthetic_scope(models::CatalogType::Capture, &capture_name),
                is_touch: false,
            });
        }
    };

    Ok(Discover {
        user_id,
        capture_name,
        data_plane,
        draft,
        update_only,
        logs_token,
    })
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use models::Id;
    use uuid::Uuid;

    #[tokio::test]
    #[serial_test::serial]
    async fn test_prepare_discover() {
        let harness =
            crate::integration_tests::harness::TestHarness::init("test_prepare_discover").await;

        sqlx::query(
            r#"
            with
            p1 as (
                insert into user_grants(user_id, object_role, capability) values
                ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin')
            ),
            p2 as (
                insert into drafts (id, user_id) values
                ('eeeeeeeeeeeeeeee', '11111111-1111-1111-1111-111111111111')
            ),
            p3 as (
                insert into draft_specs (draft_id, catalog_name, spec_type, spec) values
                ('eeeeeeeeeeeeeeee', 'aliceCo/dir/source-thingy', 'capture', '{
                    "bindings": [ ],
                    "endpoint": { "connector": { "config": { "a": "draftedA" }, "image": "draft/image" } },
                    "interval": "10m"
                }'),
                ('eeeeeeeeeeeeeeee', 'aliceCo/dir/another-thingy', 'collection', '{
                    "schema": { "const": 42 },
                    "key": ["/id"]
                }')
            ),
            p4 as (
                -- This is here to assert that it is ignored due to the presence of the drafted capture
                insert into live_specs (catalog_name, spec_type, spec) values
                ('aliceCo/dir/source-thingy', 'capture', '{
                    "bindings": [ {"target": "who/cares" } ],
                    "endpoint": { "connector": { "config": { "a": "liveA" }, "image": "live/image" } },
                    "interval": "90m"
                }')
            )
            select 1;
            "#,
        )
        .execute(&harness.pool)
        .await
        .unwrap();

        let draft_id = Id::from_hex("eeeeeeeeeeeeeeee").unwrap();
        let user_id = Uuid::from_str("11111111-1111-1111-1111-111111111111").unwrap();
        let capture_name = models::Capture::new("aliceCo/dir/source-thingy");
        let endpoint_config = models::RawValue::from_str(r#"{"a": "discoversA"}"#).unwrap();
        let logs_token = Uuid::from_str("22222222-3333-4444-5555-666666666666").unwrap();
        let image_composed = String::from("discovers/image:tag");
        let data_plane = tables::DataPlane {
            control_id: Id::zero(),
            data_plane_name: "test-data-plane".to_string(),
            data_plane_fqdn: "data.plane.test".to_string(),
            is_default: true,
            hmac_keys: Vec::new(),
            ops_logs_name: models::Collection::new("tha/logs"),
            ops_stats_name: models::Collection::new("tha/stats"),
            broker_address: "broker.test".to_string(),
            reactor_address: "reactor.test".to_string(),
        };

        let result = super::prepare_discover(
            user_id,
            draft_id,
            capture_name.clone(),
            endpoint_config,
            false, // !update_only
            logs_token,
            image_composed.clone(),
            data_plane.clone(),
            &harness.pool,
        )
        .await
        .unwrap();

        assert_eq!(capture_name, result.capture_name);

        assert_eq!(Id::zero(), result.data_plane.control_id);
        assert_eq!("test-data-plane", &result.data_plane.data_plane_name);
        assert_eq!("data.plane.test", &result.data_plane.data_plane_fqdn);
        assert!(result.data_plane.is_default);
        assert_eq!("tha/logs", result.data_plane.ops_logs_name.as_str());
        assert_eq!("tha/stats", result.data_plane.ops_stats_name.as_str());
        assert_eq!("broker.test", &result.data_plane.broker_address);
        assert_eq!("reactor.test", &result.data_plane.reactor_address);

        assert_eq!(logs_token, result.logs_token);
        assert_eq!(user_id, result.user_id);
        assert!(!result.update_only);

        // The draft should contain everything that was already drafted
        assert_eq!(1, result.draft.captures.len());
        assert_eq!(1, result.draft.collections.len());
        assert_eq!(2, result.draft.spec_count());

        // The resolved capture should use the endpoint config from the discovers row
        let model = result
            .draft
            .captures
            .get_by_key(&capture_name)
            .unwrap()
            .model
            .as_ref()
            .unwrap();
        let models::CaptureEndpoint::Connector(cfg) = &model.endpoint else {
            panic!("expected connector endpoint, got: {:?}", model.endpoint);
        };
        assert!(cfg.config.get().contains("discoversA"));
        assert_eq!(image_composed, cfg.image);
    }
}
