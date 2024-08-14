use anyhow::Context;
use chrono::{DateTime, Utc};
use gazette::{broker, journal, shard};
use models::CatalogType;
use proto_flow::AnyBuiltSpec;
use serde_json::value::RawValue;
use sqlx::types::Uuid;
use std::{collections::BTreeSet, ops::Deref};

use crate::publications::{JobStatus, PublicationResult, Publisher};

macro_rules! unwrap_single {
    ($catalog:expr; expect $expected:ident not $( $unexpected:ident ),+) => {
        {
            $(
                if !$catalog.$unexpected.is_empty() {
                    anyhow::bail!("expected only {} but found a {}", stringify!($expected), stringify!($unexpected));
                }
            )+
            Ok($catalog.$expected.into_iter().next())
        }
    };
}

/// Represents the specification of a connector and tag. This is currently
/// only used by controllers, but there may be an opportunity to consolidate
/// this with the types that are used by `connector_tags.rs`. It's left TBD at
/// this point, as we plan to eventually make connectors a part of the catalog
/// namespace.
pub struct ConnectorSpec {
    pub protocol: runtime::RuntimeProtocol,
    pub documentation_url: String,
    pub endpoint_config_schema: models::Schema,
    pub resource_config_schema: models::Schema,
    pub resource_path_pointers: Vec<doc::Pointer>,
    pub oauth2: Option<Box<RawValue>>,
}

/// A trait for allowing controllers access to the database.
/// This makes it much easier to test controllers, and is intended to serve as a
/// starting point for functions that we may wish to expose via an HTTP API or
/// other language bindings.
#[async_trait::async_trait]
pub trait ControlPlane: Send {
    /// Returns the current time. Having controllers access the current time through this api
    /// allows tests of controllers to be deterministic.
    fn current_time(&self) -> DateTime<Utc>;

    /// Returns a new, globally unique publication id.
    fn next_pub_id(&mut self) -> models::Id;

    /// Activates the given built spec in the data plane.
    async fn data_plane_activate(
        &mut self,
        catalog_name: String,
        spec: &AnyBuiltSpec,
        data_plane_id: models::Id,
    ) -> anyhow::Result<()>;

    /// Deletes the given entity from the data plane.
    async fn data_plane_delete(
        &mut self,
        catalog_name: String,
        spec_type: CatalogType,
        data_plane_id: models::Id,
    ) -> anyhow::Result<()>;

    /// Triggers controller runs for all dependents of the given `catalog_name`.
    async fn notify_dependents(&mut self, catalog_name: String) -> anyhow::Result<()>;

    /// Attempts to publish the given draft, returning a result that indicates
    /// whether it was successful. Returns an `Err` only if there was an error
    /// executing the publication. Unsuccessful publications are represented by
    /// an `Ok`, where the `PublicationResult` has a non-success status.
    async fn publish(
        &mut self,
        publication_id: models::Id,
        detail: Option<String>,
        logs_token: Uuid,
        draft: tables::DraftCatalog,
    ) -> anyhow::Result<PublicationResult>;

    /// Fetch the given set of live specs, returning them all as part of a `LiveCatalog`.
    async fn get_live_specs(
        &mut self,
        names: BTreeSet<String>,
    ) -> anyhow::Result<tables::LiveCatalog>;

    /// Fetch the connector spec for the given image, which should include both
    /// the name and the tag.
    async fn get_connector_spec(
        &mut self,
        connector_image: String,
    ) -> anyhow::Result<ConnectorSpec>;

    /// Fetch the inferred schema for the given collection.
    async fn get_inferred_schema(
        &mut self,
        collection: models::Collection,
    ) -> anyhow::Result<Option<tables::InferredSchema>> {
        let live = self.get_live_specs(set_of(collection)).await?;
        // it's ok for collections to be present in the live catalog. Just ignore it
        unwrap_single!(live; expect inferred_schemas not captures, materializations, tests)
    }

    /// Fetch a single collection spec.
    async fn get_collection(
        &mut self,
        collection: models::Collection,
    ) -> anyhow::Result<Option<tables::LiveCollection>> {
        let live = self.get_live_specs(set_of(collection)).await?;
        // it's ok for inferred_schemas to be present in the live catalog. Just ignore it
        unwrap_single!(live; expect collections not captures, materializations, tests)
    }

    /// Fetch a single capture spec.
    async fn get_capture(
        &mut self,
        capture: models::Capture,
    ) -> anyhow::Result<Option<tables::LiveCapture>> {
        let live = self.get_live_specs(set_of(capture)).await?;
        unwrap_single!(live; expect captures not collections, inferred_schemas, materializations, tests)
    }

    /// Fetch a single materialization spec.
    async fn get_materialization(
        &mut self,
        materialization: models::Materialization,
    ) -> anyhow::Result<Option<tables::LiveMaterialization>> {
        let live = self.get_live_specs(set_of(materialization)).await?;
        unwrap_single!(live; expect materializations not captures, collections, inferred_schemas, tests)
    }

    /// Fetch a single test spec.
    async fn get_test(&mut self, test: models::Test) -> anyhow::Result<Option<tables::LiveTest>> {
        let live = self.get_live_specs(set_of(test)).await?;
        unwrap_single!(live; expect tests not captures, collections, materializations, inferred_schemas)
    }
}

fn set_of<T: Into<String>>(s: T) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    set.insert(s.into());
    set
}

/// Implementation of `ControlPlane` that connects directly to postgres.
#[derive(Clone)]
pub struct PGControlPlane {
    pub pool: sqlx::PgPool,
    pub system_user_id: Uuid,
    pub publications_handler: Publisher,
    pub id_generator: models::IdGenerator,
}

impl PGControlPlane {
    pub fn new(
        pool: sqlx::PgPool,
        system_user_id: Uuid,
        publications_handler: Publisher,
        id_generator: models::IdGenerator,
    ) -> Self {
        Self {
            pool,
            system_user_id,
            publications_handler,
            id_generator,
        }
    }

    async fn build_data_plane_clients(
        &self,
        data_plane_id: models::Id,
    ) -> anyhow::Result<(
        shard::Client,
        journal::Client,
        broker::JournalSpec, // ops logs template.
        broker::JournalSpec, // ops stats template.
    )> {
        let mut fetched = agent_sql::data_plane::fetch_data_planes(
            &self.pool,
            vec![data_plane_id],
            "", // Don't fetch default data-plane.
            uuid::Uuid::nil(),
        )
        .await?;

        let Some(data_plane) = fetched.pop() else {
            anyhow::bail!("data-plane {data_plane_id} does not exist");
        };
        let ops_logs_template = agent_sql::data_plane::fetch_ops_journal_template(
            &self.pool,
            &data_plane.ops_logs_name,
        );
        let ops_stats_template = agent_sql::data_plane::fetch_ops_journal_template(
            &self.pool,
            &data_plane.ops_stats_name,
        );
        let (ops_logs_template, ops_stats_template) =
            futures::try_join!(ops_logs_template, ops_stats_template)?;

        let unix_ts = jsonwebtoken::get_current_timestamp();

        // Sign short-lived claims for activating journals and shards into the data-plane.
        let claims = proto_gazette::Claims {
            sel: Default::default(),
            cap: proto_gazette::capability::LIST | proto_gazette::capability::APPLY,
            sub: String::new(),
            iat: unix_ts,
            exp: unix_ts + 60,
            iss: data_plane.data_plane_fqdn.clone(),
        };

        let mut bearer_token = None;
        if let Some(hmac_key) = data_plane.hmac_keys.first() {
            let hmac_key = jsonwebtoken::EncodingKey::from_base64_secret(hmac_key)
                .context("hmac key is invalid")?;

            bearer_token = Some(
                jsonwebtoken::encode(&jsonwebtoken::Header::default(), &claims, &hmac_key)
                    .context("failed to encode authorization")?,
            );
        }
        let auth = gazette::Auth::new(bearer_token)?;

        // Create the journal and shard clients that are used for interacting with the data plane
        let journal_router =
            gazette::journal::Router::new(&data_plane.broker_address, auth.clone(), "local")?;
        let journal_client =
            gazette::journal::Client::new(reqwest::Client::default(), journal_router);
        let shard_router = gazette::shard::Router::new(&data_plane.reactor_address, auth, "local")?;
        let shard_client = gazette::shard::Client::new(shard_router);

        Ok((
            shard_client,
            journal_client,
            ops_logs_template,
            ops_stats_template,
        ))
    }
}

#[async_trait::async_trait]
impl ControlPlane for PGControlPlane {
    #[tracing::instrument(level = "debug", err, skip(self))]
    async fn notify_dependents(&mut self, catalog_name: String) -> anyhow::Result<()> {
        let now = self.current_time();
        agent_sql::controllers::notify_dependents(&catalog_name, now, &self.pool).await?;
        Ok(())
    }

    async fn get_connector_spec(&mut self, image: String) -> anyhow::Result<ConnectorSpec> {
        let (image_name, image_tag) = models::split_image_tag(&image);
        let Some(row) =
            agent_sql::connector_tags::fetch_connector_spec(&image_name, &image_tag, &self.pool)
                .await?
        else {
            anyhow::bail!("no connector spec found for image '{}'", image);
        };

        let agent_sql::connector_tags::ConnectorSpec {
            protocol,
            documentation_url,
            endpoint_config_schema,
            resource_config_schema,
            resource_path_pointers,
            oauth2,
        } = row;
        let Some(runtime_protocol) =
            runtime::RuntimeProtocol::from_database_string_value(&protocol)
        else {
            anyhow::bail!("invalid protocol {:?}", protocol);
        };

        let resource_path_pointers = resource_path_pointers
            .into_iter()
            .map(|p| doc::Pointer::from_str(&p))
            .collect::<Vec<_>>();
        Ok(ConnectorSpec {
            protocol: runtime_protocol,
            documentation_url,
            endpoint_config_schema: models::Schema::new(models::RawValue::from(
                endpoint_config_schema.0,
            )),
            resource_config_schema: models::Schema::new(models::RawValue::from(
                resource_config_schema.0,
            )),
            resource_path_pointers,
            oauth2: oauth2.map(|o| o.0),
        })
    }

    async fn get_live_specs(
        &mut self,
        names: BTreeSet<String>,
    ) -> anyhow::Result<tables::LiveCatalog> {
        let names = names.into_iter().collect::<Vec<_>>();
        let rows = agent_sql::live_specs::fetch_live_specs(self.system_user_id, &names, &self.pool)
            .await?;
        let mut live = tables::LiveCatalog::default();
        for row in rows {
            // Spec type might be null because we used to set it to null when deleting specs.
            // For recently deleted specs, it will still be present.
            let Some(catalog_type) = row.spec_type.map(Into::into) else {
                continue;
            };
            let Some(model_json) = row.spec.as_deref() else {
                continue;
            };
            let built_spec_json = row.built_spec.as_ref().ok_or_else(|| {
                tracing::warn!(catalog_name = %row.catalog_name, id = %row.id, "got row with spec but not built_spec");
                anyhow::anyhow!("missing built_spec for {:?}, but spec is non-null", row.catalog_name)
            })?.deref();

            live.add_spec(
                catalog_type,
                &row.catalog_name,
                row.id.into(),
                row.data_plane_id.into(),
                row.last_pub_id.into(),
                model_json,
                built_spec_json,
            )
            .with_context(|| format!("deserializing specs for {:?}", row.catalog_name))?;
        }

        // Fetch inferred schemas and add to live specs.
        let collection_names = live
            .collections
            .iter()
            .map(|c| c.collection.as_str())
            .collect::<Vec<_>>();
        let inferred_schema_rows =
            agent_sql::live_specs::fetch_inferred_schemas(&collection_names, &self.pool)
                .await
                .context("fetching inferred schemas")?;
        for row in inferred_schema_rows {
            let agent_sql::live_specs::InferredSchemaRow {
                collection_name,
                schema,
                md5,
            } = row;
            let collection_name = models::Collection::new(collection_name);
            let schema = models::Schema::new(models::RawValue::from(schema.0));
            live.inferred_schemas.insert(tables::InferredSchema {
                collection_name,
                schema,
                md5,
            });
        }

        Ok(live)
    }

    fn current_time(&self) -> DateTime<Utc> {
        Utc::now()
    }

    async fn publish(
        &mut self,
        publication_id: models::Id,
        detail: Option<String>,
        logs_token: Uuid,
        draft: tables::DraftCatalog,
    ) -> anyhow::Result<PublicationResult> {
        let mut maybe_draft = Some(draft);
        let mut attempt = 0;
        loop {
            let draft = maybe_draft.take().expect("draft must be Some");
            attempt += 1;
            let built = self
                .publications_handler
                .build(
                    self.system_user_id,
                    publication_id,
                    detail.clone(),
                    draft,
                    logs_token,
                    "", // No default data-plane.
                )
                .await?;
            if built.errors().next().is_some() {
                return Ok(built.build_failed());
            }
            let commit_result = self.publications_handler.commit(built).await?;
            let JobStatus::ExpectPubIdMismatch { failures } = &commit_result.status else {
                return Ok(commit_result);
            };
            // There's been an optimistic locking failure.
            if attempt == Publisher::MAX_OPTIMISTIC_LOCKING_RETRIES {
                tracing::error!(%attempt, ?failures, "giving up after maximum number of optimistic locking retries");
                return Ok(commit_result);
            } else {
                tracing::info!(%attempt, ?failures, "publish failed due to optimistic locking failures (will retry)");
                maybe_draft = Some(commit_result.draft);
            }
        }
    }

    fn next_pub_id(&mut self) -> models::Id {
        self.id_generator.next()
    }

    async fn data_plane_activate(
        &mut self,
        catalog_name: String,
        spec: &AnyBuiltSpec,
        data_plane_id: models::Id,
    ) -> anyhow::Result<()> {
        let (shard_client, journal_client, ops_logs_template, ops_stats_template) = self
            .build_data_plane_clients(data_plane_id)
            .await
            .context("failed to create data plane clients")?;

        match spec {
            AnyBuiltSpec::Capture(s) => {
                let name = models::Capture::new(catalog_name);
                activate::activate_capture(
                    &journal_client,
                    &shard_client,
                    &name,
                    Some(s),
                    Some(&ops_logs_template),
                    Some(&ops_stats_template),
                    INITIAL_SPLITS,
                )
                .await
            }
            AnyBuiltSpec::Collection(s) => {
                let name = models::Collection::new(catalog_name);
                activate::activate_collection(
                    &journal_client,
                    &shard_client,
                    &name,
                    Some(s),
                    Some(&ops_logs_template),
                    Some(&ops_stats_template),
                    INITIAL_SPLITS,
                )
                .await
            }
            AnyBuiltSpec::Materialization(s) => {
                let name = models::Materialization::new(catalog_name);
                activate::activate_materialization(
                    &journal_client,
                    &shard_client,
                    &name,
                    Some(s),
                    Some(&ops_logs_template),
                    Some(&ops_stats_template),
                    INITIAL_SPLITS,
                )
                .await
            }
            AnyBuiltSpec::Test(_) => Err(anyhow::anyhow!(
                "attempted to activate a Test, which is not a thing"
            )),
        }
    }

    async fn data_plane_delete(
        &mut self,
        catalog_name: String,
        spec_type: CatalogType,
        data_plane_id: models::Id,
    ) -> anyhow::Result<()> {
        let (shard_client, journal_client, ops_logs_template, ops_stats_template) = self
            .build_data_plane_clients(data_plane_id)
            .await
            .context("failed to create data-plane clients")?;

        match spec_type {
            CatalogType::Capture => {
                let name = models::Capture::new(catalog_name);
                activate::activate_capture(
                    &journal_client,
                    &shard_client,
                    &name,
                    None,
                    Some(&ops_logs_template),
                    Some(&ops_stats_template),
                    INITIAL_SPLITS,
                )
                .await
            }
            CatalogType::Collection => {
                let name = models::Collection::new(catalog_name);
                activate::activate_collection(
                    &journal_client,
                    &shard_client,
                    &name,
                    None,
                    Some(&ops_logs_template),
                    Some(&ops_stats_template),
                    INITIAL_SPLITS,
                )
                .await
            }
            CatalogType::Materialization => {
                let name = models::Materialization::new(catalog_name);
                activate::activate_materialization(
                    &journal_client,
                    &shard_client,
                    &name,
                    None,
                    Some(&ops_logs_template),
                    Some(&ops_stats_template),
                    INITIAL_SPLITS,
                )
                .await
            }
            CatalogType::Test => Err(anyhow::anyhow!(
                "attempted to delete a Test, which is not a thing"
            )),
        }
    }
}

const INITIAL_SPLITS: usize = 1;
