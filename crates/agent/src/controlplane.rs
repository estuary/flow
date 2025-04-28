use anyhow::Context;
use chrono::{DateTime, Utc};
use gazette::{broker, journal, shard};
use models::{status::activation::ShardFailure, CatalogType, Id};
use proto_flow::AnyBuiltSpec;
use serde_json::value::RawValue;
use sqlx::types::Uuid;
use std::collections::BTreeSet;

use crate::{
    discovers::{Discover, DiscoverOutput},
    evolution::{self, EvolutionOutput},
    logs,
    proxy_connectors::MakeConnectors,
    publications::{
        DefaultRetryPolicy, DraftPublication, NoopFinalize, NoopInitialize, NoopWithCommit,
        PublicationResult, Publisher,
    },
    DiscoverConnectors, DiscoverHandler,
};

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
    pub auto_discover_interval: chrono::Duration,
}

/// A trait for allowing controllers access to the database.
/// This makes it much easier to test controllers, and is intended to serve as a
/// starting point for functions that we may wish to expose via an HTTP API or
/// other language bindings.
#[async_trait::async_trait]
pub trait ControlPlane: Send + Sync {
    /// Returns the current time. Having controllers access the current time through this api
    /// allows tests of controllers to be deterministic.
    fn current_time(&self) -> DateTime<Utc>;

    async fn get_shard_failures(&self, catalog_name: String) -> anyhow::Result<Vec<ShardFailure>>;

    async fn delete_shard_failures(
        &self,
        catalog_name: String,
        min_build_id: Id,
        min_ts: DateTime<Utc>,
    ) -> anyhow::Result<()>;

    /// Activates the given built spec in the data plane.
    async fn data_plane_activate(
        &self,
        catalog_name: String,
        spec: &AnyBuiltSpec,
        data_plane_id: models::Id,
    ) -> anyhow::Result<()>;

    /// Deletes the given entity from the data plane.
    async fn data_plane_delete(
        &self,
        catalog_name: String,
        spec_type: CatalogType,
        data_plane_id: models::Id,
    ) -> anyhow::Result<()>;

    /// Triggers controller runs for all dependents of the live spec with the given id.
    async fn notify_dependents(&self, live_spec_id: models::Id) -> anyhow::Result<()>;

    async fn evolve_collections(
        &self,
        draft: tables::DraftCatalog,
        collections: Vec<evolution::EvolveRequest>,
    ) -> anyhow::Result<EvolutionOutput>;

    async fn discover(
        &self,
        capture_name: models::Capture,
        draft: tables::DraftCatalog,
        update_only: bool,
        logs_token: Uuid,
        data_plane_id: models::Id,
    ) -> anyhow::Result<DiscoverOutput>;

    /// Attempts to publish the given draft, returning a result that indicates
    /// whether it was successful. Returns an `Err` only if there was an error
    /// executing the publication. Unsuccessful publications are represented by
    /// an `Ok`, where the `PublicationResult` has a non-success status.
    async fn publish(
        &self,
        detail: Option<String>,
        logs_token: Uuid,
        draft: tables::DraftCatalog,
        default_data_plane: Option<String>,
    ) -> anyhow::Result<PublicationResult>;

    /// Fetch the given set of live specs, returning them all as part of a `LiveCatalog`.
    async fn get_live_specs(&self, names: BTreeSet<String>) -> anyhow::Result<tables::LiveCatalog>;

    /// Fetch the connector spec for the given image, which should include both
    /// the name and the tag.
    async fn get_connector_spec(&self, connector_image: String) -> anyhow::Result<ConnectorSpec>;

    /// Fetch the inferred schema for the given collection.
    async fn get_inferred_schema(
        &self,
        collection: models::Collection,
    ) -> anyhow::Result<Option<tables::InferredSchema>> {
        let live = self.get_live_specs(set_of(collection)).await?;
        // it's ok for collections to be present in the live catalog. Just ignore it
        unwrap_single!(live; expect inferred_schemas not captures, materializations, tests)
    }

    /// Fetch a single collection spec.
    async fn get_collection(
        &self,
        collection: models::Collection,
    ) -> anyhow::Result<Option<tables::LiveCollection>> {
        let live = self.get_live_specs(set_of(collection)).await?;
        // it's ok for inferred_schemas to be present in the live catalog. Just ignore it
        unwrap_single!(live; expect collections not captures, materializations, tests)
    }

    /// Fetch a single capture spec.
    async fn get_capture(
        &self,
        capture: models::Capture,
    ) -> anyhow::Result<Option<tables::LiveCapture>> {
        let live = self.get_live_specs(set_of(capture)).await?;
        unwrap_single!(live; expect captures not collections, inferred_schemas, materializations, tests)
    }

    /// Fetch a single materialization spec.
    async fn get_materialization(
        &self,
        materialization: models::Materialization,
    ) -> anyhow::Result<Option<tables::LiveMaterialization>> {
        let live = self.get_live_specs(set_of(materialization)).await?;
        unwrap_single!(live; expect materializations not captures, collections, inferred_schemas, tests)
    }

    /// Fetch a single test spec.
    async fn get_test(&self, test: models::Test) -> anyhow::Result<Option<tables::LiveTest>> {
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
pub struct PGControlPlane<C: DiscoverConnectors + MakeConnectors> {
    pub pool: sqlx::PgPool,
    pub system_user_id: Uuid,
    pub publications_handler: Publisher<C>,
    pub id_generator: models::IdGenerator,
    pub discovers_handler: DiscoverHandler<C>,
    pub logs_tx: logs::Tx,
}

impl<C: DiscoverConnectors + MakeConnectors> PGControlPlane<C> {
    pub fn new(
        pool: sqlx::PgPool,
        system_user_id: Uuid,
        publications_handler: Publisher<C>,
        id_generator: models::IdGenerator,
        discovers_handler: DiscoverHandler<C>,
        logs_tx: logs::Tx,
    ) -> Self {
        Self {
            pool,
            system_user_id,
            publications_handler,
            id_generator,
            discovers_handler,
            logs_tx,
        }
    }

    async fn build_data_plane_context(
        &self,
        data_plane_id: models::Id,
    ) -> anyhow::Result<(
        shard::Client,
        journal::Client,
        Option<broker::JournalSpec>, // ops logs template.
        Option<broker::JournalSpec>, // ops stats template.
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

        let mut metadata = gazette::Metadata::default();
        metadata
            .signed_claims(
                proto_gazette::capability::LIST | proto_gazette::capability::APPLY,
                &data_plane.data_plane_fqdn,
                std::time::Duration::from_secs(60),
                &data_plane.hmac_keys,
                broker::LabelSelector::default(),
                "agent",
            )
            .context("failed to sign claims for data-plane")?;

        // Create the journal and shard clients that are used for interacting with the data plane
        let router = gazette::Router::new("local");
        let journal_client = gazette::journal::Client::new(
            data_plane.broker_address,
            metadata.clone(),
            router.clone(),
        );
        let shard_client =
            gazette::shard::Client::new(data_plane.reactor_address, metadata, router);

        Ok((
            shard_client,
            journal_client,
            ops_logs_template,
            ops_stats_template,
        ))
    }
}

#[async_trait::async_trait]
impl<C: DiscoverConnectors + MakeConnectors> ControlPlane for PGControlPlane<C> {
    #[tracing::instrument(level = "debug", err, skip(self))]
    async fn notify_dependents(&self, live_spec_id: models::Id) -> anyhow::Result<()> {
        agent_sql::controllers::notify_dependents(live_spec_id, &self.pool).await?;
        Ok(())
    }

    async fn get_shard_failures(&self, catalog_name: String) -> anyhow::Result<Vec<ShardFailure>> {
        sqlx::query_scalar!(
            r#"
            select
                flow_document as "failure: ShardFailure"
            from shard_failures
            where catalog_name = $1
            order by ts asc
            "#,
            catalog_name,
        )
        .fetch_all(&self.pool)
        .await
        .context("fetching shard failures")
    }

    async fn delete_shard_failures(
        &self,
        catalog_name: String,
        min_build_id: Id,
        min_ts: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let deleted_count = sqlx::query_scalar!(
            r#"
            with del as (
                delete from shard_failures
                    where catalog_name = $1
                    and (build < $2::flowid or ts < $3)
                    returning ts
            )
            select count(*) from del
            "#,
            catalog_name,
            min_build_id as Id,
            min_ts,
        )
        .fetch_one(&self.pool)
        .await
        .context("clearing shard failures")?;
        tracing::debug!(%min_build_id, %min_ts, ?deleted_count, "deleted old failure records");
        Ok(())
    }

    async fn get_connector_spec(&self, image: String) -> anyhow::Result<ConnectorSpec> {
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
            auto_discover_interval,
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
            auto_discover_interval: auto_discover_interval.into(),
        })
    }

    async fn get_live_specs(&self, names: BTreeSet<String>) -> anyhow::Result<tables::LiveCatalog> {
        let names = names.into_iter().collect::<Vec<_>>();
        let mut live = crate::live_specs::get_live_specs(
            self.system_user_id,
            &names,
            None, // don't filter based on user capability
            &self.pool,
        )
        .await?;

        // TODO: Can we stop adding inferred schemas to live specs?
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

    async fn evolve_collections(
        &self,
        draft: tables::DraftCatalog,
        requests: Vec<evolution::EvolveRequest>,
    ) -> anyhow::Result<EvolutionOutput> {
        let evolve = evolution::Evolution {
            user_id: self.system_user_id,
            draft,
            requests,
            require_user_can_admin: false,
        };
        evolution::evolve(evolve, &self.pool).await
    }

    async fn discover(
        &self,
        capture_name: models::Capture,
        draft: tables::DraftCatalog,
        update_only: bool,
        logs_token: Uuid,
        data_plane_id: models::Id,
    ) -> anyhow::Result<DiscoverOutput> {
        let PGControlPlane {
            ref pool,
            discovers_handler,
            system_user_id,
            ..
        } = self;
        let data_planes = agent_sql::data_plane::fetch_data_planes(
            pool,
            vec![data_plane_id],
            "not-a-real-default",
            *system_user_id,
        )
        .await?;
        let Some(data_plane) = data_planes.into_iter().next() else {
            anyhow::bail!("data plane '{data_plane_id}' not found");
        };
        let req = Discover {
            user_id: *system_user_id,
            capture_name,
            draft,
            update_only,
            logs_token,
            data_plane,
        };
        discovers_handler.discover(pool, req).await
    }

    async fn publish(
        &self,
        detail: Option<String>,
        logs_token: Uuid,
        draft: tables::DraftCatalog,
        default_data_plane: Option<String>,
    ) -> anyhow::Result<PublicationResult> {
        let publication = DraftPublication {
            user_id: self.system_user_id,
            logs_token,
            draft,
            detail,
            dry_run: false,
            default_data_plane_name: default_data_plane,
            // skip authz checks for controller-initiated publications
            verify_user_authz: false,
            initialize: NoopInitialize,
            finalize: NoopFinalize,
            retry: DefaultRetryPolicy,
            with_commit: NoopWithCommit,
        };
        self.publications_handler.publish(publication).await
    }

    async fn data_plane_activate(
        &self,
        catalog_name: String,
        spec: &AnyBuiltSpec,
        data_plane_id: models::Id,
    ) -> anyhow::Result<()> {
        let (shard_client, journal_client, ops_logs_template, ops_stats_template) = self
            .build_data_plane_context(data_plane_id)
            .await
            .context("failed to create data plane clients")?;
        anyhow::ensure!(
            ops_logs_template.is_some(),
            "ops_logs_template is missing, and required for data plane activation"
        );
        anyhow::ensure!(
            ops_stats_template.is_some(),
            "ops_stats_template is missing, and required for data plane activation"
        );

        match spec {
            AnyBuiltSpec::Capture(s) => {
                let name = models::Capture::new(catalog_name);
                activate::activate_capture(
                    &journal_client,
                    &shard_client,
                    &name,
                    Some(s),
                    ops_logs_template.as_ref(),
                    ops_stats_template.as_ref(),
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
                    ops_logs_template.as_ref(),
                    ops_stats_template.as_ref(),
                    INITIAL_SPLITS,
                )
                .await
            }
            AnyBuiltSpec::Materialization(s) => {
                let name = models::Materialization::new(catalog_name);

                let initial_splits = if s.connector_type
                    == proto_flow::flow::materialization_spec::ConnectorType::Dekaf as i32
                {
                    0 // Dekaf tasks do not have actual shards, but do have ops journals.
                } else {
                    INITIAL_SPLITS
                };

                activate::activate_materialization(
                    &journal_client,
                    &shard_client,
                    &name,
                    Some(s),
                    ops_logs_template.as_ref(),
                    ops_stats_template.as_ref(),
                    initial_splits,
                )
                .await
            }
            AnyBuiltSpec::Test(_) => Err(anyhow::anyhow!(
                "attempted to activate a Test, which is not a thing"
            )),
        }
    }

    async fn data_plane_delete(
        &self,
        catalog_name: String,
        spec_type: CatalogType,
        data_plane_id: models::Id,
    ) -> anyhow::Result<()> {
        let (shard_client, journal_client, ops_logs_template, ops_stats_template) = self
            .build_data_plane_context(data_plane_id)
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
                    ops_logs_template.as_ref(),
                    ops_stats_template.as_ref(),
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
                    ops_logs_template.as_ref(),
                    ops_stats_template.as_ref(),
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
                    ops_logs_template.as_ref(),
                    ops_stats_template.as_ref(),
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
