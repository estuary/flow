use futures::{FutureExt, TryFutureExt};

#[derive(Clone)]
pub struct ControlPlane {
    pool: Option<sqlx::PgPool>,
}

impl ControlPlane {
    pub fn new(pool: Option<&sqlx::PgPool>) -> Self {
        Self {
            pool: pool.cloned(),
        }
    }
}

impl validation::ControlPlane for ControlPlane {
    fn resolve_collections<'a>(
        &'a self,
        collections: Vec<models::Collection>,
    ) -> futures::future::BoxFuture<'a, anyhow::Result<Vec<proto_flow::flow::CollectionSpec>>> {
        let Some(pool) = self.pool.clone() else {
            return validation::NoOpControlPlane.resolve_collections(collections)
        };
        let collections = collections.into_iter().map(Into::into).collect();

        agent_sql::publications::resolve_collections(collections, pool)
            .map_err(Into::into)
            .map_ok(|rows| {
                rows.into_iter()
                    .filter_map(|row| row.built_spec.map(|s| s.0))
                    .collect()
            })
            .boxed()
    }

    fn get_inferred_schemas<'a>(
        &'a self,
        collections: Vec<models::Collection>,
    ) -> futures::future::BoxFuture<
        'a,
        anyhow::Result<std::collections::BTreeMap<models::Collection, models::Schema>>,
    > {
        let Some(pool) = self.pool.clone() else {
            return validation::NoOpControlPlane.get_inferred_schemas(collections)
        };
        let collections = collections.into_iter().map(Into::into).collect();

        agent_sql::publications::get_inferred_schemas(collections, pool)
            .map_err(Into::into)
            .map_ok(|rows| {
                rows.into_iter()
                    .map(|row| {
                        (
                            models::Collection::new(row.collection_name),
                            models::Schema::new(row.schema.0.into()),
                        )
                    })
                    .collect()
            })
            .boxed()
    }
}
