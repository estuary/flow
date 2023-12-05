use std::collections::HashSet;

use futures::{FutureExt, TryFutureExt};

#[derive(Clone)]
pub struct ControlPlane {
    pool: Option<sqlx::PgPool>,
    /// A kludge to make sure we don't resolve collection specs that are being
    /// deleted by the current publication. Ideally, the validation logic would
    /// be aware that these collections were being deleted, and wouldn't ask for
    /// them in the first place. But this should work in the meantime.
    deleted_collections: HashSet<String>,
}

impl ControlPlane {
    pub fn new(pool: Option<&sqlx::PgPool>) -> Self {
        Self {
            pool: pool.cloned(),
            deleted_collections: HashSet::new(),
        }
    }

    /// Returns a copy of the `ControlPlane` that will filter out the given collections
    /// when resolving collections and inferred schemas. This is a hopefully temporary
    /// hack to ensure that we don't allow deleting collections that are still being used
    /// by an active task.
    pub fn with_deleted_collections(&self, deleted_collections: HashSet<String>) -> Self {
        Self {
            pool: self.pool.clone(),
            deleted_collections,
        }
    }
}

impl validation::ControlPlane for ControlPlane {
    fn resolve_collections<'a>(
        &'a self,
        collections: Vec<models::Collection>,
    ) -> futures::future::BoxFuture<'a, anyhow::Result<Vec<proto_flow::flow::CollectionSpec>>> {
        let Some(pool) = self.pool.clone() else {
            return validation::NoOpControlPlane.resolve_collections(collections);
        };
        let collections = collections
            .into_iter()
            .filter(|c| !self.deleted_collections.contains(c.as_str()))
            .map(Into::into)
            .collect();

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
        anyhow::Result<std::collections::BTreeMap<models::Collection, validation::InferredSchema>>,
    > {
        let Some(pool) = self.pool.clone() else {
            return validation::NoOpControlPlane.get_inferred_schemas(collections);
        };
        let collections = collections
            .into_iter()
            .filter(|c| !self.deleted_collections.contains(c.as_str()))
            .map(Into::into)
            .collect();

        agent_sql::publications::get_inferred_schemas(collections, pool)
            .map_err(Into::into)
            .map_ok(|rows| {
                rows.into_iter()
                    .map(|row| {
                        (
                            models::Collection::new(row.collection_name),
                            validation::InferredSchema {
                                schema: models::Schema::new(row.schema.0.into()),
                                md5: row.md5,
                            },
                        )
                    })
                    .collect()
            })
            .boxed()
    }
}
