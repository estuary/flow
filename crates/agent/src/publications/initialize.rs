use itertools::Itertools;
use models::Capability;
use std::{collections::BTreeMap, future::Future};
use uuid::Uuid;

/// Initialize a draft prior to build/validation. This may add additional specs to the draft.
pub trait Initialize: Send + Sync {
    fn initialize(
        &self,
        db: &sqlx::PgPool,
        user_id: Uuid,
        draft: &mut tables::DraftCatalog,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}

/// A no-op `Initialize` impl, for when you don't want to expand the draft.
pub struct NoExpansion;
impl Initialize for NoExpansion {
    async fn initialize(
        &self,
        _db: &sqlx::PgPool,
        _user_id: Uuid,
        _draft: &mut tables::DraftCatalog,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

pub struct UpdateInferredSchemas;
impl Initialize for UpdateInferredSchemas {
    async fn initialize(
        &self,
        db: &sqlx::PgPool,
        _user_id: Uuid,
        draft: &mut tables::DraftCatalog,
    ) -> anyhow::Result<()> {
        let collection_names = draft
            .collections
            .iter()
            .filter(|r| uses_inferred_schema(*r))
            .map(|c| c.collection.as_str())
            .collect::<Vec<_>>();
        let rows = agent_sql::live_specs::fetch_inferred_schemas(&collection_names, db).await?;
        tracing::debug!(
            inferred_schemas = %rows.iter().map(|r| r.collection_name.as_str()).format(", "),
            "fetched inferred schemas"
        );
        let mut by_name = rows
            .into_iter()
            .map(|r| (r.collection_name, r.schema.0))
            .collect::<BTreeMap<_, _>>();

        for drafted in draft
            .collections
            .iter_mut()
            .filter(|r| uses_inferred_schema(*r))
        {
            let maybe_inferred = by_name
                .remove(drafted.collection.as_str())
                .map(|json| models::Schema::new(json.into()));

            let draft_model = drafted.model.as_mut().unwrap();
            let draft_read_schema = draft_model.read_schema.take().unwrap();

            let new_schema = models::Schema::extend_read_bundle(
                &draft_read_schema,
                None,
                maybe_inferred.as_ref(),
            );
            draft_model.read_schema = Some(new_schema);
        }
        Ok(())
    }
}

fn uses_inferred_schema(c: &tables::DraftCollection) -> bool {
    !c.is_touch
        && c.model.as_ref().is_some_and(|s| {
            s.read_schema
                .as_ref()
                .is_some_and(models::Schema::references_inferred_schema)
        })
}

impl<I1, I2> Initialize for (I1, I2)
where
    I1: Initialize,
    I2: Initialize,
{
    async fn initialize(
        &self,
        db: &sqlx::PgPool,
        user_id: Uuid,
        draft: &mut tables::DraftCatalog,
    ) -> anyhow::Result<()> {
        self.0.initialize(db, user_id, draft).await?;
        self.1.initialize(db, user_id, draft).await?;
        Ok(())
    }
}

/// An `Initialize` that expands the draft to touch live specs that read from or write to
/// any drafted collections. This may optionally filter the specs based on whether the user
/// has `admin` capability to them.
pub struct ExpandDraft {
    /// Whether to filter specs based on the user's capability. If true, then only specs for which
    /// the user has `admin` capability will be added to the draft.
    pub filter_user_has_admin: bool,
}

impl Initialize for ExpandDraft {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        err,
        fields(filter_user_has_admin = self.filter_user_has_admin)
    )]
    async fn initialize(
        &self,
        db: &sqlx::PgPool,
        user_id: Uuid,
        draft: &mut tables::DraftCatalog,
    ) -> anyhow::Result<()> {
        // Expand the set of drafted specs to include any tasks that read from or write to any of
        // the published collections. We do this so that validation can catch any inconsistencies
        // or failed tests that may be introduced by the publication.
        let drafted_collections = draft
            .collections
            .iter()
            .map(|d| d.collection.as_str())
            .collect::<Vec<_>>();
        let all_drafted_specs = draft.all_spec_names().collect::<Vec<_>>();

        let capability_filter = if self.filter_user_has_admin {
            Some(Capability::Admin)
        } else {
            None
        };
        let expanded_catalog = crate::live_specs::get_connected_live_specs(
            user_id,
            &drafted_collections,
            &all_drafted_specs,
            capability_filter,
            db,
        )
        .await?;
        tracing::debug!(
            expanded_names = %expanded_catalog.all_spec_names().format(","),
            "expanded draft"
        );

        draft.add_live(expanded_catalog);

        Ok(())
    }
}
