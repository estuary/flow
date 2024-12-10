use agent_sql::Capability;
use itertools::Itertools;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::BTreeSet;

mod handler;

pub use handler::EvolutionHandler;

#[derive(Debug)]
pub struct Evolution {
    /// Draft into which the results of the evolution will be merged.
    pub draft: tables::DraftCatalog,
    /// Specifies which collections to evolve and how.
    pub requests: Vec<EvolveRequest>,
    /// The id of the user to act as. This is used to determine the permissions
    /// to specs, in case `require_user_can_admin` is `true`.
    pub user_id: uuid::Uuid,
    /// If `true`, then the evolution will not affect any captures or
    /// materializations that the user does not have `admin` capability to.
    /// Otherwise, user permissions will not limit which specs are affected.
    /// This should generally be set to `true` for user-initiated evolutions,
    /// and `false` for evolutions that are undertaken by our background
    /// automations.
    pub require_user_can_admin: bool,
}

#[derive(Debug)]
pub struct EvolutionOutput {
    /// The draft containing the results of the evolution.
    pub draft: tables::DraftCatalog,
    /// Summary of the actions that were taken, and which specs were affected.
    pub actions: Vec<EvolvedCollection>,
}

impl EvolutionOutput {
    pub fn is_success(&self) -> bool {
        self.draft.errors.is_empty()
    }
}

/// Rust struct corresponding to each array element of the `collections` JSON
/// input of an `evolutions` row.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct EvolveRequest {
    /// The current name of the collection.
    #[serde(alias = "old_name")]
    // alias can be removed after UI code is updated to use current_name
    pub current_name: String,
    /// Optional new name for the collection. If provided, the collection will be re-created.
    /// Otherwise, only materialization bindings will be updated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_name: Option<String>,
    /// Optionally restrict updates to only the provided materializations. This conflicts with
    /// `new_name`, and at most one of the two may be provided.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub materializations: Vec<String>,
}

impl EvolveRequest {
    pub fn of(collection_name: impl Into<String>) -> EvolveRequest {
        EvolveRequest {
            current_name: collection_name.into(),
            new_name: None,
            materializations: Vec::new(),
        }
    }

    pub fn with_new_name(mut self, new_name: impl Into<String>) -> Self {
        self.new_name = Some(new_name.into());
        self
    }

    pub fn with_version_increment(self) -> Self {
        let new_name = crate::next_name(&self.current_name);
        self.with_new_name(new_name)
    }

    pub fn with_materializations(self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let materializations = names.into_iter().map(|n| n.into()).collect();
        Self {
            materializations,
            ..self
        }
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            models::Collection::regex().is_match(&self.current_name),
            "current_name '{}' is invalid",
            self.current_name
        );
        if let Some(new_name) = &self.new_name {
            anyhow::ensure!(
                new_name != &self.current_name,
                "if new_name is provided, it must be different from current_name"
            );
            anyhow::ensure!(
                models::Collection::regex().is_match(new_name),
                "requested collection name '{new_name}' is invalid"
            );
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, JsonSchema)]
pub struct EvolvedCollection {
    /// Original name of the collection
    pub old_name: String,
    /// The new name of the collection, which may be the same as the original name if only materialization bindings were updated
    pub new_name: String,
    /// The names of any materializations that were updated as a result of evolving this collection
    pub updated_materializations: Vec<String>,
    /// The names of any captures that were updated as a result of evolving this collection
    pub updated_captures: Vec<String>,
}

#[tracing::instrument(skip_all, fields(user_id = %evolution.user_id))]
pub async fn evolve(evolution: Evolution, db: &PgPool) -> anyhow::Result<EvolutionOutput> {
    let Evolution {
        mut draft,
        requests,
        user_id,
        require_user_can_admin,
    } = evolution;
    for req in requests.iter() {
        if let Err(error) = req.validate() {
            let scope = tables::synthetic_scope(models::CatalogType::Collection, &req.current_name);
            draft.errors.insert(tables::Error {
                scope,
                error: error.context("validating evolution request"),
            });
        }
    }
    if !draft.errors.is_empty() {
        return Ok(EvolutionOutput {
            draft,
            actions: Vec::new(),
        });
    }

    // Fetch collections matching either the current or the new name. This
    // ensures that we can preserve the existing spec in case the `new_name`
    // names a collection that already exists.
    let mut fetch_collections = requests
        .iter()
        .flat_map(|r| std::iter::once(r.current_name.clone()).chain(r.new_name.clone().into_iter()))
        .collect::<BTreeSet<_>>();
    for r in draft.collections.iter() {
        fetch_collections.remove(r.collection.as_str());
    }
    let fetch_collections = fetch_collections.into_iter().collect::<Vec<_>>();
    let capability_filter = if require_user_can_admin {
        Some(Capability::Admin)
    } else {
        None
    };
    let live_collections =
        crate::live_specs::get_live_specs(user_id, &fetch_collections, capability_filter, db)
            .await?;

    draft.add_live(live_collections);

    let collection_names = requests
        .iter()
        .map(|r| r.current_name.as_str())
        .collect::<Vec<_>>();
    let exclude_names = draft.all_spec_names().collect::<Vec<_>>();
    let expanded_live = crate::live_specs::get_connected_live_specs(
        user_id,
        &collection_names,
        &exclude_names,
        capability_filter,
        db,
    )
    .await?;
    draft.add_live(expanded_live);

    let mut actions = Vec::new();
    for req in requests.iter() {
        match evolve_collection(&mut draft, req) {
            Ok(action) => {
                actions.push(action);
            }
            Err(error) => {
                let scope =
                    tables::synthetic_scope(models::CatalogType::Collection, &req.current_name);
                draft.errors.insert(tables::Error { scope, error });
            }
        }
    }
    Ok(EvolutionOutput { draft, actions })
}

#[tracing::instrument(err, skip_all, fields(current_name = %req.current_name, new_name = ?req.new_name))]
fn evolve_collection(
    draft: &mut tables::DraftCatalog,
    req: &EvolveRequest,
) -> anyhow::Result<EvolvedCollection> {
    let EvolveRequest {
        current_name,
        new_name,
        materializations,
    } = req;

    // We only re-create collections if explicitly requested.
    let (re_create_collection, new_name) = match new_name.as_ref() {
        Some(n) => (true, n.to_owned()),
        None => (false, current_name.clone()),
    };
    let old_collection = models::Collection::new(current_name);
    let new_collection = models::Collection::new(new_name);

    // Add the new collection to the draft if needed. It's possible for the draft to already contain
    // a collection with this name, and we'll skip adding a new one in that case, in order to preserve
    // any changes that the user has potentially made in the draft.
    if re_create_collection && draft.collections.get_by_key(&new_collection).is_none() {
        anyhow::ensure!(
            materializations.is_empty(),
            "specific_materializations argument must be empty if collection is being re-created"
        );
        let Some(drafted) = draft.collections.get_by_key(&old_collection) else {
            anyhow::bail!("missing spec for collection '{current_name}'");
        };
        anyhow::ensure!(
            drafted.model.is_some(),
            "draft catalog contained a deletion for collection '{current_name}'"
        );

        let new_row = tables::DraftCollection {
            scope: drafted.scope.clone(),
            collection: new_collection.clone(),
            model: drafted.model.clone(),
            expect_pub_id: Some(models::Id::zero()), // brand new collection
            is_touch: false,
        };
        draft.collections.insert(new_row);
    }

    // If re-creating the collection, remove the old one from the draft.
    if re_create_collection {
        let _ = draft.collections.remove_by_key(&old_collection);
    }

    let mut updated_materializations = Vec::new();

    for (materialization, draft_model, is_touch) in draft
        .materializations
        .iter_mut()
        .filter_map(|m| with_mat_binding(&old_collection, m))
    {
        if !materializations.is_empty()
            && !materializations
                .iter()
                .any(|m| m == materialization.as_str())
        {
            tracing::debug!(%materialization, "skipping materialization because it was not requested to be updated");
            continue;
        }

        *is_touch = false; // we're updating the materialization, so ensure it's not a touch.
        updated_materializations.push(materialization.to_string());
        for binding in draft_model
            .bindings
            .iter_mut()
            .filter(|b| b.source.collection() == &old_collection)
        {
            // If we're re-creating the collection, then update the source in place.
            // We do this even for disabled bindings, so that the spec is up to date
            // with the latest changes to the rest of the catalog.
            if re_create_collection {
                binding.source.set_collection(new_collection.clone());
            }

            // Don't update resources for disabled bindings.
            if binding.disable {
                tracing::debug!(%materialization, "skipping materialization because the binding is disabled");
                continue;
            }

            // Finally, we need to increment the backfill counter of the binding.
            // This is not _technically_ required for materializations when
            // re-creating the collection, since they'll backfill when the
            // collection name changes, anyway. But it may help to make it more
            // obvious and explicit, and certainly won't hurt anything.
            binding.backfill += 1;
        }
    }

    // If specific materializations were requested to be updated, ensure that
    // we were actually able to update all of the given materializations.
    if !materializations.is_empty() && materializations.len() != updated_materializations.len() {
        let actual = updated_materializations
            .iter()
            .map(|u| u.as_str())
            .collect::<BTreeSet<_>>();
        let diff = materializations
            .iter()
            .filter(|m| !actual.contains(m.as_str()))
            .format(", ");
        anyhow::bail!("requested to update the materialization(s) [{diff}], but no such materializations were found that source from the collection '{old_collection}'");
    }

    let mut updated_captures = Vec::new();
    // We don't need to update any captures if the collection isn't being re-created.
    if re_create_collection {
        for (capture, draft_model, is_touch) in draft
            .captures
            .iter_mut()
            .filter_map(|c| with_cap_binding(&old_collection, c))
        {
            updated_captures.push(capture.to_string());
            *is_touch = false; // we're updating the capture, so ensure it's not a touch.

            for binding in draft_model.bindings.iter_mut() {
                if &binding.target == &old_collection {
                    binding.target = new_collection.clone();
                    // When re-creating collections, it's quite likely that
                    // users will also want to trigger a new backfill. Unlike
                    // materializations, capture connectors will only backfill
                    // when the counter is incremented, not when only the
                    // collection name is changed.
                    binding.backfill += 1;
                }
            }
        }
    }

    // If we're re-creating the collection, then there's no requirement to have
    // updated any captures or materializations. But if we're _not_ re-creating
    // the collection and we still haven't updated any captures or
    // materializations, then consider this an error.
    if !re_create_collection && updated_captures.is_empty() && updated_materializations.is_empty() {
        anyhow::bail!("nothing to update for collection '{old_collection}'");
    }

    tracing::debug!(?updated_materializations, ?updated_captures, %re_create_collection, %new_collection, %old_collection, "evolved collection in draft");

    Ok(EvolvedCollection {
        old_name: old_collection.into(),
        new_name: new_collection.into(),
        updated_materializations,
        updated_captures,
    })
}

fn with_cap_binding<'a, 'b>(
    collection: &'a models::Collection,
    drafted: &'b mut tables::DraftCapture,
) -> Option<(
    &'b models::Capture,
    &'b mut models::CaptureDef,
    &'b mut bool,
)> {
    let tables::DraftCapture {
        ref capture,
        ref mut model,
        ref mut is_touch,
        ..
    } = drafted;
    let model = model.as_mut()?;
    if model.bindings.iter().any(|b| &b.target == collection) {
        Some((capture, model, is_touch))
    } else {
        None
    }
}

fn with_mat_binding<'a, 'b>(
    collection: &'a models::Collection,
    drafted: &'b mut tables::DraftMaterialization,
) -> Option<(
    &'b models::Materialization,
    &'b mut models::MaterializationDef,
    &'b mut bool,
)> {
    let tables::DraftMaterialization {
        ref materialization,
        ref mut model,
        ref mut is_touch,
        ..
    } = drafted;
    let model = model.as_mut()?;
    if model
        .bindings
        .iter()
        .any(|b| b.source.collection() == collection)
    {
        Some((materialization, model, is_touch))
    } else {
        None
    }
}
