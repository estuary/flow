use std::collections::{BTreeMap, HashSet};

use crate::proxy_connectors::DiscoverConnectors;

use anyhow::Context;
use models::split_image_tag;
use proto_flow::{capture, flow::capture_spec};
use sqlx::{types::Uuid, PgPool};

pub(crate) mod handler;
mod specs;

/// Represents the desire to discover an endpoint. The discovered bindings will be merged with
/// those in the `base_model`.
pub struct Discover {
    /// The name of the capture, which _must_ exist within the `draft`.
    pub capture_name: models::Capture,
    /// The data plane to use for the discover. For an existing capture, this
    /// _should_ be the data plane that the capture is currently running in. But
    /// that is not required.
    pub data_plane: tables::DataPlane,
    pub logs_token: Uuid,
    /// The id of the user that is performing the discover. This will be used to
    /// filter any live specs that the user does not have `read` capability to.
    pub user_id: Uuid,
    /// Whether newly discovered bindings should be enabled by default. If
    /// `true`, then newly added bindings will be added with `disable: true`.
    pub update_only: bool,
    /// The draft into which discover results will be merged. This _must_
    /// contain the capture named by `capture_name`, or an error will be
    /// returned. All pre-existing changes in the draft will be preserved, as
    /// long as they don't conflict with the discover results.
    pub draft: tables::DraftCatalog,
}

/// Identifies a resource that can be captured from. This is determined by using
/// the resource path pointers to extract the path from each `resource` spec in
/// either the capture model or the discovered response.
pub type ResourcePath = Vec<String>;

/// Represents a capture binding that was added, removed, or modified by a
/// discover.
#[derive(Debug, PartialEq, Clone)]
pub struct Changed {
    /// The name of the target collection for the binding.
    pub target: models::Collection,
    /// Whether the binding is disabled.
    pub disable: bool,
}
/// Represents a set of changes resulting from a discover.
pub type Changes = BTreeMap<ResourcePath, Changed>;

#[derive(Debug)]
pub struct DiscoverOutput {
    /// The name of the capture for which discover was run.
    pub capture_name: models::Capture,
    /// The final draft containing the merged output of the discover, if
    /// successful. If the discover was unsuccessful, the draft `errors` will be
    /// non-empty and the state of any other specs in the draft is unspecified.
    pub draft: tables::DraftCatalog,
    /// Bindings that were added by the discover. Note that added bindings will
    /// be disabled if `update_only` was `true`, and they will still be
    /// represented here.
    pub added: Changes,
    /// Bindings that were modified by the discover.
    pub modified: Changes,
    /// Bindings that were removed by the discover. The `disable` flag here
    /// reflects whether the binding _was_ disabled prior to removal.
    pub removed: Changes,
}

impl DiscoverOutput {
    fn failed(capture_name: models::Capture, error: anyhow::Error) -> DiscoverOutput {
        let mut draft = tables::DraftCatalog::default();
        draft.errors.insert(tables::Error {
            scope: tables::synthetic_scope(models::CatalogType::Capture, &capture_name),
            error,
        });
        DiscoverOutput {
            capture_name,
            draft,
            added: Default::default(),
            modified: Default::default(),
            removed: Default::default(),
        }
    }

    pub fn is_success(&self) -> bool {
        self.draft.errors.is_empty()
    }

    /// Returns true if the discover resulted in no changes to the capture or
    /// any collections. The return value should only be used if the discover
    /// was successful.
    pub fn is_unchanged(&self) -> bool {
        self.added.is_empty() && self.modified.is_empty() && self.removed.is_empty()
    }

    /// Prunes any drafted specs that would be no-op changes. This includes
    /// collection specs that are identical to the live specs, and any
    /// collection specs that correspond to disabled bindings, regardless of
    /// whether they are identical to the live specs. The `modified` set will
    /// also be updated to remove mentions of such specs. The `added` set will
    /// still contain records of the disabled bindings, though, even after the
    /// collection specs themeselves have been pruned. This is because they
    /// _were_ still added to the capture model, just in a disabled state.
    pub fn prune_unchanged_specs(&mut self) -> usize {
        assert!(
            self.draft.errors.is_empty(),
            "cannot prune_unchanged on discover output with errors"
        );

        let mut pruned_count = 0;
        if self.is_unchanged() {
            // We've discovered absolutely no changes, so remove everything from
            // the draft. Note that this will also remove any pre-existing
            // unrelated specs.
            pruned_count = self.draft.spec_count();
            self.draft = tables::DraftCatalog::default();
        } else {
            let DiscoverOutput {
                ref mut draft,
                ref added,
                ref modified,
                ..
            } = self;
            // At least one binding has changed, so the capture spec itself must
            // be changed, and we'll only remove collection specs that have not
            // been modified. Start by determining the set of modified
            // collection names. Note that removed bindings are not included
            // here because we want to remove the corresponding collection specs
            // from the draft.
            let changed_collections = added
                .values()
                .chain(modified.values())
                .filter(|changed| !changed.disable)
                .map(|changed| &changed.target)
                .collect::<HashSet<&models::Collection>>();

            draft.collections.retain(|row| {
                let retain = changed_collections.contains(&row.collection);
                if !retain {
                    pruned_count += 1;
                }
                retain
            });
        }
        // Remove any modification changes that correspond to disabled bindings,
        // since we've just removed the collection specs themselves.
        self.modified.retain(|_, changed| !changed.disable);
        pruned_count
    }
}

/// A DiscoverHandler is a Handler which performs discovery operations.
#[derive(Clone)]
pub struct DiscoverHandler<C> {
    pub connectors: C,
}

impl<C: DiscoverConnectors> DiscoverHandler<C> {
    pub fn new(connectors: C) -> Self {
        Self { connectors }
    }
}

impl<C: DiscoverConnectors> DiscoverHandler<C> {
    #[tracing::instrument(skip_all, fields(
        capture_name = %req.capture_name,
        data_plane_name = %req.data_plane.data_plane_name,
        user_id = %req.user_id,
        update_only = %req.update_only,
        image
    ))]
    pub async fn discover(&mut self, db: &PgPool, req: Discover) -> anyhow::Result<DiscoverOutput> {
        let Discover {
            capture_name,
            data_plane,
            logs_token,
            user_id,
            update_only,
            mut draft,
        } = req;

        let Some(capture_def) = draft.captures.get_mut_by_key(&capture_name) else {
            return Ok(DiscoverOutput::failed(
                capture_name.clone(),
                anyhow::anyhow!("missing capture: '{capture_name}' in draft"),
            ));
        };

        let Some(models::CaptureEndpoint::Connector(connector_cfg)) =
            capture_def.model.as_ref().map(|m| &m.endpoint)
        else {
            // TODO: better error message if drafted model is None
            anyhow::bail!("only connector endpoints are supported");
        };
        tracing::Span::current().record("image", &connector_cfg.image);

        // INFO is a good default since these are not shown in the UI, so if we're looking then
        // there's already a problem.
        let log_level = capture_def
            .model
            .as_ref()
            .and_then(|m| m.shards.log_level.as_deref())
            .and_then(ops::LogLevel::from_str_name)
            .unwrap_or(ops::LogLevel::Info);

        let (image_name, image_tag) = split_image_tag(&connector_cfg.image);
        let resource_path_pointers =
            agent_sql::connector_tags::fetch_resource_path_pointers(&image_name, &image_tag, db)
                .await?;
        if resource_path_pointers.is_empty() {
            return Ok(DiscoverOutput::failed(capture_name, anyhow::anyhow!("there are no configured resource_path_pointers for connector '{}', cannot discover", connector_cfg.image)));
        }

        let config_json = serde_json::to_string(connector_cfg).unwrap();
        let request = capture::Request {
            discover: Some(capture::request::Discover {
                connector_type: capture_spec::ConnectorType::Image as i32,
                config_json,
            }),
            ..Default::default()
        }
        .with_internal(|internal| {
            internal.set_log_level(log_level);
        });

        let task = ops::ShardRef {
            name: capture_name.as_str().to_owned(),
            kind: ops::TaskType::Capture as i32,
            ..Default::default()
        };

        let result = self
            .connectors
            .discover(request, logs_token, task, &data_plane)
            .await;

        let response = match result {
            Ok(response) => response,
            Err(err) => {
                return Ok(DiscoverOutput::failed(capture_name, err));
            }
        };

        let output = Self::build_merged_catalog(
            capture_name,
            user_id,
            update_only,
            draft,
            response,
            resource_path_pointers,
            db,
        )
        .await?;

        if output.is_success() {
            tracing::info!(
                added = ?output.added,
                modified = ?output.modified,
                removed = ?output.removed,
                "discover merge success");
        } else {
            tracing::warn!(
                errors = ?output.draft.errors,
                "discover merge failed"
            );
        }
        Ok(output)
    }

    async fn build_merged_catalog(
        capture_name: models::Capture,
        user_id: uuid::Uuid,
        update_only: bool,
        mut draft: tables::DraftCatalog,
        response: capture::Response,
        resource_path_pointers: Vec<String>,
        db: &PgPool,
    ) -> anyhow::Result<DiscoverOutput> {
        let discovered_bindings =
            specs::parse_response(response).context("converting discovery response into specs")?;

        let tables::DraftCatalog {
            ref mut captures,
            ref mut collections,
            ..
        } = &mut draft;
        let Some(drafted_capture) = captures.get_mut_by_key(&capture_name) else {
            anyhow::bail!("expected capture '{}' to exist in draft", capture_name);
        };
        let tables::DraftCapture {
            model: Some(ref mut capture_model),
            ref mut is_touch,
            ..
        } = drafted_capture
        else {
            anyhow::bail!(
                "expected model to be drafted for capture '{}', but was a deletion",
                capture_name
            );
        };

        let pointers = resource_path_pointers
            .iter()
            .map(|p| doc::Pointer::from_str(p.as_str()))
            .collect::<Vec<_>>();
        let (used_bindings, added_bindings, removed_bindings) = specs::update_capture_bindings(
            capture_name.as_str(),
            capture_model,
            discovered_bindings,
            update_only,
            &pointers,
        )?;

        let collection_names = capture_model
            .bindings
            .iter()
            .map(|b| b.target.to_string())
            .collect::<Vec<_>>();
        // Filter to only specs that the user can read. If they can't admin a spec, then that error
        // will be returned if and when they try to publish.
        let live = crate::live_specs::get_live_specs(
            user_id,
            &collection_names,
            Some(models::Capability::Read),
            db,
        )
        .await?;

        let mut modified_bindings =
            specs::merge_collections(used_bindings, collections, &live.collections)?;
        // Don't report a binding as both added and modified, because that'd just be confusing
        modified_bindings.retain(|path, _| !added_bindings.contains_key(path));

        if !added_bindings.is_empty()
            || !modified_bindings.is_empty()
            || !removed_bindings.is_empty()
        {
            *is_touch = false; // We're modifying the capture, so it's no longer a touch
        }

        Ok(DiscoverOutput {
            capture_name,
            draft,
            added: added_bindings,
            modified: modified_bindings,
            removed: removed_bindings,
        })
    }
}
