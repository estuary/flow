pub mod db;
pub mod specs;

use crate::connectors::ConnectorFactory;

use anyhow::Context;
use models::discovers::{Changed, Changes};
use proto_flow::{capture, connector, flow::capture_spec};
use sqlx::{PgPool, types::Uuid};
use std::collections::HashSet;

// Re-export key types and functions that executors will need
pub use db::{Row, fetch_discover, resolve};

/// Metadata of a discovery committed with its executor task.
pub struct CreatedDiscover {
    pub id: models::Id,
    pub data_plane_name: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Queues discovery using the capture's staged definition, or its live definition
/// when no draft entry exists and `subject` can read it.
/// The executor writes discovery results to the draft.
pub async fn create(
    pool: &sqlx::PgPool,
    snapshot: &crate::Snapshot,
    subject: &models::authz::Subject,
    draft_id: models::Id,
    capture_name: &str,
    requested_data_plane_name: Option<&str>,
) -> anyhow::Result<CreatedDiscover> {
    // A draft owned by someone else reads as missing. `staged` distinguishes a
    // draft without an entry for the capture from an entry staging a deletion.
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
        capture_name,
    )
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| anyhow::anyhow!("draft not found"))?;

    // Data plane selection is based on the live capture even when its definition
    // cannot be disclosed to this caller or the draft contains edits.
    let live_capture = sqlx::query!(
        r#"
        SELECT ls.spec::text AS spec, ls.spec_type::text AS spec_type,
               ls.data_plane_id AS "data_plane_id: models::Id"
        FROM live_specs ls
        WHERE ls.catalog_name = $1
        "#,
        capture_name,
    )
    .fetch_optional(pool)
    .await?
    .filter(|row| row.spec_type.as_deref() == Some("capture") && row.spec.is_some());

    let model = select_capture_model(
        capture_name,
        draft
            .staged
            .then_some((draft.spec_type, draft.spec.as_deref())),
        live_capture.as_ref().and_then(|row| row.spec.as_deref()),
        snapshot,
        subject,
    )?;

    // The executor copies the live definitions of binding targets into the
    // draft, filtered by the user's CatalogRead. It runs without the request's
    // capability mask and prefix scope, so a restricted token could otherwise
    // read definitions through the draft.
    if let Some(target) = model
        .bindings
        .iter()
        .map(|binding| binding.target.as_str())
        .find(|target| {
            !snapshot.is_user_authorized(subject, target, models::authz::Capability::CatalogRead)
        })
    {
        snapshot.request_refresh();
        anyhow::bail!("not authorized to read binding target {target}");
    }
    let (connector_config, image_name, image_tag) = extract_discovery_endpoint(&model)?;

    let connector_tag_id = sqlx::query_scalar!(
        r#"
        SELECT ct.id AS "id!: models::Id" FROM connector_tags ct
        JOIN connectors c ON c.id = ct.connector_id
        WHERE c.image_name = $1 AND ct.image_tag = $2
          AND ct.protocol = 'capture' AND ct.job_status->>'type' = 'success'
        "#,
        image_name,
        image_tag,
    )
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| anyhow::anyhow!("capture connector tag is not ready"))?;

    let data_plane_error = || {
        snapshot.request_refresh();
        anyhow::anyhow!("data plane not found or unauthorized")
    };
    let data_plane_name = if let Some(live) = live_capture {
        let current_data_plane_name = &snapshot
            .data_plane_by_id(live.data_plane_id)
            .ok_or_else(data_plane_error)?
            .data_plane_name;
        if requested_data_plane_name.is_some_and(|selected| selected != current_data_plane_name) {
            anyhow::bail!("data plane differs from the live capture");
        }
        current_data_plane_name.clone()
    } else {
        snapshot
            .storage_mapping_for(capture_name)
            .and_then(|mapping| {
                let (prefix, data_planes) =
                    mapping.ok_or_else(|| anyhow::anyhow!("no storage mapping for capture"))?;
                crate::storage_mappings::select_data_plane(
                    prefix.as_str(),
                    data_planes,
                    requested_data_plane_name,
                )
            })
            .inspect_err(|_| snapshot.request_refresh())?
            .to_owned()
    };
    snapshot
        .is_user_authorized(subject, &data_plane_name, models::Capability::Read)
        .then(|| snapshot.data_plane_by_catalog_name(&data_plane_name))
        .flatten()
        .filter(|data_plane| data_plane.connector_route().is_ok())
        .ok_or_else(data_plane_error)?;

    let update_only = model
        .auto_discover
        .as_ref()
        .is_some_and(|policy| !policy.add_new_bindings);
    // The `create_discover_task` trigger schedules the executor within this statement.
    let row = sqlx::query!(
        r#"
        INSERT INTO discovers (
            draft_id, capture_name, connector_tag_id, endpoint_config,
            update_only, data_plane_name
        ) VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id AS "id!: models::Id", created_at, updated_at
        "#,
        draft_id as models::Id,
        capture_name,
        connector_tag_id as models::Id,
        crate::TextJson(connector_config.config.clone()) as crate::TextJson<models::RawValue>,
        update_only,
        data_plane_name,
    )
    .fetch_one(pool)
    .await?;

    Ok(CreatedDiscover {
        id: row.id,
        data_plane_name,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

/// `staged` is the type and model of the draft's entry for the capture, if it has one.
fn select_capture_model(
    capture_name: &str,
    staged: Option<(Option<models::CatalogType>, Option<&str>)>,
    live_spec: Option<&str>,
    snapshot: &crate::Snapshot,
    subject: &models::authz::Subject,
) -> anyhow::Result<models::CaptureDef> {
    let spec = if let Some((spec_type, spec)) = staged {
        if spec_type != Some(models::CatalogType::Capture) {
            anyhow::bail!("draft entry is not a capture");
        }
        spec.ok_or_else(|| anyhow::anyhow!("draft entry is a deletion"))?
    } else {
        let spec = live_spec.ok_or_else(|| anyhow::anyhow!("capture not found"))?;
        if !snapshot.is_user_authorized(
            subject,
            capture_name,
            models::authz::Capability::CatalogRead,
        ) {
            anyhow::bail!("capture not found");
        }
        spec
    };
    serde_json::from_str(spec).map_err(|_| anyhow::anyhow!("invalid capture model"))
}

fn extract_discovery_endpoint(
    model: &models::CaptureDef,
) -> anyhow::Result<(&models::ConnectorConfig, String, String)> {
    if model.delete {
        anyhow::bail!("capture is staged for deletion");
    }
    let models::CaptureEndpoint::Connector(connector_config) = &model.endpoint else {
        anyhow::bail!("capture requires a connector endpoint");
    };
    let (image_name, image_tag) = models::split_image_tag(&connector_config.image);
    if image_name.is_empty() || image_tag.is_empty() {
        anyhow::bail!("capture requires a tagged connector image");
    }
    if !serde_json::from_str::<serde_json::Value>(connector_config.config.get())?.is_object() {
        anyhow::bail!("endpoint configuration must be an inline JSON object");
    }
    Ok((connector_config, image_name, image_tag))
}

/// Represents the desire to discover an endpoint. The discovered bindings will be merged with
/// those in the `base_model`.
pub struct Discover<'a> {
    /// The name of the capture, which _must_ exist within the `draft`.
    pub capture_name: models::Capture,
    /// The data plane to use for the discover. Existing captures should use
    /// their current data plane.
    pub data_plane_id: models::Id,
    /// Destination token for discover operation logs.
    pub logs_token: Uuid,
    /// Authorization subject used when evaluating this discover.
    pub subject: models::authz::Subject,
    /// Whether to apply authorization policies to restrict the specs that are visible
    /// to the user. If `false` then no authorization policies will be applied, so be careful.
    pub filter_user_authz: bool,
    /// Whether newly discovered bindings should be enabled by default. If
    /// `true`, then newly added bindings will be added with `disable: true`.
    pub update_only: bool,
    /// If a discovered binding's collection key changes, should we perform a data-flow reset?
    pub reset_on_key_change: bool,
    /// The draft into which discover results will be merged. This _must_
    /// contain the capture named by `capture_name`, or an error will be
    /// returned. All pre-existing changes in the draft will be preserved, as
    /// long as they don't conflict with the discover results.
    pub draft: tables::DraftCatalog,
    /// Date on which the capture task was created (UTC, YYYY-MM-DD), derived
    /// from the live task's control-plane Id. Empty if the task doesn't exist
    /// yet: the connector assumes a current date for a new task's discover.
    pub created_at: String,
    /// Authorization Snapshot pinned for the entire discover, so that
    /// authorization decisions cannot flip mid-operation (for example, during
    /// a long-running connector RPC).
    pub snapshot: &'a crate::Snapshot,
}

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
                draft,
                added,
                modified,
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
        pruned_count
    }
}

/// A DiscoverHandler is a Handler which performs discovery operations.
// TODO(johnny): flatten into pure functions, taking a dyn ConnectorFactory.
#[derive(Clone)]
pub struct DiscoverHandler {
    connector_factory: std::sync::Arc<dyn ConnectorFactory>,
}

impl DiscoverHandler {
    pub fn new(connector_factory: std::sync::Arc<dyn ConnectorFactory>) -> Self {
        Self { connector_factory }
    }

    #[tracing::instrument(skip_all, fields(
        capture_name = %req.capture_name,
        data_plane_id = %req.data_plane_id,
        user_id = %req.subject.user_id,
        update_only = %req.update_only,
        image
    ))]
    pub async fn discover(&self, db: &PgPool, req: Discover<'_>) -> anyhow::Result<DiscoverOutput> {
        let Discover {
            capture_name,
            data_plane_id,
            logs_token,
            subject,
            filter_user_authz,
            update_only,
            reset_on_key_change,
            mut draft,
            created_at,
            snapshot,
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

        // A discover runs before any built spec exists, so the drafted model is
        // the only source of the secrets which the data plane must resolve
        // before dialing the connector.
        let secrets = capture_def
            .model
            .as_ref()
            .map(|model| assemble::secrets(&model.secrets))
            .unwrap_or_default();

        // INFO is a good default since these are not shown in the UI, so if we're looking then
        // there's already a problem.
        let log_level = capture_def
            .model
            .as_ref()
            .and_then(|m| m.shards.log_level.as_deref())
            .and_then(ops::LogLevel::from_str_name)
            .unwrap_or(ops::LogLevel::Info);

        let connectors = self
            .connector_factory
            .make_connectors(snapshot, "discover", logs_token);

        let request = connector::Request {
            start: Some(connector::request::Start {
                log_level: log_level as i32,
                ..Default::default()
            }),
            kind: Some(connector::request::Kind::Capture(capture::Request {
                kind: Some(capture::request::Kind::Discover(Box::new(
                    capture::request::Discover {
                        name: capture_name.to_string(),
                        connector_type: capture_spec::ConnectorType::Image as i32,
                        config_json: serde_json::to_string(connector_cfg).unwrap().into(),
                        created_at,
                        secrets,
                    },
                ))),
                ..Default::default()
            })),
        };
        let result = async {
            let (started, response) = connectors(data_plane_id, request).await?;
            // `proto_grpc::connector::unary` verified that Started and the
            // response are of the capture protocol, so only Discovered can fail.
            match (started.spec, response) {
                (
                    Some(connector::response::started::Spec::Capture(spec)),
                    connector::response::Kind::Capture(capture::Response {
                        kind: Some(capture::response::Kind::Discovered(discovered)),
                        ..
                    }),
                ) => Ok((*spec, discovered)),
                _ => anyhow::bail!("connector did not return capture Discovered"),
            }
        }
        .await;

        let (spec, discovered) = match result {
            Ok(response) => response,
            Err(err) => {
                return Ok(DiscoverOutput::failed(capture_name, err));
            }
        };

        let output = Self::build_merged_catalog(
            capture_name,
            &subject,
            filter_user_authz,
            update_only,
            draft,
            discovered,
            spec.resource_path_pointers,
            db,
            reset_on_key_change,
            snapshot,
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
        subject: &models::authz::Subject,
        filter_user_authz: bool,
        update_only: bool,
        mut draft: tables::DraftCatalog,
        discovered: capture::response::Discovered,
        resource_path_pointers: Vec<String>,
        db: &PgPool,
        reset_on_key_change: bool,
        snapshot: &crate::Snapshot,
    ) -> anyhow::Result<DiscoverOutput> {
        let discovered_bindings = match specs::parse_response(discovered)
            .context("converting connector discovery response into specs")
        {
            Ok(b) => b,
            Err(err) => {
                return Ok(DiscoverOutput::failed(capture_name, err));
            }
        };

        let tables::DraftCatalog {
            captures,
            collections,
            ..
        } = &mut draft;
        let Some(drafted_capture) = captures.get_mut_by_key(&capture_name) else {
            anyhow::bail!("expected capture '{}' to exist in draft", capture_name);
        };
        let tables::DraftCapture {
            model: Some(capture_model),
            is_touch,
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
            .map(|p| json::Pointer::from_str(p.as_str()))
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

        let live = if filter_user_authz {
            crate::live_specs::get_live_specs_filtered(
                subject,
                &collection_names,
                models::authz::Capability::CatalogRead,
                snapshot,
                db,
            )
            .await?
        } else {
            crate::live_specs::get_live_specs_unfiltered(&collection_names, db).await?
        };

        let mut modified_bindings = specs::merge_collections(
            used_bindings,
            collections,
            &live.collections,
            reset_on_key_change,
        )?;
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
