pub mod db;

use crate::connectors::ConnectorFactory;

use anyhow::Context;
use models::discovers::Changes;
use proto_flow::{capture, connector};
use sqlx::{PgPool, types::Uuid};

// Re-export key types and functions that executors will need
pub use db::{Row, fetch_discover, resolve};

/// Metadata of a discovery committed with its executor task.
pub struct CreatedDiscover {
    pub id: models::Id,
    pub data_plane_name: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub logs_token: Uuid,
}

/// Queues discovery using the capture's staged definition, or its live definition
/// when no draft entry exists and `subject` can read it.
/// The executor writes discovery results to the draft.
/// Permission denials and data-plane failures carry a `tonic::Status` so
/// request handlers can return HTTP 307 for provisional failures.
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
        SELECT ls.spec::text AS spec,
               ls.data_plane_id AS "data_plane_id: models::Id"
        FROM live_specs ls
        WHERE ls.catalog_name = $1 AND ls.spec_type = 'capture'
          AND ls.spec IS NOT NULL
        "#,
        capture_name,
    )
    .fetch_optional(pool)
    .await?;

    let model = select_capture_model(
        capture_name,
        draft
            .staged
            .then_some((draft.spec_type, draft.spec.as_deref())),
        live_capture.as_ref().and_then(|row| row.spec.as_deref()),
        snapshot,
        subject,
    )?;
    if model.delete {
        anyhow::bail!("capture is staged for deletion");
    }

    // The executor copies the live definitions of binding targets into the
    // draft, filtered by the user's CatalogRead. It runs without the request's
    // capability mask and prefix mask, so a restricted token could otherwise
    // read definitions through the draft.
    if let Some(target) = model
        .bindings
        .iter()
        .map(|binding| binding.target.as_str())
        .find(|target| {
            !snapshot.is_user_authorized(subject, target, models::authz::Capability::CatalogRead)
        })
    {
        anyhow::bail!(tonic::Status::permission_denied(format!(
            "not authorized to read binding target {target}"
        )));
    }
    let data_plane_error = || tonic::Status::not_found("data plane not found or unauthorized");
    let data_plane_name = if let Some(live) = &live_capture {
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

    // A record of the capture's policy at creation. The executor derives its
    // options anew, from the drafted capture which it merges.
    let update_only = discover::Options::for_capture(&model, live_capture.is_some()).update_only;

    // The executor takes the endpoint from the draft or live capture, which a
    // NULL `connector_tag_id` signals. `endpoint_config` is a placeholder.
    // The `create_discover_task` trigger schedules the executor within this statement.
    let row = sqlx::query!(
        r#"
        INSERT INTO discovers (
            draft_id, capture_name, connector_tag_id, endpoint_config,
            update_only, data_plane_name
        ) VALUES ($1, $2, NULL, '{}', $3, $4)
        RETURNING id AS "id!: models::Id", created_at, updated_at, logs_token
        "#,
        draft_id as models::Id,
        capture_name,
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
        logs_token: row.logs_token,
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
        // Check permission independently of existence so a provisional denial
        // cannot reveal whether an unreadable live capture exists.
        if !snapshot.is_user_authorized(
            subject,
            capture_name,
            models::authz::Capability::CatalogRead,
        ) {
            anyhow::bail!(tonic::Status::not_found("capture not found"));
        }
        live_spec.ok_or_else(|| anyhow::anyhow!("capture not found"))?
    };
    serde_json::from_str(spec).map_err(|_| anyhow::anyhow!("invalid capture model"))
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
    /// Live collections whose key or schema was modified by the discover,
    /// each represented by one of its bindings (see `discover::merge_collections`).
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
            draft,
            created_at,
            snapshot,
        } = req;

        let Some(capture_def) = draft.captures.get_by_key(&capture_name) else {
            return Ok(DiscoverOutput::failed(
                capture_name.clone(),
                anyhow::anyhow!("missing capture: '{capture_name}' in draft"),
            ));
        };

        let Some(model) = capture_def.model.as_ref() else {
            anyhow::bail!(
                "capture '{capture_name}' is drafted for deletion and cannot be discovered"
            );
        };
        let image = match &model.endpoint {
            models::CaptureEndpoint::Connector(config) => Some(config.image.as_str()),
            models::CaptureEndpoint::Python(_) => Some(validation::CAPTURE_PYTHON_IMAGE),
            models::CaptureEndpoint::Local(_) => None,
        };
        if let Some(image) = image {
            tracing::Span::current().record("image", image);
        }

        // Discover dials the same connector, with the same configuration,
        // which validation will: for a python capture, that's the capture-python
        // image with its `files` pushed down into the configuration.
        let capture_spec_request = validation::capture_spec_request(&model.endpoint, &model.shards);

        // A discover runs before any built spec exists, so the drafted model is
        // the only source of the secrets which the data plane must resolve
        // before dialing the connector.
        let secrets = assemble::secrets(&model.secrets);

        // INFO is a good default since these are not shown in the UI, so if we're looking then
        // there's already a problem.
        let log_level = model
            .shards
            .log_level
            .as_deref()
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
                        connector_type: capture_spec_request.connector_type,
                        config_json: capture_spec_request.config_json,
                        created_at,
                        secrets,
                    },
                ))),
                ..Default::default()
            })),
            ..Default::default()
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
            discover::Options {
                update_only,
                reset_on_key_change,
            },
            draft,
            discovered,
            spec.resource_path_pointers,
            db,
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
        options: discover::Options,
        mut draft: tables::DraftCatalog,
        discovered: capture::response::Discovered,
        resource_path_pointers: Vec<String>,
        db: &PgPool,
        snapshot: &crate::Snapshot,
    ) -> anyhow::Result<DiscoverOutput> {
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

        // A malformed response, or bindings without a resource path or with
        // conflicting ones, are a failure of the connector's discover.
        let merged = match discover::merge_bindings(
            capture_name.as_str(),
            capture_model,
            discovered,
            &options,
            &pointers,
        )
        .context("merging the connector's discovered bindings")
        {
            Ok(merged) => merged,
            Err(err) => return Ok(DiscoverOutput::failed(capture_name, err)),
        };

        // The merge creates and changes the target collections of the capture's
        // bindings, which the capture must be authorized to write. A denial
        // requests an early Snapshot refresh: the needed role grant may
        // postdate this Snapshot.
        let denied = merged
            .targets()
            .into_iter()
            .filter(|target| {
                !snapshot.is_role_authorized(
                    capture_name.as_str(),
                    target.as_str(),
                    models::Capability::Write,
                )
            })
            .map(|target| target.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        if !denied.is_empty() {
            snapshot.request_refresh();
            let err = anyhow::anyhow!(
                "capture is not write-authorized to the target collections of its bindings: {denied}"
            );
            return Ok(DiscoverOutput::failed(capture_name, err));
        }

        let collection_names = merged
            .targets()
            .into_iter()
            .map(|target| target.to_string())
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

        let modified =
            discover::merge_collections(&merged, collections, &live.collections, &options)?;
        let discover::Merged { added, removed, .. } = merged;

        if !added.is_empty() || !modified.is_empty() || !removed.is_empty() {
            *is_touch = false; // We're modifying the capture, so it's no longer a touch
        }

        Ok(DiscoverOutput {
            capture_name,
            draft,
            added,
            modified,
            removed,
        })
    }
}
