use super::App;
use crate::api::error::ApiErrorExt;
use crate::publications::{
    DoNotRetry, DraftPublication, NoExpansion, NoopFinalize, NoopWithCommit,
};
use anyhow::Context;
use axum::http::StatusCode;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    #[serde(default)]
    default_data_plane: String,
    dry_run: bool,
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    diff: serde_json::Value,
}

#[axum::debug_handler]
#[tracing::instrument(
    skip(app),
    err(level = tracing::Level::WARN),
)]
pub async fn update_l2_reporting(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Extension(super::ControlClaims { sub: user_id, .. }): axum::Extension<
        super::ControlClaims,
    >,
    super::Request(Request {
        default_data_plane,
        dry_run,
    }): super::Request<Request>,
) -> Result<axum::Json<Response>, crate::api::ApiError> {
    if let None = sqlx::query!(
        "select role_prefix from internal.user_roles($1, 'admin') where role_prefix = 'ops/'",
        user_id,
    )
    .fetch_optional(&app.pg_pool)
    .await?
    {
        return Err(
            anyhow::anyhow!("authenticated user is not an admin of the 'ops/' tenant")
                .with_status(StatusCode::FORBIDDEN),
        );
    }

    let template = include_str!("../../../../ops-catalog/reporting-L2-template.bundle.json");
    let tables::DraftCatalog { collections, .. } =
        serde_json::from_str::<models::Catalog>(template)
            .unwrap()
            .into();

    // Extract draft collection templates from the bundle.
    const L2_INFERRED_NAME: &str = "ops.us-central1.v1/inferred-schemas/L2";
    const L2_STATS_NAME: &str = "ops.us-central1.v1/catalog-stats-L2";
    const L2_EVENTS_NAME: &str = "ops.us-central1.v1/events/L2";
    let mut l2_inferred: Option<tables::DraftCollection> = None;
    let mut l2_stats: Option<tables::DraftCollection> = None;
    let mut l2_events: Option<tables::DraftCollection> = None;

    for row in collections {
        match row.collection.as_str() {
            L2_INFERRED_NAME => {
                l2_inferred = Some(row);
            }
            L2_STATS_NAME => {
                l2_stats = Some(row);
            }
            L2_EVENTS_NAME => {
                l2_events = Some(row);
            }
            _ => {
                return Err(
                    anyhow::anyhow!("unrecognized template collection {}", row.collection)
                        .with_status(StatusCode::INTERNAL_SERVER_ERROR),
                );
            }
        }
    }
    let (Some(mut l2_stats), Some(mut l2_inferred), Some(mut l2_events)) =
        (l2_stats, l2_inferred, l2_events)
    else {
        return Err(anyhow::anyhow!(
            "expected template to include L2 status, inferred schemas, and catalog stats"
        )
        .with_status(StatusCode::INTERNAL_SERVER_ERROR));
    };

    let models::Derivation {
        transforms: l2_inferred_transforms,
        ..
    } = &mut l2_inferred.model.as_mut().unwrap().derive.as_mut().unwrap();

    let models::Derivation {
        transforms: l2_stats_transforms,
        using: l2_stats_using,
        ..
    } = &mut l2_stats.model.as_mut().unwrap().derive.as_mut().unwrap();

    let models::Derivation {
        transforms: l2_events_transforms,
        ..
    } = &mut l2_events.model.as_mut().unwrap().derive.as_mut().unwrap();

    let models::DeriveUsing::Typescript(models::DeriveUsingTypescript {
        module: l2_stats_module_raw,
    }) = l2_stats_using
    else {
        return Err(
            anyhow::anyhow!("L2 stats derivation must be a TypeScript module")
                .with_status(StatusCode::INTERNAL_SERVER_ERROR),
        );
    };

    let mut l2_stats_module =
        r#"import * as Types from 'flow/ops.us-central1.v1/catalog-stats-L2.ts';

export class Derivation extends Types.IDerivation {"#
            .to_string();

    // Remove template placeholders (they're used only for tests of reporting tasks).
    l2_inferred_transforms.clear();
    l2_stats_transforms.clear();
    l2_events_transforms.clear();

    // Add transforms for L1 derivations across all active data-planes.
    let data_planes = sqlx::query!(
        r#"
        select
            ops_l1_inferred_name  as "ops_l1_inferred_name: models::Collection",
            ops_l2_inferred_transform,
            ops_l1_stats_name     as "ops_l1_stats_name:    models::Collection",
            ops_l2_stats_transform,
            ops_l1_events_name    as "ops_l1_events_name:   models::Collection",
            ops_l2_events_transform,
            enable_l2
        from data_planes
        order by data_plane_name asc;
        "#,
    )
    .fetch_all(&app.pg_pool)
    .await?;

    for data_plane in &data_planes {
        l2_inferred_transforms.push(models::TransformDef {
            name: models::Transform::new(&data_plane.ops_l2_inferred_transform),
            source: models::Source::Collection(data_plane.ops_l1_inferred_name.clone()),
            disable: !data_plane.enable_l2,

            shuffle: models::Shuffle::Key(models::CompositeKey::new([models::JsonPointer::new(
                "/collection_name",
            )])),
            lambda: models::RawValue::from_value(&serde_json::json!(
                "select json($flow_document);"
            )),

            backfill: 0,
            priority: 0,
            read_delay: None,
        });

        l2_stats_transforms.push(models::TransformDef {
            name: models::Transform::new(&data_plane.ops_l2_stats_transform),
            source: models::Source::Collection(data_plane.ops_l1_stats_name.clone()),
            disable: !data_plane.enable_l2,

            backfill: 0,
            lambda: models::RawValue::default(),
            priority: 0,
            read_delay: None,
            shuffle: models::Shuffle::Any,
        });

        l2_events_transforms.push(models::TransformDef {
            name: models::Transform::new(&data_plane.ops_l2_events_transform),
            source: models::Source::Collection(data_plane.ops_l1_events_name.clone()),
            disable: !data_plane.enable_l2,
            backfill: 0,
            lambda: models::RawValue::from_value(&serde_json::json!(
                "select json($flow_document);"
            )),
            priority: 0,
            read_delay: None,
            shuffle: models::Shuffle::Any,
        });

        if !data_plane.enable_l2 {
            l2_stats_module.push_str("\n/*");
        }
        l2_stats_module.push_str(&format!(
            r#"
    {method_name}(read: {{ doc: Types.{type_name}}}): Types.Document[] {{
        return [read.doc]
    }}"#,
            method_name = camel_case(&data_plane.ops_l2_stats_transform, false),
            type_name = format!(
                "Source{}",
                camel_case(&data_plane.ops_l2_stats_transform, true)
            )
        ));
        if !data_plane.enable_l2 {
            l2_stats_module.push_str("\n*/");
        }
    }

    l2_stats_module.push_str("\n}\n");
    *l2_stats_module_raw = models::RawValue::from_value(&serde_json::json!(l2_stats_module));

    let draft = tables::DraftCatalog {
        collections: tables::DraftCollections::from_iter([l2_inferred, l2_stats, l2_events]),
        ..Default::default()
    };

    let logs_token = uuid::Uuid::new_v4();
    let publication = DraftPublication {
        user_id,
        logs_token,
        draft,
        dry_run,
        detail: Some(format!("publication for updating L2 reporting")),
        default_data_plane_name: Some(default_data_plane.clone()),
        // We've already validated that the user can admin `ops/`,
        // so further authZ checks are unnecessary.
        verify_user_authz: false,
        initialize: NoExpansion,
        finalize: NoopFinalize,
        retry: DoNotRetry,
        with_commit: NoopWithCommit,
    };
    let result = app
        .publisher
        .publish(publication)
        .await
        .context("publishing L2 reporting catalog")?;

    for err in result.draft_errors() {
        tracing::error!(error = ?err, "update-l2-reporting build error");
    }
    let result = result.error_for_status()?;

    let (live, draft) = (result.live.collections, result.draft.collections);
    tracing::info!(%logs_token, %dry_run, "updated L2 reporting");

    let previous = serde_json::json!({
        "l2_inferred": live.get_by_key(&models::Collection::new(L2_INFERRED_NAME)).map(|r| &r.model),
        "l2_stats": live.get_by_key(&models::Collection::new(L2_STATS_NAME)).map(|r| &r.model),
        "l2_events": live.get_by_key(&models::Collection::new(L2_EVENTS_NAME)).map(|r| &r.model),
    });
    let next = serde_json::json!({
        "l2_inferred": draft.get_by_key(&models::Collection::new(L2_INFERRED_NAME)).map(|r| &r.model),
        "l2_stats": draft.get_by_key(&models::Collection::new(L2_STATS_NAME)).map(|r| &r.model),
        "l2_status": draft.get_by_key(&models::Collection::new(L2_EVENTS_NAME)).map(|r| &r.model),
    });

    Ok(axum::Json(Response {
        diff: serde_json::json!(doc::diff(Some(&next), Some(&previous))),
    }))
}

// Copied from crates/derive-typescript/src/codegen/mod.rs
fn camel_case(name: &str, mut upper: bool) -> String {
    let mut w = String::new();

    for c in name.chars() {
        if !c.is_alphanumeric() {
            upper = true
        } else if upper {
            w.extend(c.to_uppercase());
            upper = false;
        } else {
            w.push(c);
        }
    }
    w
}
