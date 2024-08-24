use super::App;
use anyhow::Context;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    #[serde(default)]
    default_data_plane: String,
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Response {}

async fn do_update_l2_reporting(
    App {
        pg_pool,
        system_user_id,
        publisher,
        id_generator,
    }: &App,
    Request { default_data_plane }: Request,
) -> anyhow::Result<Response> {
    let template = include_str!("../../../../ops-catalog/reporting-L2-template.bundle.json");
    let tables::DraftCatalog { collections, .. } =
        serde_json::from_str::<models::Catalog>(template)
            .unwrap()
            .into();

    let mut l2_inferred: Option<tables::DraftCollection> = None;
    let mut l2_stats: Option<tables::DraftCollection> = None;

    for row in collections {
        match row.collection.as_str() {
            "ops.us-central1.v1/inferred-schemas/L2" => {
                l2_inferred = Some(row);
            }
            "ops.us-central1.v1/catalog-stats-L2" => {
                l2_stats = Some(row);
            }
            _ => {
                anyhow::bail!("unrecognized template collection {}", row.collection)
            }
        }
    }

    let (Some(mut l2_stats), Some(mut l2_inferred)) = (l2_stats, l2_inferred) else {
        anyhow::bail!("expected template to include L2 inferred schemas and catalog stats");
    };

    let l2_inferred_bindings = &mut l2_inferred
        .model
        .as_mut()
        .unwrap()
        .derive
        .as_mut()
        .unwrap()
        .transforms;

    let l2_stats_bindings = &mut l2_stats
        .model
        .as_mut()
        .unwrap()
        .derive
        .as_mut()
        .unwrap()
        .transforms;

    // Remove template placeholders (used only for tests of reporting tasks).
    l2_inferred_bindings.clear();
    l2_stats_bindings.clear();

    // Add bindings for L1 derivations across all active data-planes.
    let data_planes = sqlx::query!(
        r#"
        select
            ops_l1_inferred_name  as "ops_l1_inferred_name: models::Collection",
            ops_l2_inferred_transform,
            ops_l1_stats_name     as "ops_l1_stats_name:    models::Collection",
            ops_l2_stats_transform
        from data_planes
        -- Data-planes without configured HMAC keys are presumed to not be ready,
        -- and we hold back from processing their L1 derivations.
        where hmac_keys != '{}'
        order by data_plane_name asc;
        "#,
    )
    .fetch_all(pg_pool)
    .await?;

    for data_plane in &data_planes {
        l2_inferred_bindings.push(models::TransformDef {
            name: models::Transform::new(&data_plane.ops_l2_inferred_transform),
            source: models::Source::Collection(data_plane.ops_l1_inferred_name.clone()),

            shuffle: models::Shuffle::Key(models::CompositeKey::new([models::JsonPointer::new(
                "/collection_name",
            )])),
            lambda: models::RawValue::from_value(&serde_json::json!(
                "select json($flow_document);"
            )),

            backfill: 0,
            disable: false,
            priority: 0,
            read_delay: None,
        });

        l2_stats_bindings.push(models::TransformDef {
            name: models::Transform::new(&data_plane.ops_l2_stats_transform),
            source: models::Source::Collection(data_plane.ops_l1_stats_name.clone()),

            shuffle: models::Shuffle::Key(models::CompositeKey::new([models::JsonPointer::new(
                "/catalogName",
            )])),
            lambda: models::RawValue::from_value(&serde_json::json!(
                "select json($flow_document);"
            )),

            backfill: 0,
            disable: false,
            priority: 0,
            read_delay: None,
        });
    }

    let draft = tables::DraftCatalog {
        collections: tables::DraftCollections::from_iter([l2_inferred, l2_stats]),
        ..Default::default()
    };

    let pub_id = id_generator.lock().unwrap().next();
    let logs_token = uuid::Uuid::new_v4();

    let built = publisher
        .build(
            *system_user_id,
            pub_id,
            Some(format!("system publication for publishing reporting")),
            draft,
            logs_token,
            &default_data_plane,
        )
        .await?;

    if built.has_errors() {
        for err in built.output.errors() {
            tracing::error!(scope=%err.scope, err=format!("{:#}", err.error), "data-plane-template build error")
        }
        anyhow::bail!("data-plane-template build failed");
    }

    _ = publisher
        .commit(built)
        .await
        .context("committing publication")?
        .error_for_status()?;

    tracing::info!(%logs_token, "updated L2 reporting");

    Ok(Response {})
}

//#[tracing::instrument(skip(app))]
#[axum::debug_handler]
pub async fn update_l2_reporting(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    super::Request(request): super::Request<Request>,
    // TypedHeader(auth): TypedHeader<headers::Authorization<headers::authorization::Bearer>>,
) -> axum::response::Response {
    super::wrap(async move { do_update_l2_reporting(&app, request).await }).await
}
