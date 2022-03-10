use axum::extract::{Extension, Path};
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use models::Object;

use crate::context::AppContext;

use crate::error::AppError;
use crate::models::connector_images::{ConnectorImage, NewConnectorImage};
use crate::models::id::Id;
use crate::models::names::CatalogName;
use crate::repo::connector_images as images_repo;
use crate::repo::connectors as connectors_repo;
use crate::services::connectors::{self, DiscoveryOptions};

pub mod routes;
mod view;

pub async fn index(Extension(ctx): Extension<AppContext>) -> Result<impl IntoResponse, AppError> {
    let images = images_repo::fetch_all(ctx.db()).await?;

    Ok((StatusCode::OK, view::index(images)))
}

pub async fn create(
    Extension(ctx): Extension<AppContext>,
    Json(input): Json<NewConnectorImage>,
) -> Result<impl IntoResponse, AppError> {
    let image = images_repo::insert(ctx.db(), input).await?;

    Ok((StatusCode::CREATED, view::create(image)))
}

pub async fn show(
    Extension(ctx): Extension<AppContext>,
    Path(image_id): Path<Id<ConnectorImage>>,
) -> Result<impl IntoResponse, AppError> {
    let image = images_repo::fetch_one(ctx.db(), image_id).await?;
    Ok((StatusCode::OK, view::show(image)))
}

pub async fn spec(
    Extension(ctx): Extension<AppContext>,
    Path(image_id): Path<Id<ConnectorImage>>,
) -> Result<impl IntoResponse, AppError> {
    let image = images_repo::fetch_one(ctx.db(), image_id).await?;
    let connector = connectors_repo::fetch_one(ctx.db(), image.connector_id).await?;
    let spec = connectors::spec(&image).await?;

    Ok((StatusCode::OK, view::spec(connector, image, spec)))
}

#[derive(Debug, Deserialize)]
pub struct DiscoveryInput {
    /// The desired name of the Capture. This is used to generate all the
    /// related resource names as well.
    name: CatalogName,
    /// The endpoint configuration for the source connector.
    config: Object,
}

impl DiscoveryInput {
    pub fn discovery_options(&self) -> Result<DiscoveryOptions, anyhow::Error> {
        // TODO: replace this with real validations.
        let (prefix, name) = self
            .name
            .split_once('/')
            .ok_or_else(|| anyhow::anyhow!("invalid name"))?;

        Ok(DiscoveryOptions {
            capture_name: CatalogName::new(name),
            catalog_prefix: CatalogName::new(prefix),
        })
    }
}

pub async fn discovery(
    Extension(ctx): Extension<AppContext>,
    Path(image_id): Path<Id<ConnectorImage>>,
    Json(input): Json<DiscoveryInput>,
) -> Result<impl IntoResponse, AppError> {
    let image = images_repo::fetch_one(ctx.db(), image_id).await?;
    let connector = connectors_repo::fetch_one(ctx.db(), image.connector_id).await?;
    let opts = input.discovery_options()?;

    let discovered_catalog = connectors::discover(connector, image, input.config, opts).await?;

    Ok((StatusCode::OK, view::discovery(discovered_catalog)))
}
