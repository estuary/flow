use axum::extract::{Extension, Path};
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;

use crate::context::AppContext;

use crate::error::AppError;
use crate::models::connector_images::{ConnectorImage, NewConnectorImage, NewDiscoveredCatalog};
use crate::models::id::Id;
use crate::repo::connector_images as images_repo;
use crate::repo::connectors as connectors_repo;
use crate::services::connectors;

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

pub async fn discovered_catalog(
    Extension(ctx): Extension<AppContext>,
    Path(image_id): Path<Id<ConnectorImage>>,
    Json(input): Json<NewDiscoveredCatalog>,
) -> Result<impl IntoResponse, AppError> {
    let image = images_repo::fetch_one(ctx.db(), image_id).await?;
    let connector = connectors_repo::fetch_one(ctx.db(), image.connector_id).await?;
    let opts = input.discovery_options()?;

    let discover_response = connectors::discover(&connector, &image, &input.config).await?;
    let catalog = view::discovery::DiscoveredCatalog::new(
        connector,
        image,
        input.config,
        discover_response.bindings,
        opts,
    );

    Ok((StatusCode::OK, view::discovered_catalog(catalog)))
}
