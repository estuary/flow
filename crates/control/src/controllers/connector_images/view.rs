use axum::Json;

use crate::controllers::connector_images::routes;
use crate::controllers::connectors::routes as connector_routes;
use crate::controllers::json_api::{DocumentData, Links, Many, One, RawJson, Resource};
use crate::models::connector_images::ConnectorImage;
use crate::models::Id;

pub fn index(images: Vec<ConnectorImage>) -> Json<Many<ConnectorImage>> {
    let resources = images.into_iter().map(Resource::from).collect();
    let links = Links::default().put("self", routes::index());
    Json(DocumentData::new(resources, links))
}

pub fn create(image: ConnectorImage) -> Json<One<ConnectorImage>> {
    let resource = DocumentData::new(Resource::<ConnectorImage>::from(image), Links::default());
    Json(resource)
}

pub fn show(image: ConnectorImage) -> Json<One<ConnectorImage>> {
    let resource = DocumentData::new(Resource::<ConnectorImage>::from(image), Links::default());
    Json(resource)
}

pub fn spec(image: ConnectorImage, spec: RawJson) -> Json<One<RawJson>> {
    let links = Links::default()
        .put("self", routes::spec(image.id))
        .put("image", routes::show(image.id))
        .put("discovery", routes::discovery(image.id))
        .put("connector", connector_routes::show(image.connector_id));
    let resource = Resource {
        id: Id::nonce(),
        r#type: "connector_spec",
        attributes: spec,
        links: links,
    };
    Json(DocumentData::new(resource, Links::default()))
}

pub fn discovery(image: ConnectorImage, bindings: RawJson) -> Json<One<RawJson>> {
    let links = Links::default().put("image", routes::show(image.id));
    let resource = Resource {
        id: Id::nonce(),
        r#type: "discovered_bindings",
        attributes: bindings,
        links: links,
    };
    Json(DocumentData::new(resource, Links::default()))
}

impl From<ConnectorImage> for Resource<ConnectorImage> {
    fn from(image: ConnectorImage) -> Self {
        let links = Links::default()
            .put("self", routes::show(image.id))
            .put("spec", routes::spec(image.id))
            .put("connector", connector_routes::show(image.connector_id));

        Resource {
            id: image.id,
            r#type: "connector_image",
            attributes: image,
            links,
        }
    }
}
