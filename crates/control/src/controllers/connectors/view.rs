use axum::Json;

use crate::controllers::connectors::routes;
use crate::controllers::{
    json_api::DocumentData, json_api::Links, json_api::Many, json_api::One, json_api::Resource,
};
use crate::models::connector_images::ConnectorImage;
use crate::models::connectors::Connector;
use crate::models::Id;

pub fn index(connectors: Vec<Connector>) -> Json<Many<Connector>> {
    let resources = connectors.into_iter().map(Resource::from).collect();
    let links = Links::default().put("self", routes::index());
    Json(DocumentData::new(resources, links))
}

pub fn create(connector: Connector) -> Json<One<Connector>> {
    let payload = DocumentData::new(Resource::<Connector>::from(connector), Links::default());
    Json(payload)
}

pub fn images(connector_id: Id, images: Vec<ConnectorImage>) -> Json<Many<ConnectorImage>> {
    let resources = images.into_iter().map(Resource::from).collect();
    let links = Links::default().put("self", routes::images(connector_id));
    Json(DocumentData::new(resources, links))
}

impl From<Connector> for Resource<Connector> {
    fn from(connector: Connector) -> Self {
        let links = Links::default()
            .put("self", routes::show(connector.id))
            .put("images", routes::images(connector.id));

        Resource {
            id: connector.id,
            r#type: "connector",
            attributes: connector,
            links,
        }
    }
}
