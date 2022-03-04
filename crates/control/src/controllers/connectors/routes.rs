use crate::controllers::url_for;
use crate::models::connectors::Connector;
use crate::models::Id;

pub fn index() -> String {
    url_for("/connectors")
}

pub fn show(connector_id: Id<Connector>) -> String {
    url_for(format!("/connectors/{}", connector_id.to_string()))
}

pub fn images(connector_id: Id<Connector>) -> String {
    url_for(format!(
        "/connectors/{}/connector_images",
        connector_id.to_string()
    ))
}
