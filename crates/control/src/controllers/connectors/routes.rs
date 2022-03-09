use crate::controllers::url_for;
use crate::models::connectors::Connector;
use crate::models::id::Id;

pub fn index() -> String {
    url_for("/connectors")
}

pub fn show(connector_id: Id<Connector>) -> String {
    url_for(format!("/connectors/{}", connector_id))
}

pub fn images(connector_id: Id<Connector>) -> String {
    url_for(format!(
        "/connectors/{}/connector_images",
        connector_id
    ))
}
