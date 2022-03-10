use crate::controllers::url_for;
use crate::models::connector_images::ConnectorImage;
use crate::models::id::Id;

pub fn index() -> String {
    url_for("/connector_images")
}

pub fn show(image_id: Id<ConnectorImage>) -> String {
    url_for(format!("/connector_images/{}", image_id.to_string()))
}

pub fn spec(image_id: Id<ConnectorImage>) -> String {
    url_for(format!("/connector_images/{}/spec", image_id.to_string()))
}

pub fn discovered_catalog(connector_id: Id<ConnectorImage>) -> String {
    url_for(format!(
        "/connector_images/{}/discovered_catalog",
        connector_id.to_string()
    ))
}
