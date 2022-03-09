use crate::controllers::url_for;
use crate::models::connector_images::ConnectorImage;
use crate::models::id::Id;

pub fn index() -> String {
    url_for("/connector_images")
}

pub fn show(image_id: Id<ConnectorImage>) -> String {
    url_for(format!("/connector_images/{}", image_id))
}

pub fn spec(image_id: Id<ConnectorImage>) -> String {
    url_for(format!("/connector_images/{}/spec", image_id))
}

pub fn discovery(connector_id: Id<ConnectorImage>) -> String {
    url_for(format!(
        "/connector_images/{}/discovery",
        connector_id
    ))
}
