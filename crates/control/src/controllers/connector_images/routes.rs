use crate::controllers::url_for;
use crate::models::Id;

pub fn index() -> String {
    url_for("/connector_images")
}

pub fn show(image_id: Id) -> String {
    url_for(format!("/connector_images/{}", image_id.to_string()))
}

pub fn spec(image_id: Id) -> String {
    url_for(format!("/connector_images/{}/spec", image_id.to_string()))
}

pub fn discovery(connector_id: Id) -> String {
    url_for(format!(
        "/connector_images/{}/discovery",
        connector_id.to_string()
    ))
}
