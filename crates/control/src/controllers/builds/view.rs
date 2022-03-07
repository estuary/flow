use axum::Json;

use crate::controllers::builds::routes;
use crate::controllers::json_api::{DocumentData, Links, Many, One, Resource};
use crate::models::builds::Build;

pub fn index(builds: Vec<Build>) -> Json<Many<Build>> {
    let resources = builds.into_iter().map(Resource::from).collect();
    let links = Links::default().put("self", routes::index());
    Json(DocumentData::new(resources, links))
}

pub fn create(build: Build) -> Json<One<Build>> {
    let resource = DocumentData::new(Resource::from(build), Links::default());
    Json(resource)
}

pub fn show(build: Build) -> Json<One<Build>> {
    let resource = DocumentData::new(Resource::from(build), Links::default());
    Json(resource)
}

impl From<Build> for Resource<Build> {
    fn from(build: Build) -> Self {
        let links = Links::default().put("self", routes::show(build.id));

        Resource {
            id: build.id,
            r#type: "build",
            attributes: build,
            links,
        }
    }
}
