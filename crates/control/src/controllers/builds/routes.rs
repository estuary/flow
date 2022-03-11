use crate::controllers::url_for;
use crate::models::{builds::Build, id::Id};

pub fn index() -> String {
    url_for("/builds")
}

pub fn show(build_id: Id<Build>) -> String {
    url_for(format!("/builds/{}", build_id.to_string()))
}
