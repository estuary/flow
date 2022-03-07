use crate::config::settings;
use crate::models::{builds::Build, id::Id};

pub fn index() -> String {
    prefixed("/builds")
}

pub fn show(build_id: Id<Build>) -> String {
    prefixed(format!("/builds/{}", build_id.to_string()))
}

fn prefixed(path: impl Into<String>) -> String {
    format!("http://{}{}", settings().application.address(), path.into())
}
