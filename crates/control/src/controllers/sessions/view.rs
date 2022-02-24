use axum::Json;

use crate::controllers::accounts::routes as accounts_routes;
use crate::controllers::json_api::{DocumentData, Links, One, Resource};
use crate::models::sessions::Session;
use crate::models::Id;

pub fn create(session: Session) -> Json<One<Session>> {
    let payload = DocumentData::new(Resource::<Session>::from(session), Links::default());

    Json(payload)
}

impl From<Session> for Resource<Session> {
    fn from(session: Session) -> Self {
        let links = Links::default().put("account", accounts_routes::show(session.account_id));

        Resource {
            id: Id::nonce(),
            r#type: "session",
            attributes: session,
            links,
        }
    }
}
