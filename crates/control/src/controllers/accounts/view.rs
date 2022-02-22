use axum::Json;

use crate::controllers::accounts::routes;
use crate::controllers::{
    json_api::DocumentData, json_api::Links, json_api::Many, json_api::One, json_api::Resource,
};
use crate::models::accounts::Account;

pub fn index(accounts: Vec<Account>) -> Json<Many<Account>> {
    let resources = accounts.into_iter().map(Resource::from).collect();
    let links = Links::default().put("self", routes::index());

    Json(DocumentData::new(resources, links))
}

pub fn show(account: Account) -> Json<One<Account>> {
    let payload = DocumentData::new(Resource::<Account>::from(account), Links::default());

    Json(payload)
}

impl From<Account> for Resource<Account> {
    fn from(account: Account) -> Self {
        let links = Links::default().put("self", routes::show(account.id));

        Resource {
            id: account.id,
            r#type: "account",
            attributes: account,
            links,
        }
    }
}
