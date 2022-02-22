use crate::controllers::url_for;
use crate::models::Id;

pub fn index() -> String {
    url_for("/accounts")
}

pub fn show(account_id: Id) -> String {
    url_for(format!("/accounts/{}", account_id.to_string()))
}
