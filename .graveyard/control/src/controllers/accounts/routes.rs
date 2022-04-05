use crate::controllers::url_for;
use crate::models::accounts::Account;
use crate::models::id::Id;

pub fn index() -> String {
    url_for("/accounts")
}

pub fn show(account_id: Id<Account>) -> String {
    url_for(format!("/accounts/{}", account_id.to_string()))
}
