use crate::controllers::sessions::IdentityProvider;
use crate::controllers::url_for;

pub fn create(idp: IdentityProvider) -> String {
    url_for(format!("/sessions/{}", idp.as_str()))
}
