use super::Schema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Connector specification of a user-authored (built-in) connector,
/// which mirrors a connector's Spec response.
///
/// The connector answers Spec from this declaration, without running user
/// code, and generates `EndpointConfig` and `ResourceConfig` types of its
/// schemas. User code may instead parse into types of its own, in which case
/// keeping them equivalent to these schemas is the user's responsibility.
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BuiltinSpec {
    /// # JSON schema of the task's endpoint `config`.
    /// It's an inline schema, or a relative URL of one.
    /// Mark sensitive locations with `secret: true`, and supply their values
    /// through the task's `secrets` stanza. Defaults to `{}`, which allows any
    /// configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_schema: Option<Schema>,
    /// # JSON schema of the resource configuration of each binding.
    /// It's an inline schema, or a relative URL of one. Captures default to
    /// the CDK's `ResourceConfig` (a `name` and an update `interval`), and
    /// derivations to the `lambda` of each transform (`{readOnly: boolean}`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_config_schema: Option<Schema>,
    /// # OAuth2 provider of the connector, if it supports OAuth2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth2: Option<OAuth2>,
}

impl BuiltinSpec {
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

/// OAuth2 provider of a connector, which mirrors the OAuth2 of a connector's
/// Spec response (and the CDK's `OAuth2Spec`).
///
/// Header and response-map values are strings, as the CDK's `OAuth2Spec`
/// requires. Templates are mustache templates. Every template may use `client_id` and
/// `redirect_uri`. The authorization URL may also use `state`, the access
/// token request may use `code` and `client_secret`, and the refresh token
/// request may use `refresh_token` and `client_secret`.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OAuth2 {
    /// # Machine-readable name of the OAuth2 provider.
    pub provider: String,
    /// # Template of the URL to which a user is sent to authorize access.
    pub auth_url_template: String,
    /// # Template of the URL from which an access token is requested.
    pub access_token_url_template: String,
    /// # HTTP method of the access token request (default POST).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub access_token_method: String,
    /// # Template of the body of the access token request.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub access_token_body: String,
    /// # Headers of the access token request.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub access_token_headers: BTreeMap<String, String>,
    /// # Mapping of the access token response into standard names.
    /// Keys are the standard RFC 6749 names (`access_token`, `refresh_token`,
    /// and `access_token_expires_at`), and values locate them in the
    /// provider's response. Without a mapping, response keys are used as-is.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub access_token_response_map: BTreeMap<String, String>,
    /// # Template of the URL from which an access token is refreshed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub refresh_token_url_template: String,
    /// # HTTP method of the refresh token request (default POST).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub refresh_token_method: String,
    /// # Template of the body of the refresh token request.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub refresh_token_body: String,
    /// # Headers of the refresh token request.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub refresh_token_headers: BTreeMap<String, String>,
    /// # Mapping of the refresh token response into standard names.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub refresh_token_response_map: BTreeMap<String, String>,
}

#[cfg(test)]
mod test {
    #[test]
    fn oauth2_maps_are_of_strings() {
        let oauth2 = serde_json::json!({
            "provider": "acme",
            "authUrlTemplate": "https://acme.example/authorize",
            "accessTokenUrlTemplate": "https://acme.example/token",
            "accessTokenHeaders": {"Accept": "application/json"},
            "accessTokenResponseMap": {"access_token": "/access_token"},
        });
        let parsed: super::OAuth2 = serde_json::from_value(oauth2.clone()).unwrap();
        assert_eq!(serde_json::to_value(&parsed).unwrap(), oauth2);

        let mut non_string = oauth2;
        non_string["accessTokenResponseMap"]["expires_in"] = serde_json::json!(3600);
        assert!(serde_json::from_value::<super::OAuth2>(non_string).is_err());
    }
}
