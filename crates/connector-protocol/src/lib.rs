use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub mod capture;
pub mod materialize;

/// Specification of a Flow collection.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CollectionSpec {
    /// # Name of this collection.
    pub name: String,
    /// # Composite key of the collection.
    /// Keys are specified as an ordered sequence of JSON-Pointers.
    pub key: Vec<String>,
    /// # Logically-partitioned fields of this collection.
    pub partition_fields: Vec<String>,
    /// # Projections of this collection.
    pub projections: Vec<Projection>,
    /// # JSON Schema against which collection documents are validated.
    /// If set, then writeSchema and readSchema are not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Box<RawValue>>,
    /// # JSON Schema against which written collection documents are validated.
    /// If set, then readSchema is also and schema is not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_schema: Option<Box<RawValue>>,
    /// # JSON Schema against which read collection documents are validated.
    /// If set, then writeSchema is also and schema is not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_schema: Option<Box<RawValue>>,
}

/// Projections are named locations within a collection document which
/// may be used for logical partitioning, or may be mapped into a tabular
/// representation such as a SQL database table.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Projection {
    /// # Document location of this projection, as a JSON-Pointer.
    pub ptr: String,
    /// # Flattened, tabular alias of this projection.
    /// A field may correspond to a SQL table column, for example.
    pub field: String,
    /// # Was this projection explicitly provided ?
    /// (As opposed to implicitly created through static analysis of the schema).
    pub explicit: bool,
    /// # Does this projection constitute a logical partitioning of the collection?
    pub is_partition_key: bool,
    /// # Does this location form (part of) the collection key?
    pub is_primary_key: bool,
    /// # Inference of this projection.
    pub inference: Inference,
}

/// Static inference over this document location, extracted from a JSON Schema.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Inference {
    /// The possible types for this location. Subset of:
    /// ["null", "boolean", "object", "array", "integer", "numeric", "string"].
    pub types: Vec<String>,
    /// String type-specific inferences, or null iff types
    /// doesn't include "string".
    pub string: Option<StringInference>,
    /// The title from the schema, if provided.
    pub title: String,
    /// The description from the schema, if provided.
    pub description: String,
    /// The default value from the schema, or "null" if there is no default.
    pub default: Box<RawValue>,
    /// Whether this location is marked as a secret, like a credential or password.
    pub secret: bool,
    /// Existence of this document location.
    pub exists: Exists,
}

/// Static inference over a document location of type "string", extracted from a JSON schema.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StringInference {
    /// # Annotated Content-Type when the projection is of "string" type.
    pub content_type: String,
    // # Annotated format when the projection is of "string" type.
    pub format: String,
    /// # Annotated Content-Encoding when the projection is of "string" type.
    pub content_encoding: String,
    /// # Is the Content-Encoding "base64" (case-invariant)?
    pub is_base64: bool,
    /// # Maximum length when the projection is of "string" type.
    /// Zero for no limit.
    pub max_length: usize,
}

/// Enumeration which describes what's known about a location's existence
/// documents of the schema.
#[derive(Serialize, Deserialize, JsonSchema)]
pub enum Exists {
    /// The location must exist.
    Must = 1,
    /// The location may exist or be undefined.
    /// Its schema has explicit keywords which allow it to exist
    /// and which may constrain its shape, such as additionalProperties,
    /// items, unevaluatedProperties, or unevaluatedItems.
    May = 2,
    /// The location may exist or be undefined.
    /// Its schema omits any associated keywords, but the specification's
    /// default behavior allows the location to exist.
    Implicit = 3,
    /// The location cannot exist. For example, it's outside of permitted
    /// array bounds, or is a disallowed property, or has an impossible type.
    Cannot = 4,
}

/// OAuth2 describes an OAuth2 provider and templates how it should be used.
///
/// The templates are mustache templates and have a set of variables
/// available to them, the variables available everywhere are:
/// client_id: OAuth2 provider client id
/// redirect_uri: OAuth2 provider client registered redirect URI
///
/// Variables available in Auth URL request:
/// state: the state parameter, this parameter is used to prevent attacks
/// against our users. the parameter must be generated randomly and not
/// guessable. It must be associated with a user session, and we must check in
/// our redirect URI that the state we receive from the OAuth provider is the
/// same as the one we passed in. Scenario: user A can initiate an OAuth2 flow,
/// and send the OAuth Provider's Login URL to another person, user B. Once
/// this other person logs in through the OAuth2 Provider, they will be
/// redirected, and if there is no state check, we will authorise user A
/// to access user B's account. With the state check, the state will not be
/// available in user B's session, and therefore the state check will fail,
/// preventing the attack.
///
/// Variables available in Access Token request:
/// code: the code resulting from the suthorization step used to fetch the
/// token
/// client_secret: OAuth2 provider client secret
///
/// Variables available on Refresh Token request:
/// refresh_token: the refresh token
/// client_secret: OAuth2 provider client secret
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OAuth2 {
    /// # Name of the OAuth2 provider.
    /// This is a machine-readable key and must stay consistent.
    /// One example use case is to map providers to their respective style of buttons in the UI.
    pub provider: String,
    /// # Authorization URL template.
    /// This is the first step of the OAuth2 flow where the user is redirected
    /// to the OAuth2 provider to authorize access to their account.
    pub auth_url_template: String,
    /// # Template for access token URL.
    /// This is the second step of the OAuth2 flow, where we request an access token from the provider.
    pub access_token_url_template: String,
    /// # The method used to send Access Token requests.
    /// If not specified, POST is used by default.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub access_token_method: String,
    /// # The request body of the Access Token request.
    /// If not specified, the request body is empty.
    #[serde(default)]
    pub access_token_body: String,
    /// # Headers for the Access Token request.
    #[serde(default)]
    pub access_token_headers: BTreeMap<String, String>,
    /// # Mapping from OAuth provider response documents of an Access Token request.
    /// Maps keys into correct locations of the connector endpoint configuration.
    /// If the connector supports refresh tokens, must include `refresh_token` and
    /// `expires_in`. If this mapping is not provided, the keys from the response
    /// are passed as-is to the connector config.
    #[serde(default)]
    pub access_token_response_map: BTreeMap<String, String>,
    /// # Template for refresh token URL
    /// If not specified, refresh tokens are not requested.
    #[serde(default)]
    pub refresh_token_url_template: String,
    /// # The method used to send Refresh Token requests.
    /// If not specified, POST is used by default.
    #[serde(default)]
    pub refresh_token_method: String,
    /// # The request body of Refresh Token requests.
    /// If not specified, the request body is empty.
    #[serde(default)]
    pub refresh_token_body: String,
    /// # Headers for the Refresh Token request.
    #[serde(default)]
    pub refresh_token_headers: BTreeMap<String, String>,
    /// # Mapping from OAuth provider response documents of a Refresh Token request.
    #[serde(default)]
    pub refresh_token_response_map: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
pub struct RawValue(pub Box<serde_json::value::RawValue>);

impl JsonSchema for RawValue {
    fn schema_name() -> String {
        "Value".to_string()
    }
    fn is_referenceable() -> bool {
        false
    }
    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        serde_json::Value::json_schema(gen)
    }
}

impl std::ops::Deref for RawValue {
    type Target = serde_json::value::RawValue;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
