//! A custom connector, whose code the user writes: its specification and files.
//!
//! These are the shapes of a custom task's model. Validation builds a custom
//! task into an image connector whose configuration is defined by its
//! language's crate (such as `python_connector::ProjectConfig`).

use super::Schema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;

/// Specification of a custom connector, whose code you write.
///
/// It declares the JSON schemas of the connector's endpoint and resource
/// configurations, its OAuth2 provider if it has one, and the network ports
/// on which it listens. Estuary answers requests for the connector's
/// specification from this declaration alone, without running your code.
/// The schemas also generate the configuration types that your code receives.
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(rename = "CustomConnectorSpec")]
pub struct Spec {
    /// # JSON schema of the task's endpoint `config`.
    /// It's an inline schema, or a relative URL of one.
    /// Mark sensitive locations with `secret: true`, and supply their values
    /// through the task's `secrets` stanza. Annotate a location with
    /// `x-python-type` (such as `datetime.datetime`) to set its type in
    /// generated Python code. Otherwise a string is a `str`, whatever its
    /// `format`. If absent, the task takes no configuration: its schema is an
    /// object which allows no properties.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_schema: Option<Schema>,
    /// # JSON schema of the resource configuration of each binding.
    /// It's an inline schema, or a relative URL of one. If absent, the
    /// connector's default resource configuration is used, which is typically
    /// what you want.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_config_schema: Option<Schema>,
    /// # OAuth2 provider of the connector, if it supports OAuth2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth2: Option<OAuth2>,
    /// # Network ports on which the connector listens, keyed on port number.
    /// Estuary proxies connections to each port from a hostname of the task.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub network_ports: BTreeMap<u16, NetworkPort>,
}

impl Spec {
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    /// The declared `configSchema`, or else the default of every custom
    /// connector: an object which allows no properties.
    pub fn config_schema_or_default(&self) -> &str {
        self.config_schema
            .as_ref()
            .map(|schema| schema.get())
            .unwrap_or(DEFAULT_CONFIG_SCHEMA)
    }
}

const DEFAULT_CONFIG_SCHEMA: &str = r#"{"type":"object","additionalProperties":false}"#;

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

/// A network port on which a custom connector listens.
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(rename = "CustomConnectorNetworkPort")]
pub struct NetworkPort {
    /// # ALPN protocol of a public port, such as `h2` or `mqtt`.
    /// Connections to a public port with a protocol are proxied as TCP.
    /// Otherwise, they're HTTP/1.1 requests, which is the protocol of every
    /// port that isn't public.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub protocol: String,
    /// # Allow unauthenticated connections to the port?
    /// Otherwise, only authenticated users with access to the task may connect.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub public: bool,
}

/// Files of a custom connector.
///
/// Use the indirect form: an array of file paths relative to the specification.
/// The inline form is a bundled representation used within the control plane.
#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum Files {
    /// Paths of files, relative to the specification.
    Indirect(Vec<String>),
    /// Contents of files, keyed on their path relative to the specification.
    Inline(BTreeMap<String, Option<String>>),
}

impl<'de> Deserialize<'de> for Files {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Contents of a user's model are always strings.
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Model {
            Indirect(Vec<String>),
            Inline(BTreeMap<String, String>),
        }
        Ok(match Model::deserialize(deserializer)? {
            Model::Indirect(paths) => Self::Indirect(paths),
            Model::Inline(files) => Self::Inline(
                files
                    .into_iter()
                    .map(|(path, content)| (path, Some(content)))
                    .collect(),
            ),
        })
    }
}

impl Default for Files {
    fn default() -> Self {
        Self::Inline(BTreeMap::new())
    }
}

impl Files {
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Indirect(paths) => paths.is_empty(),
            Self::Inline(files) => files.is_empty(),
        }
    }

    /// Paths of these files, in either form.
    pub fn paths(&self) -> Box<dyn Iterator<Item = &str> + '_> {
        match self {
            Self::Indirect(paths) => Box::new(paths.iter().map(String::as_str)),
            Self::Inline(files) => Box::new(files.keys().map(String::as_str)),
        }
    }

    /// Contents of these files, if they've been inlined.
    pub fn inline(&self) -> Option<&BTreeMap<String, Option<String>>> {
        match self {
            Self::Indirect(_) => None,
            Self::Inline(files) => Some(files),
        }
    }

    /// Is `path` a well-formed file path? It's `/`-separated names of letters,
    /// numbers, `-`, `_`, and `.`, without `.` or `..` components, and so can't
    /// name a location outside of the specification's directory.
    pub fn is_valid_path(path: &str) -> bool {
        let is_name = super::Name::regex()
            .find(path)
            .is_some_and(|m| m.start() == 0 && m.end() == path.len());

        is_name && !path.split('/').any(|c| c == "." || c == "..")
    }
}

impl JsonSchema for Files {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "CustomConnectorFiles".into()
    }

    fn json_schema(_: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
        from_value(json!({
            "description": "Files of the custom connector, as a list of paths or a map of paths to contents. A listed path is relative to this specification's directory, and its file is read as UTF-8 text. Publishing stores the files' contents, and pulling the specification writes them back out as a list of paths. Paths are `/`-separated names of letters, numbers, `-`, `_`, and `.`, and can't have `.` or `..` components.",
            "oneOf": [
                {
                    "type": "array",
                    "items": { "type": "string" },
                },
                {
                    "type": "object",
                    "additionalProperties": { "type": "string" },
                },
            ]
        }))
        .unwrap()
    }
}

/// Directory of a custom task within its specification's directory: the final
/// component of its name, verbatim (`acmeCo/nested/orders` has directory `orders`).
pub fn task_dir(name: &str) -> &str {
    name.rsplit('/').next().unwrap()
}

/// Remove the `sentinel` property from a custom task's built `config_json`.
/// The build injects it for the connector, and user code isn't shown it.
/// A config which doesn't have one keeps its exact rendering.
pub fn strip_sentinel(config_json: &mut bytes::Bytes, sentinel: &str) {
    let Ok(serde_json::Value::Object(mut config)) = serde_json::from_slice(config_json) else {
        return;
    };
    if config.remove(sentinel).is_some() {
        *config_json = serde_json::to_vec(&config).unwrap().into();
    }
}

/// Default endpoint configuration of a custom connector: an empty object.
pub(crate) fn empty_config() -> super::RawValue {
    super::RawValue::from_str("{}").unwrap()
}

pub(crate) fn is_empty_config(config: &super::RawValue) -> bool {
    config.get().trim() == "{}"
}

#[cfg(test)]
mod test {
    use super::Files;

    #[test]
    fn sentinels_are_stripped() {
        let strip = |config: &str| {
            let mut config = bytes::Bytes::from(config.to_string());
            super::strip_sentinel(&mut config, "_python");
            String::from_utf8(config.to_vec()).unwrap()
        };
        insta::assert_debug_snapshot!([
            strip(r#"{"token":"t","_python":{"files":{}}}"#),
            // Without a sentinel, a config keeps its exact rendering.
            strip(r#"{ "token": "t" }"#),
            strip(r#"{"_typescript":{}}"#),
            strip(r#""not an object""#),
            strip(""),
        ], @r#"
        [
            "{\"token\":\"t\"}",
            "{ \"token\": \"t\" }",
            "{\"_typescript\":{}}",
            "\"not an object\"",
            "",
        ]
        "#);
    }

    #[test]
    fn forms_are_distinguished_by_shape() {
        let indirect: Files =
            serde_json::from_str(r#"["pyproject.toml", "source-acme/__init__.py"]"#).unwrap();
        let inline: Files = serde_json::from_str(r#"{"pyproject.toml": "[project]\n"}"#).unwrap();

        insta::assert_debug_snapshot!((
            &indirect,
            indirect.paths().collect::<Vec<_>>(),
            &inline,
            inline.paths().collect::<Vec<_>>(),
            serde_json::to_string(&inline).unwrap(),
        ));
        // Non-string contents are not a valid inline form,
        // and nor is the `null` of a file which failed to load.
        assert!(serde_json::from_str::<Files>(r#"{"data.json": {"a": 1}}"#).is_err());
        assert!(serde_json::from_str::<Files>(r#"{"a.py": null}"#).is_err());

        // ... but `sources` may produce one, which serializes as `null`.
        let missing = Files::Inline([("a.py".to_string(), None)].into());
        assert_eq!(serde_json::to_string(&missing).unwrap(), r#"{"a.py":null}"#);
    }

    #[test]
    fn valid_paths() {
        let paths: Vec<(&str, bool)> = [
            "pyproject.toml",
            "source-acme/__init__.py",
            "2024-orders/mod.ts",
            ".venv/lib",
            "a/./b.py",
            "../escape.py",
            "a/..",
            "x%2F..%2Fescape.py",
            "%2e%2e/escape.py",
            "/absolute.py",
            "trailing/",
            "a//b.py",
            "has space.py",
            "",
        ]
        .into_iter()
        .map(|path| (path, Files::is_valid_path(path)))
        .collect();

        insta::assert_debug_snapshot!(paths, @r#"
        [
            (
                "pyproject.toml",
                true,
            ),
            (
                "source-acme/__init__.py",
                true,
            ),
            (
                "2024-orders/mod.ts",
                true,
            ),
            (
                ".venv/lib",
                true,
            ),
            (
                "a/./b.py",
                false,
            ),
            (
                "../escape.py",
                false,
            ),
            (
                "a/..",
                false,
            ),
            (
                "x%2F..%2Fescape.py",
                false,
            ),
            (
                "%2e%2e/escape.py",
                false,
            ),
            (
                "/absolute.py",
                false,
            ),
            (
                "trailing/",
                false,
            ),
            (
                "a//b.py",
                false,
            ),
            (
                "has space.py",
                false,
            ),
            (
                "",
                false,
            ),
        ]
        "#);
    }

    #[test]
    fn spec_defaults() {
        let declared: super::Spec = serde_json::from_value(serde_json::json!({
            "configSchema": {"type": "object"},
        }))
        .unwrap();

        assert_eq!(declared.config_schema_or_default(), r#"{"type":"object"}"#);
        assert_eq!(
            super::Spec::default().config_schema_or_default(),
            r#"{"type":"object","additionalProperties":false}"#
        );
    }

    #[test]
    fn task_dirs() {
        let dirs: Vec<_> = ["acmeCo/source-acme", "acmeCo/nested/2024-orders", "single"]
            .into_iter()
            .map(super::task_dir)
            .collect();
        assert_eq!(dirs, ["source-acme", "2024-orders", "single"]);
    }

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
