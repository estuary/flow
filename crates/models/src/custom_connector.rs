//! A custom connector, whose code the user writes: its specification and files.
//!
//! Validation builds a custom task into an image connector. Its configuration
//! is the user's `config` plus a `Sentinel` property, named by the task's
//! `Language`. Each image splits the sentinel back out (`Sentinel::split`),
//! and answers Spec from its `spec` without running user code.
//!
//! Behavior which every image shares lives here: the defaults of an omitted
//! schema (`Spec`), and the starter manifest of a project
//! (`Language::starter_manifest`). A listed manifest which doesn't exist yet
//! is given the starter, and a legacy `module` is built beside it
//! (`Language::legacy_files`). Images report errors with paths relative to
//! the project (`relative_to`).

use super::Schema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;

/// Specification of a custom connector, whose code you write.
///
/// It declares the JSON schemas of the connector's endpoint and resource
/// configurations, and its OAuth2 provider if it has one. Estuary answers
/// requests for the connector's specification from this declaration alone,
/// without running your code. The schemas also generate the configuration
/// types that your code receives.
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(rename = "CustomConnectorSpec")]
pub struct Spec {
    /// # JSON schema of the task's endpoint `config`.
    /// It's an inline schema, or a relative URL of one.
    /// Mark sensitive locations with `secret: true`, and supply their values
    /// through the task's `secrets` stanza. If absent, the task takes no
    /// configuration: its schema is an object which allows no properties.
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

    /// Resolve the absent schemas of a derivation's spec to their defaults:
    /// `config_schema_or_default`, and a transform `lambda` of
    /// `{readOnly: boolean}`.
    pub fn resolve_derivation(mut self) -> Self {
        self.config_schema.get_or_insert_with(|| {
            Schema::new(super::RawValue::from_str(DEFAULT_CONFIG_SCHEMA).unwrap())
        });
        self.resource_config_schema.get_or_insert_with(|| {
            Schema::new(super::RawValue::from_value(&json!({
                "type": "object",
                "properties": {
                    "readOnly": {
                        "type": "boolean",
                        "default": false,
                        "description": "Does this transform never publish documents?",
                    },
                },
            })))
        });
        self
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

/// Dependency specifier of the Estuary CDK which Python connectors build
/// against. Their starter and legacy projects list it in `dependencies`.
// TODO(before merge): this pins a commit of the unmerged
// `johnny/cdk-lint-backlog` branch of estuary/connectors. Repin it to a
// commit of `main` once that branch merges.
pub const CDK_DEPENDENCY: &str = "estuary-cdk @ https://github.com/estuary/connectors/archive/ecee9b6e74b930ee0acf167a3b51d821cbff2e85.tar.gz#subdirectory=estuary-cdk";

/// Property of a Python connector's built configuration which holds its `Sentinel`.
pub const PYTHON_SENTINEL: &str = "_python";
/// Property of a TypeScript connector's built configuration which holds its `Sentinel`.
pub const TYPESCRIPT_SENTINEL: &str = "_typescript";

/// Protocol version of a custom connector's Spec responses.
pub const PROTOCOL: u32 = 3032023;

/// Language of a custom connector.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Language {
    Python,
    Typescript,
}

impl Language {
    /// Manifest of a project, at its root.
    pub fn manifest(self) -> &'static str {
        match self {
            Self::Python => "pyproject.toml",
            Self::Typescript => "deno.json",
        }
    }

    /// Lock of a project's dependencies, at its root. A Validated lock is
    /// baked into the built specification, unless the user's `files` list
    /// one of their own.
    pub fn lock(self) -> &'static str {
        match self {
            Self::Python => "uv.lock",
            Self::Typescript => "deno.lock",
        }
    }

    /// Sentinel property of a connector's configuration.
    pub fn sentinel(self) -> &'static str {
        match self {
            Self::Python => PYTHON_SENTINEL,
            Self::Typescript => TYPESCRIPT_SENTINEL,
        }
    }

    /// Entry file of a task, within its directory `dir`.
    pub fn entry(self, dir: &str) -> String {
        match self {
            Self::Python => format!("{dir}/__init__.py"),
            Self::Typescript => format!("{dir}/mod.ts"),
        }
    }

    /// Directories reserved within a project. Generated files are written
    /// under `flow_generated/`, and Python's virtual environment is `.venv/`.
    pub fn reserved(self) -> &'static [&'static str] {
        match self {
            Self::Python => &[".venv", "flow_generated"],
            Self::Typescript => &["flow_generated"],
        }
    }

    /// Is `path` within a reserved directory?
    pub fn is_reserved(self, path: &str) -> bool {
        self.reserved()
            .iter()
            .any(|r| path == *r || path.strip_prefix(r).is_some_and(|p| p.starts_with('/')))
    }

    /// Starter manifest of a project whose task directory is `dir`. It has
    /// the dependencies which generated types import, resolves packages
    /// under a one-day cooldown, and type-checks strictly. The user owns it
    /// once their `files` list it.
    pub fn starter_manifest(self, dir: &str) -> String {
        match self {
            Self::Python => {
                // A distribution name, which can't have other characters.
                let name: String = dir
                    .chars()
                    .map(|c| {
                        if c.is_ascii_alphanumeric() || c == '.' || c == '_' {
                            c
                        } else {
                            '-'
                        }
                    })
                    .collect();

                include_str!("custom_connector/pyproject.toml")
                    .replace("PROJECT_NAME", &name)
                    .replace("CDK_DEPENDENCY", CDK_DEPENDENCY)
            }
            Self::Typescript => include_str!("custom_connector/deno.json").to_string(),
        }
    }

    /// Files of a derivation `task` whose model has a legacy `module`:
    /// its module is the task's entry file, beside a starter manifest.
    pub fn legacy_files(self, task: &str, module: String) -> BTreeMap<String, Option<String>> {
        let dir = task_dir(task);

        [
            (self.entry(dir), Some(module)),
            (
                self.manifest().to_string(),
                Some(self.starter_manifest(dir)),
            ),
        ]
        .into()
    }
}

/// Directory of a custom task within its specification's directory: the final
/// component of its name, verbatim (`acmeCo/nested/orders` has directory `orders`).
pub fn task_dir(name: &str) -> &str {
    name.rsplit('/').next().unwrap()
}

/// Rewrite absolute paths (or URLs) of a staged project's `root` within
/// `text` to be relative to it, which mirrors the layout of the user's own
/// project.
pub fn relative_to(text: &str, root: &std::path::Path) -> String {
    let url = url::Url::from_directory_path(root).expect("project root is absolute");
    let path = format!("{}/", root.display());

    // The URL goes first, because it contains the path.
    text.replace(url.as_str(), "").replace(&path, "")
}

/// Sentinel property of a custom connector's built configuration, which
/// validation composes into the user's `config`. The connector removes it
/// before the configuration reaches user code.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Sentinel {
    /// Name of the task.
    pub task: String,
    /// Contents of the task's files, keyed on their path relative to the
    /// specification. A listed file which couldn't be fetched is `None`.
    #[serde(default)]
    pub files: BTreeMap<String, Option<String>>,
    /// Specification declared by the task's model.
    #[serde(default)]
    pub spec: Spec,
}

impl Sentinel {
    /// Split a built connector configuration into the user's own
    /// configuration, and its `language` sentinel (if it has one).
    pub fn split(
        config_json: &[u8],
        language: Language,
    ) -> anyhow::Result<(serde_json::Map<String, serde_json::Value>, Option<Self>)> {
        use anyhow::Context;

        let mut config: serde_json::Map<String, serde_json::Value> =
            serde_json::from_slice(config_json)
                .context("connector configuration must be a JSON object")?;

        let sentinel = config
            .remove(language.sentinel())
            .map(serde_json::from_value)
            .transpose()
            .with_context(|| format!("invalid `{}` configuration", language.sentinel()))?;

        Ok((config, sentinel))
    }

    /// Directory of the task within its project.
    pub fn dir(&self) -> &str {
        task_dir(&self.task)
    }

    /// Listed files which failed to load (as they don't exist yet).
    pub fn missing(&self) -> Vec<&str> {
        self.files
            .iter()
            .filter(|(_, content)| content.is_none())
            .map(|(path, _)| path.as_str())
            .collect()
    }

    /// Does the project have a lock, which is either listed or baked?
    pub fn has_lock(&self, language: Language) -> bool {
        matches!(self.files.get(language.lock()), Some(Some(_)))
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
    fn output_paths_are_relative_to_the_project() {
        let output = concat!(
            "/tmp/.tmpABC/lib/geo.py:4:5 - error: \"x\" is not defined\n",
            "  File \"file:///tmp/.tmpABC/module.py\", line 12\n",
            "/tmp/.tmpABC/flow_generated/python/acmeCo/orders/__init__.py:9:1 - error\n",
            "error: TS2322 at file:///tmp/.tmpABC/orders/mod.ts:12:5\n",
            "    at file:///tmp/.tmpABC/flow_generated/typescript/acmeCo/orders.ts:40:3\n",
            "    at /tmp/.tmpABC/flow_generated/main.ts:7:1\n",
        );
        insta::assert_snapshot!(super::relative_to(output, std::path::Path::new("/tmp/.tmpABC")), @r#"
        lib/geo.py:4:5 - error: "x" is not defined
          File "module.py", line 12
        flow_generated/python/acmeCo/orders/__init__.py:9:1 - error
        error: TS2322 at orders/mod.ts:12:5
            at flow_generated/typescript/acmeCo/orders.ts:40:3
            at flow_generated/main.ts:7:1
        "#);
    }

    #[test]
    fn sentinels_are_split_from_configurations() {
        use super::{Language, Sentinel};

        let config = br#"{
            "credentials": {"token": "secret"},
            "_python": {
                "task": "acmeCo/source-acme",
                "files": {"a.py": "x = 1\n", "b.py": null, "uv.lock": "lock"},
                "spec": {"configSchema": {"type": "object"}}
            }
        }"#;
        let (user, sentinel) = Sentinel::split(config, Language::Python).unwrap();
        let sentinel = sentinel.unwrap();

        let split = |config: &[u8], language| {
            Sentinel::split(config, language).map_err(|err| format!("{err:#}"))
        };

        insta::assert_debug_snapshot!((
            user,
            sentinel.dir(),
            sentinel.missing(),
            sentinel.has_lock(Language::Python),
            sentinel.has_lock(Language::Typescript),
            &sentinel,
            // Another language's sentinel is the user's own configuration.
            split(config, Language::Typescript).unwrap().1,
            split(
                br#"{"_typescript": {"task": "acmeCo/orders"}}"#,
                Language::Typescript
            ),
            split(
                br#"{"_python": {"task": "t", "files": {"a.json": {"not": "text"}}}}"#,
                Language::Python
            ),
            split(br#"{"_python": {}}"#, Language::Python),
            split(b"[]", Language::Python),
        ));
    }

    #[test]
    fn spec_defaults() {
        let declared: super::Spec = serde_json::from_value(serde_json::json!({
            "configSchema": {"type": "object"},
        }))
        .unwrap();

        insta::assert_debug_snapshot!((
            declared.config_schema_or_default(),
            super::Spec::default().config_schema_or_default(),
            declared.clone().resolve_derivation(),
            super::Spec::default().resolve_derivation(),
        ));
    }

    #[test]
    fn starters_and_legacy_files() {
        use super::Language;

        for language in [Language::Python, Language::Typescript] {
            insta::assert_debug_snapshot!(
                format!("legacy_files_{language:?}"),
                language.legacy_files("acmeCo/nested/2024-orders", "MODULE".to_string())
            );
        }
    }

    #[test]
    fn task_dirs_and_paths() {
        let cases: Vec<_> = ["acmeCo/source-acme", "acmeCo/nested/2024-orders", "single"]
            .into_iter()
            .map(|name| {
                (
                    super::task_dir(name),
                    super::Language::Python.entry(super::task_dir(name)),
                    super::Language::Typescript.entry(super::task_dir(name)),
                )
            })
            .collect();

        insta::assert_debug_snapshot!(cases, @r#"
        [
            (
                "source-acme",
                "source-acme/__init__.py",
                "source-acme/mod.ts",
            ),
            (
                "2024-orders",
                "2024-orders/__init__.py",
                "2024-orders/mod.ts",
            ),
            (
                "single",
                "single/__init__.py",
                "single/mod.ts",
            ),
        ]
        "#);

        let reserved: Vec<_> = [
            ".venv",
            ".venv/lib",
            ".venvy",
            "flow_generated/x",
            "a/.venv",
        ]
        .into_iter()
        .map(|path| {
            (
                path,
                super::Language::Python.is_reserved(path),
                super::Language::Typescript.is_reserved(path),
            )
        })
        .collect();
        assert_eq!(
            reserved,
            [
                (".venv", true, false),
                (".venv/lib", true, false),
                (".venvy", false, false),
                ("flow_generated/x", true, true),
                ("a/.venv", false, false),
            ]
        );
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
