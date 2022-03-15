use std::fmt::Display;
use std::str::FromStr;

use bytes::Bytes;
use protocol::flow::ContentType as ProtoContentType;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};

use super::RelativeUrl;

mod deserialization;
mod serialization;

/// A Resource is binary content with an associated ContentType.
#[derive(Debug, JsonSchema, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ResourceDef {
    /// # Content type of the Resource.
    pub content_type: ContentType,
    /// # Content of the Resource.
    #[schemars(schema_with = "serialization::content_schema")]
    pub content: Bytes,
}

/// Import a referenced Resource into the catalog.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(untagged, deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Import::example_url")]
#[schemars(example = "Import::example_extended")]
pub enum Import {
    Url(RelativeUrl),

    #[serde(rename_all = "camelCase")]
    Extended {
        /// # The resource to import.
        url: RelativeUrl,
        /// # The content-type of the imported resource.
        content_type: ContentType,
    },
}

impl Import {
    // Get the RelativeUrl of this Import.
    pub fn relative_url(&self) -> &RelativeUrl {
        match self {
            Self::Url(url) => url,
            Self::Extended { url, .. } => url,
        }
    }
    // Get the ContentType of this Import.
    pub fn content_type(&self) -> ContentType {
        match self {
            Self::Url(url) => {
                if url.ends_with(".json") {
                    ContentType::Catalog(ContentFormat::Json)
                } else {
                    ContentType::Catalog(ContentFormat::Yaml)
                }
            }
            Self::Extended { content_type, .. } => *content_type,
        }
    }

    fn example_url() -> Self {
        Self::Url(RelativeUrl::new("./a/flow.yaml"))
    }
    fn example_extended() -> Self {
        Self::Extended {
            url: RelativeUrl::new("https://example/schema.json"),
            content_type: ContentType::JsonSchema(ContentFormat::Json),
        }
    }
}

/// ContentFormat describes the format for a resource.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ContentFormat {
    Json,
    Yaml,
}

impl From<&url::Url> for ContentFormat {
    fn from(value: &url::Url) -> Self {
        if value.path().ends_with(".json") {
            ContentFormat::Json
        } else {
            // TODO: Should this be a TryFrom implementation instead? I expect
            // the callers from within most of Flow are going to default to Yaml
            // format, so I don't know there's a lot to gain by indicating an
            // inference failure.
            ContentFormat::Yaml
        }
    }
}

/// ContentType is the type of an imported resource's content.
#[derive(Clone, Copy, Debug, DeserializeFromStr, Eq, PartialEq, SerializeDisplay)]
pub enum ContentType {
    /// Resource is a Flow catalog (as YAML or JSON).
    Catalog(ContentFormat),
    /// Resource is a JSON schema (as YAML or JSON).
    JsonSchema(ContentFormat),
    /// Resource is a TypeScript module file.
    TypescriptModule,
    /// Configuration file.
    Config(ContentFormat),
    /// Fixture of documents.
    DocumentsFixture,
    /// Resource is a compiled NPM package.
    NpmPackage,
}

impl JsonSchema for ContentType {
    fn schema_name() -> String {
        "ContentType".to_string()
    }

    fn json_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        serde_json::from_value(serde_json::json!({
            "description": "ContentType is the type of an imported resource's content.",
            "type": "string",
            "enum": [
                "application/vnd.flow.catalog+json",
                "application/vnd.flow.catalog+yaml",
                "application/vnd.flow.jsonSchema+json",
                "application/vnd.flow.jsonSchema+yaml",
                "application/vnd.flow.typescript+text",
                "application/vnd.flow.config+json",
                "application/vnd.flow.config+yaml",
                "application/vnd.flow.documentsFixture+yaml",
                // Deliberately omitting npm packages from the schema. TODO: Find out why.
                // "application/vnd.flow.npmPackage+base64",
            ],
            "example": Self::example(),
        }))
        .unwrap()
    }
}

impl ContentType {
    pub fn example() -> Self {
        Self::Catalog(ContentFormat::Yaml)
    }
}

impl Display for ContentType {
    #[rustfmt::skip]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ContentFormat::*;
        use ContentType::*;

        match self {
            Catalog(Json)    => write!(f, "application/vnd.flow.catalog+json"),
            Catalog(Yaml)    => write!(f, "application/vnd.flow.catalog+yaml"),
            JsonSchema(Json) => write!(f, "application/vnd.flow.jsonSchema+json"),
            JsonSchema(Yaml) => write!(f, "application/vnd.flow.jsonSchema+yaml"),
            TypescriptModule => write!(f, "application/vnd.flow.typescript+text"),
            Config(Json)     => write!(f, "application/vnd.flow.config+json"),
            Config(Yaml)     => write!(f, "application/vnd.flow.config+yaml"),
            DocumentsFixture => write!(f, "application/vnd.flow.documentsFixture+yaml"),
            NpmPackage       => write!(f, "application/vnd.flow.npmPackage+base64"),
        }
    }
}

impl FromStr for ContentType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use ContentFormat::*;
        use ContentType::*;

        let content_type = match s {
            "application/vnd.flow.catalog+json" => Catalog(Json),
            "application/vnd.flow.catalog+yaml" => Catalog(Yaml),
            "application/vnd.flow.jsonSchema+json" => JsonSchema(Json),
            "application/vnd.flow.jsonSchema+yaml" => JsonSchema(Yaml),
            "application/vnd.flow.typescript+text" => TypescriptModule,
            "application/vnd.flow.config+json" => Config(Json),
            "application/vnd.flow.config+yaml" => Config(Yaml),
            "application/vnd.flow.documentsFixture+yaml" => DocumentsFixture,
            "application/vnd.flow.npmPackage+base64" => NpmPackage,
            otherwise => return Err(format!("ContentType not recognized: `{otherwise}`")),
        };

        Ok(content_type)
    }
}

impl From<ProtoContentType> for ContentType {
    fn from(t: ProtoContentType) -> Self {
        match t {
            ProtoContentType::CatalogJson => Self::Catalog(ContentFormat::Json),
            ProtoContentType::CatalogYaml => Self::Catalog(ContentFormat::Yaml),
            ProtoContentType::JsonSchemaJson => Self::JsonSchema(ContentFormat::Json),
            ProtoContentType::JsonSchemaYaml => Self::JsonSchema(ContentFormat::Yaml),
            ProtoContentType::TypescriptModule => Self::TypescriptModule,
            ProtoContentType::NpmPackage => Self::NpmPackage,
            ProtoContentType::ConfigJson => Self::Config(ContentFormat::Json),
            ProtoContentType::ConfigYaml => Self::Config(ContentFormat::Yaml),
            ProtoContentType::DocumentsFixture => Self::DocumentsFixture,
        }
    }
}
impl Into<ProtoContentType> for ContentType {
    fn into(self) -> ProtoContentType {
        match self {
            Self::Catalog(ContentFormat::Json) => ProtoContentType::CatalogJson,
            Self::Catalog(ContentFormat::Yaml) => ProtoContentType::CatalogYaml,
            Self::JsonSchema(ContentFormat::Json) => ProtoContentType::JsonSchemaJson,
            Self::JsonSchema(ContentFormat::Yaml) => ProtoContentType::JsonSchemaYaml,
            Self::TypescriptModule => ProtoContentType::TypescriptModule,
            Self::NpmPackage => ProtoContentType::NpmPackage,
            Self::Config(ContentFormat::Json) => ProtoContentType::ConfigJson,
            Self::Config(ContentFormat::Yaml) => ProtoContentType::ConfigYaml,
            Self::DocumentsFixture => ProtoContentType::DocumentsFixture,
        }
    }
}
