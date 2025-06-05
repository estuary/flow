mod bundle_schema;
mod indirect;
mod inline;
mod loader;
pub mod merge;
pub mod scenarios;

pub use bundle_schema::bundle_schema;
pub use indirect::{indirect_large_files, rebuild_catalog_resources};
pub use inline::{inline_capture, inline_draft_catalog};
pub use json::Scope; // Re-export.
pub use loader::{Fetcher, LoadError, Loader};

#[derive(Copy, Clone, Debug)]
pub enum Format {
    Json,
    Yaml,
}

impl Format {
    pub fn from_scope(scope: &url::Url) -> Self {
        if scope.as_str().ends_with("json") {
            Format::Json
        } else {
            Format::Yaml
        }
    }
    pub fn extension(&self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Yaml => "yaml",
        }
    }
    pub fn serialize(&self, value: &models::RawValue) -> Vec<u8> {
        let mut de = serde_json::Deserializer::from_str(value.get());
        let mut buf = Vec::new();

        match self {
            Self::Json => serde_transcode::transcode(
                &mut de,
                &mut serde_json::Serializer::with_formatter(
                    &mut buf,
                    serde_json::ser::PrettyFormatter::new(),
                ),
            )
            .unwrap(),
            Self::Yaml => {
                serde_transcode::transcode(&mut de, &mut serde_yaml::Serializer::new(&mut buf))
                    .unwrap()
            }
        }
        buf
    }
}
