mod bundle_schema;
mod indirect;
mod inline;
mod loader;
pub mod merge;
pub mod scenarios;

pub use bundle_schema::bundle_schema;
pub use indirect::{indirect_large_files, rebuild_catalog_resources};
pub use inline::{inline_capture, inline_draft_catalog};
pub use loader::{Fetcher, LoadError, Loader};

/// Is `path` a JSON or YAML document, rather than text?
pub fn is_dom_path(path: &str) -> bool {
    matches!(
        path_extension(path).as_deref(),
        Some("json" | "yaml" | "yml")
    )
}

/// Lowercase extension of the final component of `path`, if any.
fn path_extension(path: &str) -> Option<String> {
    let name = path.rsplit('/').next().unwrap();
    name.rsplit_once('.')
        .map(|(_, ext)| ext.to_ascii_lowercase())
}

#[derive(Copy, Clone, Debug)]
pub enum Format {
    Json,
    Yaml,
}

impl Format {
    /// Format of the resource of `scope`, ignoring any fragment
    /// location within it.
    pub fn from_scope(scope: &url::Url) -> Self {
        if path_extension(scope.path()).as_deref() == Some("json") {
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

#[cfg(test)]
mod test {
    use super::{Format, is_dom_path};

    #[test]
    fn formats_and_dom_paths_follow_the_extension() {
        let formats: Vec<(&str, &str)> = [
            "file:///project/flow.json",
            "file:///project/flow.JSON",
            "file:///project/flow.json#/collections/acmeCo~1orders/schema",
            "file:///project/flow.yaml#/collections/acmeCo~1orders.json",
            "file:///project/flow.yaml",
            "file:///project/notjson",
        ]
        .into_iter()
        .map(|url| {
            let ext = Format::from_scope(&url::Url::parse(url).unwrap()).extension();
            (url, ext)
        })
        .collect();

        let dom_paths: Vec<(&str, bool)> = [
            "data/regions.json",
            "data/regions.JSON",
            "data/config.yaml",
            "data/config.yml",
            "data/notjson",
            "data.json/notes.txt",
            "lib/geo.py",
        ]
        .into_iter()
        .map(|path| (path, is_dom_path(path)))
        .collect();

        insta::assert_debug_snapshot!((formats, dom_paths));
    }
}
