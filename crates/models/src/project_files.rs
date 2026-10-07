use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;

/// Files of a user-authored project, such as a Python capture or derivation.
///
/// The form is explicit: an array lists paths of files which are read as
/// text relative to the specification, and an object carries their contents.
/// Loading a specification inlines the array form into the object form,
/// and pulling a specification writes the object form back out as files.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum ProjectFiles {
    /// Paths of files, relative to the specification.
    Indirect(Vec<String>),
    /// Contents of files, keyed on their path relative to the specification.
    Inline(BTreeMap<String, String>),
}

impl Default for ProjectFiles {
    fn default() -> Self {
        Self::Inline(BTreeMap::new())
    }
}

impl ProjectFiles {
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
    pub fn inline(&self) -> Option<&BTreeMap<String, String>> {
        match self {
            Self::Indirect(_) => None,
            Self::Inline(files) => Some(files),
        }
    }
}

impl JsonSchema for ProjectFiles {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ProjectFiles".into()
    }

    fn json_schema(_: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
        from_value(json!({
            "description": "Files of the project. An array lists paths of files relative to the specification, which are read as text. An object maps each such path to its content. Paths are `/`-separated names of letters, numbers, `-`, `_`, and `.`, and cannot have `.` or `..` components.",
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

/// Default endpoint configuration of a user-authored task: an empty object.
pub(crate) fn empty_config() -> super::RawValue {
    super::RawValue::from_str("{}").unwrap()
}

pub(crate) fn is_empty_config(config: &super::RawValue) -> bool {
    config.get().trim() == "{}"
}

#[cfg(test)]
mod test {
    use super::ProjectFiles;

    #[test]
    fn forms_are_distinguished_by_shape() {
        let indirect: ProjectFiles =
            serde_json::from_str(r#"["pyproject.toml", "source_acme/__init__.py"]"#).unwrap();
        let inline: ProjectFiles =
            serde_json::from_str(r#"{"pyproject.toml": "[project]\n"}"#).unwrap();

        insta::assert_debug_snapshot!((
            &indirect,
            indirect.paths().collect::<Vec<_>>(),
            &inline,
            inline.paths().collect::<Vec<_>>(),
            serde_json::to_string(&inline).unwrap(),
        ));
        // Non-string contents are not a valid inline form.
        assert!(serde_json::from_str::<ProjectFiles>(r#"{"data.json": {"a": 1}}"#).is_err());
    }
}
