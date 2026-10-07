use anyhow::Context;
use std::collections::BTreeMap;

/// Property of a connector configuration which carries the user's Python code.
/// Validation composes it into the built configuration, and it's removed before
/// the configuration is handed to user code.
pub const SENTINEL: &str = "_python";

/// Split a connector configuration into the user's own configuration,
/// and its sentinel (if present).
pub fn split_sentinel(
    config_json: &[u8],
) -> anyhow::Result<(
    serde_json::Map<String, serde_json::Value>,
    Option<serde_json::Value>,
)> {
    let config: serde_json::Value =
        serde_json::from_slice(config_json).context("parsing connector configuration")?;

    let serde_json::Value::Object(mut config) = config else {
        anyhow::bail!("connector configuration must be an object");
    };
    let sentinel = config.remove(SENTINEL);

    Ok((config, sentinel))
}

/// A connector configuration without its sentinel.
pub fn without_sentinel(config_json: &[u8]) -> anyhow::Result<Vec<u8>> {
    let (config, _sentinel) = split_sentinel(config_json)?;
    Ok(serde_json::to_vec(&config).unwrap())
}

/// Remove the sentinel of the configuration object at `pointer` of `value`,
/// if there is one.
pub fn strip_sentinel_at(value: &mut serde_json::Value, pointer: &str) {
    if let Some(serde_json::Value::Object(config)) = value.pointer_mut(pointer) {
        config.remove(SENTINEL);
    }
}

/// Files of a project, keyed on their path relative to the project root.
/// A listed file which failed to load is `None`, and is given starter content.
pub type Files = BTreeMap<String, Option<String>>;

/// Parse the `files` of a sentinel, which map project paths to their text
/// (or `null`, for a listed file which failed to load).
pub fn text_files(files: Option<&serde_json::Value>) -> anyhow::Result<Files> {
    let Some(files) = files else {
        return Ok(BTreeMap::new());
    };
    serde_json::from_value(files.clone())
        .context("project `files` must map paths to their text, or null")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn sentinels_are_split_and_stripped() {
        let config = br#"{"credentials":{"token":"secret"},"_python":{"capture":"p","files":{"a.py":"x = 1\n","b.py":null}}}"#;

        let (user, sentinel) = split_sentinel(config).unwrap();
        let sentinel = sentinel.unwrap();
        let files = text_files(sentinel.get("files")).unwrap();

        let mut request = serde_json::json!({
            "validate": {"config": serde_json::from_slice::<serde_json::Value>(config).unwrap()},
        });
        strip_sentinel_at(&mut request, "/validate/config");
        strip_sentinel_at(&mut request, "/validate/missing");

        insta::assert_debug_snapshot!((
            serde_json::Value::Object(user),
            files,
            request,
            String::from_utf8(without_sentinel(config).unwrap()).unwrap(),
        ));
        assert!(text_files(Some(&serde_json::json!({"a.json": {"not": "text"}}))).is_err());
    }
}
