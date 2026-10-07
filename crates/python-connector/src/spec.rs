use anyhow::Context;

/// The `spec` of a sentinel: the connector specification declared by the
/// task model, with its defaults applied by validation. It mirrors the
/// connector's Spec response, which is answered from it without staging
/// or running user code.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Spec {
    /// JSON schema of the endpoint configuration.
    #[serde(default = "empty_schema")]
    pub config_schema: serde_json::Value,
    /// JSON schema of each binding's resource configuration.
    #[serde(default = "empty_schema")]
    pub resource_config_schema: serde_json::Value,
    /// OAuth2 provider of the connector, in the shape of a Spec response.
    #[serde(default)]
    pub oauth2: Option<serde_json::Value>,
}

impl Default for Spec {
    /// The spec of a configuration which doesn't declare one, such as a legacy
    /// configuration or a bare image Spec: any configuration is accepted.
    fn default() -> Self {
        Self {
            config_schema: empty_schema(),
            resource_config_schema: empty_schema(),
            oauth2: None,
        }
    }
}

impl Spec {
    /// Parse the `spec` of a sentinel, which is the default if absent.
    pub fn of_sentinel(sentinel: &serde_json::Value) -> anyhow::Result<Self> {
        let Some(spec) = sentinel.get("spec") else {
            return Ok(Self::default());
        };
        serde_json::from_value(spec.clone()).context("invalid sentinel `spec`")
    }
}

fn empty_schema() -> serde_json::Value {
    serde_json::json!({})
}

#[cfg(test)]
mod test {
    use super::Spec;

    #[test]
    fn specs_are_parsed_with_defaults() {
        let declared = serde_json::json!({"spec": {
            "configSchema": {"type": "object"},
            "resourceConfigSchema": {"type": "object", "properties": {"name": {"type": "string"}}},
            "oauth2": {"provider": "acme"},
        }});
        insta::assert_debug_snapshot!((
            Spec::of_sentinel(&declared).unwrap(),
            Spec::of_sentinel(&serde_json::json!({"package": "p"})).unwrap(),
            Spec::of_sentinel(&serde_json::json!({"spec": {"configSchema": {"type": "object"}}}))
                .unwrap(),
            // Unknown properties are allowed, as a newer control plane may add them.
            Spec::of_sentinel(&serde_json::json!({"spec": {"newerProperty": true}})).unwrap(),
            Spec::of_sentinel(&serde_json::json!({"spec": "not an object"}))
                .unwrap_err()
                .to_string(),
        ));
    }
}
