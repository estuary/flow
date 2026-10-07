use anyhow::Context;

/// The `spec` of a sentinel: the connector specification declared by the
/// task model. It mirrors the connector's Spec response, which is answered
/// from it without staging or running user code.
///
/// A schema which the model doesn't declare is absent, and each connector
/// resolves it to its own default.
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Spec {
    /// JSON schema of the endpoint configuration.
    #[serde(default)]
    pub config_schema: Option<serde_json::Value>,
    /// JSON schema of each binding's resource configuration.
    #[serde(default)]
    pub resource_config_schema: Option<serde_json::Value>,
    /// OAuth2 provider of the connector, in the shape of a Spec response.
    #[serde(default)]
    pub oauth2: Option<serde_json::Value>,
}

impl Spec {
    /// Parse the `spec` of a sentinel, which declares nothing if absent.
    pub fn of_sentinel(sentinel: &serde_json::Value) -> anyhow::Result<Self> {
        let Some(spec) = sentinel.get("spec") else {
            return Ok(Self::default());
        };
        serde_json::from_value(spec.clone()).context("invalid sentinel `spec`")
    }

    /// The declared `configSchema`, or else the default of every built-in
    /// connector: `{}`, which accepts any configuration.
    pub fn config_schema(&self) -> serde_json::Value {
        self.config_schema
            .clone()
            .unwrap_or_else(|| serde_json::json!({}))
    }
}

#[cfg(test)]
mod test {
    use super::Spec;

    #[test]
    fn specs_are_parsed() {
        let declared = serde_json::json!({"spec": {
            "configSchema": {"type": "object"},
            "resourceConfigSchema": {"type": "object", "properties": {"name": {"type": "string"}}},
            "oauth2": {"provider": "acme"},
        }});
        insta::assert_debug_snapshot!((
            Spec::of_sentinel(&declared).unwrap(),
            Spec::of_sentinel(&serde_json::json!({"capture": "acmeCo/source-acme"})).unwrap(),
            Spec::of_sentinel(&serde_json::json!({"spec": {"configSchema": {"type": "object"}}}))
                .unwrap(),
            Spec::of_sentinel(&serde_json::json!({"spec": {}}))
                .unwrap()
                .config_schema(),
            // Unknown properties are allowed, as a newer control plane may add them.
            Spec::of_sentinel(&serde_json::json!({"spec": {"newerProperty": true}})).unwrap(),
            Spec::of_sentinel(&serde_json::json!({"spec": "not an object"}))
                .unwrap_err()
                .to_string(),
        ));
    }
}
