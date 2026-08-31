//! The connector under test, when it is not the reference one.
//!
//! The suite exists to be pointed at real connectors. The reference connector is only
//! there to prove the harness itself works — which is why *it* is run twice, clean and
//! defective, and a real connector is run once.
//!
//! Two things have to be discovered rather than assumed, because a real connector's
//! configuration has nothing in common with the reference one's:
//!
//! - **The endpoint config** is supplied by the caller, as a file. Every connector in the
//!   connectors repository already keeps one for its integration tests, typically
//!   `materialize-$name/testdata/config.local.yaml`.
//! - **The resource config shape** is asked of the connector, via `spec`. A resource
//!   config is a connector-specific object, but the fields the harness must set are
//!   annotated in its JSON schema: `x-collection-name` names the table and
//!   `x-delta-updates` the delta-updates flag. (`x-schema-name` exists too, but the
//!   harness leaves the schema to the endpoint config and does not read it.) Reading those is the
//!   difference between working for any connector and working for the ones whose field
//!   names happen to be guessed right.

use anyhow::Context;

/// Where the fields the harness must set live in a connector's resource config.
#[derive(Debug, Clone)]
pub struct ResourceShape {
    /// Property naming the table, from `x-collection-name`.
    pub table: String,
    /// Property flagging delta-updates, from `x-delta-updates`.
    ///
    /// Required, not optional. A connector without one cannot take a delta binding, and a
    /// delta binding is where this suite can see a duplicate at all: on a merge binding an
    /// extra application is an idempotent upsert and therefore invisible. Silently handing
    /// such a connector a merge binding in place of each delta one would leave the run
    /// passing with its sharpest check disabled and nothing saying so.
    pub delta: String,
}

impl ResourceShape {
    /// A resource config for one binding of this connector.
    pub fn resource(&self, table: &str, delta: bool) -> serde_json::Value {
        let mut resource = serde_json::Map::new();
        resource.insert(self.table.clone(), serde_json::json!(table));

        // Set only when asked for: writing `false` where a connector treats absent and
        // false differently, or rejects unknown fields, would fail its strict config parse.
        if delta {
            resource.insert(self.delta.clone(), serde_json::json!(true));
        }
        serde_json::Value::Object(resource)
    }
}

/// Ask a connector for its `spec`, and read the resource shape out of the reply.
///
/// Driven over the protocol's JSON codec rather than protobuf: the exchange is one
/// request and one response, and `FLOW_RUNTIME_CODEC=json` makes it newline-delimited
/// JSON that needs no generated types on this side.
pub async fn resource_shape(connector: &std::path::Path) -> anyhow::Result<ResourceShape> {
    let mut command = async_process::Command::new(connector);
    command
        .env("FLOW_RUNTIME_CODEC", "json")
        .env("LOG_FORMAT", "json");

    // One request, then EOF: a connector reads until its stdin closes, and would
    // otherwise wait for a second request that is never coming.
    let request = serde_json::json!({"spec": {"connectorType": "IMAGE"}}).to_string() + "\n";

    let output = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        async_process::input_output(&mut command, request.as_bytes()),
    )
    .await
    .map_err(|_| anyhow::anyhow!("{connector:?} did not answer Spec within 60s"))?
    .with_context(|| format!("running {connector:?} to ask for its spec"))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let spec = stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find_map(|value| value.get("spec").cloned())
        .with_context(|| {
            format!(
                "{connector:?} sent no Spec response.\nstdout: {}\nstderr: {}",
                stdout.trim(),
                String::from_utf8_lossy(&output.stderr).trim(),
            )
        })?;

    // `resourceConfigSchema`, and an object rather than an embedded JSON string: the
    // protobuf field is `resource_config_schema_json`, but jsonpb renders it under its
    // `json_name` and inlines the schema. Read off a real connector's reply, not the proto.
    let schema = spec
        .get("resourceConfigSchema")
        .context("the Spec response carries no resourceConfigSchema")?;

    shape_of(schema)
}

/// Find the annotated properties in a resource config schema.
///
/// Annotations rather than names: `table` is conventional but not contractual, and the
/// delta flag is spelled `delta_updates` in some connectors and `delta` in others. The
/// annotations are what the connectors actually agree on.
fn shape_of(schema: &serde_json::Value) -> anyhow::Result<ResourceShape> {
    let properties = schema
        .get("properties")
        .and_then(|p| p.as_object())
        .context("the resource config schema has no properties")?;

    let annotated = |annotation: &str| -> Option<String> {
        properties
            .iter()
            .find(|(_, property)| property.get(annotation).and_then(|v| v.as_bool()) == Some(true))
            .map(|(name, _)| name.clone())
    };

    let table = annotated("x-collection-name").context(
        "no property of the resource config schema is annotated `x-collection-name`, so \
         the harness cannot tell which field names the table",
    )?;

    let delta = annotated("x-delta-updates").context(
        "no property of the resource config schema is annotated `x-delta-updates`, so this \
         connector cannot take a delta-updates binding. It cannot be verified by this suite: \
         a duplicate applied to a merge binding is an idempotent upsert and invisible, so \
         every scenario would pass while checking far less than it claims.",
    )?;

    Ok(ResourceShape { table, delta })
}

#[cfg(test)]
mod test {
    use super::*;

    /// Taken from `materialize-databricks`, which spells the flag `delta_updates` — the
    /// case that a hardcoded field name would get wrong.
    #[test]
    fn a_resource_shape_comes_from_annotations_not_names() {
        let schema = serde_json::json!({
            "properties": {
                "table": {"type": "string", "x-collection-name": true},
                "schema": {"type": "string", "x-schema-name": true},
                "delta_updates": {"type": "boolean", "x-delta-updates": true},
                "additional_table_create_sql": {"type": "string"},
            }
        });

        let shape = shape_of(&schema).unwrap();
        assert_eq!(shape.table, "table");
        assert_eq!(shape.delta, "delta_updates");

        assert_eq!(
            shape.resource("events", true),
            serde_json::json!({"table": "events", "delta_updates": true}),
        );
        // A merge binding must not carry the flag at all: a connector's strict parse
        // rejects unknown fields, and `false` is not always the same as absent.
        assert_eq!(
            shape.resource("accounts", false),
            serde_json::json!({"table": "accounts"}),
        );
    }

    /// A connector with no delta-updates option is refused outright. Accepting it would
    /// turn every delta binding into a merge one, which the invariants cannot see a
    /// duplicate through — a pass that means much less than it appears to.
    #[test]
    fn a_connector_without_delta_updates_is_refused() {
        let schema = serde_json::json!({
            "properties": {"path": {"type": "string", "x-collection-name": true}}
        });

        let err = shape_of(&schema).unwrap_err().to_string();
        assert!(err.contains("x-delta-updates"), "{err}");
    }

    #[test]
    fn an_unannotated_schema_is_refused() {
        let schema = serde_json::json!({"properties": {"table": {"type": "string"}}});
        assert!(shape_of(&schema).is_err());
    }
}

/// How a run gets its subject.
///
/// The reference connector is the default because the suite's own tests use it, and it
/// is the only subject whose *defects* the suite can switch on. A real connector arrives
/// through the environment instead:
///
/// - `FLOW_CONSISTENCY_SUBJECT` — path to the connector binary.
/// - `FLOW_CONSISTENCY_SUBJECT_CONFIG` — path to its endpoint config, JSON or YAML. Every
///   connector in the connectors repository keeps one for its integration tests, usually
///   `materialize-$name/testdata/config.local.yaml`.
pub const ENV_SUBJECT: &str = "FLOW_CONSISTENCY_SUBJECT";
pub const ENV_SUBJECT_CONFIG: &str = "FLOW_CONSISTENCY_SUBJECT_CONFIG";

/// The class the subject implements, which decides which scenarios apply to it.
///
/// Required rather than inferred, because `spec` does not report it and nothing else can:
/// how a connector divides durability with the runtime is a property of its
/// implementation, not of its configuration schema.
///
/// Most scenarios apply to most classes, because a fault a connector must survive is rarely a
/// property of how it divides durability with the runtime. What it does exclude is listed in
/// the crate README and enforced by [`crate::scenarios::Scenario::applies_to`], which is the
/// authority — a `documentCounter` subject currently skips four scenarios. Read a run's
/// `not-applicable` lines rather than any prose, including this.
pub const ENV_SUBJECT_CLASS: &str = "FLOW_CONSISTENCY_SUBJECT_CLASS";

/// Path to a built `tests/materialize/testctl` from the connectors repository.
///
/// Reads a destination back and drops a resource, neither of which the materialization
/// protocol offers. See "Destination reads go through connector code" in
/// `docs/materialize/consistency-testing.md` for why it is a separate program rather than a
/// connector subcommand.
pub const ENV_SUBJECT_TOOL: &str = "FLOW_CONSISTENCY_SUBJECT_TOOL";

/// The name `testctl` knows the connector by, e.g. `materialize-databricks`.
///
/// Not derived from the subject's file name, which is whatever the person building it chose.
pub const ENV_SUBJECT_NAME: &str = "FLOW_CONSISTENCY_SUBJECT_NAME";

/// A real connector to run scenarios against, if one was named.
#[derive(Debug, Clone)]
pub struct External {
    pub connector: std::path::PathBuf,
    pub config: serde_json::Value,
    pub shape: ResourceShape,
    /// A built `testctl`, and the name it knows this connector by. See [`ENV_SUBJECT_TOOL`].
    pub tool: std::path::PathBuf,
    pub name: String,
    /// The class it implements. Scenarios not in whose `applies_to` set it falls are
    /// skipped.
    pub class: crate::reference::Class,
}

/// Resolve an external subject from the environment, or `None` for the reference one.
///
/// Both variables are required together: a connector with no config cannot be validated,
/// and a config with no connector has nothing to configure.
///
/// All five or none: a partly-set group is a mistake worth failing on, because the
/// alternative is silently running the reference connector when someone meant to name a real
/// one — and a green reference run looks exactly like a green real one in the summary.
pub async fn external() -> anyhow::Result<Option<External>> {
    let named = [
        (ENV_SUBJECT, std::env::var_os(ENV_SUBJECT)),
        (ENV_SUBJECT_CONFIG, std::env::var_os(ENV_SUBJECT_CONFIG)),
        (ENV_SUBJECT_CLASS, std::env::var_os(ENV_SUBJECT_CLASS)),
        (ENV_SUBJECT_TOOL, std::env::var_os(ENV_SUBJECT_TOOL)),
        (ENV_SUBJECT_NAME, std::env::var_os(ENV_SUBJECT_NAME)),
    ];
    let missing: Vec<&str> = named
        .iter()
        .filter(|(_, value)| value.is_none())
        .map(|(name, _)| *name)
        .collect();

    if missing.len() == named.len() {
        return Ok(None); // Nothing named: the reference connector.
    }
    anyhow::ensure!(
        missing.is_empty(),
        "naming a real subject needs all of {}, and these are unset: {}",
        named.iter().map(|(n, _)| *n).collect::<Vec<_>>().join(", "),
        missing.join(", "),
    );

    let [connector, config, class, tool, name] = named
        .map(|(_, value)| value.expect("every variable is set: the partly-set case bailed above"));

    let class = class
        .into_string()
        .map_err(|raw| anyhow::anyhow!("{ENV_SUBJECT_CLASS}={raw:?} is not valid unicode"))?;
    let class: crate::reference::Class = serde_json::from_value(serde_json::json!(class))
        .with_context(|| {
            format!(
                "{ENV_SUBJECT_CLASS}={class:?} is not a class: expected one of \
                 remoteAuthoritative, postCommitApply, documentCounter, atLeastOnce",
            )
        })?;

    let connector = std::path::PathBuf::from(connector);
    anyhow::ensure!(
        connector.exists(),
        "{ENV_SUBJECT} names {connector:?}, which does not exist"
    );

    let raw = std::fs::read_to_string(&config)
        .with_context(|| format!("reading {ENV_SUBJECT_CONFIG} at {config:?}"))?;

    // Parsed as YAML, which subsumes JSON and is how these files are written.
    let config: serde_json::Value = serde_yaml::from_str(&raw)
        .with_context(|| format!("parsing {ENV_SUBJECT_CONFIG} at {config:?}"))?;

    let tool = std::path::PathBuf::from(tool);
    anyhow::ensure!(
        tool.exists(),
        "{ENV_SUBJECT_TOOL} names {tool:?}, which does not exist. Build it with \
         `go build -o {tool:?} ./tests/materialize/testctl` in the connectors repository."
    );
    let name = name
        .into_string()
        .map_err(|raw| anyhow::anyhow!("{ENV_SUBJECT_NAME}={raw:?} is not valid unicode"))?;

    let shape = resource_shape(&connector).await?;

    Ok(Some(External {
        connector,
        config,
        shape,
        class,
        tool,
        name,
    }))
}
