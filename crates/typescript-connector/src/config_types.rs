//! Generated `EndpointConfig` and `ResourceConfig` types of a connector's
//! declared spec.
//!
//! These are the garden path. User code may instead parse its configuration
//! into types of its own, in which case the user keeps them equivalent to the
//! declared schemas: they're not checked.

use crate::ts_types::{AST, Context, Mapper};
use anyhow::Context as _;
use std::fmt::Write;

/// Generate the `EndpointConfig` and `ResourceConfig` types of `spec`, whose
/// schemas are resolved with its connector's defaults.
pub fn config_types_ts(spec: &models::custom_connector::Spec) -> anyhow::Result<String> {
    let mut w = String::new();

    for (type_name, schema, prop) in [
        ("EndpointConfig", &spec.config_schema, "configSchema"),
        (
            "ResourceConfig",
            &spec.resource_config_schema,
            "resourceConfigSchema",
        ),
    ] {
        let schema = schema.as_ref().expect("spec is resolved");
        let mapper = Mapper::for_config(schema.get().as_bytes())
            .with_context(|| format!("invalid `{prop}`"))?;

        write!(
            w,
            r#"
// Generated for `spec.{prop}`. It's not validated at runtime, and fields
// having a `default` are optional: their default is the derivation's to apply.
export type {type_name} = "#,
        )
        .unwrap();

        // A trivial schema (`{}`) is an object which allows any properties.
        match mapper.map(mapper.schema()) {
            AST::Unknown => w.push_str("Record<string, unknown>"),
            ast => ast.render(&mut Context::new(&mut w)),
        }
        write!(w, ";\n").unwrap();
    }

    Ok(w)
}

#[cfg(test)]
mod test {
    use super::config_types_ts;
    use models::custom_connector::Spec;

    #[test]
    fn trivial_and_absent_schemas() {
        // A trivial `configSchema` allows any properties, and an absent one
        // (resolved by a derivation's defaults) allows none.
        let trivial: Spec = serde_json::from_value(serde_json::json!({
            "configSchema": {},
            "resourceConfigSchema": {},
        }))
        .unwrap();

        insta::assert_snapshot!(
            [
                config_types_ts(&trivial).unwrap(),
                config_types_ts(&Spec::default().resolve_derivation()).unwrap(),
            ]
            .join("\n=====\n")
        );
    }
}
