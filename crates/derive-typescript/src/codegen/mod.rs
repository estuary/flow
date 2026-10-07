use anyhow::Context as _;
use itertools::Itertools;
use proto_flow::flow;
use std::fmt::Write;

mod ast;
mod mapper;

use ast::{AST, Context};
use mapper::Mapper;

pub fn types_ts(
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec)],
    spec: &super::Spec,
) -> anyhow::Result<String> {
    let mut w = String::with_capacity(4096);

    let (w_mapper, r_mapper) = collection_mappers(collection, "Document")?;

    // Generate Document* types.
    write!(
        w,
        r#"
// Generated for published documents of derived collection {name}.
export type Document = "#,
        name = &collection.name,
    )
    .unwrap();

    w_mapper
        .map(w_mapper.schema())
        .render(&mut Context::new(&mut w));
    write!(w, ";\n\n").unwrap();

    generate_anchors(&mut w, &w_mapper, r_mapper.as_ref(), "Document");

    // Generate Source{name} collection types for each transform.
    for (name, collection) in transforms {
        let source_name = format!("Source{}", camel_case(name, true));
        let (w_mapper, r_mapper) = collection_mappers(collection, &source_name)?;
        let source_mapper = r_mapper.as_ref().unwrap_or(&w_mapper);

        // Generate Source{name}* types.
        write!(
            w,
            r#"
// Generated for read documents of sourced collection {collection}.
export type {source_name} = "#,
            collection = &collection.name,
        )
        .unwrap();

        source_mapper
            .map(source_mapper.schema())
            .render(&mut Context::new(&mut w));
        write!(w, ";\n\n").unwrap();

        generate_anchors(&mut w, &w_mapper, r_mapper.as_ref(), &source_name);
    }

    // Generate configuration types of the declared spec.
    for (type_name, schema, prop) in [
        ("EndpointConfig", &spec.config_schema, "configSchema"),
        (
            "ResourceConfig",
            &spec.resource_config_schema,
            "resourceConfigSchema",
        ),
    ] {
        let mapper = Mapper::for_config(schema.to_string().as_bytes())
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

    write_interface(&mut w, transforms);
    Ok(w)
}

// Write protocol types and the IDerivation abstract class, having an abstract
// method for each of `transforms`.
fn write_interface(w: &mut String, transforms: &[(&str, &flow::CollectionSpec)]) {
    write!(
        w,
        r#"
// Mirror of Estuary protocol's flow.ConnectorState.
export type ConnectorState = {{
    // An updated state, or patch thereof.
    updated: unknown,
    // When true, `updated` is a RFC 7396 JSON Merge Patch rather than a full replacement.
    mergePatch?: boolean,
}};

// Result type of a derivation flush().
export type FlushResponse = {{
    // Documents to publish, if any.
    published?: Document[],
    // Connector state update to contribute. It is aggregated across all shards
    // and, if `more` is set, broadcast to shard's next flush() via `statePatches`.
    state?: ConnectorState,
    // Request a further Flush iteration this transaction. The runtime ends the
    // Flush phase once every shard returns `more: false` (the default). Use this
    // to drive a multi-stage scatter/gather across shards; it is independent of
    // `state`.
    more?: boolean,
}};

// Request.Open message from which a Derivation is constructed.
// `range` is the shard's assigned key/r-clock range (camelCase, with zero
// components omitted), present at runtime and useful for shards that namespace
// cooperative state by their key range. `resources` are the resource
// configurations (`lambda`) of each transform, in order.
export type Open<R = ResourceConfig> = {{
    state: unknown,
    range?: {{ keyBegin?: number, keyEnd?: number, rClockBegin?: number, rClockEnd?: number }},
    resources: R[],
}};

// Request.Validate of a derivation of collection `name` which is being
// published, having `transforms` and the resource configuration of each.
export type Validate<R = ResourceConfig> = {{
    name: string,
    transforms: {{ name: string, resourceConfig: R }}[],
}};

// Response.Validated of a derivation, with each of its transforms in the order
// of Validate. A `readOnly` transform never publishes documents.
export type Validated = {{
    transforms: {{ readOnly?: boolean }}[],
}};

// `C` is the type of the derivation's `config`, and `R` of the resource
// configuration of each transform. They default to the types generated from
// the derivation's `spec`, and are not validated at runtime.
export abstract class IDerivation<C = EndpointConfig, R = ResourceConfig> {{
    // Construct a new Derivation instance from a Request.Open message and the
    // derivation's `config`. `config` is optional so that modules written
    // before it existed, which call `super(open)`, still type-check.
    constructor(_open: Open<R>, _config?: C) {{ }}

    // validate the derivation as it's published, throwing to fail validation.
    // No instance exists until the derivation is opened. The default marks a
    // transform as read-only if its resource configuration has a true `readOnly`.
    // Parameters are `unknown` so that an override may narrow them.
    static validate(validate: Validate<unknown>, _config: unknown): Validated {{
        return {{
            transforms: validate.transforms.map(({{ resourceConfig }}) => ({{
                readOnly: (resourceConfig as {{ readOnly?: unknown }} | null)?.readOnly === true,
            }})),
        }};
    }}

    // flush completes any deferred work for the current transaction, publishing
    // all documents derived from prior reads. It may be called more than once per
    // transaction: returning `more: true` asks the runtime for a further
    // iteration, forming a scatter/gather round across the derivation's shards.
    // On each call `statePatches` holds the aggregated `state` updates returned by
    // all participating shards in the previous iteration (empty on the first).
    // Return a `FlushResponse` to publish documents, contribute a `state` update,
    // and/or request another iteration with `more: true`; or a bare `Document[]`
    // to publish and end participation for this transaction.
    // deno-lint-ignore require-await
    async flush(_statePatches?: unknown[]): Promise<FlushResponse | Document[]> {{
        return {{}};
    }}

    // reset is called only when running catalog tests, and must reset any internal state.
    async reset() {{ }}
"#,
    )
    .unwrap();

    for (name, _) in transforms {
        let method_name = camel_case(name, false);
        let source_name = format!("Source{}", camel_case(name, true));

        write!(
            w,
            r#"
    abstract {method_name}(read: {{ doc: {source_name} }}): Document[];"#,
        )
        .unwrap();
    }
    w.push_str("\n}\n");
}

pub fn main_ts(transforms: &[(&str, &flow::CollectionSpec)]) -> String {
    let w = include_str!("main.ts.template").to_string();

    let transforms = transforms
        .iter()
        .map(|(name, _)| {
            let method_name = camel_case(name, false);
            format!("    derivation.{method_name}.bind(derivation) as Lambda,")
        })
        .join("\n");

    w.replace("TRANSFORMS", &transforms)
}

pub fn stub_ts(
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec)],
) -> String {
    let mut w = String::with_capacity(4096);

    let transforms = transforms
        .iter()
        .map(|(name, _)| {
            let method_name = camel_case(name, false);
            let source_name = format!("Source{}", camel_case(name, true));
            (method_name, source_name)
        })
        .collect::<Vec<_>>();

    let transform_sources = transforms
        .iter()
        .map(|(_, source_name)| source_name)
        .join(", ");

    write!(
        w,
        r#"import {{ IDerivation, Document, EndpointConfig, Open, {transform_sources} }} from 'flow/{name}.ts';

// Implementation for derivation {name}.
// `EndpointConfig` is generated from the derivation's `spec.configSchema`.
export class Derivation extends IDerivation {{
    constructor(open: Open, readonly config: EndpointConfig) {{
        super(open, config);
    }}

"#,
        name = &collection.name,
    )
    .unwrap();

    for (method_name, source_name) in &transforms {
        writeln!(
            w,
            "    {method_name}(_read: {{ doc: {source_name} }}): Document[] {{"
        )
        .unwrap();
        w.push_str("        throw new Error(\"Not implemented\");\n    }\n");
    }
    w.push_str("}\n");

    w
}

fn generate_anchors(w: &mut String, w_mapper: &Mapper, r_mapper: Option<&Mapper>, prefix: &str) {
    let anchor_mapper = r_mapper.unwrap_or(w_mapper);

    for (anchor_url, anchor_name) in anchor_mapper.top_level.iter() {
        write!(
            w,
            r#"
// Generated for schema $anchor {anchor_fragment}."
export type {prefix}{anchor_name} = "#,
            anchor_fragment = anchor_url.fragment().unwrap(),
        )
        .unwrap();

        let schema = anchor_mapper
            .index()
            .fetch(anchor_url.as_str())
            .expect("anchor URL must be in index")
            .0;
        anchor_mapper.map(schema).render(&mut Context::new(w));
        write!(w, ";\n\n").unwrap();
    }
}

fn collection_mappers(
    c: &flow::CollectionSpec,
    anchor_prefix: &str,
) -> anyhow::Result<(Mapper, Option<Mapper>)> {
    let context = || format!("invalid schema of collection {}", c.name);

    // We extract anchors from just one schema:
    // * The write schema, if there is no read schema.
    // * Otherwise the read schema and not the write schema.
    if c.read_schema_json.is_empty() {
        Ok((
            Mapper::new(&c.write_schema_json, anchor_prefix).with_context(context)?,
            None,
        ))
    } else {
        Ok((
            Mapper::new(&c.write_schema_json, "").with_context(context)?,
            Some(Mapper::new(&c.read_schema_json, anchor_prefix).with_context(context)?),
        ))
    }
}

fn camel_case(name: &str, mut upper: bool) -> String {
    let mut w = String::new();

    for c in name.chars() {
        if !c.is_alphanumeric() {
            upper = true
        } else if upper {
            w.extend(c.to_uppercase());
            upper = false;
        } else {
            w.push(c);
        }
    }
    w
}
