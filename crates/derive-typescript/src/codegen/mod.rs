use anyhow::Context as _;
use itertools::Itertools;
use proto_flow::flow;
use std::fmt::Write;
use typescript_connector::ts_types::{Context, Mapper, camel_case};

pub fn types_ts(
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec)],
    spec: &models::custom_connector::Spec,
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

    // Generate configuration types of the declared spec, which is resolved
    // with the connector's defaults.
    w.push_str(&typescript_connector::config_types_ts(spec)?);

    write_interface(&mut w, &collection.name, transforms);
    Ok(w)
}

// Write protocol types and the IDerivation abstract class, having an abstract
// method for each of `transforms`. Protocol types are those of `estuary.ts`
// and `derive.ts`, which the module of collection `name` imports relatively.
fn write_interface(w: &mut String, name: &str, transforms: &[(&str, &flow::CollectionSpec)]) {
    let parent = "../".repeat(name.matches('/').count());

    write!(
        w,
        r#"
// Protocol messages of the derivation, typed by its configurations.
import type * as estuary from '{parent}estuary.ts';
import type * as derive from '{parent}derive.ts';
import type {{ ConnectorStateUpdate }} from '{parent}estuary.ts';
import type {{ Flush, Flushed, Read, Validated }} from '{parent}derive.ts';
export type {{ ConnectorStateUpdate, Flush, Flushed, Read, Validated }};

export type Open<C = EndpointConfig, R = ResourceConfig, S = unknown> = derive.Open<C, R, S>;
export type Validate<C = EndpointConfig, R = ResourceConfig> = derive.Validate<C, R>;

// Result of a derivation flush(): the protocol's Flushed, and documents to publish.
export type FlushResponse<S = unknown> = derive.Flushed<S> & {{
    published?: Document[],
}};

// `C` is the type of the derivation's `config`, `R` of the resource
// configuration of each transform, and `S` of its connector state. `C` and `R`
// default to the types generated from the derivation's `spec`. They're not
// validated at runtime.
export abstract class IDerivation<C = EndpointConfig, R = ResourceConfig, S = unknown> {{
    // Construct a new Derivation instance from its Open message. Its `config`
    // is `open.collection.derivation.config`, and its state is `open.state`.
    // `open` may also be typed by its master-era shape `{{ state, range }}`, so
    // that modules written before Open was modeled, whose constructors call
    // `super(open)`, still type-check.
    constructor(_open: Open<C, R, S> | {{ state: unknown, range?: estuary.RangeSpec }}) {{ }}

    // validate the derivation as it's published, throwing to fail validation.
    // No instance exists until the derivation is opened. The default marks a
    // transform as read-only if its resource configuration has a true `readOnly`.
    // Its parameter is `unknown`-typed so that an override may narrow it.
    static validate(validate: Validate<unknown, unknown>): Validated {{
        return {{
            transforms: validate.transforms.map(({{ lambdaConfig }}) => ({{
                readOnly: (lambdaConfig as {{ readOnly?: unknown }} | null)?.readOnly === true,
            }})),
        }};
    }}

    // flush completes any deferred work for the current transaction, publishing
    // all documents derived from prior reads. It may be called more than once per
    // transaction: returning `more: true` asks the runtime for a further
    // iteration, forming a scatter/gather round across the derivation's shards.
    // On each call `flush.statePatches` holds the aggregated `state` updates
    // returned by all participating shards in the previous iteration (empty on
    // the first). Return a `FlushResponse` to publish documents, contribute a
    // `state` update, and/or request another iteration with `more: true`; or a
    // bare `Document[]` to publish and end participation for this transaction.
    // deno-lint-ignore require-await
    async flush(_flush: Flush): Promise<FlushResponse<S> | Document[]> {{
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
    abstract {method_name}(read: Read<{source_name}>): Document[];"#,
        )
        .unwrap();
    }
    w.push_str("\n}\n");
}

/// Generate the entry point of a derivation, which imports the module of its
/// directory at the relative URL `entry`.
pub fn main_ts(transforms: &[(&str, &flow::CollectionSpec)], entry: &str) -> String {
    let w = include_str!("main.ts.template").to_string();

    let transforms = transforms
        .iter()
        .map(|(name, _)| {
            let method_name = camel_case(name, false);
            format!("    derivation.{method_name}.bind(derivation) as Lambda,")
        })
        .join("\n");

    w.replace("TRANSFORMS", &transforms)
        .replace("ENTRY_MODULE", &serde_json::to_string(entry).unwrap())
}

/// Generate the starter entry of a derivation: a working derivation whose
/// transforms publish nothing, as a starting point.
pub fn starter_ts(
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
        r#"// Derivation of collection {name}.
//
// The platform runs the `Derivation` class which this module exports. Modules
// of this directory import one another relatively (`./helpers.ts`), and import
// their generated types through the `flow/` mapping of the project's deno.json.
import {{ IDerivation, Document, EndpointConfig, Open, Read, {transform_sources} }} from 'flow/{name}.ts';

export class Derivation extends IDerivation {{
    readonly config: EndpointConfig;

    constructor(open: Open) {{
        super(open);
        // `EndpointConfig` is generated from the derivation's `spec.configSchema`.
        this.config = open.collection.derivation.config;
    }}

"#,
        name = &collection.name,
    )
    .unwrap();

    for (method_name, source_name) in &transforms {
        writeln!(
            w,
            "    {method_name}(_read: Read<{source_name}>): Document[] {{"
        )
        .unwrap();
        w.push_str(
            "        // Return each `Document` derived from `_read.doc`.\n        \
             // This starter publishes nothing.\n        return [];\n    }\n",
        );
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
