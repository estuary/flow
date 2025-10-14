use itertools::Itertools;
use proto_flow::flow;
use std::fmt::Write;

mod ast;
mod mapper;

use super::LambdaConfig;
use ast::Context;
use mapper::Mapper;

pub fn types_ts(
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec, LambdaConfig)],
) -> String {
    let mut w = String::with_capacity(4096);

    let (w_mapper, r_mapper) = collection_mappers(collection, "Document");

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
    for (name, collection, _config) in transforms {
        let source_name = format!("Source{}", camel_case(name, true));
        let (w_mapper, r_mapper) = collection_mappers(collection, &source_name);
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

    // Generate the IDerivation abstract class.
    write!(
        w,
        r#"
export abstract class IDerivation {{
    // Construct a new Derivation instance from a Request.Open message.
    constructor(_open: {{ state: unknown }}) {{ }}

    // flush awaits any remaining documents to be published and returns them.
    // deno-lint-ignore require-await
    async flush(): Promise<Document[]> {{
        return [];
    }}

    // reset is called only when running catalog tests, and must reset any internal state.
    async reset() {{ }}

    // startCommit is notified of a runtime commit in progress, and returns an optional
    // connector state update to be committed.
    startCommit(_startCommit: {{ runtimeCheckpoint: unknown }}): {{ state?: {{ updated: unknown, mergePatch: boolean }} }} {{
        return {{}};
    }}
"#,
    )
    .unwrap();

    for (name, _, _) in transforms {
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

    w
}

pub fn main_ts(transforms: &[(&str, &flow::CollectionSpec, LambdaConfig)]) -> String {
    let w = include_str!("main.ts.template").to_string();

    let transforms = transforms
        .iter()
        .map(|(name, _, _)| {
            let method_name = camel_case(name, false);
            format!("    derivation.{method_name}.bind(derivation) as Lambda,")
        })
        .join("\n");

    let w = w.replace("TRANSFORMS", &transforms);

    w
}

pub fn stub_ts(
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec, LambdaConfig)],
) -> String {
    let mut w = String::with_capacity(4096);

    let transforms = transforms
        .iter()
        .map(|(name, _, _)| {
            let method_name = camel_case(name, false);
            let source_name = format!("Source{}", camel_case(name, true));
            (method_name, source_name)
        })
        .collect::<Vec<_>>();

    let transform_sources = transforms
        .iter()
        .map(|(_, source_name)| source_name)
        .join(", ");

    writeln!(
        w,
        "import {{ IDerivation, Document, {transform_sources} }} from 'flow/{name}.ts';",
        name = &collection.name,
    )
    .unwrap();

    write!(
        w,
        r#"
// Implementation for derivation {name}.
export class Derivation extends IDerivation {{
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

fn collection_mappers(c: &flow::CollectionSpec, anchor_prefix: &str) -> (Mapper, Option<Mapper>) {
    // We extract anchors from just one schema:
    // * The write schema, if there is no read schema.
    // * Otherwise the read schema and not the write schema.
    if c.read_schema_json.is_empty() {
        (Mapper::new(&c.write_schema_json, anchor_prefix), None)
    } else {
        (
            Mapper::new(&c.write_schema_json, ""),
            Some(Mapper::new(&c.read_schema_json, anchor_prefix)),
        )
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
