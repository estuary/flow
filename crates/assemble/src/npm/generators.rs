use super::interface::{Interface, Method};
use super::{build_mapper, camel_case, relative_path, relative_url};
use itertools::Itertools;
use serde_json::json;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::path;
use typescript::ast::Context;

pub fn module_types(
    package_dir: &path::Path,
    compiled: &[(url::Url, doc::Schema)],
    imports: &[tables::Import],
    collection: &tables::Collection,
    interface: Option<&Interface>,
) -> String {
    let mut w = String::with_capacity(4096);

    let w_mapper = build_mapper(compiled, imports, &collection.write_schema, false);
    let r_mapper = build_mapper(compiled, imports, &collection.read_schema, true);

    // Generate named anchor types contained within the collection read schema.
    for (anchor_url, anchor_name) in r_mapper.top_level.iter() {
        write!(
            w,
            r#"
// Generated from $anchor schema {anchor_url}."
export type {anchor_name} = "#,
            anchor_url = relative_url(anchor_url, package_dir),
        )
        .unwrap();

        r_mapper.map(anchor_url).render(&mut Context::new(&mut w));
        write!(w, ";\n\n").unwrap();
    }

    if w_mapper.schema == r_mapper.schema {
        // Generate the Document type as the collection schema.
        write!(
            w,
            r#"
// Generated from collection schema {schema}.
// Referenced from {scope}.
export type Document = "#,
            schema = relative_url(&r_mapper.schema, package_dir),
            scope = relative_url(&collection.scope, package_dir),
        )
        .unwrap();

        r_mapper
            .map(&r_mapper.schema)
            .render(&mut Context::new(&mut w));
        write!(w, ";\n\n").unwrap();

        write!(
            w,
            r#"
// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
"#,
        )
        .unwrap();
    } else {
        // Generate the SourceDocument type as the collection read schema.
        write!(
            w,
            r#"
// Generated from collection read schema {schema}.
// Referenced from {scope}.
export type SourceDocument = "#,
            schema = relative_url(&r_mapper.schema, package_dir),
            scope = relative_url(&collection.scope, package_dir),
        )
        .unwrap();

        r_mapper
            .map(&r_mapper.schema)
            .render(&mut Context::new(&mut w));
        write!(w, ";\n\n").unwrap();

        // Generate the OutputDocument type as the collection write schema.
        write!(
            w,
            r#"
// Generated from collection write schema {schema}.
// Referenced from {scope}.
export type OutputDocument = "#,
            schema = relative_url(&w_mapper.schema, package_dir),
            scope = relative_url(&collection.scope, package_dir),
        )
        .unwrap();

        w_mapper
            .map(&w_mapper.schema)
            .render(&mut Context::new(&mut w));
        write!(w, ";\n\n").unwrap();
    }

    let Interface {
        derivation,
        methods,
        typescript_module,
        ..
    } = match interface {
        None => {
            // This collection has no mapped derivation interface, and we're all done.
            return w;
        }
        Some(i) => i,
    };

    // Generate the Register type from the derivation register schema.
    let reg_mapper = build_mapper(compiled, imports, &derivation.register_schema, false);
    write!(
        w,
        r#"
// Generated from derivation register schema {schema}.
// Referenced from {scope}.
export type Register = "#,
        schema = relative_url(&derivation.register_schema, package_dir),
        scope = relative_url(&derivation.scope, package_dir),
    )
    .unwrap();

    reg_mapper
        .map(&derivation.register_schema)
        .render(&mut Context::new(&mut w));
    write!(w, ";\n\n").unwrap();

    // For each transform, export a ${transform}Source type of its source schema.
    // This is either a re-export of another collection Document,
    // or (if a source-schema is used) a generated type.
    for tables::Transform {
        scope,
        transform,
        spec:
            models::TransformDef {
                source:
                    models::TransformSource {
                        name: source_name, ..
                    },
                ..
            },
        ..
    } in Method::transforms(methods)
    {
        let source_export = format!("{}Source", camel_case(transform, true));

        if source_name == &derivation.derivation {
            write!(
                w,
                r#"
// Generated from self-referential transform {transform}.
// Referenced from {scope}."
export type {source_export} = SourceDocument;

"#,
                scope = relative_url(scope, package_dir),
                transform = transform.as_str(),
            )
            .unwrap();
        } else {
            write!(
                w,
                r#"
// Generated from transform {transform} as a re-export of collection {source}.
// Referenced from {scope}."
import {{ SourceDocument as {source_export} }} from "./{rel_path}";
export {{ SourceDocument as {source_export} }} from "./{rel_path}";

"#,
                rel_path = relative_path(&collection.collection, source_name),
                scope = relative_url(scope, package_dir),
                source = source_name.as_str(),
                transform = transform.as_str(),
            )
            .unwrap();
        }
    }

    write!(
        w,
        r#"
// Generated from derivation {scope}.
// Required to be implemented by {module}.
export interface IDerivation {{
"#,
        module = relative_url(typescript_module, package_dir),
        scope = relative_url(&derivation.scope, package_dir),
    )
    .unwrap();

    for method in methods {
        let signature = method.signature(false).into_iter().join("\n    ");
        writeln!(w, "    {};", signature).unwrap();
    }
    w.push_str("}\n");

    w
}

pub fn routes_ts<'a>(
    _package_dir: &path::Path,
    interfaces: impl Iterator<Item = &'a Interface<'a>> + Clone,
) -> String {
    let mut w = String::with_capacity(4096);
    w.push_str(ROUTES_HEADER);

    w.push_str("// Import derivation classes from their implementation modules.\n");
    for interface in interfaces.clone() {
        let import = &interface.module_import_path;
        // The ".ts" file suffix is implicit in TypeScript.
        let import = import.strip_suffix(".ts").unwrap_or(import);

        writeln!(
            w,
            "import {{ Derivation as {class} }} from '../../{import}';",
            class = camel_case(&interface.derivation.derivation, false),
        )
        .unwrap();
    }

    w.push_str(
        "\n// Build instances of each class, which will be bound to this module's router.\n",
    );
    for interface in interfaces.clone() {
        writeln!(
            w,
            "const __{class}: {class} = new {class}();",
            class = camel_case(&interface.derivation.derivation, false),
        )
        .unwrap();
    }

    w.push_str("\n// Now build the router that's used for transformation lambda dispatch.\n");
    w.push_str("const routes: { [path: string]: Lambda | undefined } = {\n");

    for interface in interfaces.clone() {
        let derivation: &str = &interface.derivation.derivation;
        let class = camel_case(derivation, false);

        for method in &interface.methods {
            writeln!(
                w,
                "    '/{group_name}/{mtype:?}': __{class}.{method}.bind(\n        __{class},\n    ) as Lambda,",
                group_name = crate::transform_group_name(&method.transform),
                mtype = method.type_,
                class = class,
                method = method.method_name(),
            )
            .unwrap();
        }
    }

    w.push_str("};\n\nexport { routes };\n");
    w
}

pub fn stubs_ts<'a>(
    package_dir: &path::Path,
    interfaces: impl Iterator<Item = &'a Interface<'a>> + Clone,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();

    for Interface {
        derivation,
        methods,
        module_is_relative,
        module_import_path,
        ..
    } in interfaces
    {
        if !module_is_relative {
            continue; // Skip stubs for non-relative modules.
        }
        let mut w = String::with_capacity(4096);

        let transform_sources = Method::transforms(methods)
            .iter()
            .map(|transform| format!("{}Source", camel_case(&transform.transform, true)))
            .join(", ");

        writeln!(
            w,
            "import {{ IDerivation, OutputDocument, Register, {transform_sources} }} from 'flow/{derivation}';",
            derivation = derivation.derivation.as_str(),
        )
        .unwrap();

        write!(
            w,
            r#"
// Implementation for derivation {scope}.
export class Derivation implements IDerivation {{
"#,
            scope = relative_url(&derivation.scope, package_dir),
        )
        .unwrap();

        for method in methods {
            let signature = method.signature(true).into_iter().join("\n    ");
            writeln!(w, "    {} {{", signature).unwrap();
            w.push_str("        throw new Error(\"Not implemented\");\n    }\n");
        }
        w.push_str("}\n");

        out.insert(module_import_path.clone(), w);
    }
    out
}

pub fn tsconfig_files<I, S>(files: I) -> String
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let files = files
        .map(|s| format!("../{}", s.as_ref()))
        .collect::<Vec<_>>();

    serde_json::to_string_pretty(&json!({
        "files": files,
    }))
    .unwrap()
}

const ROUTES_HEADER: &str = r#"
// Document is a relaxed signature for a Flow document of any kind.
export type Document = unknown;
// Lambda is a relaxed signature implemented by all Flow transformation lambdas.
export type Lambda = (source: Document, register?: Document, previous?: Document) => Document[];

"#;
