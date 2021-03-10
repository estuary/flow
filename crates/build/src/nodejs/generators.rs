use super::camel_case;
use super::interface::{Interface, Method, Module};
use super::typescript::Mapper;
use itertools::Itertools;
use models::tables;
use serde_json::json;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::path;

pub fn anchors_ts(
    package_dir: &path::Path,
    named_schemas: &[tables::NamedSchema],
    mapper: &Mapper,
) -> String {
    let mut w = String::with_capacity(4096);
    w.push_str(ANCHORS_HEADER);

    for tables::NamedSchema {
        // Use the non-#AnchorName schema URI, which coerces |mapper| to emit its full type.
        // If we used |anchor| instead, it would map to `anchors.AnchorName`.
        scope: schema,
        anchor: _,
        anchor_name,
    } in named_schemas.iter().sorted_by_key(|n| &n.anchor_name)
    {
        let schema = Module::new(schema, package_dir);

        writeln!(w, "\n// Generated from {}.", schema.relative_url()).unwrap();
        write!(w, "export type {} = ", anchor_name).unwrap();
        mapper.map(schema.absolute_url()).render(0, &mut w);
        write!(w, ";\n").unwrap();
    }

    w
}

pub fn collections_ts(
    package_dir: &path::Path,
    collections: &[tables::Collection],
    mapper: &Mapper,
) -> String {
    let mut w = String::with_capacity(4096);
    w.push_str(SCHEMAS_HEADER);

    for tables::Collection {
        collection,
        schema,
        scope,
        ..
    } in collections.iter().sorted_by_key(|c| &c.collection)
    {
        let schema = Module::new(schema, package_dir);
        let scope = Module::new(scope, package_dir);

        writeln!(w, "\n// Generated from {}.", schema.relative_url()).unwrap();
        writeln!(w, "// Referenced as schema of {}.", scope.relative_url()).unwrap();

        write!(w, "export type {} = ", camel_case(&collection, true)).unwrap();
        mapper.map(schema.absolute_url()).render(0, &mut w);
        write!(w, ";\n").unwrap();
    }

    w
}

pub fn registers_ts(
    package_dir: &path::Path,
    derivations: &[tables::Derivation],
    mapper: &Mapper,
) -> String {
    let mut w = String::with_capacity(4096);
    w.push_str(SCHEMAS_HEADER);

    for tables::Derivation {
        scope,
        derivation,
        register_schema: schema,
        ..
    } in derivations.iter().sorted_by_key(|d| &d.derivation)
    {
        let schema = Module::new(schema, package_dir);
        let scope = Module::new(scope, package_dir);

        writeln!(w, "\n// Generated from {}.", schema.relative_url()).unwrap();
        writeln!(
            w,
            "// Referenced as register_schema of {}.",
            scope.relative_url()
        )
        .unwrap();

        write!(w, "export type {} = ", camel_case(&derivation, true)).unwrap();
        mapper.map(schema.absolute_url()).render(0, &mut w);
        write!(w, ";\n").unwrap();
    }

    w
}

pub fn transforms_ts(
    package_dir: &path::Path,
    interfaces: &[Interface<'_>],
    mapper: &Mapper,
) -> String {
    let mut w = String::with_capacity(4096);
    w.push_str(SCHEMAS_HEADER);

    // Collect transforms having TypeScript methods and using alternate source schemas.
    // These are the transforms for which we must generate source schemas.
    // Such transforms may have multiple methods, so index to de-duplicate.
    let mut transforms_with_source_schemas = BTreeMap::new();

    for iface in interfaces.iter() {
        for Method { transform, .. } in iface.methods.iter() {
            if transform.source_schema.is_some() {
                transforms_with_source_schemas
                    .insert((&transform.derivation, &transform.transform), transform);
            }
        }
    }

    for (
        _,
        tables::Transform {
            source_schema,
            scope,
            derivation,
            transform,
            ..
        },
    ) in transforms_with_source_schemas.into_iter()
    {
        let schema = Module::new(source_schema.as_ref().unwrap(), package_dir);
        let scope = Module::new(scope, package_dir);

        writeln!(w, "\n// Generated from {}.", schema.relative_url()).unwrap();
        writeln!(
            w,
            "// Referenced as source schema of transform {}.",
            scope.relative_url()
        )
        .unwrap();

        write!(
            w,
            "export type {}{}Source = ",
            camel_case(derivation, true),
            camel_case(transform, false),
        )
        .unwrap();
        mapper.map(schema.absolute_url()).render(0, &mut w);
        write!(w, ";\n").unwrap();
    }

    w
}

pub fn interfaces_ts(package_dir: &path::Path, interfaces: &[Interface<'_>]) -> String {
    let mut w = String::with_capacity(4096);
    w.push_str(INTERFACES_HEADER);

    for Interface {
        derivation: tables::Derivation {
            derivation, scope, ..
        },
        module,
        methods,
    } in interfaces.iter()
    {
        let scope = Module::new(scope, package_dir);

        write!(
            w,
            r#"
// Generated from derivation {scope}.
// Required to be implemented by {module}.
export interface {name} {{
"#,
            module = module.relative_url(),
            name = camel_case(derivation, true),
            scope = scope.relative_url(),
        )
        .unwrap();

        for method in methods {
            let signature = method.signature().into_iter().join("\n    ");
            writeln!(w, "    {};", signature).unwrap();
        }
        w.push_str("}\n");
    }

    w
}

pub fn routes_ts(_package_dir: &path::Path, interfaces: &[Interface<'_>]) -> String {
    let mut w = String::with_capacity(4096);
    w.push_str(ROUTES_HEADER);

    w.push_str("// Import derivation classes from their implementation modules.\n");
    for (_, interfaces) in interfaces
        .iter()
        .sorted_by_key(|i| i.module.absolute_url())
        .group_by(|i| i.module.absolute_url())
        .into_iter()
    {
        let interfaces: Vec<&Interface<'_>> = interfaces.collect();

        let import = interfaces[0].module.relative_path();
        // The ".ts" file suffix is implicit in TypeScript.
        let import = import.strip_suffix(".ts").unwrap_or(&import);

        writeln!(w, "import {{").unwrap();
        for name in interfaces
            .into_iter()
            .map(|i| camel_case(&i.derivation.derivation, true))
        {
            writeln!(w, "    {},", name).unwrap();
        }
        writeln!(w, "}} from '../../{}';\n", import).unwrap();
    }

    w.push_str("// Build instances of each class, which will be bound to this module's router.\n");
    for interface in interfaces {
        writeln!(
            w,
            "let __{class}: interfaces.{class} = new {class}();",
            class = camel_case(&interface.derivation.derivation, true)
        )
        .unwrap();
    }

    w.push_str("\n// Now build the router that's used for transformation lambda dispatch.\n");
    w.push_str("let routes: { [path: string]: Lambda | undefined } = {\n");

    for interface in interfaces {
        let derivation: &str = &interface.derivation.derivation;
        let class = camel_case(derivation, true);

        for method in &interface.methods {
            writeln!(
                w,
                "    '/{group_name}/{type:?}': __{class}.{method}.bind(\n        __{class},\n    ) as Lambda,",
                group_name = method.transform.group_name(),
                type = method.type_,
                class = class,
                method = method.type_.method_name(&method.transform.transform),
            )
            .unwrap();
        }
    }

    w.push_str("};\n\nexport { routes };\n");
    w
}

pub fn stubs_ts(
    package_dir: &path::Path,
    interfaces: &[Interface<'_>],
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();

    for (relative_path, interfaces) in interfaces
        .iter()
        .filter(|i| i.module.is_relative())
        .sorted_by_key(|i| i.module.relative_path())
        .group_by(|i| i.module.relative_path())
        .into_iter()
    {
        let mut w = String::with_capacity(4096);
        w.push_str(STUBS_HEADER);

        for Interface {
            derivation: tables::Derivation {
                derivation, scope, ..
            },
            module: _,
            methods,
        } in interfaces
        {
            let scope = Module::new(scope, package_dir);

            write!(
                w,
                r#"
// Implementation for derivation {scope}.
export class {name} implements interfaces.{name} {{
"#,
                name = camel_case(derivation, true),
                scope = scope.relative_url(),
            )
            .unwrap();

            for method in methods {
                let signature = method.signature().into_iter().join("\n    ");
                writeln!(w, "    {} {{", signature).unwrap();
                w.push_str("        throw new Error(\"Not implemented\");\n    }\n");
            }
            w.push_str("}\n");
        }

        out.insert(relative_path, w);
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

const ANCHORS_HEADER: &str = r#"// Ensure module has at least one export, even if otherwise empty.
export type __module = null;
"#;

const SCHEMAS_HEADER: &str = r#"import * as anchors from './anchors';

// "Use" imported modules, even if they're empty, to satisfy compiler and linting.
export type __module = null;
export type __anchors_module = anchors.__module;
"#;

const INTERFACES_HEADER: &str = r#"import * as collections from './collections';
import * as registers from './registers';
import * as transforms from './transforms';

// "Use" imported modules, even if they're empty, to satisfy compiler and linting.
export type __module = null;
export type __collections_module = collections.__module;
export type __registers_module = registers.__module;
export type __transforms_module = transforms.__module;
"#;

const ROUTES_HEADER: &str = r#"import * as interfaces from './interfaces';

// Document is a relaxed signature for a Flow document of any kind.
export type Document = unknown;
// Lambda is a relaxed signature implemented by all Flow transformation lambdas.
export type Lambda = (source: Document, register?: Document, previous?: Document) => Document[];

// "Use" imported modules, even if they're empty, to satisfy compiler and linting.
export type __interfaces_module = interfaces.__module;
"#;

const STUBS_HEADER: &str = "import { collections, interfaces, registers } from 'flow/modules';\n";
