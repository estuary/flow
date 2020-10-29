use super::typescript;
use crate::catalog::{self, ContentType, Error};
use crate::doc::SchemaIndex;
use include_dir::{include_dir, Dir};
use rusqlite::{params as sql_params, Connection as DB};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::iter::Iterator;
use std::path;
use std::process::Command;
use url::Url;

static TEMPLATE_ROOT: Dir = include_dir!("catalog-js-transformer-template");

pub fn write_package_template(dir: &Dir, into: &path::Path) -> Result<(), Error> {
    fs::create_dir_all(into.join(dir.path))?;

    for file in dir.files() {
        let into = into.join(file.path);
        fs::write(&into, file.contents())?;
        log::info!("writing NodeJS package template {:?}", into);
    }
    for dir in dir.dirs() {
        write_package_template(dir, into)?;
    }
    Ok(())
}

pub fn build_package(db: &DB, pkg: &path::Path) -> Result<(), Error> {
    // Install or clobber package template files.
    write_package_template(&TEMPLATE_ROOT, pkg)?;

    patch_package_json(db, pkg)?;
    generate_schemas_ts(db, pkg)?;
    generate_lambdas_ts(db, pkg)?;

    npm_cmd(pkg, &["install", "--no-audit", "--no-fund"])?;
    npm_cmd(pkg, &["run", "prettify-generated"])?;
    npm_cmd(pkg, &["run", "compile"])?;
    npm_cmd(pkg, &["run", "lint"])?;
    npm_cmd(pkg, &["pack"])?;

    let pack = pkg.join("catalog-js-transformer-0.1.0.tgz");
    let pack = fs::canonicalize(&pack)?;

    let pack = Url::from_file_path(&pack).unwrap();
    catalog::Resource::register(db, ContentType::NpmPack, &pack)?;
    log::info!("built NodeJS pack {:?}", pack);

    Ok(())
}

// Models the bits of the "package.json" file we care about patching,
// and passes through everything else.
#[derive(Serialize, Deserialize)]
struct PackageJson {
    #[serde(default)]
    dependencies: BTreeMap<String, String>,
    #[serde(default, rename = "bundledDependencies", alias = "bundleDependencies")]
    bundled_dependencies: BTreeSet<String>,
    #[serde(flatten)]
    pass_through: BTreeMap<String, Value>,
}

fn patch_package_json(db: &DB, pkg: &path::Path) -> Result<(), Error> {
    // Read current package.json.
    let path = pkg.join("package.json");
    let mut dom: PackageJson = serde_json::from_slice(&std::fs::read(&path)?)?;

    let mut stmt = db.prepare("SELECT package, version FROM nodejs_dependencies")?;
    let mut rows = stmt.query(sql_params![])?;

    // Update with catalog dependencies and versions, if not already present.
    // Further mark that dependencies should be bundled at run-time.
    while let Some(row) = rows.next()? {
        let (pkg, version): (String, String) = (row.get(0)?, row.get(1)?);

        if !dom.dependencies.contains_key(&pkg) {
            dom.dependencies.insert(pkg.clone(), version);
        }
        dom.bundled_dependencies.insert(pkg);
    }

    // Write back out again.
    serde_json::to_writer_pretty(std::fs::File::create(&path)?, &dom)?;
    Ok(())
}

fn generate_schemas_ts(db: &DB, pkg: &path::Path) -> Result<(), Error> {
    let schemas = catalog::Schema::compile_all(db)?;

    // Index all Schemas.
    let mut index = SchemaIndex::new();
    for schema in &schemas {
        index.add(&schema)?;
    }

    // Note that the set of allowed characters in an `$anchor` is quite limited,
    // by Sec 8.2.3. We further restrict to anchors which start with a capital
    // letter and include only '_' as punctuation.
    // See: https://json-schema.org/draft/2019-09/json-schema-core.html#anchor
    let re = regex::Regex::new("^[A-Z][\\w_]+$").unwrap();

    let mut anchors = BTreeSet::new();
    let mut anchor_index = BTreeMap::new();

    // Walk |index| looking for URIs which conform to valid anchors, per |re| above.
    // These will be hoisted out into top-level types, and will be used by reference.
    for (uri, _) in index.iter() {
        if let Some(anchor) = uri.split('#').next_back().filter(|s| re.is_match(s)) {
            if anchors.insert(anchor) {
                anchor_index.insert(*uri, anchor);
            }
        }
    }

    let mapper = typescript::mapping::Mapper {
        index: &index,
        top_level: &anchor_index,
    };

    let header = br#"
/* eslint @typescript-eslint/no-unused-vars: ["error", { "argsIgnorePattern": "^_.*" }] */
/* eslint @typescript-eslint/no-explicit-any: "off" */

// Ensure module has at least one export, even if otherwise empty.
export const _module = null;

    "#;

    // Generate anchors.ts.
    let anchors_w = std::fs::File::create(&pkg.join("src/catalog/anchors.ts"))?;
    let mut anchors_w = std::io::BufWriter::new(anchors_w);
    anchors_w.write_all(header)?;

    for (uri, schema) in index.iter() {
        if let Some(anchor) = anchor_index.get(uri) {
            writeln!(anchors_w, "// Generated from {:?}", uri)?;
            write!(anchors_w, "export type {} = ", anchor)?;

            let ast = mapper.map(schema);

            let mut out = Vec::new();
            ast.render(&mut out);
            anchors_w.write_all(&out)?;
            write!(anchors_w, ";\n\n")?;
        }
    }

    let generate_each = |p, query| -> Result<(), Error> {
        let mut w = std::io::BufWriter::new(std::fs::File::create(&p)?);

        w.write_all(header)?;
        w.write_all(
            br#"
import * as anchors from './anchors';

// Artificial use of anchors, to satisfy the compiler & linting even if they're empty.
((_) : null => null)(anchors._module);

"#,
        )?;

        let mut stmt = db.prepare(query)?;
        let mut rows = stmt.query(sql_params![])?;

        while let Some(r) = rows.next()? {
            let (name, schema_url, is_alt): (String, Url, bool) = (r.get(0)?, r.get(1)?, r.get(2)?);

            writeln!(
                w,
                "// Generated from {:?} @ {:?}",
                name,
                schema_url.as_str()
            )?;
            write!(w, "export type {} = ", ts_name(&name, &schema_url, is_alt))?;
            let scm = index.must_fetch(&schema_url)?;
            let ast = mapper.map(scm);

            let mut out = Vec::new();
            ast.render(&mut out);
            w.write_all(&out)?;
            write!(w, ";\n\n")?;
        }
        Ok(())
    };

    // Generate a named type for each schema used by a collection.
    generate_each(
        pkg.join("src/catalog/collections.ts"),
        "SELECT collection_name, schema_uri, is_alternate FROM collection_schemas",
    )?;
    // Generate a named type for each register used by a derivation.
    generate_each(
        pkg.join("src/catalog/registers.ts"),
        "SELECT collection_name, register_schema_uri, FALSE FROM collections NATURAL JOIN derivations",
    )?;

    Ok(())
}

fn generate_lambdas_ts(db: &DB, pkg: &path::Path) -> Result<(), Error> {
    let p = pkg.join("src/catalog/lambdas.ts");
    let mut w = std::io::BufWriter::new(std::fs::File::create(&p)?);

    let header = r#"
/* eslint @typescript-eslint/no-unused-vars: ["error", { "argsIgnorePattern": "^register$|^previous$|^_.*" }] */
/* eslint @typescript-eslint/require-await: "off" */

import * as anchors from './anchors';
import * as collections from './collections';
import * as registers from './registers';
import {BootstrapMap, TransformMap} from '../runtime/types';

// Artificial uses of schema modules, to satisfy the compiler & linting even if they're empty.
((_) : null => null)(anchors._module);
((_) : null => null)(collections._module);
((_) : null => null)(registers._module);
    "#;
    w.write_all(header.as_bytes())?;

    // Write out dynamic imports, drawn from dependencies configured in the catalog.
    let mut stmt = db.prepare("SELECT package FROM nodejs_dependencies;")?;
    let mut rows = stmt.query(sql_params![])?;
    while let Some(row) = rows.next()? {
        let pkg: String = row.get(0)?;
        writeln!(w, "import * as {} from '{}';", pkg, pkg)?;
    }
    write!(w, "\n\n")?;

    // Write out bootstraps lambdas.
    let mut stmt = db.prepare(
        "
            SELECT
                derivation_id,
                JSON_GROUP_ARRAY(inline) AS expressions
            FROM lambdas
            NATURAL JOIN bootstraps
            WHERE runtime = 'nodeJS'
            GROUP BY derivation_id;",
    )?;
    let mut rows = stmt.query(sql_params![])?;

    writeln!(w, "export const bootstraps: BootstrapMap = {{")?;
    while let Some(row) = rows.next()? {
        let (id, expressions): (i64, Value) = (row.get(0)?, row.get(1)?);
        let expressions: Vec<String> = serde_json::from_value(expressions)?;
        let expressions = expressions
            .into_iter()
            .map(|e| format!("async () : Promise<void> => {{ {} }}", e))
            .collect::<Vec<String>>()
            .join(", ");
        writeln!(w, "\t{}: [{}],", id, expressions)?;
    }
    write!(w, "}};\n\n")?;

    // Write out update lambdas.
    let mut stmt = db.prepare(
        "
        SELECT
            transform_id,          -- 0
            transform_name,        -- 1
            register_schema_uri,   -- 2
            source_name,           -- 3
            source_schema_uri,     -- 4
            is_alt_source_schema,  -- 5
            derivation_name,       -- 6
            derivation_schema_uri, -- 7
            CASE WHEN  update_runtime = 'nodeJS' THEN update_inline  ELSE NULL END, -- 8
            CASE WHEN publish_runtime = 'nodeJS' THEN publish_inline ELSE NULL END  -- 9
        FROM transform_details
        ;",
    )?;
    let mut rows = stmt.query(sql_params![])?;

    writeln!(w, "export const transforms : TransformMap = {{")?;
    while let Some(row) = rows.next()? {
        let (id, name, reg_uri): (i64, String, Url) = (row.get(0)?, row.get(1)?, row.get(2)?);
        let (src_name, src_uri, is_alt): (String, Url, bool) =
            (row.get(3)?, row.get(4)?, row.get(5)?);
        let (der_name, der_uri): (String, Url) = (row.get(6)?, row.get(7)?);
        let (update, publish): (Option<String>, Option<String>) = (row.get(8)?, row.get(9)?);

        writeln!(w, "// Derivation {:?}, transform {:?}.", der_name, name)?;
        writeln!(w, "{}: {{", id)?;

        if let Some(update) = update {
            writeln!(
                    w,
                    "update: async (source: collections.{src}) : Promise<registers.{reg}[]> => {{ {body} }},",
                    src = ts_name(&src_name, &src_uri, is_alt),
                    reg = ts_name(&der_name, &reg_uri, false),
                    body = update,
                )?;
        }
        if let Some(publish) = publish {
            writeln!(
                w,
                "publish: async (source: collections.{src}, previous: registers.{reg}, register: registers.{reg}) : Promise<collections.{der}[]> => {{ {publish} }},",
                src = ts_name(&src_name, &src_uri, is_alt),
                reg = ts_name(&der_name, &reg_uri, false),
                der = ts_name(&der_name, &der_uri, false),
                publish = publish,
            )?;
        }
        writeln!(w, "}},")?;
    }
    write!(w, "}};\n\n")?;

    Ok(())
}

// Convert collection to a camel-case typescript token by dropping non-alphanumeric components.
// If |is_alternate|, as stable hex-encoded hash of the |schema| Url is appended.
// Eg, "company/marketing/clicks" becomes "CompanyMarketingClicks", or
// "CompanyMarketingClicks_5a89cd23" if it's an alternate source schema.
fn ts_name(collection: &str, schema: &Url, is_alternate: bool) -> String {
    let mut out = String::new();
    let mut upper = true;

    for c in collection.chars() {
        if !c.is_alphanumeric() {
            upper = true
        } else if upper {
            out.extend(c.to_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    if is_alternate {
        out = format!("{}_{:x}", out, fxhash::hash32(schema));
    }
    out
}

fn npm_cmd(pkg: &path::Path, args: &[&str]) -> Result<(), Error> {
    let mut cmd = Command::new("npm");

    for &arg in args.iter() {
        cmd.arg(arg);
    }
    cmd.current_dir(pkg);

    let status = cmd
        .spawn()
        .and_then(|mut c| c.wait())
        .map_err(|e| Error::At {
            loc: "'npm' subprocess invocation".to_owned(),
            detail: Box::new(e.into()),
        })?;

    if !status.success() {
        Err(Error::SubprocessFailed {
            process: pkg.to_owned(),
            status,
        })
    } else {
        Ok(())
    }
}
