use crate::catalog::{Resource, ContentType, Error};
use crate::doc::{Schema, SchemaIndex};
use super::typescript;
use estuary_json::schema::build::build_schema;
use rusqlite::{params as sql_params, Connection as DB};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::iter::Iterator;
use std::path;
use std::process::Command;
use url::Url;

pub fn build_package(db: &DB, pkg: &path::Path) -> Result<(), Error> {
    // TODO(johnny): If package.json doesn't exist, scaffold out from "template".
    patch_package_json(db, pkg)?;
    generate_collections_ts(db, pkg)?;
    generate_lambdas_ts(db, pkg)?;

    npm_cmd(pkg, &["install"])?;
    npm_cmd(pkg, &["run", "compile"])?;
    npm_cmd(pkg, &["run", "lint"])?;
    npm_cmd(pkg, &["pack"])?;

    let pack = pkg.join("catalog-js-transformer-0.1.0.tgz");
    let pack = Url::from_file_path(pack).unwrap();
    Resource::register(db, ContentType::NpmPack, &pack)?;

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

fn generate_collections_ts(db: &DB, pkg: &path::Path) -> Result<(), Error> {
    // Load and compile all schemas in the catalog.
    let mut stmt = db.prepare(
        "SELECT url, content FROM resources NATURAL JOIN resource_urls
                WHERE content_type = ? AND is_primary;",
    )?;
    let mut rows = stmt.query(sql_params![ContentType::Schema])?;

    let mut schemas = Vec::new();
    while let Some(row) = rows.next()? {
        let (url, blob): (Url, Vec<u8>) = (row.get(0)?, row.get(1)?);
        let dom: Value = serde_yaml::from_slice(&blob)?;
        let compiled: Schema = build_schema(url, &dom)?;
        schemas.push(compiled);
    }

    // Index all Schemas.
    let mut index = SchemaIndex::new();
    for schema in &schemas {
        index.add(&schema)?;
    }

    // TODO(johnny): _Maybe_ some schemas are hoisted out based on a keyword?
    let mapper = typescript::mapping::Mapper {
        index: &index,
        top_level: &BTreeMap::new(),
    };

    // Generate a named type for each schema used by a collection.
    let mut stmt = db.prepare("SELECT name, schema_uri, is_alternate FROM collection_schemas")?;
    let mut rows = stmt.query(sql_params![])?;

    let p = pkg.join("src/catalog/collections.ts");
    let mut w = std::io::BufWriter::new(std::fs::File::create(&p)?);

    while let Some(r) = rows.next()? {
        let (name, schema_url, is_alt): (String, Url, bool) = (r.get(0)?, r.get(1)?, r.get(2)?);

        write!(
            w,
            "// Generated from {:?} @ {:?}\n",
            name,
            schema_url.as_str()
        )?;
        write!(w, "type {} = ", ts_name(&name, &schema_url, is_alt))?;
        let scm = index.must_fetch(&schema_url)?;
        let ast = mapper.map(scm);
        let ast = ast.optimize();

        let mut out = Vec::new();
        ast.render(&mut out);
        w.write(&out)?;
        write!(w, ";\n\n")?;
    }

    Ok(())
}

fn generate_lambdas_ts(db: &DB, pkg: &path::Path) -> Result<(), Error> {
    let p = pkg.join("src/catalog/lambdas.ts");
    let mut w = std::io::BufWriter::new(std::fs::File::create(&p)?);

    let header = r#"
/*eslint @typescript-eslint/no-unused-vars: ["error", { "argsIgnorePattern": "^store$" }]*/
/*eslint @typescript-eslint/require-await: "off"*/

import './collections';
import {Store} from '../runtime/store';
import {BootstrapMap, TransformMap} from '../runtime/types';
    "#;
    w.write(header.as_bytes())?;

    // Write out dynamic imports, drawn from dependencies configured in the catalog.
    let mut stmt = db.prepare("SELECT package FROM nodejs_dependencies;")?;
    let mut rows = stmt.query(sql_params![])?;
    while let Some(row) = rows.next()? {
        let pkg: String = row.get(0)?;
        write!(w, "import * as {} from '{}';\n", pkg, pkg)?;
    }
    write!(w, "\n\n")?;

    // Write out bootstraps lambdas.
    let mut stmt = db.prepare(
        "SELECT derivation_id, JSON_GROUP_ARRAY(inline) AS expressions
                FROM lambdas NATURAL JOIN bootstraps WHERE runtime = 'nodeJS';",
    )?;
    let mut rows = stmt.query(sql_params![])?;

    write!(w, "export const bootstraps: BootstrapMap = {{\n")?;
    while let Some(row) = rows.next()? {
        let (id, expressions): (i64, Value) = (row.get(0)?, row.get(1)?);
        let expressions: Vec<String> = serde_json::from_value(expressions)?;
        let expressions = expressions
            .into_iter()
            .map(|e| format!("async (store: Store) : Promise<void> => {{ {} }}", e))
            .collect::<Vec<String>>()
            .join(", ");
        write!(w, "\t{}: [{}],\n", id, expressions)?;
    }
    write!(w, "}};\n\n")?;

    // Write out transforms.
    let mut stmt = db.prepare(
        "SELECT
                    transform_id,          -- 0
                    source_name,           -- 1
                    source_schema_uri,     -- 2
                    derivation_name,       -- 3
                    derivation_schema_uri, -- 4
                    is_alt_source_schema,  -- 5
                    lambda_inline          -- 6
            FROM transform_details
                WHERE lambda_runtime = 'nodeJS';",
    )?;
    let mut rows = stmt.query(sql_params![])?;

    write!(w, "export const transforms : TransformMap = {{\n")?;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let (src_name, src_uri): (String, Url) = (row.get(1)?, row.get(2)?);
        let (der_name, der_uri): (String, Url) = (row.get(3)?, row.get(4)?);
        let (is_alt, body): (bool, String) = (row.get(5)?, row.get(6)?);

        write!(
            w,
            "\t{}: async (doc: {}, store: Store) : Promise<{}[] | void> => {{ {} }},\n",
            id,
            ts_name(&src_name, &src_uri, is_alt),
            ts_name(&der_name, &der_uri, false),
            body
        )?;
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

    let status = cmd.spawn()?.wait()?;

    if !status.success() {
        Err(Error::SubprocessFailed {
            process: pkg.to_owned(),
            status,
        })
    } else {
        Ok(())
    }
}
