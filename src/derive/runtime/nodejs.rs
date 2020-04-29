use crate::catalog::typescript;
use crate::catalog::{ContentType, Error};
use crate::doc::{Schema, SchemaIndex};
use estuary_json::schema::build::build_schema;
use rusqlite::{params as sql_params, Connection as DB};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::Write;
use std::path;
use std::process::Command;
use url::Url;
use std::iter::Iterator;

pub struct Config {
    pub tsc_path: path::PathBuf,
    pub prettier_path: path::PathBuf,
    pub npm_path: path::PathBuf,
    pub node_path: path::PathBuf,
    pub pkg_dir: path::PathBuf,
}

fn prettify(path: &path::Path, cfg: &Config) -> Result<(), Error> {
    let status = Command::new(&cfg.prettier_path)
        .arg("--write") // Prettify in-place.
        .arg("--no-config")
        .arg(path)
        .spawn()?
        .wait()?;

    if !status.success() {
        Err(Error::SubprocessFailed {
            process: cfg.prettier_path.to_owned(),
            status,
        })
    } else {
        Ok(())
    }
}

fn npm_install(cfg: &Config) -> Result<(), Error> {
    let status = Command::new(&cfg.npm_path)
        .arg("install")
        .current_dir(&cfg.pkg_dir)
        .spawn()?
        .wait()?;

    if !status.success() {
        Err(Error::SubprocessFailed {
            process: cfg.npm_path.to_owned(),
            status,
        })
    } else {
        Ok(())
    }
}

fn tsc_compile(cfg: &Config) -> Result<(), Error> {
    let status = Command::new(&cfg.tsc_path)
        .arg("--build")
        .arg(&cfg.pkg_dir)
        .spawn()?
        .wait()?;

    if !status.success() {
        Err(Error::SubprocessFailed {
            process: cfg.tsc_path.to_owned(),
            status,
        })
    } else {
        Ok(())
    }
}

fn npm_pack(db: &DB, cfg: &Config) -> Result<(), Error> {
    let status = Command::new(&cfg.npm_path)
        .arg("pack")
        .current_dir(&cfg.pkg_dir)
        .spawn()?
        .wait()?;

    if !status.success() {
        return Err(Error::SubprocessFailed {
            process: cfg.npm_path.to_owned(),
            status,
        })
    }

    let pack = cfg.pkg_dir.join("catalog-js-transforms-0.1.0.tgz");
    let pack = Url::from_file_path(pack).unwrap();

    crate::catalog::Resource::register(db, ContentType::NpmPack, &pack)?;
    Ok(())
}

pub fn build_nodejs_package(db: &DB, cfg: Config) -> Result<(), Error> {
    let src_dir = cfg.pkg_dir.join("src");
    std::fs::create_dir_all(&src_dir)?;

    // Write index.ts.
    let p = src_dir.join("index.ts");
    let mut w = std::io::BufWriter::new(std::fs::File::create(&p)?);
    write!(w, "#!/usr/bin/env node\n")?;
    generate_typescript_types(db, &mut w)?;
    generate_imports(db, &mut w)?;
    generate_bootstraps(db, &mut w)?;
    generate_transforms(db, &mut w)?;
    write!(w, "estuary.main(bootstraps, transforms);\n")?;
    drop(w);
    prettify(&p, &cfg)?;

    // Write package.json.
    let p = cfg.pkg_dir.join("package.json");
    let mut w = std::io::BufWriter::new(std::fs::File::create(&p)?);
    generate_package_json(db, &mut w)?;
    drop(w);

    // Write tsconfig.json.
    let p = cfg.pkg_dir.join("tsconfig.json");
    let mut w = std::io::BufWriter::new(std::fs::File::create(&p)?);
    generate_tsconfig(&mut w)?;
    drop(w);

    // Write .eslintrc
    let p = cfg.pkg_dir.join(".eslintrc");
    let mut w = std::io::BufWriter::new(std::fs::File::create(&p)?);
    generate_eslintrc(&mut w)?;
    drop(w);

    npm_install(&cfg)?;
    tsc_compile(&cfg)?;
    npm_pack(db, &cfg)?;

    Ok(())
}

fn generate_eslintrc(w: impl Write) -> Result<(), Error> {
    serde_json::to_writer_pretty(w, &json!({
        "parser": "@typescript-eslint/parser", // Specifies the ESLint parser
        "parserOptions": {
            "ecmaVersion": 2018, // Allows for the parsing of modern ECMAScript features
            "sourceType": "module" // Allows for the use of imports
        },
        "extends": [
            "plugin:@typescript-eslint/recommended" // Uses the recommended rules from the @typescript-eslint/eslint-plugin
        ],
        "rules": {
            // Place to specify ESLint rules. Can be used to overwrite rules specified from the extended configs
            // e.g. "@typescript-eslint/explicit-function-return-type": "off",
        }
    }))?;
    Ok(())
}

fn generate_package_json(db: &DB, w: impl Write) -> Result<(), Error> {
    // Build NPM dependencies map of package => version.
    let mut deps = serde_json::Map::new();
    let mut stmt = db.prepare("SELECT package, version FROM nodejs_dependencies")?;
    let mut rows = stmt.query(sql_params![])?;

    while let Some(row) = rows.next()? {
        let (pkg, version): (String, String) = (row.get(0)?, row.get(1)?);
        deps.insert(pkg, Value::String(version));
    }
    deps.insert("estuary_runtime".to_owned(), Value::String(
        "file:///home/johnny/estuary/src/derive/runtime/nodejs/estuary_runtime-0.1.0.tgz".to_owned()));

    serde_json::to_writer_pretty(w, &json!({
        "name": "catalog-js-transforms",
        "version": "0.1.0",
        "description": "NodeJS runtime of Estuary catalog transform lambdas",
        "files": [ "build/src" ],
        "engines": { "node": ">=10.10" },
        "enginesStrict": true,
        "bin": "./build/src/index.js",
        "dependencies": deps,
        "bundleDependencies": Value::Array(deps.keys().cloned().map(|k| Value::String(k)).collect()),
    }))?;

    Ok(())
}

fn generate_tsconfig(w: impl Write) -> Result<(), Error> {
    serde_json::to_writer_pretty(w, &json!({
        "include": [ "src/**/*.ts" ],
        "compilerOptions": {
            "rootDir": ".",
            "outDir": "build",
            "module": "commonjs",
            "target": "es2018",
            "strict": true,
        }
    }))?;

    Ok(())
}

fn generate_imports(db: &DB, mut w: impl Write) -> Result<(), Error> {
    let mut stmt = db.prepare("SELECT package FROM nodejs_dependencies")?;
    let mut rows = stmt.query(sql_params![])?;
    while let Some(row) = rows.next()? {
        let pkg: String = row.get(0)?;
        write!(w, "import * as {} from '{}';\n", pkg, pkg)?;
    }
    write!(w, "import * as estuary from 'estuary_runtime';\n")?;
    write!(w, "\n\n")?;
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

fn generate_typescript_types(db: &DB, mut w: impl Write) -> Result<(), Error> {
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

fn generate_bootstraps(db: &DB, mut w: impl Write) -> Result<(), Error> {
    // Write out bootstraps.
    let mut stmt = db.prepare(
        "SELECT derivation_id, JSON_GROUP_ARRAY(inline) AS expressions
                FROM lambdas NATURAL JOIN bootstraps WHERE runtime = 'nodeJS';",
    )?;
    let mut rows = stmt.query(sql_params![])?;

    write!(w, "let bootstraps : estuary.BootstrapMap = {{\n")?;
    while let Some(row) = rows.next()? {
        let (id, expressions): (i64, Value) = (row.get(0)?, row.get(1)?);
        let expressions: Vec<String> = serde_json::from_value(expressions)?;
        let expressions = expressions
            .into_iter()
            .map(|e| {
                format!(
                    "async (state: estuary.StateStore) : Promise<void> => {{ {} }}",
                    e
                )
            })
            .collect::<Vec<String>>()
            .join(", ");
        write!(w, "\t{}: [{}],\n", id, expressions)?;
    }
    write!(w, "}};\n\n")?;

    Ok(())
}

fn generate_transforms(db: &DB, mut w: impl Write) -> Result<(), Error> {
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

    write!(w, "let transforms : estuary.TransformMap = {{\n")?;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let (src_name, src_uri): (String, Url) = (row.get(1)?, row.get(2)?);
        let (der_name, der_uri): (String, Url) = (row.get(3)?, row.get(4)?);
        let (is_alt, body): (bool, String) = (row.get(5)?, row.get(6)?);

        write!(w, "\t{}: async (doc: {}, state: estuary.StateStore) : Promise<{}[] | void> => {{ {} }},\n",
               id,
               ts_name(&src_name, &src_uri, is_alt),
               ts_name(&der_name, &der_uri, false),
               body)?;
    }
    write!(w, "}};\n\n")?;

    Ok(())
}
