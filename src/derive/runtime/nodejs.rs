use crate::catalog::typescript;
use crate::catalog::{ContentType, Error};
use crate::doc::{Schema, SchemaIndex};
use estuary_json::schema::build::build_schema;
use rusqlite::{params as sql_params, Connection as DB};
use serde_json::Value;
use url::Url;
use std::collections::BTreeMap;
use std::io::Write;

pub fn generate_imports(db: &DB, mut w: impl Write) -> Result<(), Error> {
    write!(w, "import * as estuary from 'estuary_runtime';\n")?;

    let mut stmt = db.prepare("SELECT package FROM nodejs_dependencies")?;
    let mut rows = stmt.query(sql_params![])?;
    while let Some(row) = rows.next()? {
        let pkg : String = row.get(0)?;
        write!(w, "import * as {} from '{}';\n", pkg, pkg)?;
    }
    write!(w, "\n\n")?;
    Ok(())
}

// Convert collection to a camel-case typescript token by dropping non-alphanumeric components.
// If |is_alternate|, as stable hex-encoded hash of the |schema| Url is appended.
// Eg, "company/marketing/clicks" becomes "CompanyMarketingClicks", or
// "CompanyMarketingClicks_5a89cd23" if it's an alternate source schema.
pub fn ts_name(collection: &str, schema: &Url, is_alternate: bool) -> String {
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

pub fn generate_typescript_types(db: &DB, mut w: impl Write) -> Result<(), Error> {
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
    let mut stmt = db.prepare(
        "SELECT name, schema_uri, is_alternate FROM collection_schemas")?;
    let mut rows = stmt.query(sql_params![])?;

    while let Some(r) = rows.next()? {
        let (name, schema_url, is_alt) : (String, Url, bool) = (r.get(0)?, r.get(1)?, r.get(2)?);

        write!(w, "// Generated from {:?} @ {:?}\n", name, schema_url.as_str())?;
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

pub fn generate_bootstraps(db: &DB, mut w: impl Write) -> Result<(), Error> {
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
                format!("async (state: estuary.StateStore) : Promise<void> => {{ {} }}", e)
            })
            .collect::<Vec<String>>()
            .join(", ");
        write!(w, "\t{}: [{}],\n", id, expressions)?;
    }
    write!(w, "}};\n\n")?;

    Ok(())
}

pub fn generate_transforms(db: &DB, mut w: impl Write) -> Result<(), Error> {
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
                WHERE lambda_runtime = 'nodeJS';")?;
    let mut rows = stmt.query(sql_params![])?;

    write!(w, "let transforms : estuary.TransformMap = {{\n")?;
    while let Some(row) = rows.next()? {
        let id : i64 = row.get(0)?;
        let (src_name, src_uri) : (String, Url) = (row.get(1)?, row.get(2)?);
        let (der_name, der_uri) : (String, Url) = (row.get(3)?, row.get(4)?);
        let (is_alt, body) : (bool, String) = (row.get(5)?, row.get(6)?);

        write!(w, "\t{}: async (doc: {}, state: estuary.StateStore) : Promise<{}[] | void> => {{ {} }},\n",
               id,
               ts_name(&src_name, &src_uri, is_alt),
               ts_name(&der_name, &der_uri, false),
               body)?;
    }
    write!(w, "}};\n\n")?;

    Ok(())
}
