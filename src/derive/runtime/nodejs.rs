use crate::catalog::Error;
use rusqlite::{params as sql_params, Connection as DB};
use serde_json::Value;
use std::io::Write;

pub fn compile_bundle(db: &DB) -> Result<(), Error> {
    let mut w = std::io::stdout();

    // Write out bootstraps.
    let mut stmt = db.prepare(
        "SELECT derivation_id, JSON_GROUP_ARRAY(expression)
                FROM nodejs_expressions WHERE type = 'bootstrap';",
    )?;
    let mut rows = stmt.query(sql_params![])?;

    write!(w, "let bootstrap_lambdas : BootstrapLambdaMap = {{\n")?;
    while let Some(row) = rows.next()? {
        let (id, expressions): (i64, Value) = (row.get(0)?, row.get(1)?);
        let expressions: Vec<String> = serde_json::from_value(expressions)?;
        let expressions = expressions.join(", ");
        write!(w, "\t{}: [{}],\n", id, expressions)?;
    }
    write!(w, "}};\n\n")?;

    // Write out transforms.
    let mut stmt = db.prepare(
        "SELECT lambda_id, expression
                FROM nodejs_expressions WHERE type = 'transform';",
    )?;
    let mut rows = stmt.query(sql_params![])?;

    write!(w, "let transform_lambdas : TransformLambdaMap = {{\n")?;
    while let Some(row) = rows.next()? {
        let (id, body): (i64, String) = (row.get(0)?, row.get(1)?);
        write!(w, "\t{}: {},\n", id, body)?;
    }
    write!(w, "}};\n\n")?;

    Ok(())
}
