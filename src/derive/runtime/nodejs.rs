use crate::catalog::Error;
use rusqlite::{params as sql_params, Connection as DB};
use std::io::Write;

pub fn compile_bundle(db: &DB) -> Result<(), Error> {
    let mut w = std::io::stdout();

    // Write out bootstraps.
    let mut stmt = db.prepare_cached("SELECT id, body FROM nodejs_lambdas WHERE is_bootstrap")?;
    let mut rows = stmt.query(sql_params![])?;

    write!(w, "let bootstrap_lambdas : BootstrapLambdaMap = {{\n")?;
    while let Some(row) = rows.next()? {
        let (id, body): (i64, String) = (row.get(0)?, row.get(1)?);
        write!(w, "\t{}: {},\n", id, body)?;
    }
    write!(w, "}};\n\n")?;

    // Write out transforms.
    let mut stmt = db.prepare_cached("SELECT id, body FROM nodejs_lambdas WHERE is_transform")?;
    let mut rows = stmt.query(sql_params![])?;

    write!(w, "let transform_lambdas : TransformLambdaMap = {{\n")?;
    while let Some(row) = rows.next()? {
        let (id, body): (i64, String) = (row.get(0)?, row.get(1)?);
        write!(w, "\t{}: {},\n", id, body)?;
    }
    write!(w, "}};\n\n")?;

    /*
    write!(w, "let bootstraps : BootstrapMap = {{\n")?;
    for (id, body, _) in bootstraps {
        write!(w, "\t{}: {},\n", id, body)?;
    }
    write!(w, "}};\n")?;
    */

    Ok(())
}
