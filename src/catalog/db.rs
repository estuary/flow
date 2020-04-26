use super::{regexp_sql_fn, Result};
use rusqlite::{params as sql_params, Connection as DB, Result as DBResult};
use serde_json::Value;
use std::borrow::Cow;

/// Create the catalog SQL schema in the connected database.
pub fn init(db: &DB) -> Result<()> {
    regexp_sql_fn::install(db)?; // Install support for REGEXP operator.
    db.execute_batch(include_str!("catalog.sql"))?;
    Ok(())
}

pub fn dump_tables(db: &DB, names: &[&str]) -> Result<Value> {
    let mut map = serde_json::Map::new();

    for &name in names {
        map.insert(name.to_owned(), dump_table(db, name)?);
    }
    Ok(Value::Object(map))
}

pub fn dump_table(db: &DB, name: &str) -> Result<Value> {
    let mut s = db.prepare(&format!("SELECT * FROM {}", name))?;

    let rows = s.query_map(sql_params![], |row| {
        let mut out: Vec<Value> = Vec::with_capacity(row.column_count());

        for (ind, col) in row.columns().iter().enumerate() {
            // Map the column into a JSON serialization.
            let val_str = match col.decl_type() {
                Some("INTEGER") => {
                    let n: Option<i64> = row.get(ind)?;
                    match n {
                        Some(n) => Cow::from(n.to_string()),
                        None => Cow::from("null"),
                    }
                }
                Some("BOOLEAN") => {
                    let b: Option<bool> = row.get(ind)?;
                    match b {
                        Some(true) => Cow::from("true"),
                        Some(false) => Cow::from("false"),
                        None => Cow::from("null"),
                    }
                }
                Some("TEXT") => {
                    let s: Option<String> = row.get(ind)?;
                    match s {
                        Some(s) if col.name().ends_with("_json") => Cow::from(s),
                        Some(s) => Cow::from(Value::String(s).to_string()),
                        None => Cow::from("null"),
                    }
                }
                Some("BLOB") => {
                    let b: Option<Vec<u8>> = row.get(ind)?;
                    match b {
                        Some(b) => {
                            let b = String::from_utf8(b).unwrap();
                            Cow::from(Value::String(b).to_string())
                        }
                        None => Cow::from("null"),
                    }
                }
                other @ _ => panic!("unhandled case: {:?}", other),
            };
            out.push(serde_json::from_str(&val_str).unwrap());
        }
        Ok(Value::Array(out))
    })?;

    let rows: DBResult<Vec<serde_json::Value>> = rows.collect();
    Ok(Value::Array(rows?))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_init() -> Result<()> {
        let db = DB::open_in_memory()?;
        init(&db)
    }
}
