use super::{regexp_sql_fn, Result};
use rusqlite::{params as sql_params, Connection as DB, Result as DBResult};
use serde_json::Value;

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
        let mut v: Vec<Value> = Vec::with_capacity(row.column_count());

        for (ind, c) in row.columns().iter().enumerate() {
            match c.decl_type() {
                Some("INTEGER") => {
                    let num: i64 = row.get(ind)?;
                    v.push(serde_json::from_str(&num.to_string()).unwrap());
                }
                Some("BOOLEAN") => {
                    v.push(Value::Bool(row.get(ind)?));
                }
                Some("TEXT") | Some("BLOB") if c.name().ends_with("_json") => {
                    v.push(row.get(ind)?);
                }
                Some("TEXT") | Some("BLOB") => {
                    v.push(Value::String(row.get(ind)?));
                }
                other @ _ => panic!("unhandled case: {:?}", other),
            }
        }
        Ok(Value::Array(v))
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
