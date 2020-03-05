use rusqlite;

mod builder;
mod regexp_sql_fn;

pub use builder::Builder;

pub fn create_schema(db: &rusqlite::Connection) -> rusqlite::Result<()> {
    regexp_sql_fn::create(db)?; // Install support for REGEXP operator.
    db.execute_batch(include_str!("schema.sql"))?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_create_schema() -> rusqlite::Result<()> {
        let db = rusqlite::Connection::open_in_memory()?;
        create_schema(&db)
    }
}