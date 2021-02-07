use regex::Regex;
use rusqlite::{functions::FunctionFlags, Connection, Error, Result};
use std::sync::Arc;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub fn install(db: &Connection) -> Result<()> {
    // See: https://github.com/rusqlite/rusqlite/blob/master/src/functions.rs
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
    db.create_scalar_function("regexp", 2, flags, move |ctx| {
        assert_eq!(ctx.len(), 2, "called with unexpected number of arguments");
        let regexp: Arc<Regex> = ctx.get_or_create_aux(0, |vr| -> Result<_, BoxError> {
            Ok(Regex::new(vr.as_str()?)?)
        })?;
        let is_match = {
            let text = ctx
                .get_raw(1)
                .as_str()
                .map_err(|e| Error::UserFunctionError(e.into()))?;

            regexp.is_match(text)
        };

        Ok(is_match)
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_function() -> Result<()> {
        let db = Connection::open_in_memory()?;
        install(&db)?;

        let is_match: bool = db.query_row(
            "SELECT 'aaaaeeeiii' REGEXP '^[aeiou]*$';",
            rusqlite::NO_PARAMS,
            |row| row.get(0),
        )?;

        assert!(is_match);
        Ok(())
    }

    #[test]
    fn test_install_twice() -> Result<()> {
        // Can be installed multiple times without issue.
        let db = Connection::open_in_memory()?;
        install(&db)?;
        install(&db)?;
        Ok(())
    }
}
