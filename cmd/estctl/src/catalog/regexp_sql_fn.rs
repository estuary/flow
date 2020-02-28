
use regex::Regex;
use rusqlite::{Connection, Error, Result, NO_PARAMS};

pub fn create(db: &Connection) -> Result<()> {
    db.create_scalar_function("regexp", 2, true, move |ctx| {
        assert_eq!(ctx.len(), 2, "called with unexpected number of arguments");

        let saved_re: Option<&Regex> = ctx.get_aux(0)?;
        let new_re = match saved_re {
            None => {
                let s = ctx.get::<String>(0)?;
                match Regex::new(&s) {
                    Ok(r) => Some(r),
                    Err(err) => return Err(Error::UserFunctionError(Box::new(err))),
                }
            }
            Some(_) => None,
        };

        let is_match = {
            let re = saved_re.unwrap_or_else(|| new_re.as_ref().unwrap());

            let text = ctx
                .get_raw(1)
                .as_str()
                .map_err(|e| Error::UserFunctionError(e.into()))?;

            re.is_match(text)
        };

        if let Some(re) = new_re {
            ctx.set_aux(0, re);
        }

        Ok(is_match)
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_function() -> Result<()> {
        let db = Connection::open_in_memory()?;
        create(&db)?;

        let is_match: bool = db.query_row(
            "SELECT 'aaaaeeeiii' REGEXP '^[aeiou]*$';",
            NO_PARAMS,
            |row| row.get(0),
        )?;

        assert!(is_match);
        Ok(())
    }
}
