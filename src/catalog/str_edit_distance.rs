use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, Result};

/// Installs the `osa_distance` function, which accepts two string arguments and returns in
/// integer that represents some concept of edit distance. A return value of 0 means the strings
/// are identical. A value greater than 0 means that the strings are different, with larger values
/// indicating relatively more difference. Specific values returned from here may or may not match
/// one's subjective assessment of "similarity", especially for values larger than about 4.
pub fn install(db: &Connection) -> Result<()> {
    let edit_dist_flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
    db.create_scalar_function("osa_distance", 2, edit_dist_flags, &osa_distance_fun)?;
    Ok(())
}

fn osa_distance_fun(c: &Context) -> Result<i64> {
    if c.len() != 2 {
        return Err(rusqlite::Error::InvalidParameterCount(c.len(), 2));
    }
    let left = get_str(0, c)?;
    let right = get_str(1, c)?;
    let similarity = osa_distance(left, right);
    Ok(similarity)
}

fn osa_distance(a: &str, b: &str) -> i64 {
    strsim::osa_distance(a, b) as i64
}

fn get_str<'a>(i: usize, c: &'a Context) -> Result<&'a str> {
    match c.get_raw(i) {
        ValueRef::Text(bytes) => std::str::from_utf8(bytes).map_err(rusqlite::Error::Utf8Error),
        other => Err(rusqlite::Error::InvalidFunctionParameterType(
            i,
            other.data_type(),
        )),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn osa_distance_is_callable_from_sql() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        install(&db).unwrap();

        let result = db
            .query_row(
                "select osa_distance('foo', 'bar');",
                rusqlite::NO_PARAMS,
                |r| r.get::<usize, i64>(0),
            )
            .unwrap();
        assert_eq!(3, result);
    }

    #[test]
    fn osa_distance_returns_0_for_equal_strings() {
        let eq = vec![
            "foo",
            "a longer string with spaces",
            "a/realistic/collection/name",
            "💩",
            "大家好",
        ];
        for s in eq {
            let dist = osa_distance(s, s);
            assert_eq!(0, dist);
        }
    }

    #[test]
    fn osa_distance_returns_greater_than_0_for_unequal_strings() {
        let neq = vec![
            ("", " "),
            ("foo", "fooo"),
            ("a/realistic/collection/name", "a/realistic/collection/diff"),
            (
                "an/unrealistic/collection/name/with/a/diff",
                "an/unrealistic/collection/name/with/a/dif",
            ),
            ("abcde", "abcdf"),
            ("abcde", "aabcdf"),
            ("abcde", "0abcdf"),
            ("abcde", "abcd"),
            ("marketing/offer/views", "marketing/campaigns"),
            // Test cases to ensure that case counts as a difference
            ("A", "a"),
            ("ß Minnow", "ss Minnow"),
            ("spiﬃest", "spiffiest"),
        ];
        for (a, b) in neq {
            let dist = osa_distance(a, b);
            assert!(dist > 0, "expected > 0 for inputs: a: {:?}, b: {:?}", a, b);
        }
    }
}
