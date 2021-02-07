use rusqlite::{Connection, Result};
use std::cmp::Ordering;

/// Overrides the `NOCASE` collation in sqlite with one that handles unicode.
/// This is an alternative to compiling sqlite with its own ICU compile-time option. We're
/// primarily doing it this way just because it's easier than dealing with loading a dynamic
/// library, but a side benefit is that this method will also handle unicode normalization.
pub fn install(db: &Connection) -> Result<()> {
    db.create_collation("NOCASE", compare_strings)
}

/// Compares two strings in a case-insensitive way.
/// This follows the conformance guidelines in:
/// http://www.unicode.org/versions/Unicode13.0.0/ch03.pdf
/// in Section 3.13 - "Default Caseless Matching"
fn compare_strings(a: &str, b: &str) -> Ordering {
    use caseless::Caseless;
    use unicode_normalization::UnicodeNormalization;

    // Follows the "Itentifier Caseless Matching" guidelines from the unicode standard. If you're
    // following along in the pdf linked above, it's all the way at the end. This implementation
    // uses lazy iterators, so this will short circuit as soon as we're able to determine the
    // `Ordering`.
    let a_chars = a.chars().nfd().default_case_fold().nfkc();
    let b_chars = b.chars().nfd().default_case_fold().nfkc();
    a_chars.cmp(b_chars)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn collation_works_in_database_with_unique_index() {
        let db = Connection::open_in_memory().unwrap();
        install(&db).unwrap();

        db.execute(
            r##"CREATE TABLE test (
                       x TEXT NOT NULL,
                       UNIQUE(x COLLATE NOCASE)
                   );"##,
            rusqlite::NO_PARAMS,
        )
        .unwrap();

        // We'll insert the first row with the decomposed version of Å, and then try to insert
        // another row with the composed version of å to make sure that both case folding and
        // normalization are being done.
        db.execute(
            "INSERT INTO test (x) VALUES ('valu\u{0041}\u{030A}');",
            rusqlite::NO_PARAMS,
        )
        .unwrap();

        let count: i64 = db
            .query_row("SELECT COUNT(*) FROM test;", rusqlite::NO_PARAMS, |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(1, count);

        let result = db.execute(
            "INSERT INTO test (x) VALUES ('valu\u{00E5}');",
            rusqlite::NO_PARAMS,
        );
        assert!(result.is_err());

        let count: i64 = db
            .query_row("SELECT COUNT(*) FROM test;", rusqlite::NO_PARAMS, |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(1, count);
    }

    #[test]
    fn exaxt_same_strings_are_equal() {
        // We want to test some naughty strings here in an attempt to smoke out any
        // misbehaviors with our unicode normalization.
        let inputs = &[
            "",
            "foo",
            // gotta test some emoji
            "💩👨‍👩‍👦 👨‍👩‍👧‍👦 👨‍👨‍👦 👩‍👩‍👧 👨‍👦 👨‍👧‍👦 👩‍👦 👩‍👧‍👦",
            // These go from 2 to 3 codepoints when lowercased
            "Ⱥ Ⱦ",
            // a mix of various CJK, ligatures, and accented characters
            "表ポあA鷗ŒéＢ逍Üßªąñ丂㐀𠀀",
        ];
        for example in inputs {
            assert_comare_result(Ordering::Equal, example, example);
        }
    }

    #[test]
    fn strings_are_equal_when_they_only_differ_in_case() {
        let inputs = &[
            ("FOO bar", "foo BAR"),
            ("faſt carſ", "fast cars"),
            ("ß Minnow", "ss Minnow"),
            ("spiﬃest", "spiffiest"),
        ];
        for (a, b) in inputs {
            assert_comare_result(Ordering::Equal, a, b);
        }
    }

    #[test]
    fn strings_are_equal_after_unicode_normalization() {
        // Here we're asserting that two strings that differ in case are considered equal, even
        // when their representations are different. The uppercase 'È' (or 'È' if that first one
        // doesn't display correctly in your editor) is composed of ascii 'E' (\u{0045}), plus the
        // combining diacritic '\u{0300}'. The lowercase version 'è' is represented by its own codepoint.
        assert_comare_result(Ordering::Equal, "\u{0045}\u{0300}", "\u{00e8}")
    }

    #[test]
    fn strings_return_consistent_ordering_when_they_are_not_equal() {
        let inputs = &[
            ("FOO", "fo"),
            ("abcde", "AAAAA"),
            ("ß Minnowsss", "ss Minnow"),
            ("ss Minnowsss", "ß Minnow"),
        ];
        for (a, b) in inputs {
            assert_comare_result(Ordering::Greater, a, b);
            // assert that the inverse is always less
            assert_comare_result(Ordering::Less, b, a);
        }
    }

    fn assert_comare_result(expected: Ordering, a: &str, b: &str) {
        let actual = compare_strings(a, b);
        assert_eq!(
            expected, actual,
            "expected compare_strings({:?}, {:?}) to return {:?}, but got {:?}",
            a, b, expected, actual
        );
    }
}
