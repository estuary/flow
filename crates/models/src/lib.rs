pub mod source;
pub mod tables;
pub mod validation;

use caseless::Caseless;
use unicode_normalization::UnicodeNormalization;

/// Map input characters (e.x. String::chars()) into its collated form,
/// which ignores casing and is unicode-normalized.
/// This follows the conformance guidelines in:
/// http://www.unicode.org/versions/Unicode13.0.0/ch03.pdf
/// in Section 3.13 - "Default Caseless Matching" (all the way at the bottom).
pub fn collate<I>(i: I) -> impl Iterator<Item = char>
where
    I: Iterator<Item = char>,
{
    i.nfd().default_case_fold().nfkc()
}

#[cfg(test)]
mod test {
    use super::collate;

    #[test]
    fn test_collation_cases() {
        let table = vec![
            ("", ""),
            ("Foo/Bar", "foo/bar"),
            // These go from 2 to 3 code points when lower-cased
            ("ȺȾ", "ⱥⱦ"),
            // A mix of various CJK, ligatures, and accented characters
            ("表ポあA鷗Œé/Ｂ逍Üßªąñ丂㐀𠀀", "表ポあa鷗œé/b逍üssaąñ丂㐀𠀀"),
            ("Faſt/Carſ", "fast/cars"),
            ("a/ß/Minnow", "a/ss/minnow"),
            ("spiﬃest", "spiffiest"),
            // The uppercase 'È' (or 'È' if that first one doesn't display correctly
            // in your editor) is composed of ascii 'E' (\u{0045}), plus the
            // combining diacritic '\u{0300}'. The lowercase version 'è' is
            // represented by its own code point.
            ("a\u{0045}\u{0300}", "a\u{00e8}"),
            ("\u{00e8}", "è"),
        ];

        for (input, expect) in table {
            assert_eq!(collate(input.chars()).collect::<String>().as_str(), expect);
        }
    }
}
