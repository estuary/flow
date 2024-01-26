use super::{build_fixture, simd_and_fallback, to_hex};
use serde_json::{json, Value};

#[test]
fn fixture_cases() {
    let cases = json!([
        [],
        [true],
        [true, false],
        [true, [false], true],
        [123, -456, 78.910],
        ["inline"],
        ["out of line"],
        ["aaa", "bbb"],
        [
            "",
            "a",
            "ab",
            "abc",
            "abcd",
            "abcde",
            "abcdef",
            "abcdefg",
            "too big to inline"
        ],
        [
            "aaaaaaaaaa",
            "bbbbbbbb",
            ["inline", "big big big big"],
            ["ccccccccc"]
        ],
        {"a":{"b":9.007199254740997e16}},
        {
            "hello": {"big": "worldddddd", "wide": true, "big big key": "smol"},
            "aaaaaaaaa": 1,
            "bbbbbbbbb": 2,
            "unicode": "语言处理 😊",
        },
        {
            "a": 1,
            "ab": 2,
            "abc": 3,
            "abcd": 4,
            "abcde": 5,
            "abcdef": 6,
            "abcdefg": 7,
            "abcdefgh": 8,
            "zzzzzzzzz": 9,
        },
        {
            "a\ta": { "b\tb": -9007, "z\tz": true},
            "c\tc": "string!",
            "d\td": { "e\te": 1234, "zz\tzz": false, "s\ts": "other string!"},
            "last": false
        },
        ["one", ["two", ["three"], "four"]],
        {"\u{80}111abc": "ࠀ\u{80}222"},
    ]);
    let cases: Vec<Value> = serde_json::from_value(cases).unwrap();
    let mut input = build_fixture(cases.iter());
    let (simd, fallback) = simd_and_fallback(&mut input);

    let mut failed = false;

    for ((case, (s_offset, s_doc)), (f_offset, f_doc)) in
        cases.iter().zip(simd.iter()).zip(fallback.iter())
    {
        let (s_doc, f_doc) = (to_hex(s_doc), to_hex(f_doc));

        if s_offset != f_offset || s_doc != f_doc {
            eprintln!("case:\n{case}");
            eprintln!("simd:\n{s_doc}");
            eprintln!("fallback:\n{f_doc}");
            failed = true;
        }
    }
    assert!(!failed)
}
