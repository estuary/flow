use super::{parsed_and_fallback, to_hex, transcoded_and_fallback};
use crate::Parser;
use doc::AsNode;
use serde_json::{json, Value};

#[test]
fn test_simd_and_fallback_results_are_equal() {
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

    // Build up an input fixture which has lots of whitespace, but consists of a whole documents.
    let mut input = Vec::new();
    for doc in cases.iter() {
        serde_json::to_writer_pretty(&mut input, &doc).unwrap();
        input.push(b'\t');
    }
    let (transcoded, fallback) = transcoded_and_fallback(&mut input);

    let mut failed = false;

    for ((case, (s_offset, s_doc)), (f_offset, f_doc)) in
        cases.iter().zip(transcoded.iter()).zip(fallback.iter())
    {
        let (s_doc, f_doc) = (to_hex(s_doc), to_hex(f_doc));

        if s_offset != f_offset || s_doc != f_doc {
            eprintln!("transcode case:\n{case}");
            eprintln!("simd     @{s_offset}:\n{s_doc}");
            eprintln!("fallback @{f_offset}:\n{f_doc}");
            failed = true;
        }
    }

    let alloc = doc::Allocator::new();
    let (parsed, fallback) = parsed_and_fallback(&mut input, &alloc);

    for ((case, (s_offset, s_doc)), (f_offset, f_doc)) in
        cases.iter().zip(parsed.iter()).zip(fallback.iter())
    {
        if s_offset != f_offset || doc::compare(s_doc, f_doc) != std::cmp::Ordering::Equal {
            eprintln!("parse case:\n{case}");
            eprintln!("simd     @{s_offset}:\n{s_doc:?}");
            eprintln!("fallback @{f_offset}:\n{f_doc:?}");
            failed = true;
        }
    }

    assert!(!failed);
}

#[test]
fn test_basic_parser_apis() {
    let cases = json!([
        {
            "hello": {"big": "worldddddd", "wide": true, "big big key": "smol"},
            "aaaaaaaaa": 1,
            "bbbbbbbbb": 2,
            "unicode": "语言处理 😊",
        },
        {
            "a\ta": { "b\tb": -9007, "z\tz": true},
            "c\tc": "string!",
            "d\td": { "e\te": 1234, "zz\tzz": false, "s\ts": "other string!"},
            "last": false
        },
        {"\u{80}111abc": "ࠀ\u{80}222"},
    ]);
    let cases: Vec<Value> = serde_json::from_value(cases).unwrap();

    let mut input = Vec::new();
    for doc in cases.iter() {
        serde_json::to_writer(&mut input, &doc).unwrap();
        input.push(b'\n');
    }
    let (chunk_1, chunk_2) = input.split_at(input.len() / 2);

    let alloc = doc::Allocator::new();
    let mut parser = Parser::new();

    let mut snap = Vec::new();

    snap.push((0, json!("PARSE_CHUNK")));
    for (offset, doc) in parser.parse_chunk(chunk_1, 123, &alloc).unwrap() {
        snap.push((offset, doc.to_debug_json_value()));
    }
    for (offset, doc) in parser
        .parse_chunk(chunk_2, 123 + chunk_1.len() as i64, &alloc)
        .unwrap()
    {
        snap.push((offset, doc.to_debug_json_value()));
    }

    snap.push((0, json!("TRANSCODE_CHUNK")));
    for (offset, doc) in parser
        .transcode_chunk(chunk_1, 123, Default::default())
        .unwrap()
        .into_iter()
    {
        snap.push((offset, doc.get().to_debug_json_value()));
    }
    for (offset, doc) in parser
        .transcode_chunk(chunk_2, 123 + chunk_1.len() as i64, Default::default())
        .unwrap()
        .into_iter()
    {
        snap.push((offset, doc.get().to_debug_json_value()));
    }

    snap.push((0, json!("PARSE_ONE")));
    let input = json!({"one": [2, "three"], "four": {"five": 6}, "done": true});
    let input = serde_json::to_string_pretty(&input).unwrap(); // Allows whitespace.
    let doc = parser.parse_one(input.as_bytes(), &alloc).unwrap();
    snap.push((0, doc.to_debug_json_value()));

    let input = input.repeat(3);
    insta::assert_debug_snapshot!(parser.parse_one(input.as_bytes(), &alloc).unwrap_err(), @r###"
    Custom {
        kind: InvalidData,
        error: "expected one document, but parsed 3",
    }
    "###);

    insta::assert_json_snapshot!(snap, @r###"
    [
      [
        0,
        "PARSE_CHUNK"
      ],
      [
        123,
        {
          "aaaaaaaaa": 1,
          "bbbbbbbbb": 2,
          "hello": {
            "big": "worldddddd",
            "big big key": "smol",
            "wide": true
          },
          "unicode": "语言处理 😊"
        }
      ],
      [
        245,
        {
          "a\ta": {
            "b\tb": -9007,
            "z\tz": true
          },
          "c\tc": "string!",
          "d\td": {
            "e\te": 1234,
            "s\ts": "other string!",
            "zz\tzz": false
          },
          "last": false
        }
      ],
      [
        370,
        {
          "111abc": "ࠀ222"
        }
      ],
      [
        0,
        "TRANSCODE_CHUNK"
      ],
      [
        394,
        {
          "aaaaaaaaa": 1,
          "bbbbbbbbb": 2,
          "hello": {
            "big": "worldddddd",
            "big big key": "smol",
            "wide": true
          },
          "unicode": "语言处理 😊"
        }
      ],
      [
        245,
        {
          "a\ta": {
            "b\tb": -9007,
            "z\tz": true
          },
          "c\tc": "string!",
          "d\td": {
            "e\te": 1234,
            "s\ts": "other string!",
            "zz\tzz": false
          },
          "last": false
        }
      ],
      [
        370,
        {
          "111abc": "ࠀ222"
        }
      ],
      [
        0,
        "PARSE_ONE"
      ],
      [
        0,
        {
          "done": true,
          "four": {
            "five": 6
          },
          "one": [
            2,
            "three"
          ]
        }
      ]
    ]
    "###);
}
