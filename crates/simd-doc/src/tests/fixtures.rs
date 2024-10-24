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

    // Build up an input fixture of whole documents.
    let mut input = Vec::new();
    for doc in cases.iter() {
        serde_json::to_writer(&mut input, &doc).unwrap();
        input.push(b'\n');
    }
    let (transcoded, fallback) = transcoded_and_fallback(&mut input);
    assert_eq!(transcoded.offset, fallback.offset);

    let mut failed = false;

    for ((case, (s_doc, s_next_offset)), (f_doc, f_next_offset)) in
        cases.iter().zip(transcoded.iter()).zip(fallback.iter())
    {
        let (s_doc, f_doc) = (to_hex(s_doc), to_hex(f_doc));

        if s_doc != f_doc || s_next_offset != f_next_offset {
            eprintln!("transcode case:\n{case}");
            eprintln!("simd     @{s_next_offset}:\n{s_doc}");
            eprintln!("fallback @{f_next_offset}:\n{f_doc}");
            failed = true;
        }
    }

    let alloc = doc::Allocator::new();
    let (parsed, fallback) = parsed_and_fallback(&mut input, &alloc);

    for ((case, (s_doc, s_next_offset)), (f_doc, f_next_offset)) in
        cases.iter().zip(parsed.iter()).zip(fallback.iter())
    {
        if s_next_offset != f_next_offset || doc::compare(s_doc, f_doc) != std::cmp::Ordering::Equal
        {
            eprintln!("parse case:\n{case}");
            eprintln!("simd     @{s_next_offset}:\n{s_doc:?}");
            eprintln!("fallback @{f_next_offset}:\n{f_doc:?}");
            failed = true;
        }
    }

    assert!(!failed);
}

/// Make sure that we don't parse and return partial data from the middle of a row
#[test]
fn test_incomplete_row_parsing() {
    let mut inputs = Vec::new();
    inputs.push(r###"st": {"this": "is", "a": "doc"}}"###.as_bytes());
    inputs.push(r###""test": {"this": "is", "a": "doc"}}"###.as_bytes());
    inputs.push(r###"{"test": 5}, {"test": 6"}]"###.as_bytes());
    inputs.push(r###"{"real": "object"}"###.as_bytes());

    let real = r###"{"this": "is a real doc"}"###;

    let alloc = doc::Allocator::new();

    let mut snaps = vec![];

    for input in inputs {
        let real_input = input
            .iter()
            .chain(b"\n")
            .chain(real.as_bytes())
            .chain(b"\n")
            .cloned()
            .collect::<Vec<u8>>();

        let mut parser = Parser::new();
        parser.chunk(&real_input.clone(), 0).unwrap();
        let mut parse_snap = vec![];

        loop {
            match parser.parse_many(&alloc) {
                Ok((_, chunk)) => {
                    if chunk.len() == 0 {
                        break;
                    }
                    for (doc, next_offset) in chunk {
                        parse_snap.push((next_offset, doc.to_debug_json_value()));
                    }
                }
                Err((err, location)) => {
                    parse_snap.push((-1, json!(format!("{err} @ {location:?}"))));
                }
            }
        }

        let mut parser = Parser::new();
        parser.chunk(&real_input.clone(), 0).unwrap();
        let mut transcode_snap = vec![];

        loop {
            match parser.transcode_many(Default::default()) {
                Ok(transcoded) => {
                    if transcoded.is_empty() {
                        break;
                    }
                    for (doc, next_offset) in transcoded.into_iter() {
                        transcode_snap.push((next_offset, doc.get().to_debug_json_value()));
                    }
                }
                Err((err, location)) => {
                    transcode_snap.push((-1, json!(format!("{err} @ {location:?}"))));
                }
            }
        }

        debug_assert_eq!(parse_snap, transcode_snap);

        snaps.push((String::from_utf8(real_input).unwrap(), parse_snap));
    }

    insta::assert_debug_snapshot!(snaps, @r###"
      [
          (
              "st\": {\"this\": \"is\", \"a\": \"doc\"}}\n{\"this\": \"is a real doc\"}\n",
              [
                  (
                      -1,
                      String("expected value at line 1 column 1 @ 0..33"),
                  ),
                  (
                      59,
                      Object {
                          "this": String("is a real doc"),
                      },
                  ),
              ],
          ),
          (
              "\"test\": {\"this\": \"is\", \"a\": \"doc\"}}\n{\"this\": \"is a real doc\"}\n",
              [
                  (
                      -1,
                      String("incomplete row @ 0..45"),
                  ),
                  (
                      62,
                      Object {
                          "this": String("is a real doc"),
                      },
                  ),
              ],
          ),
          (
              "{\"test\": 5}, {\"test\": 6\"}]\n{\"this\": \"is a real doc\"}\n",
              [
                  (
                      -1,
                      String("incomplete row @ 0..33"),
                  ),
                  (
                      53,
                      Object {
                          "this": String("is a real doc"),
                      },
                  ),
              ],
          ),
          (
              "{\"real\": \"object\"}\n{\"this\": \"is a real doc\"}\n",
              [
                  (
                      19,
                      Object {
                          "real": String("object"),
                      },
                  ),
                  (
                      45,
                      Object {
                          "this": String("is a real doc"),
                      },
                  ),
              ],
          ),
      ]
    "###);
}

#[test]
fn test_basic_parser_apis() {
    let mut input = Vec::new();
    // Build up a fixture to parse, which includes an invalid document.
    {
        input.extend(
            json!(
            {
                "hello": {"big": "worldddddd", "wide": true, "big big key": "smol"},
                "aaaaaaaaa": 1,
                "bbbbbbbbb": 2,
                "unicode": "语言处理 😊",
            })
            .to_string()
            .into_bytes(),
        );
        input.push(b'\n');

        input.extend(
            json!({
                "a\ta": { "b\tb": -9007, "z\tz": true},
                "c\tc": "string!",
                "d\td": { "e\te": 1234, "zz\tzz": false, "s\ts": "other string!"},
                "last": false
            })
            .to_string()
            .into_bytes(),
        );
        input.push(b'\n');

        input.extend(b"{\"whoops\": !\n"); // Invalid JSON.

        input.extend(
            json!({"\u{80}111abc": "ࠀ\u{80}222"})
                .to_string()
                .into_bytes(),
        );
        input.push(b'\n');
    }
    let (chunk_1, chunk_2) = input.split_at(input.len() / 2);
    let (chunk_1a, chunk_1b) = chunk_1.split_at(chunk_1.len() / 2);

    let alloc = doc::Allocator::new();
    let mut parser = Parser::new();

    let mut snap = Vec::new();
    snap.push((
        0,
        json!(format!(
            "input: {} chunk_1: {} chunk_2: {}",
            input.len(),
            chunk_1.len(),
            chunk_2.len()
        )),
    ));

    let mut poll_parse = |parser: &mut Parser, step: &str| loop {
        match parser.parse_many(&alloc) {
            Ok((begin, chunk)) => {
                if chunk.len() == 0 {
                    break;
                }
                snap.push((begin, json!(step)));
                for (doc, next_offset) in chunk {
                    snap.push((next_offset, doc.to_debug_json_value()));
                }
            }
            Err((err, location)) => {
                snap.push((-1, json!(format!("{step}: {err} @ {location:?}"))));
            }
        }
    };

    () = parser.chunk(chunk_1, 1000).unwrap();
    poll_parse(&mut parser, "PARSE_CHUNK_1");

    () = parser.chunk(chunk_2, 1000 + chunk_1.len() as i64).unwrap();
    poll_parse(&mut parser, "PARSE_CHUNK_2");

    let mut poll_transcode = |parser: &mut Parser, step: &str| loop {
        match parser.transcode_many(Default::default()) {
            Ok(transcoded) => {
                if transcoded.is_empty() {
                    break;
                }
                snap.push((transcoded.offset, json!(step)));

                for (doc, next_offset) in transcoded.into_iter() {
                    snap.push((next_offset, doc.get().to_debug_json_value()));
                }
            }
            Err((err, location)) => {
                snap.push((-1, json!(format!("{step}: {err} @ {location:?}"))));
            }
        }
    };

    // This time, use multiple calls to chunk.
    () = parser.chunk(chunk_1a, 1000).unwrap();
    () = parser
        .chunk(chunk_1b, 1000 + chunk_1a.len() as i64)
        .unwrap();
    poll_transcode(&mut parser, "TRANSCODE_CHUNK_1");

    () = parser.chunk(chunk_2, 1000 + chunk_1.len() as i64).unwrap();
    poll_transcode(&mut parser, "TRANSCODE_CHUNK_2");

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
        "input: 284 chunk_1: 142 chunk_2: 142"
      ],
      [
        1000,
        "PARSE_CHUNK_1"
      ],
      [
        1122,
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
        1122,
        "PARSE_CHUNK_2"
      ],
      [
        1247,
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
        -1,
        "PARSE_CHUNK_2: expected value at line 1 column 12 @ 1247..1260"
      ],
      [
        1260,
        "PARSE_CHUNK_2"
      ],
      [
        1284,
        {
          "111abc": "ࠀ222"
        }
      ],
      [
        1000,
        "TRANSCODE_CHUNK_1"
      ],
      [
        1122,
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
        1122,
        "TRANSCODE_CHUNK_2"
      ],
      [
        1247,
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
        -1,
        "TRANSCODE_CHUNK_2: expected value at line 1 column 12 @ 1247..1260"
      ],
      [
        1260,
        "TRANSCODE_CHUNK_2"
      ],
      [
        1284,
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
