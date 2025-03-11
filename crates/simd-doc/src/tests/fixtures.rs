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
    inputs.push(r###"{"test": 5}, {"test": 6}]"###.as_bytes());
    inputs.push(r###"{"test": 7}  {"test": 8}"###.as_bytes());
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

        assert_eq!(parse_snap, transcode_snap);

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
                    String("trailing characters at line 1 column 7 @ 0..36"),
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
            "{\"test\": 5}, {\"test\": 6}]\n{\"this\": \"is a real doc\"}\n",
            [
                (
                    -1,
                    String("trailing characters at line 1 column 12 @ 0..26"),
                ),
                (
                    52,
                    Object {
                        "this": String("is a real doc"),
                    },
                ),
            ],
        ),
        (
            "{\"test\": 7}  {\"test\": 8}\n{\"this\": \"is a real doc\"}\n",
            [
                (
                    -1,
                    String("trailing characters at line 1 column 14 @ 0..25"),
                ),
                (
                    51,
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

    {
        let input = vec![input.as_str(), input.as_str(), input.as_str()].join("\n");
        let input = input.as_bytes();

        // Verify error handling of multiple valid documents in the input.
        insta::assert_debug_snapshot!(parser.parse_one(input, &alloc).unwrap_err(), @r###"
        Custom {
            kind: InvalidData,
            error: "expected one document, but parsed 3",
        }
        "###);

        // Verify error handling of a trailing partial document.
        insta::assert_debug_snapshot!(parser.parse_one(&input[..input.len()/2], &alloc).unwrap_err(), @r###"
        Custom {
            kind: InvalidData,
            error: Error("trailing characters", line: 11, column: 1),
        }
        "###);
    }

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

#[test]
fn test_fix_031125_handling() {
    let fixture = r#"{"_meta":{"uuid":"395fc8d8-febd-11ef-8400-0f47f728d675"},"shard":{"kind":"capture","name":"Foobar/old-sendgrid/source-sendgrid","keyBegin":"00000000","rClockBegin":"00000000","build":"10560a8d2c80005d"},"ts":"2025-03-11T21:10:13.662352169Z","level":"info","message":"inferred schema updated","fields":{"binding":1,"collection_generation_id":102272023e800296,"collection_name":"Foobar/old-sendgrid/bounces","module":"runtime::capture::protocol","schema":{"$schema":"https://json-schema.org/draft/2019-09/schema","additionalProperties":false,"properties":{"_meta":{"additionalProperties":false,"properties":{"uuid":{"maxLength":64,"minLength":32,"type":"string"}},"required":["uuid"],"type":"object"},"created":{"maximum":10000000000,"minimum":1000000000,"type":"integer"},"email":{"maxLength":32,"minLength":16,"type":"string"},"reason":{"maxLength":512,"minLength":256,"type":"string"},"status":{"maxLength":8,"minLength":4,"type":"string"}},"required":["_meta","created","email","reason","status"],"type":"object","x-collection-generation-id":"102272023e800296"}}}
{ "_meta": { "uuid": "dd82b106-febb-11ef-8400-0b7f6ae14070" }, "shard": { "kind": "capture", "name": "Foobar/prod_pbj_pj_node2/source-postgres", "keyBegin": "00000000", "rClockBegin": "00000000", "build": "10732e3676000391" }, "ts": "2025-03-11T21:00:30.043993059Z", "level": "info", "message": "inferred schema updated", "fields": { "binding": 0, "collection_generation_id": 10732e3606800391, "collection_name": "Foobar/prod_pbj_pj_node2/public/account", "module": "runtime::capture::protocol", "schema": { "$schema": "https://json-schema.org/draft/2019-09/schema", "additionalProperties": false, "properties": { "_meta": { "additionalProperties": false, "properties": { "op": { "maxLength": 1, "type": "string" }, "source": { "additionalProperties": false, "properties": { "loc": { "items": { "maximum": 100000000000, "minimum": 10000000000, "type": "integer" }, "maxItems": 4, "minItems": 2, "type": "array" }, "schema": { "maxLength": 8, "minLength": 4, "type": "string" }, "table": { "maxLength": 8, "minLength": 4, "type": "string" }, "ts_ms": { "maximum": 10000000000000, "minimum": 1000000000000, "type": "integer" }, "txid": { "maximum": 10000000, "minimum": 1000000, "type": "integer" } }, "required": ["loc", "schema", "table", "ts_ms", "txid"], "type": "object" }, "uuid": { "maxLength": 64, "minLength": 32, "type": "string" } }, "required": ["op", "source", "uuid"], "type": "object" }, "account_approved_balance": { "format": "integer", "maxLength": 4, "minLength": 2, "type": "string" }, "account_balance": { "format": "integer", "maxLength": 4, "minLength": 2, "type": "string" }, "account_base_currency_id": { "maxLength": 4, "minLength": 2, "type": "string" }, "account_buckets_enabled": { "type": "boolean" }, "account_currency_id": { "maxLength": 4, "minLength": 2, "type": "string" }, "account_id": { "maximum": 100000000, "minimum": 10000000, "type": "integer" }, "account_is_cluster_wide": { "type": "boolean" }, "account_last_summary_timestamp": { "format": "date-time", "maxLength": 32, "minLength": 16, "type": "string" }, "account_name": { "maxLength": 8, "minLength": 4, "type": "string" }, "account_restrict_cr_balance": { "type": "boolean" }, "account_restrict_dr_balance": { "type": "boolean" }, "account_timestamp": { "format": "date-time", "maxLength": 32, "minLength": 16, "type": "string" }, "account_type": { "maxLength": 1, "type": "string" }, "customer_id": { "maximum": 100000000, "minimum": 10000000, "type": "integer" } }, "required": ["_meta", "account_approved_balance", "account_balance", "account_base_currency_id", "account_buckets_enabled", "account_currency_id", "account_id", "account_is_cluster_wide", "account_last_summary_timestamp", "account_name", "account_restrict_cr_balance", "account_restrict_dr_balance", "account_timestamp", "account_type", "customer_id"], "type": "object", "x-collection-generation-id": "10732e3606800391" } } }
{"hi": 32}
{ "_meta": { "uuid": "5fb1ed4f-feb6-11ef-8400-25246bb5df41" }, "shard": { "kind": "capture", "uni": "你好世界", "name": "Foobar/woo_order_analytics_data_web/source-woocommerce", "keyBegin": "00000000", "rClockBegin": "00000000", "build": "107beea58780036a" }, "ts": "2025-03-11T20:21:11.476669337Z", "level": "info", "message": "inferred schema updated", "fields": { "binding": 1, "collection_generation_id": 103e567187800062, "collection_name": "Foobar/woo_order_analytics_data_web/customers", "module": "runtime::capture::protocol", "schema": { "$schema": "https://json-schema.org/draft/2019-09/schema", "additionalProperties": false, "properties": { "_links": { "additionalProperties": false, "properties": { "collection": { "items": { "additionalProperties": false, "properties": { "href": { "maxLength": 64, "minLength": 32, "type": "string" } }, "required": ["href"], "type": "object" }, "maxItems": 1, "type": "array" }, "self": { "items": { "additionalProperties": false, "properties": { "href": { "maxLength": 64, "minLength": 32, "type": "string" }, "targetHints": { "additionalProperties": false, "properties": { "allow": { "items": { "maxLength": 8, "minLength": 2, "type": "string" }, "maxItems": 8, "minItems": 4, "type": "array" } }, "required": ["allow"], "type": "object" } }, "required": ["href", "targetHints"], "type": "object" }, "maxItems": 1, "type": "array" } }, "required": ["collection", "self"], "type": "object" }, "_meta": { "additionalProperties": false, "properties": { "uuid": { "maxLength": 64, "minLength": 32, "type": "string" } }, "required": ["uuid"], "type": "object" }, "avatar_url": { "maxLength": 128, "minLength": 64, "type": "string" }, "billing": { "additionalProperties": false, "properties": { "address_1": { "maxLength": 256, "type": "string" }, "address_2": { "maxLength": 128, "type": "string" }, "city": { "maxLength": 64, "type": "string" }, "company": { "maxLength": 64, "type": "string" }, "country": { "maxLength": 2, "type": "string" }, "email": { "maxLength": 64, "type": "string" }, "first_name": { "maxLength": 32, "type": "string" }, "last_name": { "maxLength": 32, "type": "string" }, "phone": { "maxLength": 16, "type": "string" }, "postcode": { "maxLength": 8, "type": "string" }, "state": { "maxLength": 2, "type": "string" } }, "required": ["address_1", "address_2", "city", "company", "country", "email", "first_name", "last_name", "phone", "postcode", "state"], "type": "object" }, "date_created": { "format": "date-time", "maxLength": 32, "minLength": 16, "type": "string" }, "date_created_gmt": { "format": "date-time", "maxLength": 32, "minLength": 16, "type": "string" }, "date_modified": { "format": "date-time", "maxLength": 32, "minLength": 16, "type": ["null", "string"] }, "date_modified_gmt": { "format": "date-time", "maxLength": 32, "minLength": 16, "type": ["null", "string"] }, "email": { "maxLength": 64, "minLength": 8, "type": "string" }, "first_name": { "maxLength": 32, "type": "string" }, "id": { "maximum": 10000, "minimum": 1, "type": "integer" }, "is_paying_customer": { "type": "boolean" }, "last_name": { "maxLength": 32, "type": "string" }, "meta_data": { "items": { "additionalProperties": false, "properties": { "id": { "maximum": 1000000, "minimum": 10, "type": "integer" }, "key": { "maxLength": 32, "minLength": 8, "type": "string" }, "value": { "additionalProperties": false, "items": { "maxLength": 16, "type": "string" }, "maxItems": 1, "maxLength": 32, "properties": { "ID": { "maximum": 1000000000, "minimum": 100000000, "type": "integer" }, "description": { "maxLength": 0, "type": "string" }, "display_name": { "maxLength": 16, "minLength": 8, "type": "string" }, "email": { "maxLength": 32, "minLength": 16, "type": "string" }, "external_user_id": { "maximum": 0, "minimum": 0, "type": "integer" }, "first_name": { "maxLength": 8, "minLength": 4, "type": "string" }, "last_name": { "maxLength": 8, "minLength": 4, "type": "string" }, "login": { "maxLength": 16, "minLength": 8, "type": "string" }, "two_step_enabled": { "type": "boolean" }, "url": { "maxLength": 0, "type": "string" } }, "required": ["ID", "description", "display_name", "email", "external_user_id", "first_name", "last_name", "login", "two_step_enabled", "url"], "type": ["array", "object", "string"] } }, "required": ["id", "key", "value"], "type": "object" }, "maxItems": 16, "type": "array" }, "role": { "maxLength": 8, "minLength": 4, "type": "string" }, "shipping": { "additionalProperties": false, "properties": { "address_1": { "maxLength": 256, "type": "string" }, "address_2": { "maxLength": 128, "type": "string" }, "city": { "maxLength": 64, "type": "string" }, "company": { "maxLength": 64, "type": "string" }, "country": { "maxLength": 2, "type": "string" }, "first_name": { "maxLength": 32, "type": "string" }, "last_name": { "maxLength": 32, "type": "string" }, "phone": { "maxLength": 0, "type": "string" }, "postcode": { "maxLength": 8, "type": "string" }, "state": { "maxLength": 2, "type": "string" } }, "required": ["address_1", "address_2", "city", "company", "country", "first_name", "last_name", "phone", "postcode", "state"], "type": "object" }, "username": { "maxLength": 64, "type": "string" } }, "required": ["_links", "_meta", "avatar_url", "billing", "date_created", "date_created_gmt", "date_modified", "date_modified_gmt", "email", "first_name", "id", "is_paying_customer", "last_name", "meta_data", "role", "shipping", "username"], "type": "object", "x-collection-generation-id": "103e567187800062" } } }
{"foo": "bar"}
{ "_meta": { "uuid": "08589fe5-feb5-11ef-8400-690cc61619c1" }, "shard": { "kind": "capture", "name": "Foobar/byod/source-azure-sqlserver", "keyBegin": "00000000", "rClockBegin": "00000000", "build": "10610edf748003e7" }, "ts": "2025-03-11T20:11:35.431948027Z", "level": "info", "message": "inferred schema updated", "fields": { "binding": 26, "collection_generation_id": 100630e429800398, "collection_name": "Foobar/byod/salesorderheaderv4staging", "module": "runtime::capture::protocol", "schema": { "redacted": "because the schema is too big for slack", "x-collection-generation-id": "100630e429800398" } } }
"#;
    let fixture = fixture.as_bytes();

    let mut scratch = String::new();
    let fixup = crate::fixup_031125(fixture, &mut scratch);

    let alloc = doc::Allocator::new();
    let mut fallback = Vec::new();
    let (_consumed, maybe_err) = crate::parse_fallback(fixup, 123_000_000, &alloc, &mut fallback);
    assert_eq!(None, maybe_err.map(|(err, _location)| err.to_string()));
}
