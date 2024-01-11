use quickcheck::quickcheck;
use serde_json::{json, Value};
use std::fmt::Write;

mod arbitrary_value;
use arbitrary_value::ArbitraryValue;

quickcheck! {
    fn parser_fuzz(input: Vec<ArbitraryValue>) -> bool {
        let mut buf = Vec::new();

        for doc in input {
            let doc = match doc.0 {
                doc @ Value::Object(_) | doc @ Value::Array(_) => doc,
                doc => Value::Array(vec![doc]),
            };
            serde_json::to_writer(&mut buf, &doc).unwrap();
            buf.push(b'\n');
        }
        let (serde, simd) = parse_case(&buf);

        if serde != simd {
            eprintln!("serde:\n{serde}\n");
            eprintln!("simd:\n{simd}\n");
        }

        return serde == simd;
    }
}

fn parse_case(input: &[u8]) -> (String, String) {
    let mut parser = simd_doc::Parser::new();

    let (mut buf1, mut buf2) = (input.to_vec(), input.to_vec());
    let (mut out1, mut out2) = (Vec::new(), Vec::new());
    () = parser.parse_serde(&mut buf1, &mut out1).unwrap();
    () = parser.parse_simd(&mut buf2, &mut out2).unwrap();

    (to_desc(out1, buf1), to_desc(out2, buf2))
}

fn to_desc(docs: Vec<(u32, doc::OwnedArchivedNode)>, rem: Vec<u8>) -> String {
    let mut w = String::new();

    for (offset, doc) in docs {
        writeln!(
            &mut w,
            "offset {offset}:\n{}",
            hexdump::hexdump_iter(doc.bytes())
                .map(|line| format!(" {line}"))
                .collect::<Vec<_>>()
                .join("\n")
        )
        .unwrap();
    }

    writeln!(
        &mut w,
        "remainder:\n{}",
        hexdump::hexdump_iter(&rem)
            .map(|line| format!(" {line}"))
            .collect::<Vec<_>>()
            .join("\n")
    )
    .unwrap();

    w
}

#[test]
fn test_wizzle() {
    let mut b = Vec::new();

    for fixture in [
        json!([]),
        json!([true]),
        json!([true, false]),
        json!([true, [false], true]),
        json!([123, -456, 78.910]),
        json!([
            "aaaaaaaaaa",
            "bbbbbbbb",
            ["inline", "big big big big"],
            ["ccccccccc"]
        ]),
        json!({"":{"":9.007199254740997e16}}),
        json!({
            "hello": {"big": "worldddddd", "wide": true}, "aaaaaaaaa": 1, "bbbbbbbbb": 2,
        }),
        json!({
            "a\ta": { "b\tb": -9007, "z\tz": true},
            "c\tc": "string!",
            "d\td": { "e\te": 1234, "zz\tzz": false, "s\ts": "other string!"},
            "last": false
        }),
        json!(["one", ["two", ["three"], "four"]]),
        json!({"\u{80}111abc": "ࠀ\u{80}222"}),
    ] {
        serde_json::to_writer(&mut b, &fixture).unwrap();
        b.push(b'\n');
    }
    b.extend_from_slice(b"[{\"remainder\":\"");

    let (serde, simd) = parse_case(&b);

    if serde != simd {
        eprintln!("serde:\n{serde}\n");
        eprintln!("simd:\n{simd}\n");
        assert!(false);
    }
}
