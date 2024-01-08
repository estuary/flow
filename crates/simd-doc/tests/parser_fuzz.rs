use quickcheck::quickcheck;
use serde_json::{json, Value};

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
        let (docs1, docs2, buf1, buf2) = parse_case(&buf);

        return docs1 == docs2 && buf1 == buf2;
    }
}

fn parse_case(input: &[u8]) -> (String, String, String, String) {
    let mut parser = simd_doc::Parser::new();

    let (mut buf1, mut buf2) = (input.to_vec(), input.to_vec());
    let out1 = parser.parse_serde(&mut buf1).unwrap();
    let out2 = parser.parse_simd(&mut buf2).unwrap();

    let m = |v: Vec<u8>| {
        hexdump::hexdump_iter(&v)
            .map(|line| format!(" {line}"))
            .collect::<Vec<_>>()
            .join("\n")
    };

    (m(out1), m(out2), m(buf1), m(buf2))
}

#[test]
fn test_wizzle() {
    let cases = [
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
        json!({
            "hello": {"big": "worldddddd", "wide": true}, "aaaaaaaaa": 1, "bbbbbbbbb": 2,
        }),
        //json!([false]),
        //json!([null]),
    ];

    for case in cases {
        let mut input = case.to_string();
        input.push('\n');
        let (out1, out2, rem1, rem2) = parse_case(input.as_bytes());

        if out1 != out2 || rem1 != rem2 {
            eprintln!("out1:\n{out1}\n");
            eprintln!("out2:\n{out2}\n");

            assert!(false);
        }
        //eprintln!("out1:\n{out1}\n");
        //eprintln!("out2:\n{out2}\n");
    }
}

#[test]
fn test_number_regression() {
    let fixture = b"{\"\":{\"\":9.007199254740997e16}}\n";
    let (docs1, docs2, buf1, buf2) = parse_case(fixture);

    assert_eq!(docs1, docs2);
    assert_eq!(buf1, buf2);
}

/*
#[test]
fn test_foobar() {
    use serde_json::json;

    let a = json!({
        "a\ta": { "b\tb": 9.007199254740997e16, "z\tz": true},
        "c\tc": "string!",
        "d\td": { "e\te": 1234, "zz\tzz": false, "s\ts": "other string!"},
        "last": false
    });
    let b = json!(["one", ["two", ["three"], "four"]]);

    let fixture = format!("{}\n{}\n", a.to_string(), b.to_string());

    let mut parser = simd_doc::Parser::new();

    let mut buf = fixture.as_bytes().to_vec();
    let alloc = doc::Allocator::new();
    let mut docs = Vec::new();

    () = parser.parse_simd(&alloc, &mut docs, &mut buf).unwrap();

    eprintln!("{docs:?}");
}
*/
