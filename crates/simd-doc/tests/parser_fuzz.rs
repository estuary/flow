use quickcheck::quickcheck;
use serde_json::json;

mod arbitrary_value;
use arbitrary_value::ArbitraryValue;

quickcheck! {
    fn parser_fuzz(input: Vec<ArbitraryValue>) -> bool {
        let mut buf = Vec::new();

        for doc in input {
            serde_json::to_writer(&mut buf, &doc.0).unwrap();
            buf.push(b'\n');
        }
        let ((docs1, docs2), (buf1, buf2)) = round_trip(&buf);

        return docs1 == docs2 && buf1 == buf2;
    }
}

fn round_trip(input: &[u8]) -> ((String, String), (Vec<u8>, Vec<u8>)) {
    let mut parser = simd_doc::Parser::new();

    let (mut buf1, mut buf2) = (input.to_vec(), input.to_vec());
    let (alloc1, alloc2) = (doc::Allocator::new(), doc::Allocator::new());
    let (mut docs1, mut docs2) = (Vec::new(), Vec::new());

    () = parser.parse_serde(&alloc1, &mut docs1, &mut buf1).unwrap();
    () = parser.parse_simd(&alloc2, &mut docs2, &mut buf2).unwrap();

    let docs1 = format!("{docs1:?}");
    let docs2 = format!("{docs2:?}");

    ((docs1, docs2), (buf1, buf2))
}

#[test]
fn test_fixture_cases() {
    let mut b = Vec::new();

    for fixture in [
        json!([]),
        json!([true]),
        json!([true, false]),
        json!([true, [false], true]),
        json!([123, -456, 78.910]),
        json!(["inline"]),
        json!(["out of line"]),
        json!(["aaa", "bbb"]),
        json!([
            "aaaaaaaaaa",
            "bbbbbbbb",
            ["inline", "big big big big"],
            ["ccccccccc"]
        ]),
        json!({"":{"":9.007199254740997e16}}),
        json!({
            "hello": {"big": "worldddddd", "wide": true, "big big key": "smol"}, "aaaaaaaaa": 1, "bbbbbbbbb": 2,
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

    let ((docs1, docs2), (buf1, buf2)) = round_trip(&b);
    assert_eq!(docs1, docs2);
    assert_eq!(buf1, buf2);
}
