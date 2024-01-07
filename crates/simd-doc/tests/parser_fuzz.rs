use quickcheck::quickcheck;

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
fn test_number_regression() {
    let fixture = b"{\"\":{\"\":9.007199254740997e16}}\n";
    let ((docs1, docs2), (buf1, buf2)) = round_trip(fixture);

    assert_eq!(docs1, docs2);
    assert_eq!(buf1, buf2);
}

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
