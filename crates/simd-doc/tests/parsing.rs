#[test]
fn test_foobar() {
    let fixture = b"\"Hello 80World\" [0,1,[2,3]] {\"c\":{\"d\":[true],\"bb\":1}, \"a\":[1, \"2\", null]} \"hi hello world\"\n42\n[1,true] \"\" false\n \"extra ";
    let mut input = fixture.to_vec();

    let mut parser = simd_doc::new_parser();

    let docs = simd_doc::do_it(&mut parser, &mut input).unwrap();

    for (offset, doc) in docs {
        let s = serde_json::to_string(&doc::SerPolicy::default().on(doc.get())).unwrap();
        eprintln!("offset {offset}: {s}");
    }
    eprintln!("remainder: {input:?}");
}
