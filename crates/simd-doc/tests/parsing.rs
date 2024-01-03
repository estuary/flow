#[test]
fn test_foobar() {
    let fixture = b"\"Hello 80World\" [0,1,[2,3]] {\"c\":{\"d\":[true],\"bb\":1}, \"a\":[1, \"2\"]} \"hi hello world\"\n42\n[1,true] \"\" false\n \"extra ";
    //let fixture = b"\"hi hello\\n world\"\n42\ntrue \"\" false\n \"extra ";

    let mut input = fixture.to_vec();
    input.extend_from_slice(&[0; 64]);
    input.truncate(fixture.len());

    let mut parser = simd_doc::new_parser();
    let alloc = simd_doc::Alloc(doc::Allocator::new());
    let mut node = simd_doc::Node(doc::HeapNode::Null);

    let remainder = simd_doc::parse_many(&mut parser, &mut input, &alloc, &mut node).unwrap();
    eprintln!("remainder: {remainder}");
}
