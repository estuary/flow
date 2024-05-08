mod arbitrary;
mod fixtures;
mod fuzz;
mod rkyv_types;

use arbitrary::ArbitraryValue;

fn transcoded_and_fallback(input: &mut Vec<u8>) -> (crate::Transcoded, crate::Transcoded) {
    let mut simd = crate::Transcoded {
        v: Default::default(),
        offset: 0,
    };
    () = crate::transcode_simd(input, &mut simd, &mut crate::ffi::new_parser(1_000_000)).unwrap();

    let fallback = crate::transcode_fallback(&input, Default::default()).unwrap();
    let fallback = crate::Transcoded {
        v: fallback,
        offset: 0,
    };

    (simd, fallback)
}

fn parsed_and_fallback<'a>(
    input: &mut Vec<u8>,
    alloc: &'a doc::Allocator,
) -> (Vec<(doc::HeapNode<'a>, i64)>, Vec<(doc::HeapNode<'a>, i64)>) {
    let mut simd = Vec::new();
    crate::parse_simd(
        input,
        123_000_000,
        alloc,
        &mut simd,
        &mut crate::ffi::new_parser(1_000_000),
    )
    .unwrap();

    let mut fallback = Vec::new();
    () = crate::parse_fallback(input, 123_000_000, alloc, &mut fallback).unwrap();

    (simd, fallback)
}

fn to_hex(v: &[u8]) -> String {
    hexdump::hexdump_iter(v)
        .map(|line| format!(" {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}
