mod arbitrary;
mod fixtures;
mod fuzz;
mod rkyv_types;

use arbitrary::ArbitraryValue;

fn build_fixture<S, I>(it: I) -> Vec<u8>
where
    S: serde::Serialize,
    I: Iterator<Item = S>,
{
    let mut b = Vec::new();

    for doc in it {
        serde_json::to_writer(&mut b, &doc).unwrap();
        b.push(b'\n');
    }
    b
}

fn simd_and_fallback(input: &mut Vec<u8>) -> (crate::Output, crate::Output) {
    let fallback = crate::parse_fallback(&input, Default::default()).unwrap();
    let fallback = crate::Output {
        v: fallback,
        offset: 0,
    };

    let mut simd = crate::Output {
        v: Default::default(),
        offset: 0,
    };
    () = crate::parse_simd(input, &mut simd, &mut crate::ffi::new_parser(1_000_000)).unwrap();

    (simd, fallback)
}

fn to_hex(v: &[u8]) -> String {
    hexdump::hexdump_iter(v)
        .map(|line| format!(" {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}
