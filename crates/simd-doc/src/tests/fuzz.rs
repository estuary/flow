use quickcheck::quickcheck;
use serde_json::Value;
use std::hash::Hasher;
use xxhash_rust::xxh3::Xxh3;

use super::ArbitraryValue;

quickcheck! {
    fn transcode_matches_fallback_fuzz(input: Vec<ArbitraryValue>) -> bool {
        let (simd, fallback) = super::transcoded_and_fallback(&mut extend_fixture(Vec::new(), input));
        return fallback.v.as_slice() == simd.v.as_slice();
    }

    fn parse_matches_fallback_fuzz(input: Vec<ArbitraryValue>) -> bool {
        let alloc = doc::Allocator::new();
        let (simd, fallback) = super::parsed_and_fallback(&mut extend_fixture(Vec::new(), input), &alloc);
        return simd.iter().zip(fallback.iter()).all(|((l_d, l_o), (r_d, r_o))| l_o == r_o && doc::compare(l_d, r_d).is_eq());
    }

    fn parse_and_transcode_with_errors( in1: Vec<ArbitraryValue>, in2: Vec<ArbitraryValue>, in3: Vec<ArbitraryValue>, s1: u16, s2: u16) -> bool {
        parse_and_transcode_with_errors_case(in1, in2, in3, s1, s2)
    }
}

fn parse_and_transcode_with_errors_case(
    in1: Vec<ArbitraryValue>,
    in2: Vec<ArbitraryValue>,
    in3: Vec<ArbitraryValue>,
    s1: u16,
    s2: u16,
) -> bool {
    let mut input = extend_fixture(Vec::new(), in1);
    input.extend_from_slice(b"{error one}\n");
    input = extend_fixture(input, in2);
    input.extend_from_slice(b"{error: two}\n");
    input = extend_fixture(input, in3);

    let mut p1 = crate::Parser::new();
    let mut p2 = crate::Parser::new();
    let mut p3 = crate::Parser::new();

    let mut h1 = Xxh3::with_seed(0);
    let mut h2 = h1.clone();
    let mut h3 = h1.clone();

    drive_parse(&mut p1, &input, s1 as usize, &mut h1);
    drive_transcode(&mut p2, &input, s2 as usize, &mut h2);
    drive_parse(&mut p3, &input, 0, &mut h3);

    return h1.digest() == h2.digest() && h1.digest() == h3.digest();
}

fn drive_transcode(p: &mut crate::Parser, input: &[u8], split: usize, hash: &mut Xxh3) {
    let split = split % input.len();
    let mut scratch = Default::default();

    for (chunk, chunk_offset) in [(&input[..split], 0), (&input[split..], split as i64)] {
        () = p.chunk(chunk, chunk_offset).unwrap();

        scratch = loop {
            match p.transcode_many(scratch) {
                Ok(out) if out.is_empty() => break out.into_inner(),
                Ok(out) => {
                    for (doc, next_offset) in out.iter() {
                        let doc = doc::ArchivedNode::from_archive(doc);
                        let doc = serde_json::to_string(&doc::SerPolicy::noop().on(doc)).unwrap();
                        hash.write(doc.as_bytes());
                        hash.write_i64(next_offset);
                    }
                    scratch = out.into_inner();
                }
                Err((err, location)) => {
                    hash.write(format!("{err} @ {location:?}").as_bytes());
                    scratch = Default::default();
                }
            }
        };
    }
}

fn drive_parse(p: &mut crate::Parser, input: &[u8], split: usize, hash: &mut Xxh3) {
    let split = split % input.len();
    let mut alloc = doc::Allocator::new();

    for (chunk, chunk_offset) in [(&input[..split], 0), (&input[split..], split as i64)] {
        () = p.chunk(chunk, chunk_offset).unwrap();

        loop {
            alloc.reset();

            match p.parse_many(&alloc) {
                Ok((_begin, drained)) if drained.len() == 0 => break,
                Ok((_begin, drained)) => {
                    for (doc, next_offset) in drained {
                        let doc = serde_json::to_string(&doc::SerPolicy::noop().on(&doc)).unwrap();
                        hash.write(doc.as_bytes());
                        hash.write_i64(next_offset);
                    }
                }
                Err((err, location)) => hash.write(format!("{err} @ {location:?}").as_bytes()),
            }
        }
    }
}

fn extend_fixture(mut b: Vec<u8>, it: Vec<ArbitraryValue>) -> Vec<u8> {
    for doc in it {
        serde_json::to_writer(
            &mut b,
            &match doc.0 {
                doc @ Value::Object(_) | doc @ Value::Array(_) => doc,
                doc => Value::Array(vec![doc]),
            },
        )
        .unwrap();

        b.push(b'\n');
    }
    b
}
