#[test]
pub fn rkyv_types() {
    let alloc = doc::HeapNode::new_allocator();

    let strings = [
        doc::BumpStr::from_str("", &alloc),
        doc::BumpStr::from_str("aaaaaaa", &alloc),
        doc::BumpStr::from_str("hello", &alloc),
        doc::BumpStr::from_str("big big big big big", &alloc),
    ];

    insta::assert_snapshot!(to_snap(&strings), @r###"
    case: "":
     |ffffffff ffffffff|                   ........         00000000
                                                            00000008
    case: "aaaaaaa":
     |61616161 616161ff|                   aaaaaaa.         00000000
                                                            00000008
    case: "hello":
     |68656c6c 6fffffff|                   hello...         00000000
                                                            00000008
    case: "big big big big big":
     |62696720 62696720 62696720 62696720| big big big big  00000000
     |62696700 93000000 ecffffff|          big.........     00000010
                                                            0000001c
    "###);

    let fields = [
        doc::HeapField {
            property: doc::BumpStr::from_str("", &alloc),
            value: doc::HeapNode::Bool(true),
        },
        doc::HeapField {
            property: doc::BumpStr::from_str("aaaaaaa", &alloc),
            value: doc::HeapNode::Bool(true),
        },
        doc::HeapField {
            property: doc::BumpStr::from_str("big big big big big big big", &alloc),
            value: doc::HeapNode::Bool(true),
        },
        doc::HeapField {
            property: doc::BumpStr::from_str("aaaaaaaaa", &alloc),
            value: doc::HeapNode::String(doc::BumpStr::from_str("bbbbbbbbb", &alloc)),
        },
    ];

    insta::assert_snapshot!(to_snap(&fields), @r###"
    case: HeapField { property: "", value: Bool(true) }:
     |ffffffff ffffffff 01010000 00000000| ................ 00000000
     |00000000 00000000|                   ........         00000010
                                                            00000018
    case: HeapField { property: "aaaaaaa", value: Bool(true) }:
     |61616161 616161ff 01010000 00000000| aaaaaaa......... 00000000
     |00000000 00000000|                   ........         00000010
                                                            00000018
    case: HeapField { property: "big big big big big big big", value: Bool(true) }:
     |62696720 62696720 62696720 62696720| big big big big  00000000
     |62696720 62696720 62696700 00000000| big big big..... 00000010
     |9b000000 e0ffffff 01010000 00000000| ................ 00000020
     |00000000 00000000|                   ........         00000030
                                                            00000038
    case: HeapField { property: "aaaaaaaaa", value: String("bbbbbbbbb") }:
     |61616161 61616161 61626262 62626262| aaaaaaaaabbbbbbb 00000000
     |62620000 00000000 89000000 e8ffffff| bb.............. 00000010
     |08000000 89000000 e5ffffff 00000000| ................ 00000020
                                                            00000030
    "###);

    let nodes = [
        doc::HeapNode::Array(doc::BumpVec::new()),
        doc::HeapNode::Array(doc::BumpVec::with_contents(
            &alloc,
            [doc::HeapNode::Bool(true), doc::HeapNode::Bool(false)].into_iter(),
        )),
        doc::HeapNode::Array(doc::BumpVec::with_contents(
            &alloc,
            [
                doc::HeapNode::String(doc::BumpStr::from_str("aaaaaaaaa", &alloc)),
                doc::HeapNode::String(doc::BumpStr::from_str("bbbbbbbbb", &alloc)),
            ]
            .into_iter(),
        )),
        doc::HeapNode::Bool(false),
        doc::HeapNode::Bool(true),
        doc::HeapNode::Bytes(doc::BumpVec::new()),
        doc::HeapNode::Bytes(doc::BumpVec::with_contents(
            &alloc,
            [0x1, 0x2, 0x3].into_iter(),
        )),
        doc::HeapNode::Float(0f64),
        doc::HeapNode::Float(f64::MIN),
        doc::HeapNode::Float(f64::MAX),
        doc::HeapNode::NegInt(0),
        doc::HeapNode::NegInt(i64::MIN),
        doc::HeapNode::NegInt(i64::MAX),
        doc::HeapNode::Null,
        doc::HeapNode::Object(doc::BumpVec::new()),
        doc::HeapNode::Object(doc::BumpVec::with_contents(
            &alloc,
            [
                doc::HeapField {
                    property: doc::BumpStr::from_str("key", &alloc),
                    value: doc::HeapNode::Bool(false),
                },
                doc::HeapField {
                    property: doc::BumpStr::from_str("two", &alloc),
                    value: doc::HeapNode::Bool(true),
                },
            ]
            .into_iter(),
        )),
        doc::HeapNode::Object(doc::BumpVec::with_contents(
            &alloc,
            [
                doc::HeapField {
                    property: doc::BumpStr::from_str("aaaaaaaaa", &alloc),
                    value: doc::HeapNode::String(doc::BumpStr::from_str("bbbbbbbbb", &alloc)),
                },
                doc::HeapField {
                    property: doc::BumpStr::from_str("ccccccccc", &alloc),
                    value: doc::HeapNode::String(doc::BumpStr::from_str("ddddddddd", &alloc)),
                },
            ]
            .into_iter(),
        )),
        doc::HeapNode::PosInt(0),
        doc::HeapNode::PosInt(u64::MAX),
        doc::HeapNode::String(doc::BumpStr::from_str("", &alloc)),
        doc::HeapNode::String(doc::BumpStr::from_str("hello", &alloc)),
        doc::HeapNode::String(doc::BumpStr::from_str("big big big big big", &alloc)),
    ];

    insta::assert_snapshot!(to_snap(&nodes), @r###"
    case: Array([]):
     |00000000 fcffffff 00000000 00000000| ................ 00000000
                                                            00000010
    case: Array([Bool(true), Bool(false)]):
     |01010000 00000000 00000000 00000000| ................ 00000000
     |01000000 00000000 00000000 00000000| ................ 00000010
     |00000000 dcffffff 02000000 00000000| ................ 00000020
                                                            00000030
    case: Array([String("aaaaaaaaa"), String("bbbbbbbbb")]):
     |61616161 61616161 61626262 62626262| aaaaaaaaabbbbbbb 00000000
     |62620000 00000000 08000000 89000000| bb.............. 00000010
     |e4ffffff 00000000 08000000 89000000| ................ 00000020
     |ddffffff 00000000 00000000 dcffffff| ................ 00000030
     |02000000 00000000|                   ........         00000040
                                                            00000048
    case: Bool(false):
     |01000000 00000000 00000000 00000000| ................ 00000000
                                                            00000010
    case: Bool(true):
     |01010000 00000000 00000000 00000000| ................ 00000000
                                                            00000010
    case: Bytes([]):
     |02000000 fcffffff 00000000 00000000| ................ 00000000
                                                            00000010
    case: Bytes([1, 2, 3]):
     |01020300 00000000 02000000 f4ffffff| ................ 00000000
     |03000000 00000000|                   ........         00000010
                                                            00000018
    case: Float(0.0):
     |03000000 00000000 00000000 00000000| ................ 00000000
                                                            00000010
    case: Float(-1.7976931348623157e308):
     |03000000 00000000 ffffffff ffffefff| ................ 00000000
                                                            00000010
    case: Float(1.7976931348623157e308):
     |03000000 00000000 ffffffff ffffef7f| ................ 00000000
                                                            00000010
    case: NegInt(0):
     |04000000 00000000 00000000 00000000| ................ 00000000
                                                            00000010
    case: NegInt(-9223372036854775808):
     |04000000 00000000 00000000 00000080| ................ 00000000
                                                            00000010
    case: NegInt(9223372036854775807):
     |04000000 00000000 ffffffff ffffff7f| ................ 00000000
                                                            00000010
    case: Null:
     |05000000 00000000 00000000 00000000| ................ 00000000
                                                            00000010
    case: Object([]):
     |06000000 fcffffff 00000000 00000000| ................ 00000000
                                                            00000010
    case: Object([HeapField { property: "key", value: Bool(false) }, HeapField { property: "two", value: Bool(true) }]):
     |6b6579ff ffffffff 01000000 00000000| key............. 00000000
     |00000000 00000000 74776fff ffffffff| ........two..... 00000010
     |01010000 00000000 00000000 00000000| ................ 00000020
     |06000000 ccffffff 02000000 00000000| ................ 00000030
                                                            00000040
    case: Object([HeapField { property: "aaaaaaaaa", value: String("bbbbbbbbb") }, HeapField { property: "ccccccccc", value: String("ddddddddd") }]):
     |61616161 61616161 61626262 62626262| aaaaaaaaabbbbbbb 00000000
     |62626363 63636363 63636364 64646464| bbcccccccccddddd 00000010
     |64646464 00000000 89000000 d8ffffff| dddd............ 00000020
     |08000000 89000000 d5ffffff 00000000| ................ 00000030
     |89000000 d2ffffff 08000000 89000000| ................ 00000040
     |cfffffff 00000000 06000000 ccffffff| ................ 00000050
     |02000000 00000000|                   ........         00000060
                                                            00000068
    case: PosInt(0):
     |07000000 00000000 00000000 00000000| ................ 00000000
                                                            00000010
    case: PosInt(18446744073709551615):
     |07000000 00000000 ffffffff ffffffff| ................ 00000000
                                                            00000010
    case: String(""):
     |08000000 ffffffff ffffffff 00000000| ................ 00000000
                                                            00000010
    case: String("hello"):
     |08000000 68656c6c 6fffffff 00000000| ....hello....... 00000000
                                                            00000010
    case: String("big big big big big"):
     |62696720 62696720 62696720 62696720| big big big big  00000000
     |62696700 00000000 08000000 93000000| big............. 00000010
     |e4ffffff 00000000|                   ........         00000020
                                                            00000028
    "###);
}

fn to_snap<S>(things: &[S]) -> String
where
    S: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        > + std::fmt::Debug,
{
    use std::fmt::Write;
    let mut out = String::new();

    for thing in things {
        let b = rkyv::to_bytes::<rkyv::rancor::Error>(thing).unwrap();
        write!(&mut out, "case: {thing:?}:\n{}\n", super::to_hex(&b)).unwrap();
    }

    out
}
