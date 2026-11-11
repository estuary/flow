//! Just enough DNS to gate answers: read a message's header and question,
//! read the A and CNAME records of its answer section, patch their TTLs in
//! place, and synthesize the replies the resolver invents on its own.
//!
//! `domain` parses, borrowing the message and reading only the sections asked
//! for, so the authority and additional sections are never parsed. It bounds
//! a name at 255 bytes on the wire and 255 compression pointers, each of which
//! must point backwards.
//!
//! Nothing here re-encodes a message. Rewriting a TTL is a four-byte store at
//! a known offset, and anything not understood is forwarded byte for byte,
//! which is the only safe thing to do with EDNS, DNSSEC and the compression
//! pointers a re-encoder would have to keep valid.
//!
//! Every name this module produces is ASCII-lowercased as it is read, so
//! callers compare names without repeating that themselves. The bytes on the
//! wire keep whatever case they arrived with. A dot or backslash inside a label
//! is written as its RFC 1035 decimal escape, so the dots of a produced name
//! are exactly its label boundaries on the wire and callers may compare names
//! as strings. `domain`'s own rendering escapes differently, so names are
//! rendered here from their labels.

use anyhow::Context;
use std::net::Ipv4Addr;

pub const TYPE_A: u16 = 1;
pub const TYPE_CNAME: u16 = 5;
pub const TYPE_AAAA: u16 = 28;
pub const CLASS_IN: u16 = 1;

pub const OPCODE_QUERY: u8 = 0;

pub const RCODE_NOERROR: u16 = 0;
pub const RCODE_SERVFAIL: u16 = 2;
pub const RCODE_REFUSED: u16 = 5;

const FLAG_RESPONSE: u16 = 0x8000;
const FLAG_RECURSION_AVAILABLE: u16 = 0x0080;
/// Opcode and the recursion-desired bit, which a reply echoes back.
const FLAGS_ECHOED: u16 = 0x7900;

pub struct Header {
    pub id: u16,
    pub response: bool,
    pub opcode: u8,
    pub truncated: bool,
    pub rcode: u16,
    pub qdcount: u16,
}

pub struct Question {
    pub name: String,
    pub qtype: u16,
    pub qclass: u16,
    /// Offset one past the question, where the answer section begins and where
    /// a synthesized reply is truncated.
    pub end: usize,
}

pub struct Record {
    pub kind: Kind,
    pub ttl: u32,
    /// Offset of the record's 32-bit TTL field, for `set_ttl`.
    pub ttl_offset: usize,
}

pub enum Kind {
    Address(Ipv4Addr),
    Target(String),
}

pub fn header(message: &[u8]) -> anyhow::Result<Header> {
    let message = parse(message)?;
    let header = message.header();

    Ok(Header {
        id: header.id(),
        response: header.qr(),
        opcode: header.opcode().to_int(),
        truncated: header.tc(),
        rcode: header.rcode().to_int().into(),
        qdcount: message.header_counts().qdcount(),
    })
}

/// The first question. The resolver acts only on a query carrying exactly one.
pub fn first_question(message: &[u8]) -> anyhow::Result<Question> {
    let mut questions = parse(message)?.question();
    let question = questions
        .next()
        .context("message has no question")?
        .context("question")?;

    Ok(Question {
        name: render_name(question.qname()),
        qtype: question.qtype().to_int(),
        qclass: question.qclass().to_int(),
        end: questions.pos(),
    })
}

/// Every class-IN A and CNAME record of the answer section, with where each
/// one's TTL lives. The authority and additional sections are never read: the
/// guest resolves from the answer section, so nothing else can authorize a
/// destination.
pub fn answer_records(message: &[u8]) -> anyhow::Result<Vec<Record>> {
    let mut answer = parse(message)?.answer().context("question section")?;
    let mut records = Vec::new();

    while let Some(record) = answer.next() {
        let record = record.context("answer record")?;
        // The section now stands just past the record's data, which follows
        // its 32-bit TTL and 16-bit data length.
        let ttl_offset = answer.pos() - usize::from(record.rdlen()) - 6;

        if record.class().to_int() != CLASS_IN {
            continue;
        }
        let kind = match record.rtype().to_int() {
            TYPE_A if record.rdlen() == 4 => {
                let a = record
                    .to_record::<domain::rdata::A>()?
                    .expect("an A record parses as A");
                Kind::Address(a.data().addr())
            }
            TYPE_CNAME => {
                // Bound the target to one name so the next record's owner
                // cannot become part of a remembered target.
                let cname = record
                    .to_record::<domain::rdata::Cname<_>>()
                    .context("CNAME record data")?
                    .expect("a CNAME record parses as CNAME");
                let target = cname.data().cname();

                // The root is not a name anything can be re-queried under.
                if target.is_root() {
                    continue;
                }
                Kind::Target(render_name(target))
            }
            _ => continue,
        };
        records.push(Record {
            kind,
            ttl: record.ttl().as_secs(),
            ttl_offset,
        });
    }
    Ok(records)
}

pub fn set_ttl(message: &mut [u8], record: &Record, ttl: u32) {
    message[record.ttl_offset..record.ttl_offset + 4].copy_from_slice(&ttl.to_be_bytes());
}

/// A reply to `query` with no records and the given rcode, keeping the
/// question section so the client matches it to its query. Any EDNS option the
/// query carried is dropped with the rest of the additional section, which a
/// requestor must already handle as "no EDNS here".
pub fn respond(query: &[u8], question: &Question, rcode: u16) -> Vec<u8> {
    let mut response = query[..question.end].to_vec();
    let flags = (u16::from_be_bytes([query[2], query[3]]) & FLAGS_ECHOED)
        | FLAG_RESPONSE
        | FLAG_RECURSION_AVAILABLE
        | rcode;

    response[2..4].copy_from_slice(&flags.to_be_bytes());
    response[4..6].copy_from_slice(&1u16.to_be_bytes());
    response[6..12].fill(0);
    response
}

fn parse(message: &[u8]) -> anyhow::Result<&domain::base::Message<[u8]>> {
    domain::base::Message::from_slice(message)
        .with_context(|| format!("message of {} bytes", message.len()))
}

fn render_name<Octs: AsRef<[u8]>>(name: &domain::base::ParsedName<Octs>) -> String {
    let mut rendered = String::new();

    for label in name.iter().filter(|label| !label.is_root()) {
        if !rendered.is_empty() {
            rendered.push('.');
        }
        for byte in label.as_slice() {
            // Unescaped, a label `eu.mirror` beneath `acmeco.example` would
            // read as a name beneath `mirror.acmeco.example`, and the gate
            // would admit it by an entry for a zone the query never reaches.
            match byte {
                b'.' => rendered.push_str("\\046"),
                b'\\' => rendered.push_str("\\092"),
                _ => rendered.push(byte.to_ascii_lowercase() as char),
            }
        }
    }
    rendered
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    fn encode_name(name: &str, out: &mut Vec<u8>) {
        for label in name.split('.') {
            out.push(label.len() as u8);
            out.extend(label.as_bytes());
        }
        out.push(0);
    }

    fn message(question: &str, qtype: u16, ancount: u16, answer: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(0x1234u16.to_be_bytes());
        out.extend(0x8180u16.to_be_bytes());
        out.extend(1u16.to_be_bytes());
        out.extend(ancount.to_be_bytes());
        out.extend([0u8; 4]);
        encode_name(question, &mut out);
        out.extend(qtype.to_be_bytes());
        out.extend(super::CLASS_IN.to_be_bytes());
        out.extend(answer);
        out
    }

    fn a_record(owner: &str, address: Ipv4Addr, ttl: u32, class: u16) -> Vec<u8> {
        let mut out = Vec::new();
        encode_name(owner, &mut out);
        out.extend(super::TYPE_A.to_be_bytes());
        out.extend(class.to_be_bytes());
        out.extend(ttl.to_be_bytes());
        out.extend(4u16.to_be_bytes());
        out.extend(address.octets());
        out
    }

    fn cname_record(owner: &str, rdata: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        encode_name(owner, &mut out);
        out.extend(super::TYPE_CNAME.to_be_bytes());
        out.extend(super::CLASS_IN.to_be_bytes());
        out.extend(300u32.to_be_bytes());
        out.extend((rdata.len() as u16).to_be_bytes());
        out.extend(rdata);
        out
    }

    fn record(owner: &[u8], rtype: u16, class: u16, rdata: &[u8]) -> Vec<u8> {
        let mut out = owner.to_vec();
        out.extend(rtype.to_be_bytes());
        out.extend(class.to_be_bytes());
        out.extend(300u32.to_be_bytes());
        out.extend((rdata.len() as u16).to_be_bytes());
        out.extend(rdata);
        out
    }

    fn wire_name(lengths: &[usize], byte: u8) -> Vec<u8> {
        let mut out = Vec::new();
        for &length in lengths {
            out.push(length as u8);
            out.extend(vec![byte; length]);
        }
        out.push(0);
        out
    }

    /// An answer whose `A` owner reaches the question's name through
    /// `pointers` compression pointers: its own, then a chain held as the data
    /// of a record of an unknown type, each naming the one before it.
    fn pointer_chain(pointers: usize) -> Vec<u8> {
        // Two bytes of owner pointer, then type, class, TTL and data length.
        let data = message("acmeco.example", super::TYPE_A, 0, &[]).len() + 12;
        let pointer = |offset: usize| (0xC000 | offset as u16).to_be_bytes();

        let mut chain = vec![0xC0, 12];
        for index in 1..pointers - 1 {
            chain.extend(pointer(data + 2 * (index - 1)));
        }
        let mut answer = record(&[0xC0, 12], 65280, super::CLASS_IN, &chain);
        answer.extend(record(
            &pointer(data + 2 * (pointers - 2)),
            super::TYPE_A,
            super::CLASS_IN,
            &[93, 184, 216, 34],
        ));
        message("acmeco.example", super::TYPE_A, 2, &answer)
    }

    #[test]
    fn parsing() {
        let mut table = String::new();

        for (label, message) in cases() {
            table.push_str(&format!("# {label}\n"));

            match super::answer_records(&message) {
                Ok(records) => {
                    for record in &records {
                        let field = &message[record.ttl_offset..record.ttl_offset + 4];
                        assert_eq!(field, record.ttl.to_be_bytes(), "{label}");
                    }
                    let rendered: Vec<String> = records
                        .iter()
                        .map(|record| match &record.kind {
                            super::Kind::Address(address) => {
                                format!("A {address} ttl {}", record.ttl)
                            }
                            super::Kind::Target(target) => {
                                format!("CNAME {target} ttl {}", record.ttl)
                            }
                        })
                        .collect();
                    table.push_str(&format!("records: [{}]\n\n", rendered.join("; ")));
                }
                Err(error) => table.push_str(&format!("refused: {error:#}\n\n")),
            }
        }
        insta::assert_snapshot!(table);
    }

    fn cases() -> Vec<(&'static str, Vec<u8>)> {
        let address = Ipv4Addr::new(93, 184, 216, 34);

        // A compression pointer back to the question's name, which is what a
        // real nameserver sends.
        let mut backward = Vec::new();
        backward.extend(0xC00Cu16.to_be_bytes());
        backward.extend(super::TYPE_A.to_be_bytes());
        backward.extend(super::CLASS_IN.to_be_bytes());
        backward.extend(300u32.to_be_bytes());
        backward.extend(4u16.to_be_bytes());
        backward.extend(address.octets());

        // A pointer to a later offset, which no legitimate message contains
        // and which is how a cycle would be built.
        let mut forward = backward.clone();
        forward[0..2].copy_from_slice(&0xC0FFu16.to_be_bytes());

        let mut root_target = Vec::new();
        encode_name("acmeco.example", &mut root_target);
        root_target.extend(super::TYPE_CNAME.to_be_bytes());
        root_target.extend(super::CLASS_IN.to_be_bytes());
        root_target.extend(300u32.to_be_bytes());
        root_target.extend(1u16.to_be_bytes());
        root_target.push(0);

        let mut cdn = Vec::new();
        encode_name("cdn.acmeco.example", &mut cdn);

        // The question `www.acmeco.example` begins at offset 12, so its
        // `acmeco.example` is at 16.
        let apex_pointer = [0xC0, 16];
        let mut cdn_pointer = vec![3, b'c', b'd', b'n'];
        cdn_pointer.extend(apex_pointer);

        // Labels with no root label to end them: read on, they take the next
        // record's owner as their own and make `cdn.evil.example`.
        let mut spill = cname_record("acmeco.example", &[3, b'c', b'd', b'n']);
        spill.extend(a_record("evil.example", address, 300, super::CLASS_IN));

        let mut empty = cname_record("acmeco.example", &[]);
        empty.extend(a_record("evil.example", address, 300, super::CLASS_IN));

        // The pointer's second byte lies past rdlength.
        let mut cut_pointer = cname_record("acmeco.example", &[0xC0]);
        cut_pointer.push(0x0C);

        let mut trailing = cdn.clone();
        trailing.extend(b"xx");

        let mut long_label = Vec::new();
        long_label.push(64); // above the 63-byte maximum, but not a pointer
        long_label.extend([b'a'; 64]);
        long_label.push(0);
        long_label.extend(super::TYPE_A.to_be_bytes());
        long_label.extend(super::CLASS_IN.to_be_bytes());
        long_label.extend(300u32.to_be_bytes());
        long_label.extend(4u16.to_be_bytes());
        long_label.extend(address.octets());

        let mut reserved = long_label.clone();
        reserved[0] = 0x80; // a reserved label length

        let mut overlong_rdata = a_record("acmeco.example", address, 300, super::CLASS_IN);
        let rdlength = overlong_rdata.len() - 4 - 2;
        overlong_rdata[rdlength..rdlength + 2].copy_from_slice(&4096u16.to_be_bytes());

        vec![
            ("shorter than a header", vec![0u8; 8]),
            (
                "one A record",
                message(
                    "acmeco.example",
                    super::TYPE_A,
                    1,
                    &a_record("acmeco.example", address, 300, super::CLASS_IN),
                ),
            ),
            (
                "an A record in another class",
                message(
                    "acmeco.example",
                    super::TYPE_A,
                    1,
                    &a_record("acmeco.example", address, 300, 3),
                ),
            ),
            (
                "the owner name as a backward compression pointer",
                message("acmeco.example", super::TYPE_A, 1, &backward),
            ),
            (
                "a forward compression pointer",
                message("acmeco.example", super::TYPE_A, 1, &forward),
            ),
            (
                "a CNAME target of the root",
                message("acmeco.example", super::TYPE_A, 1, &root_target),
            ),
            (
                "a CNAME",
                message(
                    "acmeco.example",
                    super::TYPE_A,
                    1,
                    &cname_record("acmeco.example", &cdn),
                ),
            ),
            (
                "a CNAME target as a backward compression pointer",
                message(
                    "www.acmeco.example",
                    super::TYPE_A,
                    1,
                    &cname_record("www.acmeco.example", &apex_pointer),
                ),
            ),
            (
                "a CNAME target of labels then a compression pointer",
                message(
                    "www.acmeco.example",
                    super::TYPE_A,
                    1,
                    &cname_record("www.acmeco.example", &cdn_pointer),
                ),
            ),
            (
                "a CNAME target running on into the next record",
                message("acmeco.example", super::TYPE_A, 2, &spill),
            ),
            (
                "a CNAME with no record data",
                message("acmeco.example", super::TYPE_A, 2, &empty),
            ),
            (
                "a CNAME target's compression pointer cut short by rdlength",
                message("acmeco.example", super::TYPE_A, 1, &cut_pointer),
            ),
            (
                "a CNAME target with bytes after it",
                message(
                    "acmeco.example",
                    super::TYPE_A,
                    1,
                    &cname_record("acmeco.example", &trailing),
                ),
            ),
            (
                "a label above 63 bytes",
                message("acmeco.example", super::TYPE_A, 1, &long_label),
            ),
            (
                "a reserved label length",
                message("acmeco.example", super::TYPE_A, 1, &reserved),
            ),
            (
                "rdlength running past the message",
                message("acmeco.example", super::TYPE_A, 1, &overlong_rdata),
            ),
            (
                "more answers claimed than carried",
                message(
                    "acmeco.example",
                    super::TYPE_A,
                    4,
                    &a_record("acmeco.example", address, 300, super::CLASS_IN),
                ),
            ),
            ("a record past the answer section is not read", {
                let mut message = message(
                    "acmeco.example",
                    super::TYPE_A,
                    1,
                    &a_record("acmeco.example", address, 300, super::CLASS_IN),
                );
                // One answer, and an additional record the walk must stop
                // short of: only the answer section authorizes anything.
                message[10..12].copy_from_slice(&1u16.to_be_bytes());
                message.extend(a_record(
                    "glue.acmeco.example",
                    Ipv4Addr::new(10, 0, 0, 1),
                    300,
                    super::CLASS_IN,
                ));
                message
            }),
            ("a truncated question", {
                let mut message = message("acmeco.example", super::TYPE_A, 0, &[]);
                message.truncate(message.len() - 3);
                message
            }),
            ("a truncated resource record", {
                let mut message = message(
                    "acmeco.example",
                    super::TYPE_A,
                    1,
                    &a_record("acmeco.example", address, 300, super::CLASS_IN),
                );
                message.truncate(message.len() - 6);
                message
            }),
            (
                "a 255-byte owner",
                owned(&wire_name(&[63, 63, 63, 61], b'a')),
            ),
            (
                "a 256-byte owner",
                owned(&wire_name(&[63, 63, 63, 62], b'a')),
            ),
            (
                "a 257-byte owner",
                owned(&wire_name(&[63, 63, 63, 63], b'a')),
            ),
            ("an owner compressed past 255 bytes", {
                let mut owner = wire_name(&[63, 63, 63, 60], b'a');
                owner.pop();
                owner.extend([0xC0, 12]);
                owned(&owner)
            }),
            ("255 compression pointers", pointer_chain(255)),
            ("256 compression pointers", pointer_chain(256)),
            ("a compression pointer to itself", {
                let at = message("acmeco.example", super::TYPE_A, 0, &[]).len();
                owned(&[0xC0, at as u8])
            }),
            ("A records of three and five bytes are skipped", {
                let mut answer = record(&[0xC0, 12], super::TYPE_A, super::CLASS_IN, &[1, 2, 3]);
                answer.extend(record(
                    &[0xC0, 12],
                    super::TYPE_A,
                    super::CLASS_IN,
                    &[1, 2, 3, 4, 5],
                ));
                answer.extend(a_record("acmeco.example", address, 300, super::CLASS_IN));
                message("acmeco.example", super::TYPE_A, 3, &answer)
            }),
            ("the data of another type is not read", {
                let mut answer = record(&[0xC0, 12], 65280, super::CLASS_IN, &[0xC0, 0xFF, 0x80]);
                answer.extend(a_record("acmeco.example", address, 300, super::CLASS_IN));
                message("acmeco.example", super::TYPE_A, 2, &answer)
            }),
            ("a malformed authority section is not read", {
                let mut message = message(
                    "acmeco.example",
                    super::TYPE_A,
                    1,
                    &a_record("acmeco.example", address, 300, super::CLASS_IN),
                );
                message[8..10].copy_from_slice(&1u16.to_be_bytes());
                message.extend([0xC0, 0xFF, 0x80]);
                message
            }),
            (
                "a CNAME target in mixed case, with a dot and a backslash in labels",
                message(
                    "acmeco.example",
                    super::TYPE_A,
                    1,
                    &cname_record("acmeco.example", b"\x09EU.Mirror\x03a\\b\x07Example\x00"),
                ),
            ),
            (
                "a CNAME target as a forward compression pointer",
                message(
                    "acmeco.example",
                    super::TYPE_A,
                    1,
                    &cname_record("acmeco.example", &[0xC0, 0xFF]),
                ),
            ),
        ]
    }

    /// A one-record answer whose `A` owner is these bytes.
    fn owned(owner: &[u8]) -> Vec<u8> {
        let answer = record(owner, super::TYPE_A, super::CLASS_IN, &[93, 184, 216, 34]);
        message("acmeco.example", super::TYPE_A, 1, &answer)
    }

    #[test]
    fn a_question_is_lowercased() {
        let message = message("PyPI.ORG", super::TYPE_A, 0, &[]);
        let question = super::first_question(&message).expect("the question parses");

        assert_eq!(question.name, "pypi.org");
        assert_eq!(question.qtype, super::TYPE_A);
        assert_eq!(question.qclass, super::CLASS_IN);
    }

    #[test]
    fn a_dot_inside_a_label_is_escaped() {
        let mut message = message("eu-mirror.a-b.example", super::TYPE_A, 0, &[]);
        message[12 + 1 + 2] = b'.';
        message[12 + 1 + 9 + 1 + 1] = b'\\';
        let question = super::first_question(&message).expect("the question parses");

        assert_eq!(question.name, r"eu\046mirror.a\092b.example");
    }

    /// Any other byte is ASCII-lowercased and kept as the `char` of its value,
    /// so a control or non-ASCII byte is still one character of the name.
    #[test]
    fn other_label_bytes_are_kept() {
        let mut message = message("aaaaaaa.example", super::TYPE_A, 0, &[]);
        message[13..20].copy_from_slice(&[b'Q', b'z', b' ', 0x00, b'\n', 0xC9, 0xFF]);
        let question = super::first_question(&message).expect("the question parses");

        assert_eq!(question.name, "qz \u{0}\n\u{c9}\u{ff}.example");
    }

    /// The bound is the name's 255 bytes on the wire. Escapes lengthen the
    /// rendered name well past that without lengthening the name.
    #[test]
    fn a_name_is_bounded_on_the_wire() {
        let question = |last: usize| {
            let mut message = message("acmeco.example", super::TYPE_A, 0, &[]);
            message.truncate(12);
            message.extend(wire_name(&[63, 63, 63, last], b'\\'));
            message.extend(super::TYPE_A.to_be_bytes());
            message.extend(super::CLASS_IN.to_be_bytes());
            message
        };
        let name = super::first_question(&question(61))
            .expect("a 255-byte name parses")
            .name;

        assert_eq!(name.len(), 4 * 250 + 3, "250 escaped bytes and 3 dots");
        assert!(super::first_question(&question(62)).is_err());
    }

    /// Every truncation and one-byte substitution of a few well-formed
    /// answers. Each read ends without panicking, and every TTL offset names
    /// the TTL that was read.
    #[test]
    fn malformed_messages_are_read_in_bounds() {
        let cname_then_a = {
            // The CNAME's data starts after the question's 20-byte name, its
            // type and class, and the CNAME's own owner pointer and header.
            let mut answer = record(
                &[0xC0, 12],
                super::TYPE_CNAME,
                super::CLASS_IN,
                &[3, b'c', b'd', b'n', 0xC0, 16],
            );
            answer.extend(record(
                &[0xC0, 48],
                super::TYPE_A,
                super::CLASS_IN,
                &[93, 184, 216, 34],
            ));
            message("www.acmeco.example", super::TYPE_A, 2, &answer)
        };
        let with_edns = {
            let mut answer = record(&[0xC0, 12], 65280, super::CLASS_IN, &[0xDE, 0xAD]);
            answer.extend(record(&[0xC0, 12], super::TYPE_A, 3, &[10, 0, 0, 1]));
            answer.extend(a_record(
                "acmeco.example",
                Ipv4Addr::new(93, 184, 216, 34),
                60,
                super::CLASS_IN,
            ));
            let mut message = message("acmeco.example", super::TYPE_A, 3, &answer);
            message[10..12].copy_from_slice(&1u16.to_be_bytes());
            message.extend([0, 0, 41, 4, 208, 0, 0, 0x80, 0, 0, 4, 0, 10, 0, 0]);
            message
        };
        let (mut read, mut refused) = (0, 0);

        for seed in [cname_then_a, with_edns, pointer_chain(8)] {
            let mut variants: Vec<Vec<u8>> =
                (0..seed.len()).map(|len| seed[..len].to_vec()).collect();
            for index in 0..seed.len() {
                for byte in [0x00, 0x01, 0x04, 0x0C, 0x3F, 0x40, 0x80, 0xBF, 0xC0, 0xFF] {
                    let mut variant = seed.clone();
                    variant[index] = byte;
                    variants.push(variant);
                }
            }
            for variant in variants {
                if let Ok(question) = super::first_question(&variant) {
                    let reply = super::respond(&variant, &question, super::RCODE_REFUSED);
                    super::header(&reply).expect("a synthesized reply has a header");
                }
                let Ok(records) = super::answer_records(&variant) else {
                    refused += 1;
                    continue;
                };
                read += 1;
                let mut rewritten = variant.clone();
                for record in &records {
                    let field = &variant[record.ttl_offset..record.ttl_offset + 4];
                    assert_eq!(field, record.ttl.to_be_bytes());
                    super::set_ttl(&mut rewritten, record, 0x0102_0304);
                }
                // A pointer may name bytes of a rewritten TTL, so the second
                // read need only end.
                _ = super::answer_records(&rewritten);
            }
        }
        assert!(read > 0 && refused > 0, "read {read}, refused {refused}");
    }

    #[test]
    fn a_synthesized_reply_keeps_the_question() {
        let query = {
            let mut query = message("acmeco.example", super::TYPE_AAAA, 0, &[]);
            query[2..4].copy_from_slice(&0x0100u16.to_be_bytes()); // a query, RD set
            // An EDNS OPT record in the additional section, which the reply
            // does not carry back.
            query[10..12].copy_from_slice(&1u16.to_be_bytes());
            query.extend([0, 0, 41, 16, 0, 0, 0, 0, 0, 0, 0]);
            query
        };
        let question = super::first_question(&query).expect("the question parses");
        let reply = super::respond(&query, &question, super::RCODE_REFUSED);

        let header = super::header(&reply).expect("the reply parses");
        assert_eq!(header.id, 0x1234);
        assert_eq!(header.rcode, super::RCODE_REFUSED);
        assert!(header.response);
        assert_eq!(header.qdcount, 1);
        assert_eq!(&reply[6..12], &[0; 6], "no records in any section");
        assert_eq!(reply.len(), question.end, "every section past the question");

        let echoed = super::first_question(&reply).expect("the question survives");
        assert_eq!(echoed.name, "acmeco.example");
        assert_eq!(echoed.qtype, super::TYPE_AAAA);
    }
}
